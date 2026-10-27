"""Cliente de trading autenticado sobre WebSocket (gateway OTP).

Flujo (https://developers.deriv.com/llms/getting-started.md):

1. Con el Bearer token (OAuth2 o PAT) se pide una URL OTP por REST:
   ``POST /trading/v1/options/accounts/{accountId}/otp``.
2. Se conecta a esa URL (``wss://.../ws/demo?otp=...`` o ``/ws/real``). No hay
   ningún handshake de autenticación dentro del WebSocket: la conexión ya está
   asociada a la cuenta.
3. Se envían/reciben mensajes JSON con el protocolo clásico de Deriv
   (``proposal``, ``buy``, ``sell``, ``balance``, ``portfolio``…).

El cliente gestiona en segundo plano: keep-alive (``ping`` cada ~30 s),
correlación petición/respuesta vía ``req_id``, despacho de streams a callbacks
y reconexión automática pidiendo una URL OTP nueva.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
from collections.abc import AsyncIterator, Awaitable, Callable
from typing import Any

import websockets

from . import _constants as C
from .exceptions import DerivAuthError, DerivConnectionError, DerivTimeoutError
from .models import Balance, BuyReceipt, OpenContract, Proposal, SellReceipt
from .rest import RestClient

logger = logging.getLogger("derivpy.trading")

#: Callback de stream: recibe el dict crudo del mensaje. Puede ser sync o async.
StreamHandler = Callable[[dict[str, Any]], Any | Awaitable[Any]]


class TradingClient:
    """Cliente autenticado para operar sobre una cuenta de Deriv.

    Args:
        account_id: ID de cuenta (el devuelto por ``GET /trading/v1/options/accounts``).
        token: Bearer token OAuth2 **o** Personal Access Token con scope ``trade``.
        app_id: App ID; obligatorio si ``token`` es un PAT.
        request_timeout: segundos de espera máxima por respuesta a una petición.
        ping_interval: segundos entre pings de keep-alive (0 lo desactiva).
        max_reconnect_attempts: intentos de reconexión antes de fallar.

    Ejemplo::

        async with TradingClient(account_id, token, app_id) as api:
            prop = await api.propose(contract_type="CALL", amount=10, ...)
            receipt = await api.buy(prop.id, prop.ask_price)
            async for state in api.watch_contract(receipt.contract_id):
                if not state.is_open:
                    break
    """

    def __init__(
        self,
        account_id: str,
        token: str,
        app_id: str | None = None,
        *,
        request_timeout: float = C.DEFAULT_REQUEST_TIMEOUT,
        ping_interval: float = C.DEFAULT_PING_INTERVAL,
        max_reconnect_attempts: int = C.DEFAULT_MAX_RECONNECT_ATTEMPTS,
    ) -> None:
        self.account_id = str(account_id)
        self._rest = RestClient(token=token, app_id=app_id)
        self.request_timeout = request_timeout
        self.ping_interval = ping_interval
        self.max_reconnect_attempts = max_reconnect_attempts

        self._ws: websockets.ClientConnection | None = None
        self._recv_task: asyncio.Task[None] | None = None
        self._ping_task: asyncio.Task[None] | None = None
        self._req_counter = 0
        # req_id -> Future que resuelve la PRIMERA respuesta con ese id.
        self._pending: dict[int, asyncio.Future[dict[str, Any]]] = {}
        # msg_type -> handlers de streams (ticks, balance, poc…)
        self._streams: dict[str, list[StreamHandler]] = {}
        self._connected = asyncio.Event()
        self._closing = False

    # ------------------------------------------------------------------ #
    # Ciclo de vida
    # ------------------------------------------------------------------ #
    async def connect(self) -> None:
        """Pide un OTP por REST y abre la conexión WebSocket."""
        self._closing = False
        url = await self._rest.get_otp_ws_url(self.account_id)
        try:
            self._ws = await websockets.connect(
                url, open_timeout=self.request_timeout, close_timeout=5
            )
        except (OSError, asyncio.TimeoutError, websockets.WebSocketException) as exc:
            raise DerivConnectionError(f"No se pudo conectar al WebSocket OTP: {exc}") from exc

        self._connected.set()
        self._recv_task = asyncio.create_task(self._recv_loop(), name="deriv-recv")
        if self.ping_interval:
            self._ping_task = asyncio.create_task(self._ping_loop(), name="deriv-ping")
        logger.info("Conectado a gateway de trading (cuenta %s)", self.account_id)

    async def disconnect(self) -> None:
        """Cierra la conexión y cancela las tareas internas."""
        self._closing = True
        self._connected.clear()
        for task in (self._recv_task, self._ping_task):
            if task:
                task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await task
        self._recv_task = self._ping_task = None
        if self._ws is not None:
            with contextlib.suppress(Exception):
                await self._ws.close()
            self._ws = None
        # Fallar todas las peticiones pendientes.
        for fut in self._pending.values():
            if not fut.done():
                fut.set_exception(DerivConnectionError("Conexión cerrada"))
        self._pending.clear()

    async def __aenter__(self) -> TradingClient:
        await self.connect()
        return self

    async def __aexit__(self, *exc_info: object) -> None:
        await self.disconnect()

    @property
    def is_connected(self) -> bool:
        """True si la conexión WebSocket está activa."""
        return self._ws is not None and self._connected.is_set()

    # ------------------------------------------------------------------ #
    # Envío / recepción
    # ------------------------------------------------------------------ #
    def _next_req_id(self) -> int:
        self._req_counter += 1
        return self._req_counter

    async def send(self, payload: dict[str, Any]) -> None:
        """Envía un mensaje JSON crudo (sin esperar respuesta)."""
        if not self.is_connected or self._ws is None:
            raise DerivConnectionError("La conexión no está abierta; llama a connect() primero.")
        try:
            await self._ws.send(json.dumps(payload))
        except websockets.WebSocketException as exc:
            raise DerivConnectionError(f"Fallo al enviar: {exc}") from exc

    async def request(self, payload: dict[str, Any], *, timeout: float | None = None) -> dict[str, Any]:
        """Envía una petición y espera su primera respuesta (por ``req_id``).

        Las respuestas con objeto ``error`` lanzan la excepción adecuada
        (:class:`DerivAuthError`, :class:`DerivRateLimitError` o
        :class:`DerivAPIError`).
        """
        from .exceptions import DerivAPIError  # local para evitar ciclo

        req_id = self._next_req_id()
        message = {**payload, "req_id": req_id}
        fut: asyncio.Future[dict[str, Any]] = asyncio.get_running_loop().create_future()
        self._pending[req_id] = fut
        try:
            await self.send(message)
            resp = await asyncio.wait_for(fut, timeout or self.request_timeout)
        except asyncio.TimeoutError as exc:
            raise DerivTimeoutError(f"Timeout esperando respuesta a '{payload}'") from exc
        finally:
            self._pending.pop(req_id, None)

        err = resp.get("error")
        if err:
            code, msg = err.get("code"), err.get("message", "")
            if code in ("InvalidToken", "AuthorizationRequired", "OTPExpired"):
                raise DerivAuthError(code, msg)
            if code == "RateLimit":
                from .exceptions import DerivRateLimitError

                raise DerivRateLimitError(code, msg)
            raise DerivAPIError(code, msg)
        return resp

    async def subscribe(self, payload: dict[str, Any], handler: StreamHandler) -> dict[str, Any]:
        """Envía una petición con ``subscribe: 1`` y registra ``handler``.

        El primer mensaje (la respuesta inicial) se devuelve también como dict;
        los siguientes llegan a ``handler`` en caliente.
        """
        msg_type = next(iter(payload))
        self._streams.setdefault(msg_type, []).append(handler)
        try:
            return await self.request({**payload, "subscribe": 1})
        except BaseException:
            self.remove_handler(msg_type, handler)
            raise

    def remove_handler(self, msg_type: str, handler: StreamHandler | None = None) -> None:
        """Quita un callback (o todos) para un ``msg_type`` dado."""
        if handler is None:
            self._streams.pop(msg_type, None)
        else:
            handlers = self._streams.get(msg_type, [])
            if handler in handlers:
                handlers.remove(handler)

    async def forget(self, subscription_id: str) -> dict[str, Any]:
        """``forget`` — cancela una suscripción concreta por su id."""
        return await self.request({"forget": subscription_id})

    async def forget_all(self, *msg_types: str) -> dict[str, Any]:
        """``forget_all`` — cancela todas las suscripciones de esos tipos."""
        return await self.request({"forget_all": list(msg_types)})

    # ------------------------------------------------------------------ #
    # Loops internos + reconexión
    # ------------------------------------------------------------------ #
    async def _recv_loop(self) -> None:
        while not self._closing:
            try:
                assert self._ws is not None
                raw = await self._ws.recv()
            except (websockets.ConnectionClosed, AssertionError):
                if self._closing:
                    return
                logger.warning("Conexión caída; reintentando…")
                if not await self._reconnect():
                    return
                continue

            try:
                msg = json.loads(raw)
            except json.JSONDecodeError:
                logger.debug("Mensaje no-JSON ignorado: %r", raw[:200])
                continue
            self._dispatch(msg)

    def _dispatch(self, msg: dict[str, Any]) -> None:
        """Reparte un mensaje: futures pendientes primero, luego streams."""
        req_id = msg.get("req_id")
        if req_id is not None:
            fut = self._pending.get(req_id)
            if fut is not None and not fut.done():
                fut.set_result(msg)
                # No hacemos return: un mensaje puede ser respuesta Y stream.
        for handler in list(self._streams.get(msg.get("msg_type", ""), [])):
            try:
                result = handler(msg)
                if asyncio.iscoroutine(result):
                    asyncio.create_task(result)
            except Exception:  # un callback roto no debe tumbar el cliente
                logger.exception("Error en handler de stream %s", msg.get("msg_type"))

    async def _ping_loop(self) -> None:
        while not self._closing:
            await asyncio.sleep(self.ping_interval)
            if self.is_connected:
                with contextlib.suppress(DerivConnectionError):
                    await self.send({"ping": 1})

    async def _reconnect(self) -> bool:
        """Reintenta conectar con backoff exponencial. True si lo logró."""
        delay = 1.0
        for attempt in range(1, self.max_reconnect_attempts + 1):
            await asyncio.sleep(delay)
            delay = min(delay * 2, 30.0)
            logger.info("Reconexión %d/%d…", attempt, self.max_reconnect_attempts)
            try:
                await self.disconnect_quietly()
                await self.connect()  # pide OTP NUEVO: el anterior caducó
                return True
            except (DerivConnectionError, DerivAuthError) as exc:
                logger.warning("Fallo de reconexión: %s", exc)
        logger.error("No se pudo reconectar tras %d intentos.", self.max_reconnect_attempts)
        for fut in self._pending.values():
            if not fut.done():
                fut.set_exception(DerivConnectionError("Reconexión agotada"))
        return False

    async def disconnect_quietly(self) -> None:
        """Cierra sin propagar errores (uso interno de reconexión)."""
        with contextlib.suppress(Exception):
            await self.disconnect()

    # ------------------------------------------------------------------ #
    # API de alto nivel: datos de cuenta
    # ------------------------------------------------------------------ #
    async def get_server_time(self) -> int:
        """``time`` — hora del servidor (epoch)."""
        resp = await self.request({"time": 1})
        return int(resp["time"])

    async def get_balance(self, subscribe: bool = False) -> Balance | dict[str, Any]:
        """``balance`` — saldo actual (o suscripción a cambios)."""
        if subscribe:
            return await self.request({"balance": 1})  # inicial; updates via watch_balance
        resp = await self.request({"balance": 1})
        return Balance.from_api(resp["balance"])

    async def watch_balance(self, handler: Callable[[Balance], Any]) -> dict[str, Any]:
        """Suscribe el stream ``balance``; ``handler(Balance)`` en cada cambio.

        Args:
            handler: callback (sync o async) que recibe un :class:`Balance`.

        Returns:
            El mensaje inicial (dict crudo), cuyo ``subscription.id`` sirve para
            cancelar la suscripción con :meth:`forget`.
        """

        def _on_msg(msg: dict[str, Any]) -> Any:
            return handler(Balance.from_api(msg["balance"]))

        return await self.subscribe({"balance": 1}, _on_msg)

    async def get_portfolio(self) -> list[dict[str, Any]]:
        """``portfolio`` — contratos abiertos (lista cruda)."""
        resp = await self.request({"portfolio": 1})
        return resp.get("portfolio", [])

    async def get_transactions(self, subscribe: bool = False) -> list[dict[str, Any]]:
        """``transaction`` — historial de transacciones recientes."""
        payload: dict[str, Any] = {"transaction": 1}
        if subscribe:
            payload["subscribe"] = 1
        resp = await self.request(payload)
        return resp.get("transactions", [])

    async def get_statement(self, **params: Any) -> list[dict[str, Any]]:
        """``statement`` — extracto de movimientos (``limit``, ``date_from``…)."""
        resp = await self.request({"statement": 1, **params})
        return resp.get("statements", [])

    async def get_profit_table(self, **params: Any) -> list[dict[str, Any]]:
        """``profit_table`` — tabla de resultados (``sortby``, ``limit``…)."""
        resp = await self.request({"profit_table": 1, **params})
        return resp.get("profit_table", [])

    # ------------------------------------------------------------------ #
    # API de alto nivel: cotizar y operar
    # ------------------------------------------------------------------ #
    async def propose(
        self,
        *,
        contract_type: str,
        underlying_symbol: str,
        amount: float,
        duration: int | None = None,
        duration_unit: str = "t",
        currency: str = "USD",
        basis: str = "stake",
        date_expiry: int | None = None,
        **extra: Any,
    ) -> Proposal:
        """``proposal`` — cotiza un contrato. Campos obligatorios según la doc:

        ``contract_type``, ``underlying_symbol``, ``currency`` y bien
        ``duration``+``duration_unit`` o ``date_expiry``. ``extra`` admite
        ``barrier``, ``barrier2``, ``multiplier``, ``growth_rate``,
        ``selected_tick``, ``limit_order``…
        """
        payload: dict[str, Any] = {
            "proposal": 1,
            "amount": amount,
            "basis": basis,
            "contract_type": contract_type,
            "currency": currency,
            "underlying_symbol": underlying_symbol,
        }
        if date_expiry is not None:
            payload["date_expiry"] = date_expiry
        elif duration is not None:
            payload["duration"] = duration
            payload["duration_unit"] = duration_unit
        else:
            raise ValueError("propose() requiere 'duration'+'duration_unit' o 'date_expiry'.")
        payload.update(extra)

        resp = await self.request(payload)
        return Proposal.from_api(resp["proposal"])

    async def buy(self, proposal_id: str, price: float) -> BuyReceipt:
        """``buy`` — compra el contrato cotizado.

        Args:
            proposal_id: ``id`` de la respuesta de :meth:`propose`.
            price: precio máximo aceptado (usa ``ask_price`` de la propuesta).
        """
        resp = await self.request({"buy": proposal_id, "price": price})
        return BuyReceipt.from_api(resp["buy"])

    async def sell(self, contract_id: int, price: float = 0) -> SellReceipt:
        """``sell`` — cierra un contrato abierto.

        Args:
            contract_id: id devuelto por :meth:`buy` / :meth:`get_portfolio`.
            price: mínimo aceptado, o ``0`` para vender a mercado.
        """
        resp = await self.request({"sell": contract_id, "price": price})
        return SellReceipt.from_api(resp["sell"])

    async def trade(
        self,
        *,
        contract_type: str,
        underlying_symbol: str,
        amount: float,
        duration: int = 5,
        duration_unit: str = "t",
        currency: str = "USD",
        basis: str = "stake",
        **extra: Any,
    ) -> BuyReceipt:
        """Atajo proponer→comprar (el workflow oficial de la doc)."""
        proposal = await self.propose(
            contract_type=contract_type,
            underlying_symbol=underlying_symbol,
            amount=amount,
            duration=duration,
            duration_unit=duration_unit,
            currency=currency,
            basis=basis,
            **extra,
        )
        return await self.buy(proposal.id, proposal.ask_price)

    async def get_contract(self, contract_id: int) -> OpenContract:
        """Estado puntual de un contrato (``proposal_open_contract`` sin subscribir)."""
        resp = await self.request({"proposal_open_contract": 1, "contract_id": contract_id})
        return OpenContract.from_api(resp["proposal_open_contract"])

    async def watch_contract(
        self, contract_id: int, stop_when_closed: bool = True
    ) -> AsyncIterator[OpenContract]:
        """Stream en vivo del estado de un contrato hasta que cierre.

        Ejemplo::

            async for c in api.watch_contract(cid):
                print(c.status, c.profit)
        """
        queue: asyncio.Queue[OpenContract | None] = asyncio.Queue()

        def _on_msg(msg: dict[str, Any]) -> None:
            oc = OpenContract.from_api(msg["proposal_open_contract"])
            queue.put_nowait(oc)
            if stop_when_closed and not oc.is_open:
                queue.put_nowait(None)

        initial = await self.subscribe(
            {"proposal_open_contract": 1, "contract_id": contract_id}, _on_msg
        )
        first = OpenContract.from_api(initial["proposal_open_contract"])
        yield first
        try:
            if stop_when_closed and not first.is_open:
                return
            while True:
                item = await queue.get()
                if item is None:
                    break
                yield item
        finally:
            sub_id = initial.get("subscription", {}).get("id")
            if sub_id:
                with contextlib.suppress(Exception):
                    await self.forget(sub_id)
            self.remove_handler("proposal_open_contract", _on_msg)
