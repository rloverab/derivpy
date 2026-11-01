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
import logging
from collections.abc import AsyncIterator, Awaitable, Callable
from typing import Any, Literal

from . import _constants as C
from ._base_client import BaseDerivWSClient
from .exceptions import DerivAuthError, DerivConnectionError
from .models import Balance, BuyReceipt, OpenContract, Proposal, SellReceipt, Contract, Transaction
from .rest import RestClient

logger = logging.getLogger("derivpy.trading")

#: Callback de stream: recibe el dict crudo del mensaje. Puede ser sync o async.
StreamHandler = Callable[[dict[str, Any]], Any | Awaitable[Any]]


class TradingClient(BaseDerivWSClient):
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
        super().__init__("", request_timeout=request_timeout, ping_interval=ping_interval)
        self.account_id = str(account_id)
        self._rest = RestClient(token=token, app_id=app_id)
        self.max_reconnect_attempts = max_reconnect_attempts
        self._callbacks: dict[str, list[StreamHandler]] = {}
        self._subscriptions: dict[str, list[tuple[dict[str, Any], StreamHandler]]] = {}
        self._connected = asyncio.Event()

    # ------------------------------------------------------------------ #
    # Ciclo de vida
    # ------------------------------------------------------------------ #
    async def connect(self) -> None:
        """Pide un OTP por REST y abre la conexión WebSocket."""
        self._closing = False
        url = await self._rest.get_otp_ws_url(self.account_id)
        self._url = url  # Actualizar URL para la clase base
        await super().connect()
        self._connected.set()
        logger.info("Conectado a gateway de trading (cuenta %s)", self.account_id)

    async def disconnect(self) -> None:
        self._connected.clear()
        await super().disconnect()

    @property
    def is_connected(self) -> bool:
        """True si la conexión WebSocket está activa."""
        return super().is_connected and self._connected.is_set()

    # ------------------------------------------------------------------ #
    # Envío / recepción
    # ------------------------------------------------------------------ #
    async def subscribe(self, payload: dict[str, Any], handler: StreamHandler) -> dict[str, Any]:
        """Envía una petición con ``subscribe: 1`` y registra ``handler``.

        El primer mensaje (la respuesta inicial) se devuelve también como dict;
        los siguientes llegan a ``handler`` en caliente.
        """
        msg_type = next(iter(payload))

        if msg_type not in self._subscriptions:
            self._subscriptions[msg_type] = []
        self._subscriptions[msg_type].append((payload, handler))
        
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
            subs = self._subscriptions.get(msg_type, [])
            self._subscriptions[msg_type] = [
                (payload, h) for payload, h in subs if h != handler
            ]
            if not self._subscriptions[msg_type]:
                self._subscriptions.pop(msg_type, None)

    async def forget(self, subscription_id: str) -> dict[str, Any]:
        """``forget`` — cancela una suscripción concreta por su id."""
        return await self.request({"forget": subscription_id})

    async def forget_all(self, *msg_types: str) -> dict[str, Any]:
        """``forget_all`` — cancela todas las suscripciones de esos tipos."""
        return await self.request({"forget_all": list(msg_types)})

    # ------------------------------------------------------------------ #
    # Sobrescritura del enrutador de mensajes (usa callbacks, no colas)
    # ------------------------------------------------------------------ #
    async def _process_stream_message(self, msg: dict[str, Any]) -> None:
        """Reparte mensajes a los callbacks registrados."""
        msg_type = msg.get("msg_type", "")
        subs = self._subscriptions.get(msg_type, [])
        for payload, handler in list(subs):
            try:
                result = handler(msg)
                if asyncio.iscoroutine(result):
                    asyncio.create_task(result)
            except Exception:
                logger.exception("Error en handler de stream %s", msg_type)
        
    # ------------------------------------------------------------------ #
    # Reconexión (ESPECÍFICO de TradingClient)
    # ------------------------------------------------------------------ #
    async def _handle_disconnect(self) -> None:
        """Limpia colas (por si acaso) y lanza reconexión automática."""
        await super()._handle_disconnect()  # Limpieza estándar
        
        if self._closing:
            return
            
        logger.warning("Conexión de trading caída; intentando reconexión...")
        if await self._reconnect():
            logger.info("Reconexión exitosa. Resuscribiendo streams...")
            await self._resubscribe_all()
        else:
            logger.error("Reconexión fallida. Streams no resuscritos.")
            self._closing = True
            await self.disconnect()

    async def _reconnect(self) -> bool:
        """Reintenta conectar con backoff exponencial."""
        delay = 1.0
        for attempt in range(1, self.max_reconnect_attempts + 1):
            await asyncio.sleep(delay)
            delay = min(delay * 2, 30.0)
            logger.info("Intento de reconexión %d/%d…", attempt, self.max_reconnect_attempts)

            try:
                for task in (self._recv_task, self._ping_task):
                    if task:
                        task.cancel()
                        with contextlib.suppress(asyncio.CancelledError):
                            await task  # Esperar a que la cancelación se complete
                self._recv_task = None
                self._ping_task = None

                # Cerrar conexión actual silenciosamente
                with contextlib.suppress(Exception):
                    if self._ws is not None:
                        await self._ws.close()
                        self._ws = None  # Resetear para que connect() parta de cero

                for fut in self._pending.values():
                    if not fut.done():
                        fut.set_exception(DerivConnectionError("Reconectando..."))
                self._pending.clear()
                
                await self.connect()  # Pide OTP NUEVO

                logger.info("WebSocket reconectado exitosamente")
                return True
            
            except (DerivConnectionError, DerivAuthError, Exception) as exc:
                logger.warning("Fallo en intento de reconexión: %s", exc)

        logger.error("No se pudo reconectar tras %d intentos.", self.max_reconnect_attempts)        
        return False

    async def _resubscribe_all(self) -> None:
        """Resuscribe todos los streams activos tras una reconexión."""
        for msg_type, subs in list(self._subscriptions.items()):
            for payload, handler in subs:
                try:
                    logger.debug("Resuscribiendo %s...", msg_type)
                    await self.request({**payload, "subscribe": 1})
                except Exception as exc:
                    logger.error("Error al resuscribir %s: %s", msg_type, exc)

    async def get_balance(self, subscribe: bool = False) -> Balance:
        """``balance`` — saldo actual (o suscripción a cambios)."""
        if subscribe:
            return await self.request({"balance": 1})  # inicial; updates via watch_balance
        resp = await self.request({"balance": 1})
        return Balance.model_validate(resp["balance"])

    async def watch_balance(self) -> AsyncIterator[Balance]:
        """Stream en vivo del saldo de la cuenta.
        
        Ejemplo::
        
            async for balance in api.watch_balance():
                print(f"Saldo actual: {balance.balance} {balance.currency}")
                if balance.balance < 100:
                    # Lógica de gestión de riesgo
                    break  # Cleanup automático en finally
        """
        queue: asyncio.Queue[Balance | None] = asyncio.Queue()

        def _on_msg(msg: dict[str, Any]) -> None:
            bal = msg.get("balance")
            if bal:
                queue.put_nowait(Balance.model_validate(bal))

        initial = await self.subscribe({"balance": 1}, _on_msg)

        try:
            # El saldo inicial siempre viene en la respuesta de suscripción
            first = initial.get("balance")
            if first:
                yield Balance.model_validate(first)

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
            self.remove_handler("balance", _on_msg)
    
    async def get_profit_table(
            self, 
            contract_type: list[str] | None = None,
            date_from: str | None = None,
            date_to: str | None = None,
            description: Literal[0, 1] | None = None,
            limit: int | None = None,
            offset: int | None = None,
            sort: Literal["ASC", "DESC"] | None = None,
            passthrough: dict | None = None,
            req_id: int | None = None

    ) -> list[Contract]:
        """
        ``profit_table`` — Retrieve a summary of account profit/loss.

        Args:
            contract_type (list, optional): Return only contracts of the specified types.
            date_from (str, optional): Start date (epoch or YYYY-MM-DD). Pattern: ^([0-9]{4}-(0?[1-9]|1[012])-(0?[1-9]|[12][0-9]|3[01])|[0-9]{1,10})$
            date_to (str, optional): End date (epoch or YYYY-MM-DD). Pattern: ^([0-9]{4}-(0?[1-9]|1[012])-(0?[1-9]|[12][0-9]|3[01])|[0-9]{1,10})$
            description (int, optional): If set to 1, will return full contracts description.
            limit (int, optional): Apply upper limit to count of transactions received. Minimum: 0 · Maximum: 500. Default: 50
            offset (int, optional): Number of transactions to skip. Minimum: 0
            sort (str, optional): Sort direction. Default: "DESC"
            passthrough (dict, optional): Used to pass data through the websocket, which may be retrieved via the echo_req output field.
            req_id (int, optional): Used to map request to response.
        """
        payload = {"profit_table": 1}

        if contract_type is not None:
            payload["contract_type"] = contract_type

        if date_from is not None:
            payload["date_from"] = date_from

        if date_to is not None:
            payload["date_to"] = date_to

        if description is not None:
            payload["description"] = description

        if limit is not None:
            payload["limit"] = limit

        if offset is not None:
            payload["offset"] = offset

        if sort is not None:
            payload["sort"] = sort

        if passthrough is not None:
            payload["passthrough"] = passthrough

        if req_id is not None:
            payload["req_id"] = req_id

        resp = await self.request(payload)
        profit_data = resp.get("profit_table") or {}
        transactions = profit_data.get("transactions") or []
        return [Transaction.model_validate(transaction) for transaction in transactions]

    async def get_portfolio(self) -> list[Contract]:
        """``portfolio`` — contratos abiertos (lista cruda)."""
        resp = await self.request({"portfolio": 1})
        portfolio = resp.get("portfolio") or {}
        contracts = portfolio.get("contracts") or []        
        return [Contract.model_validate(contract) for contract in contracts]

    async def get_statement(
            self, 
            action_type: Literal["buy", "sell", "deposit", "withdrawal"] | None = None,
            date_from: int | None = None,
            date_to: int | None = None,
            description: Literal[0, 1] | None = None,
            limit: int | None = None,
            offset: int | None = None,
            passthrough: dict | None = None,
            req_id: int | None = None
    ) -> list[Transaction]:
        """
        ``statement`` — Retrieve account statement

        Args:
            action_type (str, optional): {'buy', 'sell', 'deposit', 'withdrawal'}. To filter the statement according to the type of transaction.
            date_from: (int, optional): Start date (epoch). Minimum: 0 · Maximum: 9999999999
            date_to: (int, optional): End date (epoch). Minimum: 0 · Maximum: 9999999999
            description: (int, optional): If set to 1, will return full contracts description.
            limit: (int, optional):  Maximum number of transactions to receive. Minimum: 0 · Maximum: 999. Default: 100
            offset: (int, optional): Number of transactions to skip. Minimum: 0
            passthrough: (dict, optional): Used to pass data through the websocket, which may be retrieved via the echo_req output field.
            req_id: (int, optional): Used to map request to response.
        """
        payload = {"statement": 1}

        if action_type is not None:
            payload["action_type"] = action_type
            
        if date_from is not None:
            payload["date_from"] = min(max(0,date_from), 9999999999)

        if date_to is not None:
            payload["date_to"] = min(max(0,date_to), 9999999999)

        if description is not None:
            payload["description"] = description

        if limit is not None:
            payload["limit"] = min(max(0, limit),999)

        if offset is not None:
            payload["offset"] = max(0, offset)

        if passthrough is not None:
            payload["passthrough"] = passthrough

        if req_id is not None:
            payload["req_id"] = req_id

        resp = await self.request(payload)

        statement = resp.get("statement") or {}
        transactions = statement.get("transactions") or []
        return [Transaction.model_validate(transaction) for transaction in transactions]

    # async def get_profit_table(self, **params: Any) -> list[dict[str, Any]]:
    #     """``profit_table`` — tabla de resultados (``sortby``, ``limit``…)."""
    #     resp = await self.request({"profit_table": 1, **params})
    #     return resp.get("profit_table", [])

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
        return Proposal.model_validate(resp["proposal"])

    async def buy(self, proposal_id: str, price: float) -> BuyReceipt:
        """``buy`` — compra el contrato cotizado.

        Args:
            proposal_id: ``id`` de la respuesta de :meth:`propose`.
            price: precio máximo aceptado (usa ``ask_price`` de la propuesta).
        """
        resp = await self.request({"buy": proposal_id, "price": price})
        return BuyReceipt.model_validate(resp["buy"])

    async def sell(self, contract_id: int, price: float = 0) -> SellReceipt:
        """``sell`` — cierra un contrato abierto.

        Args:
            contract_id: id devuelto por :meth:`buy` / :meth:`get_portfolio`.
            price: mínimo aceptado, o ``0`` para vender a mercado.
        """
        resp = await self.request({"sell": contract_id, "price": price})
        return SellReceipt.model_validate(resp["sell"])

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
        return OpenContract.model_validate(resp["proposal_open_contract"])

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
            oc = OpenContract.model_validate(msg["proposal_open_contract"])
            queue.put_nowait(oc)
            if stop_when_closed and not oc.is_open:
                queue.put_nowait(None)

        initial = await self.subscribe(
            {"proposal_open_contract": 1, "contract_id": contract_id}, _on_msg
        )
        first = OpenContract.model_validate(initial["proposal_open_contract"])
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
