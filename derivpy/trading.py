# trading.py
"""Cliente de trading autenticado sobre WebSocket (gateway OTP).

Flujo:
1. Con el Bearer token (OAuth2 o PAT) se pide una URL OTP por REST:
   ``POST /trading/v1/options/accounts/{accountId}/otp``.
2. Se conecta a esa URL (``wss://.../ws/demo?otp=...`` o ``/ws/real``).
3. Se envían/reciben mensajes JSON con el protocolo clásico de Deriv
   (``proposal``, ``buy``, ``sell``, ``balance``, ``portfolio``…).
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
from collections.abc import AsyncIterator, Awaitable, Callable
from typing import Any, Literal

from derivpy.reconnection import ReconnectionConfig

from . import _constants as C
from ._base_client import BaseDerivWSClient
from .models import Balance, BuyReceipt, Contract, OpenContract, Proposal, SellReceipt, Transaction
from .rest import RestClient

logger = logging.getLogger("derivpy.trading")

StreamHandler = Callable[[dict[str, Any]], Any | Awaitable[Any]]


class TradingClient(BaseDerivWSClient):
    """Cliente autenticado para operar sobre una cuenta de Deriv."""

    def __init__(
        self,
        account_id: str,
        token: str,
        app_id: str | None = None,
        *,
        request_timeout: float = C.DEFAULT_REQUEST_TIMEOUT,
        ping_interval: float = C.DEFAULT_PING_INTERVAL,
        reconnection_config: ReconnectionConfig | None = None,
    ) -> None:
        super().__init__(
            "",
            request_timeout=request_timeout,
            ping_interval=ping_interval,
            reconnection_config=reconnection_config,
        )
        self.account_id = str(account_id)
        self._rest = RestClient(token=token, app_id=app_id)
        self._subscriptions: dict[str, list[tuple[dict[str, Any], StreamHandler]]] = {}
        self._connected = asyncio.Event()

    # ------------------------------------------------------------------ #
    # Ciclo de vida
    # ------------------------------------------------------------------ #
    async def connect(self) -> None:
        """Pide un OTP por REST y abre la conexión WebSocket."""
        self._closing = False
        self._connected.clear()
        url = await self._rest.get_otp_ws_url(self.account_id)
        self._url = url
        await self._open_socket()
        self._connected.set()
        self._connection_event.set()
        logger.info("Conectado a gateway de trading (cuenta %s)", self.account_id)

    async def disconnect(self) -> None:
        self._connected.clear()
        await super().disconnect()

    async def _reconnect(self) -> bool:
        """Reconecta el TradingClient obteniendo un OTP nuevo y restaurando suscripciones."""
        try:
            logger.info("[TradingClient] Intentando reconexión (cuenta %s)...", self.account_id)
            self._connected.clear()
            self._connection_event.clear()
            self._fail_pending_requests("Reconectando TradingClient...")

            logger.debug("[TradingClient] Solicitando nuevo OTP...")
            url = await self._rest.get_otp_ws_url(self.account_id)
            self._url = url
            logger.debug("[TradingClient] OTP obtenido: %s...", url[:50])

            await self._open_socket()
            await self._resubscribe_all()

            self._connected.set()
            self._connection_event.set()
            logger.info("[TradingClient] ✅ Reconexión exitosa (cuenta %s)", self.account_id)
            return True

        except Exception as exc:
            logger.error("[TradingClient] ❌ Error en reconexión: %s", exc, exc_info=True)
            self._connected.clear()
            self._connection_event.clear()
            await self._cancel_background_tasks()
            await self._close_socket()
            return False

    @property
    def is_connection_stable(self) -> bool:
        """True si el socket está abierto, el OTP está activo y no hay reconexión en curso."""
        return super().is_connection_stable and self._connected.is_set()

    # ------------------------------------------------------------------ #
    # Envío / recepción
    # ------------------------------------------------------------------ #
    async def subscribe(self, payload: dict[str, Any], handler: StreamHandler) -> dict[str, Any]:
        """Envía una petición con ``subscribe: 1`` y registra ``handler``."""
        msg_type = next(iter(payload))

        if msg_type not in self._subscriptions:
            self._subscriptions[msg_type] = []
        self._subscriptions[msg_type].append((dict(payload), handler))

        try:
            return await self.request({**payload, "subscribe": 1})
        except BaseException:
            self.remove_handler(msg_type, handler)
            raise

    def remove_handler(self, msg_type: str, handler: StreamHandler) -> None:
        """Quita un callback específico para un ``msg_type`` dado."""
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
        for _, handler in list(subs):
            try:
                result = handler(msg)
                if asyncio.iscoroutine(result):
                    asyncio.create_task(result)
            except Exception:
                logger.exception("Error en handler de stream %s", msg_type)

    async def _resubscribe_all(self) -> None:
        """Resuscribe todos los streams activos tras una reconexión."""
        for msg_type, subs in list(self._subscriptions.items()):
            for payload, _ in list(subs):
                try:
                    logger.debug("[%s] Resuscribiendo %s...", self.__class__.__name__, msg_type)
                    await self.request({**payload, "subscribe": 1})
                except Exception as exc:
                    logger.error(
                        "[%s] Error al resuscribir %s: %s",
                        self.__class__.__name__,
                        msg_type,
                        exc,
                    )
                    raise

    async def get_balance(self, subscribe: bool = False) -> Balance:
        """``balance`` — saldo actual (o suscripción a cambios)."""
        if subscribe:
            return await self.request({"balance": 1})
        resp = await self.request({"balance": 1})
        return Balance.model_validate(resp["balance"])

    async def watch_balance(self) -> AsyncIterator[Balance]:
        """Stream en vivo del saldo de la cuenta."""
        queue: asyncio.Queue[Balance | None] = asyncio.Queue()

        def _on_msg(msg: dict[str, Any]) -> None:
            bal = msg.get("balance")
            if bal:
                queue.put_nowait(Balance.model_validate(bal))

        initial = await self.subscribe({"balance": 1}, _on_msg)

        try:
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
        req_id: int | None = None,
    ) -> list[Contract]:
        """``profit_table`` — Retrieve a summary of account profit/loss."""
        payload: dict[str, Any] = {"profit_table": 1}

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
        """``portfolio`` — contratos abiertos."""
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
        req_id: int | None = None,
    ) -> list[Transaction]:
        """``statement`` — Retrieve account statement."""
        payload: dict[str, Any] = {"statement": 1}

        if action_type is not None:
            payload["action_type"] = action_type
        if date_from is not None:
            payload["date_from"] = min(max(0, date_from), 9999999999)
        if date_to is not None:
            payload["date_to"] = min(max(0, date_to), 9999999999)
        if description is not None:
            payload["description"] = description
        if limit is not None:
            payload["limit"] = min(max(0, limit), 999)
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
        """``proposal`` — cotiza un contrato."""
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
        """``buy`` — compra el contrato cotizado."""
        resp = await self.request({"buy": proposal_id, "price": price})
        return BuyReceipt.model_validate(resp["buy"])

    async def sell(self, contract_id: int, price: float = 0) -> SellReceipt:
        """``sell`` — cierra un contrato abierto."""
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
        """Atajo proponer→comprar."""
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
        """Stream en vivo del estado de un contrato hasta que cierre."""
        queue: asyncio.Queue[OpenContract | None] = asyncio.Queue()

        def _on_msg(msg: dict[str, Any]) -> None:
            poc = msg.get("proposal_open_contract")
            if not poc or int(poc.get("contract_id", 0)) != int(contract_id):
                return
            oc = OpenContract.model_validate(poc)
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
