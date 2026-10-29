"""Modelos de datos (``dataclasses``) para las respuestas más comunes de Deriv.

Estos modelos son *opcionales*: todas las llamadas del cliente devuelven también
el ``dict`` crudo de la API. Los modelos simplemente ofrecen un acceso cómodo y
tipado a los campos que interesan en un bot.
"""

from __future__ import annotations

from typing import Annotated, Any, Literal
from pydantic import BaseModel, BeforeValidator, Field


def _f(value: Any) -> float | None:
    """Convierte a ``float`` tolerando ``None``/strings vacíos."""
    if value is None or value == "":
        return None
    return float(value)

FloatTolerante = Annotated[float | None, BeforeValidator(_f)]


class Account(BaseModel):
    """Cuenta de trading devuelta por ``REST GET /trading/v1/options/accounts``."""

    account_id: str 
    balance: float = 0.0
    currency: str = ""
    group: str = ""
    status: str = ""
    account_type: str = "" # "demo" | "real"

    @property
    def is_demo(self) -> bool:
        """True si es una cuenta de demostración."""
        return self.account_type == "demo"

    @classmethod
    def from_api(cls, d: dict[str, Any]) -> Account:
        return cls(
            account_id=str(d["account_id"]),
            balance=float(d.get("balance", 0)),
            currency=d.get("currency", ""),
            group=d.get("group", ""),
            status=d.get("status", ""),
            account_type=d.get("account_type", ""),
        )


class Tick(BaseModel):
    """Un tick (actualización de precio) del stream ``ticks`` o ``history``."""

    symbol: str = ""
    epoch: int
    quote: float
    bid: FloatTolerante = Field(default=None, exclude=True)
    ask: FloatTolerante = Field(default=None, exclude=True)
    pip_size: int = 2
    subscription_id: str | None = Field(default=None, exclude=True)  # id para ``forget``

    @property
    def datetime(self):
        """El ``epoch`` como ``datetime`` local (usa la stdlib)."""
        from datetime import datetime

        return datetime.fromtimestamp(self.epoch)


class Candle(BaseModel):
    """Una vela OHLC de ``ticks_history`` con ``style='candles'``."""

    id: str = Field(default="", exclude=True)
    epoch: int = Field(description="It is an epoch value")
    open: float = Field(description="It is the open price value for the given time")
    high: float = Field(description="It is the high price value for the given time")
    low: float = Field(description="It is the low price value for the given time")
    close: float = Field(description="It is the close price value for the given time")
    granularity: int = Field(default=0, exclude=True)
    open_time: int = Field(default=0, exclude=False)
    pipe_size: int = Field(default=1, exclude=True)
    symbol: str = Field(default="", exclude=True)


class Proposal(BaseModel):
    """Respuesta a ``proposal``: cotización para comprar un contrato."""

    id: str                # se pasa a ``buy``
    ask_price: float = 0.0 # precio máximo a pagar
    payout: float = 0.0    # pago potencial
    spot: float = 0.0      # spot actual del subyacente
    longcode: str = ""
    raw: dict[str, Any] = Field(default_factory=dict, repr=False)

    @classmethod
    def from_api(cls, d: dict[str, Any]) -> Proposal:
        return cls(
            id=d["id"],
            ask_price=float(d.get("ask_price", 0)),
            payout=float(d.get("payout", 0)),
            spot=float(d.get("spot", 0)),
            longcode=d.get("longcode", ""),
            raw=d,
        )


class BuyReceipt(BaseModel):
    """Recibo de compra (respuesta a ``buy``)."""

    contract_id: int
    transaction_id: int = 0
    buy_price: float = 0.0
    payout: float = 0.0
    balance_after: float = 0.0
    longcode: str = ""
    start_time: int = 0
    purchase_time: int = 0

    @classmethod
    def from_api(cls, d: dict[str, Any]) -> BuyReceipt:
        return cls(
            contract_id=int(d["contract_id"]),
            transaction_id=int(d.get("transaction_id", 0)),
            buy_price=float(d.get("buy_price", 0)),
            payout=float(d.get("payout", 0)),
            balance_after=float(d.get("balance_after", 0)),
            longcode=d.get("longcode", ""),
            start_time=int(d.get("start_time", 0)),
            purchase_time=int(d.get("purchase_time", 0)),
        )


class SellReceipt(BaseModel):
    """Recibo de venta (respuesta a ``sell``)."""

    contract_id: int = 0
    sold_for: float = 0.0
    profit: float = 0.0
    balance_after: float = 0.0

    @classmethod
    def from_api(cls, d: dict[str, Any]) -> SellReceipt:
        return cls(
            contract_id=int(d.get("contract_id", 0)),
            sold_for=float(d.get("sold_for", 0)),
            profit=float(d.get("profit", 0)),
            balance_after=float(d.get("balance_after", 0)),
        )


class OpenContract:
    """Estado vivo de un contrato (respuesta a ``proposal_open_contract``)."""

    contract_id: int = 0
    status: str = ""                # e.g. "open", "settled", "sold", "expired"
    current_spot: FloatTolerante = None
    bid_price: FloatTolerante = None
    ask_price: FloatTolerante = None
    buy_price: FloatTolerante = None
    payout: FloatTolerante = None
    profit: FloatTolerante = None
    entry_spot: FloatTolerante = None
    exit_spot: FloatTolerante = None
    date_start: int | None = None
    date_expiry: int | None = None
    is_sold: bool = False
    is_expired: bool = False
    is_settled: bool = False
    raw: dict[str, Any] = Field(default_factory=dict, repr=False)

    @property
    def is_open(self) -> bool:
        """True mientras el contrato siga abierto (vendible/expirable)."""
        return not (self.is_sold or self.is_expired or self.is_settled)

    @classmethod
    def from_api(cls, d: dict[str, Any]) -> OpenContract:
        return cls(
            contract_id=int(d.get("contract_id", 0)),
            status=d.get("status", ""),
            current_spot=_f(d.get("current_spot")),
            bid_price=_f(d.get("bid_price")),
            ask_price=_f(d.get("ask_price")),
            buy_price=_f(d.get("buy_price")),
            payout=_f(d.get("payout")),
            profit=_f(d.get("profit")),
            entry_spot=_f(d.get("entry_spot")),
            exit_spot=_f(d.get("exit_spot")),
            date_start=d.get("date_start"),
            date_expiry=d.get("date_expiry"),
            is_sold=bool(d.get("is_sold")),
            is_expired=bool(d.get("is_expired")),
            is_settled=bool(d.get("is_settled")),
            raw=d,
        )


class Balance(BaseModel):
    """Saldo reportado por el stream ``balance``.

    La API puede devolver un saldo único (campos en la raíz) o una lista
    ``accounts`` cuando el token cubre varias cuentas.
    """

    balance: float = 0.0
    currency: str = ""
    loginid: str = ""
    accounts: list[dict[str, Any]] = Field(default_factory=list, repr=False)

    @classmethod
    def from_api(cls, d: dict[str, Any]) -> Balance:
        accounts = d.get("accounts") or []
        first = accounts[0] if accounts else d
        return cls(
            balance=float(first.get("balance", 0)),
            currency=first.get("currency", d.get("currency", "")),
            loginid=str(first.get("loginid", d.get("loginid", ""))),
            accounts=accounts,
        )


class Symbol(BaseModel):
    """Instrumento devuelto por ``active_symbols``."""

    exchange_is_open: Literal[0, 1] = Field(description="1 if market is currently open, 0 if closed.")
    is_trading_suspended: Literal[0, 1] = Field(description="1 indicates that trading is currently suspended, 0 if not.")
    market: str = Field(description="Market category (forex, indices, etc).")
    pip_size: float = Field(description="ip size (i.e. minimum fluctuation amount).")
    subgroup: str = Field(description="Subgroup name")
    submarket: str = Field(description="Submarket name.")
    trade_count: int = Field(description="Trade count.")
    underlying_symbol: str = Field(description="The symbol code for this underlying.")
    underlying_symbol_name: str = Field(description="Symbol display name.")
    underlying_symbol_type: str = Field(description="Symbol type (forex, commodities, etc).")

class Contract(BaseModel):
    """Instrumento devuelto por ``portfolio``."""

    auto_run_id: str = Field(default="", description="Identifier of the automated run that purchased this contract", exclude=True)
    buy_price: float = Field(default=0.0, description="Buy price")
    contract_id: int = Field(default=0, description="Internal contract identifier number (to be used in a proposal_open_contract API call)")
    contract_type: str = Field(default="", description="Contract type")
    currency: str = Field(default="", description="Contract currency")
    date_start: int = Field(default=0, description="Epoch of start date")
    expiry_time: int = Field(default=0, description="Epoch of expiry time")
    longcode: str = Field(default="", description="Contract description")
    payout: float = Field(default=0.0, description="Payout price")
    purchase_time: int = Field(default=0, description="Epoch of purchase time")
    shortcode: str = Field(default="", description="Contract short code description")
    transaction_id: int = Field(default=0, description="It is the transaction ID. Every contract (buy or sell) and every payment has a unique ID")
    underlying_symbol: str = Field(default="", description="Symbol code")
