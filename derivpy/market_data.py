"""Cliente de datos de mercado PUBLICOS (WebSocket sin autenticación).

Conecta a ``wss://api.derivws.com/trading/v1/options/ws/public`` y ofrece:

* símbolos activos, historial de ticks/velas,
* streams en vivo como iteradores async (``async for tick in api.stream_ticks(...)``).

No requiere token alguno.

Ejemplo::

    async with MarketDataClient() as api:
        candles = await api.candles("R_100", count=50)
        async for tick in api.stream_ticks("R_100"):
            print(tick.quote)
"""

from __future__ import annotations

import logging
from collections.abc import AsyncIterator
from typing import Any, Literal
from ._base_client import BaseDerivWSClient


from . import _constants as C
from .models import Candle, Symbol, Tick

logger = logging.getLogger("derivpy.marketdata")


class MarketDataClient(BaseDerivWSClient):
    """Conexión al gateway público de datos de mercado.

    Args:
        url: gateway alternativo si el público cambiara en el futuro.
        request_timeout: segundos máximos de espera por una respuesta puntual.
        ping_interval: segundos entre pings de keep-alive.
        max_reconnect_attempts: intentos de reconexión antes de fallar.
    """

    def __init__(
        self,
        url: str = C.PUBLIC_WS_URL,
        *,
        request_timeout: float = C.DEFAULT_REQUEST_TIMEOUT,
        ping_interval: float = C.DEFAULT_PING_INTERVAL,
        max_reconnect_attempts: int = 3
    ) -> None:
        super().__init__(url, request_timeout=request_timeout, ping_interval=ping_interval)
        self.max_reconnect_attempts = max_reconnect_attempts
        # Guardar payloads para resuscripción
        self._active_subscriptions: dict[int, dict[str, Any]] = {}
        

    # ------------------------------------------------------------------ #
    # API de alto nivel
    # ------------------------------------------------------------------ #
    async def active_symbols(
        self, 
        active_symbols: Literal['full', 'brief'] = 'brief', 
        contract_type: str | None = None,
        passthrough: dict | None = None,
        req_id: int | None = None
    ) -> list[Symbol]:
        """``active_symbols`` — instrumentos disponibles para operar."""
        payload = {"active_symbols": active_symbols}

        if contract_type is not None:
            payload["contract_type"] = contract_type

        if passthrough is not None:
            payload["passthrough"] = passthrough

        if req_id is not None:
            payload["req_id"] = req_id

        resp = await self.request(payload)
        key = "active_symbols" if "active_symbols" in resp else "underlying"
        return [Symbol.model_validate(d) for d in resp.get(key, [])]

    async def ticks_history(
        self,
        symbol: str,
        *,
        adjust_start_time: Literal[1] | None = None,
        count: int | None = None,
        end: str | int = "latest",
        start: int | None = None,
        passthrough: dict | None = None,
        req_id: int | None = None
    ) -> tuple[list[Tick], int]:
        """``ticks_history`` (style=ticks) — últimos ticks de un símbolo.

        Returns:
            ``(lista_de_ticks, pip_size)``.
        """
        payload = {
            "ticks_history": symbol,
            "style": "ticks"
        }

        if adjust_start_time is not None:
            payload["adjust_start_time"] = adjust_start_time

        if count is not None:
            payload["count"] = min(max(1, count), 1000)

        if end is not None:
            payload["end"] = end

        if start is not None:
            payload["start"] = min(max(0, start), 9999999999)

        if passthrough is not None:
            payload["passthrough"] = passthrough

        if req_id is not None:
            payload["req_id"] = req_id

        resp = await self.request(payload)
        history = resp.get("history") or {}
        prices = history.get("prices") or []
        times = history.get("times") or []
        pip_size = int(resp.get("pip_size", 2))
        ticks = [
            Tick(symbol=symbol, epoch=int(t), quote=float(p), pip_size=pip_size)
            for t, p in zip(times, prices)
        ]
        return ticks, pip_size

    async def candles(
        self,
        symbol: str,
        *,
        adjust_start_time: Literal[1] | None = None,
        count: int | None = None,
        end: str | int = "latest",
        granularity: Literal[60, 120, 180, 300, 600, 900, 1800, 3600, 7200, 14400, 28800, 86400] = 60,
        start: int | None = None,
        passthrough: dict | None = None,
        req_id: int | None = None
    ) -> list[Candle]:
        """``ticks_history`` (style=candles) — velas OHLC.

        Args:
            granularity: tamaño de vela en segundos (ver ``CANDLE_GRANULARITIES``).
        """
        if granularity not in C.CANDLE_GRANULARITIES:
            raise ValueError(
                f"granularity={granularity} no válido; usa uno de {C.CANDLE_GRANULARITIES}"
            )
        payload = {
            "ticks_history": symbol,
            "style": "ticks"
        }
        
        if adjust_start_time is not None:
            payload["adjust_start_time"] = adjust_start_time
        
        if count is not None:
            payload["count"] = min(max(1, count), 1000)
        
        if end is not None:
            payload["end"] = end

        if granularity is not None:
            payload["granularity"] = granularity
        
        if start is not None:
            payload["start"] = min(max(0, start), 9999999999)
        
        if passthrough is not None:
            payload["passthrough"] = passthrough
        
        if req_id is not None:
            payload["req_id"] = req_id

        resp = await self.request(payload)
        return [
            Candle.model_validate(c)
            for c in resp.get("candles", [])
        ]

    async def stream_ticks(self, symbol: str) -> AsyncIterator[Tick]:
        """Stream en vivo de ticks de un símbolo (``ticks`` + ``subscribe``)."""
        async for msg in self._stream({"ticks": symbol}):
            if msg.get("tick"):
                yield Tick.model_validate(msg["tick"])

    async def stream_candles(
            self, 
            symbol: str, 
            *, 
            granularity: Literal[60, 120, 180, 300, 600, 900, 1800, 3600, 7200, 14400, 28800, 86400] = 60) -> AsyncIterator[Candle]:
        """Stream en vivo de velas OHLC (``ohlc`` + ``subscribe``)."""
        async for msg in self._stream(
            {"ticks_history": symbol, "end": "latest", "style": "candles", "granularity": granularity}
        ):
            ohlc = msg.get("ohlc")
            if ohlc:
                yield Candle.model_validate(ohlc)

    async def get_server_time(self) -> int:
        """``time`` — hora del servidor (epoch)."""
        resp = await self.request({"time": 1})
        return int(resp["time"])