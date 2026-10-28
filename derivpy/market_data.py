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

import asyncio
import contextlib
import json
import logging
from collections.abc import AsyncIterator
from typing import Any

import websockets

from . import _constants as C
from .exceptions import DerivAPIError, DerivConnectionError, DerivTimeoutError
from .models import Candle, Symbol, Tick

logger = logging.getLogger("derivpy.marketdata")


class MarketDataClient:
    """Conexión al gateway público de datos de mercado.

    Args:
        url: gateway alternativo si el público cambiara en el futuro.
        request_timeout: segundos máximos de espera por una respuesta puntual.
    """

    def __init__(
        self,
        url: str = C.PUBLIC_WS_URL,
        *,
        request_timeout: float = C.DEFAULT_REQUEST_TIMEOUT,
    ) -> None:
        self._url = url
        self.request_timeout = request_timeout

        self._ws: websockets.ClientConnection | None = None
        self._recv_task: asyncio.Task[None] | None = None
        self._req_counter = 0
        #: Primer ``req_id`` "adoptado" por el gateway (ver ``_stream``).
        self._req_id_floor: int | None = None
        self._pending: dict[int, asyncio.Future[dict[str, Any]]] = {}
        # id(queue) -> (msg_types aceptados, cola) para los generadores de stream
        self._streams: dict[int, tuple[set[str], asyncio.Queue]] = {}
        self._sub_ids: dict[int, str | None] = {}
        self._closing = False

    # ------------------------------------------------------------------ #
    # Ciclo de vida
    # ------------------------------------------------------------------ #
    async def connect(self) -> None:
        """Abre la conexión con el gateway público."""
        self._closing = False
        try:
            self._ws = await websockets.connect(
                self._url, open_timeout=self.request_timeout, close_timeout=5
            )
        except (OSError, asyncio.TimeoutError, websockets.WebSocketException) as exc:
            raise DerivConnectionError(f"No se pudo conectar al gateway público: {exc}") from exc
        self._recv_task = asyncio.create_task(self._recv_loop(), name="deriv-public-recv")

    async def disconnect(self) -> None:
        """Cierra la conexión y limpia recursos."""
        self._closing = True
        if self._recv_task:
            self._recv_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._recv_task
            self._recv_task = None
        if self._ws is not None:
            with contextlib.suppress(Exception):
                await self._ws.close()
            self._ws = None
        for fut in self._pending.values():
            if not fut.done():
                fut.set_exception(DerivConnectionError("Conexión cerrada"))
        self._pending.clear()

    async def __aenter__(self) -> MarketDataClient:
        await self.connect()
        return self

    async def __aexit__(self, *exc_info: object) -> None:
        await self.disconnect()

    @property
    def is_connected(self) -> bool:
        return self._ws is not None

    # ------------------------------------------------------------------ #
    # Núcleo petición/respuesta + streams
    # ------------------------------------------------------------------ #
    async def request(self, payload: dict[str, Any]) -> dict[str, Any]:
        """Petición única; devuelve la primera respuesta con ese ``req_id``."""
        if self._ws is None:
            raise DerivConnectionError("Llama a connect() antes de hacer peticiones.")
        self._req_counter += 1
        if self._req_id_floor is not None and self._req_counter == self._req_id_floor:
            # Reservado por el gateway (ver `_stream`).
            self._req_counter += 1
        req_id = self._req_counter
        fut: asyncio.Future[dict[str, Any]] = asyncio.get_running_loop().create_future()
        self._pending[req_id] = fut
        try:
            await self._ws.send(json.dumps({**payload, "req_id": req_id}))
            resp = await asyncio.wait_for(fut, self.request_timeout)
        except asyncio.TimeoutError as exc:
            raise DerivTimeoutError(f"Timeout esperando respuesta a {payload}") from exc
        finally:
            self._pending.pop(req_id, None)

        err = resp.get("error")
        if err:
            raise DerivAPIError(err.get("code"), err.get("message", ""))
        return resp

    async def _stream(self, payload: dict[str, Any]) -> AsyncIterator[dict[str, Any]]:
        """Generador genérico de suscripciones vía cola async.

        Cada mensaje que comparte el ``msg_type`` de la petición se emite hasta
        que el consumidor abandone el bucle (entonces envía ``forget``).
        """
        queue: asyncio.Queue[dict[str, Any] | None] = asyncio.Queue()
        msg_type = next(iter(payload))
        loop_msg_types = {msg_type, _response_type(msg_type)}

        # El gateway Deriv "adopta" el req_id de la primera petición que recibe
        # y lo reenvía en todos los mensajes de stream posteriores; por eso:
        #   1) registramos la cola ANTES de suscribirnos (la respuesta inicial
        #      es también el primer mensaje del stream), y
        #   2) marcamos ese req_id como reservado (`_req_id_floor`) para que
        #      nunca colisione con peticiones futuras.
        token = id(queue)
        self._streams[token] = (loop_msg_types, queue)
        try:
            initial = await self.request({**payload, "subscribe": 1})
        except BaseException:
            self._streams.pop(token, None)
            raise
        self._sub_ids[token] = initial.get("subscription", {}).get("id")
        yield initial
        try:
            while True:
                item = await queue.get()
                if item is None:
                    break
                yield item
        finally:
            self._streams.pop(token, None)
            self._sub_ids.pop(token, None)
            # Nota: no enviamos `forget` aquí porque el `finally` puede ejecutarse
            # con la tarea ya cancelada. Al cerrar la conexión el servidor
            # libera todas las suscripciones; para streams largos usa
            # `async with MarketDataClient()` y sal limpiamente.

    async def _recv_loop(self) -> None:
        assert self._ws is not None
        while not self._closing:
            try:
                raw = await self._ws.recv()
                msg = json.loads(raw)
            except websockets.ConnectionClosed:
                if not self._closing:
                    logger.warning("Conexión pública caída.")
                    for _, (_, q) in self._streams.items():
                        q.put_nowait(None)
                return
            except json.JSONDecodeError:
                continue

            req_id = msg.get("req_id")
            if req_id is not None:
                if self._req_id_floor is None:
                    # El gateway adopta el primer req_id que ve y lo reenvía en
                    # los mensajes de stream; lo reservamos (ver `_stream`).
                    self._req_id_floor = int(req_id)
                fut = self._pending.get(req_id)
                if fut is not None and not fut.done():
                    fut.set_result(msg)

            for _, (types, q) in list(self._streams.items()):
                if msg.get("msg_type") in types:
                    q.put_nowait(msg)

    # ------------------------------------------------------------------ #
    # API de alto nivel
    # ------------------------------------------------------------------ #
    async def active_symbols(
        self, full: bool = False, contract_type: str | None = None
    ) -> list[Symbol]:
        """``active_symbols`` — instrumentos disponibles para operar."""
        payload: dict[str, Any] = {"active_symbols": "full" if full else "brief"}
        if contract_type:
            payload["contract_type"] = [contract_type]
        resp = await self.request(payload)
        key = "active_symbols" if "active_symbols" in resp else "underlying"
        return [Symbol.model_validate(d) for d in resp.get(key, [])]

    async def ticks_history(
        self,
        symbol: str,
        *,
        count: int = 1000,
        end: str = "latest",
        start: int | None = None,
    ) -> tuple[list[Tick], int]:
        """``ticks_history`` (style=ticks) — últimos ticks de un símbolo.

        Returns:
            ``(lista_de_ticks, pip_size)``.
        """
        payload: dict[str, Any] = {
            "ticks_history": symbol,
            "end": str(end),
            "style": "ticks",
            "count": count,
        }
        if start is not None:
            payload["start"] = start
        resp = await self.request(payload)
        history = resp.get("history", {})
        prices = history.get("prices", [])
        times = history.get("times", [])
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
        granularity: int = 60,
        count: int = 100,
        end: str = "latest",
        start: int | None = None,
    ) -> list[Candle]:
        """``ticks_history`` (style=candles) — velas OHLC.

        Args:
            granularity: tamaño de vela en segundos (ver ``CANDLE_GRANULARITIES``).
        """
        if granularity not in C.CANDLE_GRANULARITIES:
            raise ValueError(
                f"granularity={granularity} no válido; usa uno de {C.CANDLE_GRANULARITIES}"
            )
        payload: dict[str, Any] = {
            "ticks_history": symbol,
            "end": str(end),
            "style": "candles",
            "granularity": granularity,
            "count": count,
        }
        if start is not None:
            payload["start"] = start
        resp = await self.request(payload)
        return [
            Candle(
                epoch=int(c["epoch"]),
                open=float(c["open"]),
                high=float(c["high"]),
                low=float(c["low"]),
                close=float(c["close"]),
            )
            for c in resp.get("candles", [])
        ]

    async def stream_ticks(self, symbol: str) -> AsyncIterator[Tick]:
        """Stream en vivo de ticks de un símbolo (``ticks`` + ``subscribe``)."""
        async for msg in self._stream({"ticks": symbol}):
            if msg.get("tick"):
                yield Tick.model_validate(msg["tick"])

    async def stream_candles(self, symbol: str, *, granularity: int = 60) -> AsyncIterator[Candle]:
        """Stream en vivo de velas OHLC (``ohlc`` + ``subscribe``)."""
        async for msg in self._stream(
            {"ticks_history": symbol, "end": "latest", "style": "candles", "granularity": granularity}
        ):
            ohlc = msg.get("ohlc")
            if ohlc:
                yield Candle(
                    epoch=int(ohlc["open_epoch"]),
                    open=float(ohlc["open"]),
                    high=float(ohlc["high"]),
                    low=float(ohlc["low"]),
                    close=float(ohlc["close"]),
                )


def _response_type(request_type: str) -> str:
    """Mapúa tipo de petición → tipo de respuesta (p. ej. ``ticks``→``tick``)."""
    table = {
        "ticks": "tick",
        "ticks_history": "ohlc",
        "balance": "balance",
        "proposal": "proposal",
        "proposal_open_contract": "proposal_open_contract",
        "transaction": "transaction",
    }
    return table.get(request_type, request_type)
