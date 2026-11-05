# base_client.py
from __future__ import annotations

import asyncio
import logging
import contextlib
import json
import websockets

from websockets import State
from collections.abc import AsyncIterator
from abc import ABC
from typing import Any
    

from . import _constants as C
from .exceptions import DerivAPIError, DerivConnectionError

logger = logging.getLogger("derivpy.base")

class BaseDerivWSClient(ABC):
    """Clase base para gestionar la fontanería de WebSocket de Deriv."""

    def __init__(
        self,
        url: str,
        *,
        request_timeout: float = 10.0,
        ping_interval: float = 30.0,
    ) -> None:
        self._url = url
        self.request_timeout = request_timeout
        self.ping_interval = ping_interval

        self._ws: websockets.ClientConnection | None = None
        self._recv_task: asyncio.Task[None] | None = None
        self._ping_task: asyncio.Task[None] | None = None
        self._req_counter = 0
        self._req_id_floor: int | None = None
        self._pending: dict[int, asyncio.Future[dict[str, Any]]] = {}
        self._streams: dict[int, tuple[set[str], asyncio.Queue]] = {}
        self._closing = False        


    async def connect(self) -> None:
        self._closing = False
        try:
            self._ws = await websockets.connect(
                self._url, 
                open_timeout=self.request_timeout, 
                close_timeout=5,
                ping_interval=self.ping_interval,
                ping_timeout=10
            )
        except (OSError, asyncio.TimeoutError, websockets.WebSocketException) as exc:
            raise DerivConnectionError(f"Fallo de conexión a {self._url}: {exc}") from exc
        
        self._recv_task = asyncio.create_task(self._recv_loop(), name="deriv-recv")

        if self.ping_interval > 0:
            self._ping_task = asyncio.create_task(self._ping_loop(), name="deriv-ping")

        await self._on_connected()  # Hook para clases hijas

    async def disconnect(self) -> None:
        self._closing = True
        
        if self._ping_task:
            self._ping_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._ping_task
                
        if self._recv_task:
            self._recv_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._recv_task
        
        if self._ws:
            await self._ws.close()
            self._ws = None
            
        for fut in self._pending.values():
            if not fut.done():
                fut.set_exception(DerivConnectionError("Conexión cerrada"))
        self._pending.clear()
        await self._on_disconnected()

    async def __aenter__(self):
        await self.connect()
        return self

    async def __aexit__(self, *exc_info):
        await self.disconnect()

    def _next_req_id(self) -> int:
        self._req_counter += 1
        if self._req_id_floor is not None and self._req_counter == self._req_id_floor:
            self._req_counter += 1
        return self._req_counter

    async def send(self, payload: dict[str, Any]) -> None:
        if not self._ws:
            raise DerivConnectionError("No conectado.")
        await self._ws.send(json.dumps(payload))

    async def request(self, payload: dict[str, Any]) -> dict[str, Any]:
        req_id = self._next_req_id()
        fut = asyncio.get_running_loop().create_future()
        self._pending[req_id] = fut
        
        try:
            await self.send({**payload, "req_id": req_id})
            resp = await asyncio.wait_for(fut, self.request_timeout)
        except asyncio.TimeoutError as exc:
            raise DerivConnectionError(f"Timeout en petición: {payload}") from exc
        finally:
            self._pending.pop(req_id, None)

        if "error" in resp:
            err = resp["error"]
            raise DerivAPIError(err.get("code"), err.get("message", ""))
        return resp
    
    @property
    def is_connected(self) -> bool:
        """True si la conexión WebSocket está activa y no ha sido cerrada."""
        return self._ws is not None and self._ws.state == State.OPEN
    
    async def _recv_loop(self) -> None:
        while not self._closing:
            try:
                assert self._ws is not None
                raw = await self._ws.recv()
                msg = json.loads(raw)
            except websockets.ConnectionClosed:
                if not self._closing:
                    logger.warning("Conexión cerrada por el servidor.")
                    await self._handle_disconnect()
                return
            except json.JSONDecodeError:
                continue

            req_id = msg.get("req_id")
            if req_id is not None:
                if self._req_id_floor is None:
                    self._req_id_floor = int(req_id)
                fut = self._pending.get(req_id)
                if fut and not fut.done():
                    fut.set_result(msg)
            
            # Hook para que la clase hija procese streams (colas o callbacks)
            await self._process_stream_message(msg)

    async def _ping_loop(self) -> None:
        """
        Envía pings a nivel de aplicación (JSON) para evitar que el servidor 
        cierre la conexión por inactividad (típicamente a los 120s).
        """
        logger.info(
            "✅ [%s] Bucle de ping iniciado. Intervalo: %s segundos.", 
            self.__class__.__name__, 
            self.ping_interval
        )
        
        while not self._closing:
            try:
                await asyncio.sleep(self.ping_interval)
                
                if self._closing:
                    break
                    
                if self._ws is None:
                    logger.warning(
                        "⚠️ [%s] WebSocket es None. Saliendo del bucle de ping.",
                        self.__class__.__name__
                    )
                    break

                # Payload oficial de keep-alive de Deriv
                payload = {"ping": 1}
                logger.debug(
                    "📤 [%s] Enviando ping de aplicación: %s", 
                    self.__class__.__name__, payload
                )
                await self._ws.send(json.dumps(payload))
                
            except asyncio.CancelledError:
                logger.info(
                    "🛑 [%s] Bucle de ping cancelado limpiamente.", 
                    self.__class__.__name__
                )
                break
            except websockets.ConnectionClosed:
                logger.warning(
                    "⚠️ [%s] Conexión cerrada durante el envío de ping. Saliendo del bucle.", 
                    self.__class__.__name__
                )
                break
            except Exception as exc:
                logger.error(
                    "❌ [%s] Error inesperado en bucle de ping: %s", 
                    self.__class__.__name__, exc
                )
                break

    async def _reconnect(self) -> bool:
        """Reintenta conectar con backoff exponencial."""
        delay = 1.0
        for attempt in range(1, self.max_reconnect_attempts + 1):
            await asyncio.sleep(delay)
            delay = min(delay * 2, 30.0)
            logger.info("Intento de reconexión %d/%d…", attempt, self.max_reconnect_attempts)
            
            try:
                if self._recv_task:
                    self._recv_task.cancel()
                    with contextlib.suppress(asyncio.CancelledError):
                        await self._recv_task
                self._recv_task = None

                if self._ping_task:
                    self._ping_task.cancel()
                    with contextlib.suppress(asyncio.CancelledError):
                        await self._ping_task
                self._ping_task = None

                # Limpieza quirúrgica del socket huérfano
                with contextlib.suppress(Exception):
                    if self._ws is not None:
                        await self._ws.close()
                        self._ws = None

                for fut in self._pending.values():
                    if not fut.done():
                        fut.set_exception(DerivConnectionError("Reconectando..."))
                self._pending.clear()
                
                # Reutilizar el método oficial de conexión
                await self.connect()
                logger.info("WebSocket reconectado exitosamente")
                return True
                
            except (DerivConnectionError, Exception) as exc:
                logger.warning("Fallo en intento de reconexión: %s", exc)
        
        logger.error("No se pudo reconectar tras %d intentos.", self.max_reconnect_attempts)
        return False

    async def _resubscribe_all(self) -> None:
        """Resuscribe todos los streams activos tras una reconexión."""
        for token, payload in list(self._active_subscriptions.items()):
            try:
                msg_type = next(iter(payload))
                logger.debug("Resuscribiendo %s...", msg_type)
                await self.request({**payload, "subscribe": 1})
            except Exception as exc:
                logger.error("Error al resuscribir %s: %s", payload, exc)
                # Si falla la resuscripción, notificar a la cola
                if token in self._streams:
                    _, q = self._streams[token]
                    q.put_nowait(None)

    async def _handle_disconnect(self) -> None:
        # Limpieza de colas al caer la conexión
        if self._closing:
            # Desconexión intencional: cerrar todas las colas
            for _, (_, q) in self._streams.items():
                q.put_nowait(None)
            return

        # Desconexión inesperada: intentar reconexión
        logger.warning("Conexión de market data caída; intentando reconexión...")
        if await self._reconnect():
            logger.info("Reconexión exitosa. Resuscribiendo streams...")
            await self._resubscribe_all()
        else:
            logger.error("Reconexión fallida. Cerrando streams.")
            for _, (_, q) in self._streams.items():
                q.put_nowait(None)
            self._closing = True
            await self.disconnect()


    async def _process_stream_message(self, msg: dict[str, Any]) -> None:
        """Enrutador genérico: envía el mensaje a las colas registradas."""
        msg_type = msg.get("msg_type")
        if msg_type:
            for _, (types, q) in list(self._streams.items()):
                if msg_type in types:
                    q.put_nowait(msg)

    async def _stream(self, payload: dict[str, Any]) -> AsyncIterator[dict[str, Any]]:
        """Generador genérico de suscripciones vía cola async.
        
        Este es el método que faltaba. Maneja el ciclo de vida completo de una suscripción.
        """
        queue: asyncio.Queue[dict[str, Any] | None] = asyncio.Queue()
        msg_type = next(iter(payload))
        
        response_type = C.REQUEST_TO_RESPONSE_TYPE.get(msg_type, msg_type)
        loop_msg_types = {msg_type, response_type}

        token = id(queue)
        
        # 1. Registrar la cola
        self._streams[token] = (loop_msg_types, queue)

        # Guardar payload para resuscripción
        self._active_subscriptions[token] = payload
        
        try:
            # 2. Enviar suscripción y obtener respuesta inicial
            initial = await self.request({**payload, "subscribe": 1})
        except BaseException:
            # 3. Limpieza en caso de fallo inmediato (evita memory leaks)
            self._streams.pop(token, None)
            self._active_subscriptions.pop(token, None)
            raise
            
        yield initial
        
        try:
            # 4. Bucle de consumo de la cola
            while True:
                item = await queue.get()
                if item is None:  # Señal de desconexión forzada
                    break
                yield item
        finally:
            # 5. Limpieza garantizada al salir del bucle (ej. break del usuario)
            self._streams.pop(token, None)
            self._active_subscriptions.pop(token, None)

    # Hooks abstractos para personalización sin romper la base
    async def _on_connected(self) -> None: pass
    async def _on_disconnected(self) -> None: pass