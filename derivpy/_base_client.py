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
from .reconnection import (
    ReconnectionConfig, 
    ReconnectionEvent,
    ReconnectionMode,
    EventCallback
)

logger = logging.getLogger("derivpy.base")


class BaseDerivWSClient(ABC):
    """Clase base para gestionar la fontanería de WebSocket de Deriv."""

    def __init__(
        self,
        url: str,
        *,
        request_timeout: float = 10.0,
        ping_interval: float = 30.0,
        reconnection_config: ReconnectionConfig | None = None,
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
        self._active_subscriptions: dict[int, dict[str, Any]] = {}
        self._closing = False
        
        # 🆕 Sistema de reconexión robusto
        self._reconnection_config = reconnection_config or ReconnectionConfig()
        self._reconnect_lock = asyncio.Lock()
        self._connection_event = asyncio.Event()
        self._consecutive_failures = 0
        self._event_callback: EventCallback | None = None
        self._event_callback_timeout: float = (
            self._reconnection_config.event_callback_timeout
        )

    @property 
    def is_connection_stable(self) -> bool: 
        return self._connection_event.is_set()

    def set_event_callback(
        self, 
        callback: EventCallback,
        timeout: float | None = None
    ) -> None:
        """Registra un callback robusto con timeout y manejo de errores.
        
        Args:
            callback: Función async que recibe (client_name, event, failure_count)
                     Retorna True para continuar reintentando, False para parar.
            timeout: Segundos máximos de espera. Si None, usa el de la config.
        """
        self._event_callback = callback
        if timeout is not None:
            self._event_callback_timeout = timeout

    async def _invoke_event_callback(
        self, 
        event: ReconnectionEvent,
        failure_count: int
    ) -> bool:
        """Invoca el callback de forma segura con timeout y manejo de errores.
        
        Returns:
            True si se debe continuar reintentando, False para parar.
            En caso de error o timeout, retorna el fallback seguro.
        """
        if not self._event_callback:
            # Sin callback: comportamiento por defecto según el evento
            return event != ReconnectionEvent.FATAL_FAILURE
        
        client_name = self.__class__.__name__
        
        try:
            # Protección con timeout para evitar bloqueos indefinidos
            result = await asyncio.wait_for(
                asyncio.shield(self._event_callback(client_name, event, failure_count)),
                timeout=self._event_callback_timeout
            )
            return bool(result)
            
        except asyncio.TimeoutError:
            logger.error(
                "[%s] ⏱️ Callback timeout tras %.1fs para evento %s. "
                "Asumiendo comportamiento por defecto.",
                client_name, self._event_callback_timeout, event.name
            )
            return event != ReconnectionEvent.FATAL_FAILURE
            
        except Exception as exc:
            logger.error(
                "[%s] ❌ Excepción en callback para evento %s: %s. "
                "Continuando con comportamiento por defecto.",
                client_name, event.name, exc, exc_info=True
            )
            return event != ReconnectionEvent.FATAL_FAILURE

    async def connect(self) -> None:
        self._closing = False
        self._connection_event.clear()      # 🆕 Marcar como inestable durante la conexión
        try:
            self._ws = await websockets.connect(
                self._url, 
                open_timeout=self.request_timeout, 
                close_timeout=5,
                ping_interval=None,     # ⚠️ CRÍTICO: Desactivar ping nativo de websockets
                ping_timeout=None       # ⚠️ CRÍTICO: Delegar todo a nuestro _ping_loop
            )
        except (OSError, asyncio.TimeoutError, websockets.WebSocketException) as exc:
            raise DerivConnectionError(f"Fallo de conexión a {self._url}: {exc}") from exc
        
        self._recv_task = asyncio.create_task(self._recv_loop(), name="deriv-recv")

        if self.ping_interval > 0:
            self._ping_task = asyncio.create_task(self._ping_loop(), name="deriv-ping")

        await self._on_connected()      # Hook para clases hijas
        self._connection_event.set()    # 🆕 Marcar como estable

    async def disconnect(self) -> None:
        self._connection_event.clear()
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
                if not self._closing:
                    # Disparar la reconexión de forma segura si el _recv_loop no lo hizo
                    await self._handle_disconnect()
                break
            except Exception as exc:
                logger.error(
                    "❌ [%s] Error inesperado en bucle de ping: %s", 
                    self.__class__.__name__, exc
                )
                break

    async def _reconnect(self) -> bool:
        """Reintenta conectar con limpieza completa de estado."""
        try:
            # Cancelar tareas existentes
            for task in (self._recv_task, self._ping_task):
                if task and not task.done():
                    task.cancel()
                    with contextlib.suppress(asyncio.CancelledError):
                        await task
            
            self._recv_task = None
            self._ping_task = None

            # Cerrar socket huérfano
            with contextlib.suppress(Exception):
                if self._ws is not None:
                    await self._ws.close()
                    self._ws = None

            # Limpiar pendientes
            for fut in self._pending.values():
                if not fut.done():
                    fut.set_exception(DerivConnectionError("Reconectando..."))
            self._pending.clear()
            
            # Conectar de nuevo (usa el método sobrescrito de la clase hija)
            await self.connect()
            return True
            
        except Exception as exc:
            logger.warning("Fallo en intento de reconexión: %s", exc)
            return False

    async def _resubscribe_all(self) -> None:
        """Resuscribe todos los streams activos tras una reconexión.
        
        Implementación por defecto para MarketDataClient.
        TradingClient sobrescribe este método con su propia lógica.
        """
        for token, payload in list(self._active_subscriptions.items()):
            try:
                logger.debug("[%s] Resuscribiendo stream %s...", self.__class__.__name__, token)
                await self.request({**payload, "subscribe": 1})
            except Exception as exc:
                logger.error("[%s] Error al resuscribir %s: %s", self.__class__.__name__, token, exc)

    async def _should_continue_retrying(self) -> bool:
        """Evalúa si debe seguir reintentando según la estrategia configurada."""
        cfg = self._reconnection_config
        
        if cfg.mode == ReconnectionMode.STOP_ON_FATAL:
            return False
        
        if cfg.mode == ReconnectionMode.LIMITED_RETRY:
            return self._consecutive_failures < cfg.max_attempts
        
        if cfg.mode == ReconnectionMode.INFINITE_RETRY:
            return await self._invoke_event_callback(
                ReconnectionEvent.FATAL_FAILURE,
                self._consecutive_failures
            )
        
        return False
        
    async def _handle_disconnect(self) -> None:
        if self._closing:
            for _, (_, q) in self._streams.items():
                q.put_nowait(None)
            return

        async with self._reconnect_lock:
            if self.is_connected:
                return

            self._connection_event.clear()
            logger.warning(
                "[%s] Conexión caída. Modo: %s | Fallos consecutivos: %d", 
                self.__class__.__name__, 
                self._reconnection_config.mode.name,
                self._consecutive_failures
            )
            
            while not self._closing:
                self._consecutive_failures += 1
                delay = self._reconnection_config.calculate_backoff_delay(
                    self._consecutive_failures
                )
                
                logger.info(
                    "[%s] Intento de reconexión (fallo #%d) en %.1fs...", 
                    self.__class__.__name__, 
                    self._consecutive_failures, 
                    delay
                )
                await asyncio.sleep(delay)
                
                if await self._reconnect():
                    needs_history_reload = (
                        self._consecutive_failures >= 
                        self._reconnection_config.force_history_reload_after
                    )
                    self._consecutive_failures = 0
                    self._connection_event.set()
                    
                    # Determinar evento a emitir
                    event = (
                        ReconnectionEvent.REQUIRES_HISTORY_RELOAD 
                        if needs_history_reload 
                        else ReconnectionEvent.HEALTH_CHECK_PASSED
                    )
                    
                    logger.info(
                        "[%s] ✅ Reconexión exitosa. Evento: %s",
                        self.__class__.__name__, event.name
                    )
                    await self._resubscribe_all()
                    
                    # Invocar callback de forma segura
                    await self._invoke_event_callback(event, 0)
                    return
                
                # Evaluar si continuar reintentando
                should_continue = await self._should_continue_retrying()
                if not should_continue:
                    logger.critical(
                        "[%s] 💀 Deteniendo tras %d intentos.", 
                        self.__class__.__name__, 
                        self._consecutive_failures
                    )
                    
                    # Notificar fallo fatal antes de cerrar
                    await self._invoke_event_callback(
                        ReconnectionEvent.FATAL_FAILURE, 
                        self._consecutive_failures
                    )
                    
                    for _, (_, q) in self._streams.items():
                        q.put_nowait(None)
                    self._closing = True
                    await self.disconnect()
                    return

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