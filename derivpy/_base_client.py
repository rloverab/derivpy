# _base_client.py
from __future__ import annotations

import asyncio
import contextlib
import json
import logging
from abc import ABC
from collections.abc import AsyncIterator
from typing import Any

import websockets
from websockets import State

from . import _constants as C
from .exceptions import DerivAPIError, DerivConnectionError
from .reconnection import (
    EventCallback,
    ReconnectionConfig,
    ReconnectionEvent,
    ReconnectionMode,
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
        self._reconnect_task: asyncio.Task[None] | None = None
        self._req_counter = 0
        self._req_id_floor: int | None = None
        self._pending: dict[int, asyncio.Future[dict[str, Any]]] = {}
        self._streams: dict[int, tuple[set[str], asyncio.Queue]] = {}
        self._active_subscriptions: dict[int, dict[str, Any]] = {}
        self._closing = False

        # Sistema de reconexión robusto
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
        return self._connection_event.is_set() and self.is_connected

    @property
    def is_connected(self) -> bool:
        """True si la conexión WebSocket está activa y no ha sido cerrada."""
        return self._ws is not None and self._ws.state == State.OPEN

    @property
    def is_reconnecting(self) -> bool:
        """True si hay un ciclo de reconexión en progreso."""
        return self._reconnect_lock.locked() or (
            self._reconnect_task is not None and not self._reconnect_task.done()
        )

    def set_event_callback(
        self,
        callback: EventCallback,
        timeout: float | None = None,
    ) -> None:
        """Registra un callback robusto con timeout y manejo de errores."""
        self._event_callback = callback
        if timeout is not None:
            self._event_callback_timeout = timeout

    async def _invoke_event_callback(
        self,
        event: ReconnectionEvent,
        failure_count: int,
    ) -> bool:
        """Invoca el callback de forma segura con timeout y manejo de errores."""
        if not self._event_callback:
            return event != ReconnectionEvent.FATAL_FAILURE

        client_name = self.__class__.__name__

        try:
            result = await asyncio.wait_for(
                asyncio.shield(self._event_callback(client_name, event, failure_count)),
                timeout=self._event_callback_timeout,
            )
            return bool(result)

        except asyncio.TimeoutError:
            logger.error(
                "[%s] ⏱️ Callback timeout tras %.1fs para evento %s. "
                "Asumiendo comportamiento por defecto.",
                client_name,
                self._event_callback_timeout,
                event.name,
            )
            return event != ReconnectionEvent.FATAL_FAILURE

        except Exception as exc:
            logger.error(
                "[%s] ❌ Excepción en callback para evento %s: %s. "
                "Continuando con comportamiento por defecto.",
                client_name,
                event.name,
                exc,
                exc_info=True,
            )
            return event != ReconnectionEvent.FATAL_FAILURE

    async def _cancel_background_tasks(self) -> None:
        """Cancela limpiamente _ping_task y _recv_task evitando auto-cancelación."""
        current = asyncio.current_task()
        for attr in ("_ping_task", "_recv_task"):
            task: asyncio.Task[None] | None = getattr(self, attr)
            setattr(self, attr, None)
            if task and task is not current and not task.done():
                task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await task

    async def _close_socket(self) -> None:
        """Cierra el WebSocket actual de forma segura."""
        ws = self._ws
        self._ws = None
        if ws is not None:
            with contextlib.suppress(Exception):
                await ws.close()

    def _fail_pending_requests(self, reason: str) -> None:
        """Falla todas las peticiones pendientes en vuelo."""
        for fut in self._pending.values():
            if not fut.done():
                fut.set_exception(DerivConnectionError(reason))
        self._pending.clear()

    async def _open_socket(self) -> None:
        """Abre el socket WebSocket e inicia los loops internos sin marcar conexión estable aún."""
        self._connection_event.clear()
        await self._cancel_background_tasks()
        await self._close_socket()

        try:
            self._ws = await websockets.connect(
                self._url,
                open_timeout=self.request_timeout,
                close_timeout=5,
                ping_interval=None,  # Desactivar ping nativo de websockets
                ping_timeout=None,   # Delegar keep-alive a _ping_loop
            )
        except (OSError, asyncio.TimeoutError, websockets.WebSocketException) as exc:
            raise DerivConnectionError(f"Fallo de conexión a {self._url}: {exc}") from exc

        client_name = self.__class__.__name__
        self._recv_task = asyncio.create_task(
            self._recv_loop(), name=f"{client_name}-recv"
        )

        if self.ping_interval > 0:
            self._ping_task = asyncio.create_task(
                self._ping_loop(), name=f"{client_name}-ping"
            )

        await self._on_connected()

    async def connect(self) -> None:
        """Conecta al WebSocket y marca la conexión como estable."""
        self._closing = False
        await self._open_socket()
        self._connection_event.set()

    async def disconnect(self) -> None:
        """Cierra la conexión de forma ordenada y detiene cualquier reconexión en curso."""
        self._connection_event.clear()
        self._closing = True

        current = asyncio.current_task()
        if (
            self._reconnect_task
            and self._reconnect_task is not current
            and not self._reconnect_task.done()
        ):
            self._reconnect_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._reconnect_task
        self._reconnect_task = None

        await self._cancel_background_tasks()
        await self._close_socket()
        self._fail_pending_requests("Conexión cerrada")

        for _, (_, q) in list(self._streams.items()):
            q.put_nowait(None)

        await self._on_disconnected()

    async def wait_until_stable(self, timeout: float | None = None) -> bool:
        """Espera a que la conexión vuelva a estar estable."""
        if self.is_connection_stable:
            return True
        try:
            if timeout is None:
                await self._connection_event.wait()
            else:
                await asyncio.wait_for(self._connection_event.wait(), timeout=timeout)
            return self.is_connection_stable
        except asyncio.TimeoutError:
            return False

    async def force_reconnect(self, reason: str = "Forzada por monitor de salud") -> None:
        """Fuerza la caída controlada del socket actual y dispara el gestor de reconexión."""
        if self._closing:
            return
        logger.warning("[%s] 🔄 Reconexión forzada: %s", self.__class__.__name__, reason)
        self._connection_event.clear()
        await self._close_socket()
        self._trigger_disconnect_handler()

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
        if not self.is_connected or self._ws is None:
            raise DerivConnectionError("No conectado.")
        try:
            await self._ws.send(json.dumps(payload))
        except websockets.WebSocketException as exc:
            self._connection_event.clear()
            self._trigger_disconnect_handler()
            raise DerivConnectionError(f"Error enviando mensaje: {exc}") from exc

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

    def _trigger_disconnect_handler(self) -> None:
        """Programa _handle_disconnect como tarea independiente para evitar auto-cancelación."""
        if self._closing:
            return
        if self._reconnect_task is None or self._reconnect_task.done():
            self._reconnect_task = asyncio.create_task(
                self._handle_disconnect(),
                name=f"{self.__class__.__name__}-reconnect",
            )

    async def _recv_loop(self) -> None:
        try:
            while not self._closing:
                ws = self._ws
                if ws is None:
                    break
                try:
                    raw = await ws.recv()
                    msg = json.loads(raw)
                except asyncio.CancelledError:
                    raise
                except websockets.ConnectionClosed:
                    if not self._closing:
                        logger.warning(
                            "[%s] Conexión cerrada por el servidor.",
                            self.__class__.__name__,
                        )
                        self._connection_event.clear()
                        self._trigger_disconnect_handler()
                    return
                except json.JSONDecodeError:
                    continue
                except Exception as exc:
                    if not self._closing:
                        logger.error(
                            "[%s] Error inesperado en _recv_loop: %s",
                            self.__class__.__name__,
                            exc,
                        )
                        self._connection_event.clear()
                        self._trigger_disconnect_handler()
                    return

                req_id = msg.get("req_id")
                if req_id is not None:
                    if self._req_id_floor is None:
                        self._req_id_floor = int(req_id)
                    fut = self._pending.get(req_id)
                    if fut and not fut.done():
                        fut.set_result(msg)

                await self._process_stream_message(msg)
        except asyncio.CancelledError:
            pass

    async def _ping_loop(self) -> None:
        """Envía pings de aplicación para mantener viva la sesión de Deriv."""
        client_name = self.__class__.__name__
        logger.info(
            "✅ [%s] Bucle de ping iniciado. Intervalo: %s segundos.",
            client_name,
            self.ping_interval,
        )

        while not self._closing:
            try:
                await asyncio.sleep(self.ping_interval)

                if self._closing:
                    break

                ws = self._ws
                if ws is None or ws.state != State.OPEN:
                    logger.warning(
                        "⚠️ [%s] WebSocket no abierto en ping. Disparando reconexión.",
                        client_name,
                    )
                    self._connection_event.clear()
                    self._trigger_disconnect_handler()
                    break

                payload = {"ping": 1}
                logger.debug("📤 [%s] Enviando ping de aplicación: %s", client_name, payload)
                await asyncio.wait_for(ws.send(json.dumps(payload)), timeout=self.request_timeout)

            except asyncio.CancelledError:
                logger.info("🛑 [%s] Bucle de ping cancelado limpiamente.", client_name)
                break
            except (websockets.ConnectionClosed, asyncio.TimeoutError, OSError) as exc:
                logger.warning(
                    "⚠️ [%s] Fallo enviando ping (%s). Disparando reconexión.",
                    client_name,
                    exc,
                )
                if not self._closing:
                    self._connection_event.clear()
                    self._trigger_disconnect_handler()
                break
            except Exception as exc:
                logger.error(
                    "❌ [%s] Error inesperado en bucle de ping: %s",
                    client_name,
                    exc,
                )
                if not self._closing:
                    self._connection_event.clear()
                    self._trigger_disconnect_handler()
                break

    async def _reconnect(self) -> bool:
        """Reintenta conectar con limpieza completa de estado."""
        try:
            self._fail_pending_requests("Reconectando...")
            await self._open_socket()
            await self._resubscribe_all()
            self._connection_event.set()
            return True
        except Exception as exc:
            logger.warning(
                "[%s] Fallo en intento de reconexión: %s",
                self.__class__.__name__,
                exc,
            )
            self._connection_event.clear()
            await self._cancel_background_tasks()
            await self._close_socket()
            return False

    async def _resubscribe_all(self) -> None:
        """Resuscribe todos los streams activos tras una reconexión."""
        for token, payload in list(self._active_subscriptions.items()):
            try:
                logger.debug(
                    "[%s] Resuscribiendo stream %s...",
                    self.__class__.__name__,
                    token,
                )
                await self.request({**payload, "subscribe": 1})
            except Exception as exc:
                logger.error(
                    "[%s] Error al resuscribir %s: %s",
                    self.__class__.__name__,
                    token,
                    exc,
                )
                raise

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
                self._consecutive_failures,
            )

        return False

    async def _handle_disconnect(self) -> None:
        if self._closing:
            for _, (_, q) in list(self._streams.items()):
                q.put_nowait(None)
            return

        async with self._reconnect_lock:
            if self.is_connection_stable:
                return

            self._connection_event.clear()
            logger.warning(
                "[%s] Conexión caída. Modo: %s | Fallos consecutivos: %d",
                self.__class__.__name__,
                self._reconnection_config.mode.name,
                self._consecutive_failures,
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
                    delay,
                )
                await asyncio.sleep(delay)

                if self._closing:
                    return

                if await self._reconnect():
                    needs_history_reload = (
                        self._consecutive_failures
                        >= self._reconnection_config.force_history_reload_after
                    )
                    self._consecutive_failures = 0

                    event = (
                        ReconnectionEvent.REQUIRES_HISTORY_RELOAD
                        if needs_history_reload
                        else ReconnectionEvent.HEALTH_CHECK_PASSED
                    )

                    logger.info(
                        "[%s] ✅ Reconexión exitosa. Evento: %s",
                        self.__class__.__name__,
                        event.name,
                    )
                    await self._invoke_event_callback(event, 0)
                    return

                should_continue = await self._should_continue_retrying()
                if not should_continue:
                    logger.critical(
                        "[%s] 💀 Deteniendo tras %d intentos.",
                        self.__class__.__name__,
                        self._consecutive_failures,
                    )
                    # En INFINITE_RETRY el callback ya fue notificado dentro de _should_continue_retrying()
                    if self._reconnection_config.mode != ReconnectionMode.INFINITE_RETRY:
                        await self._invoke_event_callback(
                            ReconnectionEvent.FATAL_FAILURE,
                            self._consecutive_failures,
                        )
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
        """Generador genérico de suscripciones vía cola async."""
        queue: asyncio.Queue[dict[str, Any] | None] = asyncio.Queue()
        msg_type = next(iter(payload))

        response_type = C.REQUEST_TO_RESPONSE_TYPE.get(msg_type, msg_type)
        loop_msg_types = {msg_type, response_type}

        token = id(queue)
        self._streams[token] = (loop_msg_types, queue)
        self._active_subscriptions[token] = dict(payload)

        try:
            initial = await self.request({**payload, "subscribe": 1})
        except BaseException:
            self._streams.pop(token, None)
            self._active_subscriptions.pop(token, None)
            raise

        yield initial

        try:
            while True:
                item = await queue.get()
                if item is None:
                    break
                yield item
        finally:
            self._streams.pop(token, None)
            self._active_subscriptions.pop(token, None)

    async def _on_connected(self) -> None:
        pass

    async def _on_disconnected(self) -> None:
        pass
