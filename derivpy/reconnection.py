"""
Módulo de Reconexión y Resiliencia
==================================

Este módulo centraliza toda la lógica de manejo de fallos de conexión
para los clientes WebSocket de Deriv. Proporciona:

1. ReconnectionMode: Estrategias de comportamiento ante fallos
2. ReconnectionEvent: Eventos de ciclo de vida notificados al motor
3. ReconnectionConfig: Configuración granular del sistema
"""

from enum import Enum, auto
from typing import Callable, Awaitable
import random

from pydantic import BaseModel, Field


class ReconnectionMode(Enum):
    """Estrategia de comportamiento ante fallos fatales de conexión."""
    STOP_ON_FATAL = auto()       # Detener motor (comportamiento seguro)
    INFINITE_RETRY = auto()      # Reintentar indefinidamente (24/7)
    LIMITED_RETRY = auto()       # N intentos luego parar (híbrido)


class ReconnectionEvent(Enum):
    """Eventos de ciclo de vida que el cliente notifica al motor.
    
    Contrato de Comunicación
    ========================
    
    El CLIENTE emite eventos. El MOTOR los maneja de forma rápida
    y no bloqueante (timeout recomendado: 5 segundos).
    
    RESPONSABILIDADES:
    - El cliente NUNCA debe esperar indefinidamente por el callback
    - El motor NUNCA debe lanzar excepciones hacia el cliente
    - En caso de timeout, se asume comportamiento por defecto
    """
    
    # Eventos de fallo
    FATAL_FAILURE = "fatal_failure"
    """Fallo definitivo tras agotar intentos. El motor decide si parar."""
    
    # Eventos de recuperación
    REQUIRES_HISTORY_RELOAD = "requires_history_reload"
    """Reconexión exitosa tras N fallos. Buffer potencialmente inconsistente."""
    
    HEALTH_CHECK_PASSED = "health_check_passed"
    """Reconexión exitosa simple. Streams restaurados normalmente."""
    
    EMERGENCY_CLEANUP = "emergency_cleanup"
    """El cliente solicita limpieza profunda de estado local."""
    
    CONNECTION_DEGRADED = "connection_degraded"
    """Conexión inestable pero operativa. Monitorear de cerca."""
    
    RATE_LIMITED = "rate_limited"
    """El broker está aplicando throttling. Aumentar backoff."""


class ReconnectionConfig(BaseModel):
    """Configuración granular para el sistema de reconexión.
    
    Attributes:
        mode: Estrategia de comportamiento ante fallos
        max_attempts: Máximo de intentos (solo para LIMITED_RETRY)
        base_delay: Segundos iniciales entre intentos
        max_delay: Techo del backoff exponencial
        backoff_multiplier: Factor de crecimiento exponencial
        jitter: Añadir aleatoriedad para evitar thundering herd
        force_history_reload_after: Recargar histórico tras N fallos consecutivos
        zombie_timeout: Segundos sin ticks = reinicio limpio
        health_check_interval: Frecuencia del watchdog
        event_callback_timeout: Segundos máximos para el callback del motor
    """
    mode: ReconnectionMode = Field(default=ReconnectionMode.STOP_ON_FATAL, description="Estrategia de comportamiento ante fallos")
    max_attempts: int = Field(default=5, description="Máximo de intentos (solo para LIMITED_RETRY)")
    base_delay: float = Field(default=1.0, description="Segundos iniciales entre intentos")
    max_delay: float = Field(default=300.0, description="Techo del backoff exponencial")
    backoff_multiplier: float = Field(default=2.0, description="Factor de crecimiento exponencial")
    jitter: bool = Field(default=True, description="Añadir aleatoriedad para evitar thundering herd")
    force_history_reload_after: int = Field(default=3, description="Recargar histórico tras N fallos consecutivos")
    zombie_timeout: float = Field(default=300.0, description="Segundos sin ticks = reinicio limpio")
    health_check_interval: float = Field(default=30.0, description="Frecuencia del watchdog")
    event_callback_timeout: float = Field(default=5.0, description="Segundos máximos para el callback del motor")
    
    def calculate_backoff_delay(self, attempt_number: int) -> float:
        """Calcula el delay para un intento específico con backoff exponencial."""
        delay = min(
            self.base_delay * (self.backoff_multiplier ** (attempt_number - 1)),
            self.max_delay
        )
        if self.jitter:
            # Jitter uniforme: +/- 25% para evitar sincronización
            delay *= random.uniform(0.75, 1.25)
        return delay


# Type alias para el callback de eventos
EventCallback = Callable[[str, ReconnectionEvent, int], Awaitable[bool]]
"""Firma del callback: (client_name, event, failure_count) -> should_continue"""