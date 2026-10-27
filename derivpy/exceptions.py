"""Excepciones de la librería ``derivpy``.

Jerarquía::

    DerivError                 # base de todo
    ├── DerivAuthError         # problemas de token/OTP (401/403, InvalidToken…)
    ├── DerivRateLimitError    # demasiadas peticiones (429 / RateLimit)
    ├── DerivAPIError          # cualquier otro error devuelto por la API
    └── DerivConnectionError   # fallo de red / WebSocket caído
"""

from __future__ import annotations


class DerivError(Exception):
    """Error base de la librería."""


class DerivAPIError(DerivError):
    """La API devolvió un error (objeto ``error`` en WebSocket o ``errors`` en REST).

    Attributes:
        code: código de error devuelto por Deriv, p. ej. ``InputValidationFailed``,
            ``InsufficientBalance``, ``ContractNotFound``…
        message: descripción legible del error.
        status: estado HTTP (solo en REST) o ``None`` en WebSocket.
    """

    def __init__(self, code: str | None, message: str, status: int | None = None) -> None:
        super().__init__(f"[{code}] {message}" if code else message)
        self.code = code
        self.message = message
        self.status = status


class DerivAuthError(DerivAPIError):
    """Token inválido/expirado, OTP caducado o escopo insuficiente."""


class DerivRateLimitError(DerivAPIError):
    """Se superó alguna tasa de límite de la API (código ``RateLimit`` / HTTP 429)."""


class DerivConnectionError(DerivError):
    """No se pudo establecer o mantener la conexión con el servidor."""


class DerivTimeoutError(DerivError):
    """La API no respondió dentro del tiempo máximo esperado."""
