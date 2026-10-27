"""Cliente REST de Deriv (``https://api.derivws.com``).

Implementado con :mod:`urllib` de la stdlib y ejecutado en un hilo aparte
vía :func:`asyncio.to_thread`, de modo que no bloquea el event loop ni añade
dependencias.

Autenticación (según https://developers.deriv.com/llms/authentication.md):

* ``Authorization: Bearer <token>`` — token OAuth2 **o** PAT.
* ``Deriv-App-ID: <app_id>`` — obligatorio cuando se usa un PAT.
"""

from __future__ import annotations

import asyncio
import json
import urllib.error
import urllib.parse
import urllib.request
from typing import Any

from . import _constants as C
from .exceptions import DerivAPIError, DerivAuthError, DerivConnectionError, DerivRateLimitError

#: Códigos de error de WebSocket/REST que implican problemas de autenticación.
_AUTH_CODES = {"InvalidToken", "Unauthorized", "AccessDenied", "AuthorizationRequired"}


def _raise_for_api_error(code: str | None, message: str, status: int | None) -> None:
    """Traduce un error de la API a la excepción correspondiente."""
    if code in _AUTH_CODES or status in (401, 403):
        raise DerivAuthError(code, message, status)
    if code == "RateLimit" or status == 429:
        raise DerivRateLimitError(code, message, status)
    raise DerivAPIError(code, message, status)


class RestClient:
    """Cliente HTTP síncrono interno. Usa :meth:`request` (thread-safe).

    Args:
        token: access token OAuth2 o Personal Access Token (PAT).
        app_id: ID de aplicación; requerido si ``token`` es un PAT.
        base_url: permite apuntar a otro entorno si hiciera falta.
    """

    def __init__(
        self,
        token: str | None = None,
        app_id: str | None = None,
        base_url: str = C.REST_BASE_URL,
        timeout: float = 30.0,
    ) -> None:
        self.token = token
        self.app_id = app_id
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout

    # ------------------------------------------------------------------ #
    # Núcleo
    # ------------------------------------------------------------------ #
    def request(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, Any] | None = None,
        body: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Ejecuta una petición REST y devuelve el JSON decodificado.

        Raises:
            DerivAuthError: token inválido/expirado o sin escopo (401/403).
            DerivRateLimitError: límite de peticiones superado (429).
            DerivAPIError: cualquier otro error estructurado de la API.
            DerivConnectionError: fallo de red.
        """
        url = f"{self.base_url}{path}"
        if params:
            url += "?" + urllib.parse.urlencode(params)

        data = json.dumps(body).encode() if body is not None else None
        headers = {"Accept": "application/json"}
        if data is not None:
            headers["Content-Type"] = "application/json"
        if self.token:
            headers["Authorization"] = f"Bearer {self.token}"
        if self.app_id:
            headers["Deriv-App-ID"] = self.app_id

        req = urllib.request.Request(url, data=data, headers=headers, method=method.upper())
        try:
            with urllib.request.urlopen(req, timeout=self.timeout) as resp:
                payload = json.loads(resp.read().decode() or "{}")
        except urllib.error.HTTPError as exc:
            self._handle_http_error(exc)
        except (urllib.error.URLError, TimeoutError, OSError) as exc:
            raise DerivConnectionError(f"Fallo de red contra {url}: {exc}") from exc

        # Algunas respuestas incluyen un array "errors" incluso con 200.
        errors = payload.get("errors")
        if errors:
            first = errors[0]
            _raise_for_api_error(first.get("code"), first.get("message", ""), first.get("status"))
        return payload

    @staticmethod
    def _handle_http_error(exc: urllib.error.HTTPError) -> None:
        """Convierte un HTTPError en la excepción derivpy adecuada."""
        try:
            payload = json.loads(exc.read().decode() or "{}")
            first = (payload.get("errors") or [{}])[0]
            _raise_for_api_error(first.get("code"), first.get("message", exc.reason), exc.code)
        except DerivAPIError:
            raise
        except Exception:
            _raise_for_api_error(None, str(exc.reason), exc.code)

    # ------------------------------------------------------------------ #
    # Endpoints concretos
    # ------------------------------------------------------------------ #
    async def arequest(self, method: str, path: str, **kwargs: Any) -> dict[str, Any]:
        """Versión async de :meth:`request` (corre en un hilo)."""
        return await asyncio.to_thread(self.request, method, path, **kwargs)

    async def list_accounts(self) -> list[dict[str, Any]]:
        """``GET /trading/v1/options/accounts`` — cuentas del usuario."""
        payload = await self.arequest("GET", "/trading/v1/options/accounts")
        return payload.get("data", [])

    async def get_otp_ws_url(self, account_id: str) -> str:
        """``POST /trading/v1/options/accounts/{accountId}/otp``.

        Devuelve la URL WebSocket de un solo uso (OTP) para operar sobre esa
        cuenta (gateway ``ws/demo`` o ``ws/real`` según el tipo de cuenta).
        """
        payload = await self.arequest(
            "POST", f"/trading/v1/options/accounts/{account_id}/otp"
        )
        url = (payload.get("data") or {}).get("url")
        if not url:
            raise DerivAPIError(None, f"La respuesta de OTP no contiene 'data.url': {payload}")
        return url

    async def health(self) -> dict[str, Any]:
        """``GET /v1/health`` — comprobación simple de disponibilidad."""
        return await self.arequest("GET", "/v1/health")
