"""Punto de único para las constantes de la API de Deriv.

Todos los valores provienen de la documentación oficial:
https://developers.deriv.com/llms.txt
"""

from __future__ import annotations

# ---------------------------------------------------------------------------
# URLs base
# ---------------------------------------------------------------------------

#: Base para todas las llamadas REST.
REST_BASE_URL = "https://api.derivws.com"

#: Gateway WebSocket de datos de mercado PUBLICOS (sin autenticación).
PUBLIC_WS_URL = "wss://api.derivws.com/trading/v1/options/ws/public"

#: Endpoint OAuth2 de autorización (login del usuario).
OAUTH_AUTHORIZE_URL = "https://auth.deriv.com/oauth2/auth"

#: Endpoint OAuth2 de intercambio de código por token.
OAUTH_TOKEN_URL = "https://auth.deriv.com/oauth2/token"

# ---------------------------------------------------------------------------
# Tiempo de vida y reintentos
# ---------------------------------------------------------------------------

#: Cada cuánto se envía ``ping`` para mantener viva la conexión (la doc
#: recomienda ~30 s).
DEFAULT_PING_INTERVAL = 30.0

#: Timeout por defecto al esperar la respuesta a una petición.
DEFAULT_REQUEST_TIMEOUT = 30.0

#: Reintentos máximos de reconexión antes de rendirse.
DEFAULT_MAX_RECONNECT_ATTEMPTS = 5

#: Parámetro de duración ``duration_unit`` aceptados por la API.
DURATION_UNITS = ("t", "s", "m", "h", "d")

#: Grano (en segundos) válido para ``granularity`` en velas de ticks_history.
CANDLE_GRANULARITIES = (60, 120, 180, 300, 600, 900, 1800, 3600, 7200, 14400, 28800, 86400)

#: Escopes OAuth2 documentados.
OAUTH_SCOPES = ("payment", "trade", "account_manage", "application_read")
