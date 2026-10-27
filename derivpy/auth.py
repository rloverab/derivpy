"""Utilidades OAuth2 (Authorization Code + PKCE) para obtener un Bearer token.

Referencia: https://developers.deriv.com/llms/oauth.md

Flujo típico para un bot personal::

    url, verifier = build_authorize_url(client_id, redirect_uri, scopes=["trade"])
    # 1. Abre `url` en el navegador; el usuario autoriza y Deriv redirige a
    #    `redirect_uri?code=...&state=...`
    # 2. Captura `code` (por ejemplo corriendo un mini-servidor local, o
    #    copiándolo de la barra del navegador).
    token = await exchange_code(client_id, redirect_uri, code, verifier)

Nota: Deriv **no emite refresh tokens**; cuando el access token expire hay que
repetir el flujo de login. Para evitarlo en un bot personal, la alternativa más
simple es usar un Personal Access Token (PAT) generado desde la web de Deriv.
"""

from __future__ import annotations

import asyncio
import base64
import hashlib
import json
import secrets
import urllib.parse
import urllib.request
from typing import Any

from . import _constants as C
from .exceptions import DerivAPIError

# Longitud recomendada por RFC 7636 (43-128 caracteres).
_CODE_VERIFIER_BYTES = 32


def _b64url(raw: bytes) -> str:
    """Base64 sin relleno y con alfabeto seguro para URLs."""
    return base64.urlsafe_b64encode(raw).decode().rstrip("=")


def generate_pkce_pair() -> tuple[str, str]:
    """Devuelve ``(code_verifier, code_challenge)`` para el flujo PKCE."""
    verifier = _b64url(secrets.token_bytes(_CODE_VERIFIER_BYTES))
    challenge = _b64url(hashlib.sha256(verifier.encode()).digest())
    return verifier, challenge


def build_authorize_url(
    client_id: str,
    redirect_uri: str,
    scopes: list[str] | None = None,
    state: str | None = None,
) -> tuple[str, str]:
    """Construye la URL de login OAuth2 con PKCE.

    Args:
        client_id: App ID creado en https://app.deriv.com/account/api-token.
        redirect_uri: URI registrada de la aplicación.
        scopes: subconjunto de ``payment``, ``trade``, ``account_manage``,
            ``application_read``. Pide **todos** los que necesites: un token no
            puede ganar escopos después sin volver a pasar por OAuth.
        state: valor anti-CSRF aleatorio; se autogenera si se omite.

    Returns:
        ``(authorize_url, code_verifier)`` — guarda el verifier para luego
        intercambiar el código por el token.
    """
    verifier, challenge = generate_pkce_pair()
    params = {
        "app_id": client_id,
        "response_type": "code",
        "redirect_uri": redirect_uri,
        "code_challenge": challenge,
        "code_challenge_method": "S256",
        "state": state or _b64url(secrets.token_bytes(16)),
    }
    if scopes:
        params["scopes"] = ",".join(scopes)
    url = C.OAUTH_AUTHORIZE_URL + "?" + urllib.parse.urlencode(params)
    return url, verifier


def parse_callback_url(callback_url: str) -> dict[str, str]:
    """Extrae ``code``/``state`` (o ``error``) de la URL de redirección."""
    query = urllib.parse.parse_qs(urllib.parse.urlsplit(callback_url).query)
    flat = {k: v[0] for k, v in query.items()}
    if "error" in flat:
        raise DerivAPIError(flat["error"], flat.get("error_description", "OAuth error"))
    if "code" not in flat:
        raise DerivAPIError(None, f"No se encontró 'code' en la URL: {callback_url}")
    return flat


def _exchange_code_sync(client_id: str, redirect_uri: str, code: str, verifier: str) -> dict[str, Any]:
    data = urllib.parse.urlencode(
        {
            "grant_type": "authorization_code",
            "client_id": client_id,
            "redirect_uri": redirect_uri,
            "code": code,
            "code_verifier": verifier,
        }
    ).encode()
    req = urllib.request.Request(
        C.OAUTH_TOKEN_URL,
        data=data,
        headers={"Content-Type": "application/x-www-form-urlencoded", "Accept": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            payload = json.loads(resp.read().decode())
    except urllib.error.HTTPError as exc:  # pragma: no cover - depende de la red
        body = exc.read().decode(errors="replace")
        raise DerivAPIError(str(exc.code), f"Fallo al intercambiar el código OAuth: {body}", exc.code) from exc
    except (urllib.error.URLError, OSError) as exc:  # pragma: no cover
        raise DerivAPIError(None, f"Fallo de red contra el endpoint de tokens: {exc}") from exc

    if "access_token" not in payload:
        raise DerivAPIError(None, f"Respuesta sin access_token: {payload}")
    return payload


async def exchange_code(client_id: str, redirect_uri: str, code: str, verifier: str) -> dict[str, Any]:
    """Intercambia el ``code`` por un token. Versión async (hilo aparte).

    Returns:
        El JSON del endpoint de tokens: ``{"access_token", "expires_in", ...}``.
    """
    return await asyncio.to_thread(_exchange_code_sync, client_id, redirect_uri, code, verifier)
