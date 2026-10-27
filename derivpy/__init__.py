"""``derivpy`` — librería ligera y legible para la API de Deriv (www.deriv.com).

Diseñada para consumo personal (bots), Python >= 3.12, asíncrona (asyncio) y
con una única dependencia externa: `websockets <https://pypi.org/project/websockets/>`_.

Tres superficies según la documentación oficial (https://developers.deriv.com/llms.txt):

* :class:`~derivpy.market_data.MarketDataClient` — datos de mercado públicos
  (ticks, velas, símbolos) **sin autenticación**.
* :class:`~derivpy.trading.TradingClient` — trading autenticado sobre el gateway
  WebSocket OTP (proponer, comprar, vender, saldo, portafolio…).
* :class:`~derivpy.rest.RestClient` — REST (listar cuentas, pedir URLs OTP).

Obtener credenciales
--------------------
1. Crea una aplicación en https://app.deriv.com/account/api-token → obtén un
   ``App ID`` y un ``Personal Access Token`` con los escopos necesarios
   (``trade`` para operar). El PAT es la vía más simple para un bot personal.
2. Alternativa OAuth2 (Authorization Code + PKCE) en :mod:`derivpy.auth`.

Uso rápido
----------
::

    import asyncio
    from derivpy import MarketDataClient, TradingClient

    async def main() -> None:
        # Datos públicos, sin token:
        async with MarketDataClient() as market:
            candles = await market.candles("R_100", granularity=60, count=10)
            print(candles[-1].close)

        # Trading autenticado (PAT + App ID + account_id):
        async with TradingClient("CRD12345", token="mi-pat", app_id="1000") as api:
            balance = await api.get_balance()
            receipt = await api.trade(
                contract_type="CALL", underlying_symbol="R_100",
                amount=10, duration=5, duration_unit="t", currency="USD",
            )
            async for state in api.watch_contract(receipt.contract_id):
                print(state.status, state.profit)

    asyncio.run(main())
"""

from __future__ import annotations

from .auth import build_authorize_url, exchange_code, parse_callback_url
from .exceptions import (
    DerivAPIError,
    DerivAuthError,
    DerivConnectionError,
    DerivError,
    DerivRateLimitError,
    DerivTimeoutError,
)
from .market_data import MarketDataClient
from .models import (
    Account,
    Balance,
    BuyReceipt,
    Candle,
    OpenContract,
    Proposal,
    SellReceipt,
    Symbol,
    Tick,
)
from .rest import RestClient
from .trading import TradingClient

__version__ = "0.1.0"

__all__ = [
    # Clientes
    "MarketDataClient",
    "TradingClient",
    "RestClient",
    # Auth OAuth2
    "build_authorize_url",
    "exchange_code",
    "parse_callback_url",
    # Excepciones
    "DerivError",
    "DerivAPIError",
    "DerivAuthError",
    "DerivRateLimitError",
    "DerivConnectionError",
    "DerivTimeoutError",
    # Modelos
    "Account",
    "Balance",
    "BuyReceipt",
    "Candle",
    "OpenContract",
    "Proposal",
    "SellReceipt",
    "Symbol",
    "Tick",
]
