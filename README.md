# derivpy

Librería ligera y legible en **Python ≥ 3.12** para interactuar con la API de [Deriv](https://www.deriv.com) (datos de mercado, autenticación y trading). Pensada para consumo personal — por ejemplo, un bot propio — priorizando simplicidad sobre exhaustividad.

- **Asíncrona** (`asyncio`) de principio a fin.
- **Una sola dependencia externa**: [`websockets`](https://pypi.org/project/websockets/). El REST usa `urllib` de la stdlib.
- Basada en la documentación oficial: <https://developers.deriv.com/llms.txt>.

## Instalación

```bash
git clone https://github.com/rloverab/derivpy.git
cd derivpy
pip install -e .
```

Requiere Python 3.12 o superior.

## Credenciales

1. Entra en [home.deriv.com → Token de API](https://home.deriv.com/dashboard/profile/api-tokens).
2. Crea una aplicación: anota el **App ID**.
3. Genera un **Personal Access Token (PAT)** con los escopos necesarios (`read` para datos/cuentas, `trade` para operar).
4. Expórtalos como variables de entorno (nunca los escribas en el código):

```bash
export DERIV_TOKEN="tu-pat"
export DERIV_APP_ID="1000"
```

> Alternativa OAuth2 (Authorization Code + PKCE) disponible en `derivpy.auth`:
> `build_authorize_url()` → redirigir al usuario → `parse_callback_url()` → `exchange_code()`.

## Uso rápido

### Datos de mercado (públicos, sin token)

```python
import asyncio
from derivpy import MarketDataClient

async def main():
    async with MarketDataClient() as market:
        # Últimas 10 velas de 1 minuto del índice Volatility 100
        candles = await market.candles("R_100", granularity=60, count=10)
        print(candles[-1].close)

        # Stream en vivo de ticks
        async for tick in market.stream_ticks("R_100"):
            print(tick.epoch, tick.quote)
            break

asyncio.run(main())
```

### Trading (autenticado, vía gateway OTP)

```python
import asyncio
from derivpy import TradingClient

async def main():
    async with TradingClient("CRD12345", token="mi-pat", app_id="1000") as api:
        balance = await api.get_balance()
        print(balance.balance, balance.currency)

        # Cotiza y compra en un solo paso (propose + buy)
        receipt = await api.trade(
            contract_type="CALL", underlying_symbol="R_100",
            amount=10, duration=5, duration_unit="t", currency="USD",
        )
        # Monitoriza el contrato hasta que liquide
        async for state in api.watch_contract(receipt.contract_id):
            print(state.status, state.profit)

asyncio.run(main())
```

Ejemplos completos y ejecutables en [`examples/`](examples):

- `examples/market_data.py` — velas, símbolos y streams en vivo.
- `examples/trading_bot.py` — ciclo completo: descubrir cuentas → saldo → cotizar → comprar → seguir el contrato.

## Superficies de la API

| Clase / módulo | Qué hace | ¿Requiere token? |
|---|---|---|
| `MarketDataClient` | WebSocket público: `active_symbols`, `candles`, `ticks_history`, `stream_ticks`, `stream_candles` | No |
| `TradingClient` | WebSocket autenticado (OTP): `get_balance`, `watch_balance`, `propose`, `buy`, `sell`, `trade`, `watch_contract`, `get_portfolio`, `get_transactions`, `get_statement`, `get_profit_table`, peticiones arbitrarias con `request()` / `subscribe()` | Sí |
| `RestClient` | REST: `list_accounts`, `get_otp_ws_url`, `health` | Sí |
| `derivpy.auth` | OAuth2 con PKCE: `build_authorize_url`, `parse_callback_url`, `exchange_code` | — |

Los resultados llegan como modelos tipados e inmutables (`Tick`, `Candle`, `Symbol`, `Balance`, `Proposal`, `BuyReceipt`, `SellReceipt`, `OpenContract`), parseados desde las respuestas JSON de Deriv.

## Manejo de errores

Todas las excepciones heredan de `DerivError`:

- `DerivAPIError` — la API devolvió un objeto `error` (expone `.code` y `.message`).
- `DerivAuthError` — problemas de autenticación/autorización.
- `DerivRateLimitError` — límite de peticiones superado.
- `DerivConnectionError` — fallo al conectar o caída del WebSocket.
- `DerivTimeoutError` — expiró el tiempo de espera de una respuesta.

## Estructura del proyecto

```
derivpy/
├── __init__.py        # API pública y docstring general
├── _constants.py      # URLs y valores por defecto (único punto de cambio)
├── exceptions.py      # Jerarquía de errores
├── models.py          # Modelos tipados (dataclasses) + parsing
├── market_data.py     # WebSocket público de datos de mercado
├── rest.py            # Cliente REST (stdlib urllib)
├── auth.py            # OAuth2 Authorization Code + PKCE
└── trading.py         # WebSocket autenticado de trading (flujo OTP)
examples/              # Scripts ejecutables de ejemplo
tests/                 # Tests offline con un servidor WebSocket falso
```

## Ejecutar los tests

Los tests no tocan la red: levantan un servidor WebSocket local que imita el protocolo JSON de Deriv.

```bash
pip install pytest
python -m pytest tests/ -q
# o bien: python tests/test_derivpy.py
```

## Notas y advertencias

- ⚠️ **Usa siempre una cuenta DEMO** para probar estrategias. Operar con dinero real bajo tu propia responsabilidad.
- Los tokens son credenciales sensibles: manténlos en variables de entorno o un gestor de secretos; `.env` ya está excluido en `.gitignore`.
- La librería es deliberadamente simple: no intenta cubrir toda la API de Deriv. Para llamadas no envueltas, usa `TradingClient.request({...})` con el payload JSON crudo documentado en la web de desarrolladores.
- Proyecto de uso personal, sin garantías de mantenimiento continuo. Contribuciones bienvenidas.

## Licencia

MIT
