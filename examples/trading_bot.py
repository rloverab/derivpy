#!/usr/bin/env python3
"""Ejemplo: bot de trading mínimo sobre una cuenta DEMO de Deriv.

Necesitas un Personal Access Token (PAT) con scope ``trade`` y tu App ID:

* Créalos en https://home.deriv.com/dashboard/profile/api-tokens
* Expórtalos como variables de entorno::

    export DERIV_TOKEN="tu-pat"
    export DERIV_APP_ID="1000"

El script:

1. Lista tus cuentas vía REST y escoge la primera demo.
2. Abre el WebSocket autenticado (flujo OTP) y muestra el saldo.
3. Cotiza y compra un contrato CALL de 5 ticks sobre R_100.
4. Monitoriza el contrato hasta que liquida y muestra el resultado.

⚠️  Usa SIEMPRE una cuenta demo para probar estrategias. Ejecutar::

    python examples/trading_bot.py
"""

from __future__ import annotations

import asyncio
import logging
import os

from dotenv import load_dotenv

from derivpy import RestClient, TradingClient

load_dotenv()


async def main() -> None:
    token = os.environ["DERIV_TOKEN"]      # PAT (o access token OAuth2)
    app_id = os.environ.get("DERIV_APP_ID")  # obligatorio con PAT

    # 1) REST: descubrir las cuentas del titular del token.
    rest = RestClient(token=token, app_id=app_id)
    accounts = await rest.list_accounts()
    for a in accounts:
        print(f"Cuenta {a['account_id']:>10} | {a['account_type']:<4} | "
              f"{a['balance']} {a['currency']}")

    demo = next((a for a in accounts if a["account_type"] == "demo"), None)
    if demo is None:
        raise SystemExit("No hay cuenta demo; crea una en la web de Deriv.")

    # 2) WebSocket autenticado mediante URL OTP para esa cuenta.
    async with TradingClient(demo["account_id"], token=token, app_id=app_id) as api:
        print("Hora del servidor:", await api.get_server_time())
        balance = await api.get_balance()
        print(f"Saldo: {balance.balance} {balance.currency}")

        # 3) Proponer → comprar (workflow oficial).
        receipt = await api.trade(
            contract_type="CALL",       # gana si el precio sube al expirar
            underlying_symbol="R_100",
            amount=10.0,                # stake
            basis="stake",
            duration=5,
            duration_unit="t",          # 5 ticks
            currency=demo["currency"],
        )
        print(f"Comprado contrato {receipt.contract_id} por {receipt.buy_price} "
              f"(payout potencial {receipt.payout})")

        # 4) Monitorizar hasta el cierre.
        async for state in api.watch_contract(receipt.contract_id):
            print(f"  estado={state.status:>8} profit={state.profit}")

        final = await api.get_balance()
        print(f"Saldo final: {final.balance} {final.currency}")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
    asyncio.run(main())
