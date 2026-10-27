#!/usr/bin/env python3
"""Ejemplo: datos de mercado PÚBLICOS (sin token).

Muestra símbolos disponibles, últimas velas y unos pocos ticks en vivo
de un índice sintético. Ejecutar::

    python examples/market_data.py [SIMBOLO]
"""

from __future__ import annotations

import asyncio
import sys

from derivpy import MarketDataClient


async def main(symbol: str = "R_100") -> None:
    async with MarketDataClient() as market:
        # 1) Símbolos del mercado sintético (breve)
        symbols = await market.active_symbols()
        synth = [s for s in symbols if s.market == "synthetic_index"][:5]
        print("Índices sintéticos:", ", ".join(f"{s.symbol} ({s.display_name})" for s in synth))

        # 2) Últimas 10 velas de 1 minuto
        candles = await market.candles(symbol, granularity=60, count=10)
        last = candles[-1]
        print(
            f"Última vela {symbol}: open={last.open} high={last.high} "
            f"low={last.low} close={last.close}"
        )

        # 3) Historial de ticks
        ticks, pip_size = await market.ticks_history(symbol, count=5)
        print(f"Últimos {len(ticks)} ticks (pip_size={pip_size}):", [t.quote for t in ticks])

        # 4) Stream en vivo: lee 5 ticks y termina
        print(f"Recibiendo 5 ticks en vivo de {symbol}…")
        count = 0
        async for tick in market.stream_ticks(symbol):
            count += 1
            print(f"  tick #{count}: {tick.quote} @ {tick.datetime.time()}")
            if count >= 5:
                break


if __name__ == "__main__":
    asyncio.run(main(sys.argv[1] if len(sys.argv) > 1 else "R_100"))
