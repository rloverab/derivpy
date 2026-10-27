"""Tests offline de ``derivpy`` usando un servidor WebSocket local falso.

No tocan la red externa: se levanta ``websockets.serve`` en localhost que
imita el protocolo JSON de Deriv (req_id, msg_type, streams, errores).

Ejecutar::

    python -m pytest tests/  (o)  python tests/test_derivpy.py
"""

from __future__ import annotations

import asyncio
import json
import unittest

import websockets

from derivpy.exceptions import DerivAPIError, DerivAuthError
from derivpy.market_data import MarketDataClient
from derivpy.models import OpenContract, Tick
from derivpy.trading import TradingClient

HOST = "localhost"


async def fake_server(ws: websockets.ServerConnection) -> None:
    """Servidor que responde como el gateway de Deriv."""
    async for raw in ws:
        msg = json.loads(raw)
        req_id = msg.get("req_id")

        # Error simulado para pedir OTP de cuenta inexistente.
        if "ticks" in msg and msg["ticks"] == "BAD":
            await ws.send(json.dumps(
                {"error": {"code": "InputValidationFailed", "message": "bad symbol"},
                 "msg_type": "error", "echo_req": msg, **({"req_id": req_id} if req_id else {})}
            ))
            continue

        if "ticks" in msg:
            reply = {
                "msg_type": "tick",
                "tick": {"symbol": msg["ticks"], "epoch": 1700000000, "quote": 1234.56,
                         "pip_size": 2, "id": "sub-1"},
                "subscription": {"id": "sub-1"},
                **({"req_id": req_id} if req_id else {}),
            }
            await ws.send(json.dumps(reply))
            if msg.get("subscribe") == 1:
                for i in range(3):  # empuja algunos updates al stream
                    await ws.send(json.dumps({
                        "msg_type": "tick",
                        "tick": {"symbol": msg["ticks"], "epoch": 1700000001 + i,
                                 "quote": 1234.56 + i, "pip_size": 2},
                    }))
            continue

        if "ticks_history" in msg:
            reply = {
                "msg_type": "candles",
                "candles": [
                    {"epoch": 1, "open": 1.0, "high": 2.0, "low": 0.5, "close": 1.5},
                    {"epoch": 2, "open": 1.5, "high": 3.0, "low": 1.0, "close": 2.5},
                ],
                "pip_size": 2,
                **({"req_id": req_id} if req_id else {}),
            }
            await ws.send(json.dumps(reply))
            if msg.get("subscribe") == 1:
                # luego empuja velas parciales (msg_type ohlc), sin req_id
                for i in range(2):
                    await ws.send(json.dumps({
                        "msg_type": "ohlc",
                        "ohlc": {"open_epoch": 100 + i, "open": 1.0, "high": 2.0,
                                 "low": 0.5, "close": 1.5 + i},
                    }))
            continue

        if "forget" in msg:
            await ws.send(json.dumps({
                "msg_type": "forget", "forget": ["sub-1"],
                **({"req_id": req_id} if req_id else {}),
            }))
            continue

        if "active_symbols" in msg:
            await ws.send(json.dumps({
                "msg_type": "active_symbols",
                "active_symbols": [
                    {"symbol": "R_100", "display_name": "Volatility 100 Index",
                     "market": "synthetic_index", "symbol_type": "pseudoforex"}
                ],
                **({"req_id": req_id} if req_id else {}),
            }))
            continue

        if "balance" in msg:
            body = {"balance": 10000.0, "currency": "USD", "loginid": "CRD1"}
            reply = {"msg_type": "balance", "balance": body,
                     **({"req_id": req_id} if req_id else {})}
            if msg.get("subscribe") == 1:
                reply["subscription"] = {"id": "bal-1"}
            await ws.send(json.dumps(reply))
            if msg.get("subscribe") == 1:
                await ws.send(json.dumps({"msg_type": "balance",
                                          "balance": {**body, "balance": 10050.0}}))
            continue

        if "proposal" in msg:
            await ws.send(json.dumps({
                "msg_type": "proposal",
                "proposal": {"id": "prop-abc", "ask_price": 9.8, "payout": 19.6,
                             "spot": 1234.5, "longcode": "CALL R_100"},
                **({"req_id": req_id} if req_id else {}),
            }))
            continue

        if "buy" in msg:
            await ws.send(json.dumps({
                "msg_type": "buy",
                "buy": {"contract_id": 42, "transaction_id": 43, "buy_price": 9.8,
                        "payout": 19.6, "balance_after": 9990.2, "longcode": "CALL",
                        "start_time": 1, "purchase_time": 1},
                **({"req_id": req_id} if req_id else {}),
            }))
            continue

        if "sell" in msg:
            await ws.send(json.dumps({
                "msg_type": "sell",
                "sell": {"contract_id": 42, "sold_for": 12.0, "profit": 2.2,
                         "balance_after": 10002.2},
                **({"req_id": req_id} if req_id else {}),
            }))
            continue

        if "proposal_open_contract" in msg:
            def _body(**kw):
                base = {"contract_id": msg.get("contract_id", 42), "status": "open",
                        "is_sold": 0, "is_expired": 0, "is_settled": 0,
                        "profit": "1.0", "payout": "19.6", "current_spot": "1235.0"}
                base.update(kw)
                return base
            first = {"msg_type": "proposal_open_contract",
                     "proposal_open_contract": _body(),
                     "subscription": {"id": "poc-1"}}
            if req_id:
                first["req_id"] = req_id
            await ws.send(json.dumps(first))
            if msg.get("subscribe") == 1:
                closed = {"msg_type": "proposal_open_contract",
                          "proposal_open_contract": _body(status="settled", is_settled=1,
                                                           profit="9.8")}
                await ws.send(json.dumps(closed))
            continue

        if msg.get("ping"):
            await ws.send(json.dumps({"msg_type": "ping", "ping": 1234567890}))
            continue

        await ws.send(json.dumps({
            "error": {"code": "UnknownRequest", "message": "no such call"},
            "msg_type": "error", **({"req_id": req_id} if req_id else {}),
        }))


class FakeWSMixin:
    """Arranca el servidor falso y patchea las URLs de los clientes."""

    @classmethod
    def setUpClass(cls):
        cls.server = None

    async def start_server(self):
        self.server = await websockets.serve(fake_server, HOST, 0)
        self.port = self.server.sockets[0].getsockname()[1]
        return f"ws://{HOST}:{self.port}"


class TestMarketData(FakeWSMixin, unittest.IsolatedAsyncioTestCase):
    async def test_candles_and_ticks_stream(self):
        url = await self.start_server()
        try:
            api = MarketDataClient(url=url)
            async with api:
                candles = await api.candles("R_100", granularity=60, count=2)
                self.assertEqual(len(candles), 2)
                self.assertEqual(candles[1].close, 2.5)

                symbols = await api.active_symbols()
                self.assertEqual(symbols[0].symbol, "R_100")

                seen = []
                async for tick in api.stream_ticks("R_100"):
                    seen.append(tick.quote)
                    if len(seen) >= 3:
                        break
                self.assertEqual(len(seen), 3)

                candles_live = []
                async for candle in api.stream_candles("R_100"):
                    candles_live.append(candle.close)
                    if len(candles_live) >= 2:
                        break
                self.assertEqual(candles_live, [1.5, 2.5])
        finally:
            self.server.close()
            await self.server.wait_closed()

    async def test_error_raises_api_exception(self):
        url = await self.start_server()
        try:
            api = MarketDataClient(url=url)
            async with api:
                with self.assertRaises(DerivAPIError) as ctx:
                    await api.request({"ticks": "BAD"})
                self.assertEqual(ctx.exception.code, "InputValidationFailed")
        finally:
            self.server.close()
            await self.server.wait_closed()


class TestTrading(FakeWSMixin, unittest.IsolatedAsyncioTestCase):
    async def make_client(self, url: str) -> TradingClient:
        client = TradingClient("CRD1", token="fake", app_id="1")
        # Sustituye la obtención de OTP REST por la URL del servidor falso.
        client._rest.get_otp_ws_url = lambda account_id: _coro(url)  # type: ignore[method-assign]
        return client

    async def test_full_trade_cycle(self):
        url = await self.start_server()
        try:
            client = await self.make_client(url)
            async with client:
                bal = await client.get_balance()
                self.assertEqual(bal.balance, 10000.0)

                prop = await client.propose(
                    contract_type="CALL", underlying_symbol="R_100",
                    amount=10, duration=5, duration_unit="t", currency="USD",
                )
                self.assertEqual(prop.id, "prop-abc")

                receipt = await client.buy(prop.id, prop.ask_price)
                self.assertEqual(receipt.contract_id, 42)

                states = [s async for s in client.watch_contract(42)]
                self.assertGreaterEqual(len(states), 2)   # >= abierto -> cerrado
                self.assertTrue(states[0].is_open)
                self.assertFalse(states[-1].is_open)      # se detuvo al cerrar
                self.assertEqual(states[-1].profit, 9.8)

                received: list[float] = []
                done = asyncio.Event()

                def on_balance(b):
                    received.append(b.balance)
                    if len(received) >= 2:
                        done.set()

                await client.watch_balance(on_balance)
                await asyncio.wait_for(done.wait(), 2)
                self.assertEqual(received, [10000.0, 10050.0])

                sold = await client.sell(42, 0)
                self.assertEqual(sold.profit, 2.2)
        finally:
            self.server.close()
            await self.server.wait_closed()

    async def test_auth_error_maps_correctly(self):
        url = await self.start_server()
        try:
            client = await self.make_client(url)
            async with client:
                with self.assertRaises(DerivAPIError):
                    await client.request({"nonexistent_call": 1})
        finally:
            self.server.close()
            await self.server.wait_closed()


async def _coro(value):
    """Future ya resuelto: stub de RestClient.get_otp_ws_url."""
    return value


# Smoke test de modelos puros (sin red)
class TestModels(unittest.TestCase):
    def test_tick_from_api(self):
        t = Tick.from_api({"symbol": "R_10", "epoch": 1, "quote": "10.5", "pip_size": "1"})
        self.assertEqual(t.quote, 10.5)
        self.assertEqual(t.pip_size, 1)

    def test_open_contract_is_open(self):
        oc = OpenContract.from_api({"contract_id": 1, "status": "open", "is_sold": 0,
                                    "is_expired": 0, "is_settled": 0})
        self.assertTrue(oc.is_open)


if __name__ == "__main__":
    unittest.main(verbosity=2)
