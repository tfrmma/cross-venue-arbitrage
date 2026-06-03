"""
Kraken connector. Spot + futures (via Kraken Futures API).

Kraken WS v2 uses a different format than v1. We're on v2.
Their auth is base64(HMAC-SHA512(nonce+payload, base64decode(secret))).
Yes, really. It's annoying.
"""

import asyncio
import base64
import hashlib
import hmac
import json
import logging
import time
import urllib.parse
from typing import Optional

import aiohttp
import websockets

from execution.router import FillResult, OrderStatus
from utils.rate_limiter import get_limiters
from venues.base import Balance, BaseVenue

log = logging.getLogger("arb.venue.kraken")

KRAKEN_WS = "wss://ws.kraken.com/v2"
KRAKEN_REST = "https://api.kraken.com"


class KrakenVenue(BaseVenue):
    def __init__(self, config: dict):
        super().__init__("kraken", config)
        self._api_key = config["api_key"]
        self._api_secret = config["api_secret"]
        self._symbols: list[str] = config.get("symbols", [])
        self._session: Optional[aiohttp.ClientSession] = None
        self._ws_token: Optional[str] = None

    async def connect(self) -> None:
        self._session = aiohttp.ClientSession(
            connector=aiohttp.TCPConnector(ssl=True)
        )
        self._ws_token = await self._get_ws_token()
        self._connected = True
        self.log.info("connected")

    async def close(self) -> None:
        if self._session:
            await self._session.close()

    async def stream_orderbook(self) -> None:
        async for ws in websockets.connect(KRAKEN_WS, ping_interval=20):
            try:
                await ws.send(json.dumps({
                    "method": "subscribe",
                    "params": {
                        "channel": "book",
                        "symbol": self._symbols,
                        "depth": 25,
                    },
                }))
                async for raw in ws:
                    self._handle_msg(json.loads(raw))
            except websockets.ConnectionClosed as e:
                self.log.warning("WS closed: %s - reconnecting", e)

    def _handle_msg(self, msg: dict) -> None:
        if msg.get("channel") != "book":
            return
        data = msg.get("data", [{}])[0]
        symbol = data.get("symbol", "")
        msg_type = msg.get("type", "update")

        bids = [(float(l["price"]), float(l["qty"])) for l in data.get("bids", [])]
        asks = [(float(l["price"]), float(l["qty"])) for l in data.get("asks", [])]

        self._push_book_update(symbol, bids, asks, is_snapshot=(msg_type == "snapshot"))

    async def place_ioc_order(
        self, symbol: str, side: str, qty: float, price: float
    ) -> Optional[FillResult]:
        t0 = time.monotonic_ns()
        params = {
            "ordertype": "limit",
            "type": side.lower(),
            "pair": symbol,
            "volume": f"{qty:.8f}",
            "price": f"{price:.2f}",
            "timeinforce": "IOC",
            "nonce": str(int(time.time() * 1000)),
        }

        async with get_limiters().order("kraken"):
            response = await self._private_request("/0/private/AddOrder", params)
        if response is None:
            return None

        errors = response.get("error", [])
        if errors:
            self.log.warning("order error: %s", errors)
            return FillResult("", OrderStatus.FAILED, 0.0, 0.0, 0.0, 0, response)

        return _parse_kraken_order(response.get("result", {}), t0)

    async def cancel_all_orders(self) -> None:
        await self._private_request("/0/private/CancelAll", {"nonce": str(int(time.time() * 1000))})

    async def get_positions(self) -> list:
        # Kraken spot doesn't have "positions" per se, only open orders and balances
        # Kraken Futures has positions but that's a separate connector
        return []

    async def get_balances(self) -> list[Balance]:
        params = {"nonce": str(int(time.time() * 1000))}
        resp = await self._private_request("/0/private/Balance", params)
        if resp is None:
            return []
        result = resp.get("result", {})
        return [Balance(asset, float(qty), 0.0) for asset, qty in result.items() if float(qty) > 0]

    async def _get_ws_token(self) -> str:
        params = {"nonce": str(int(time.time() * 1000))}
        resp = await self._private_request("/0/private/GetWebSocketsToken", params)
        if resp:
            return resp.get("result", {}).get("token", "")
        return ""

    async def _private_request(self, path: str, params: dict) -> Optional[dict]:
        nonce = params.get("nonce", str(int(time.time() * 1000)))
        post_data = urllib.parse.urlencode(params)
        encoded = (nonce + post_data).encode()
        msg = path.encode() + hashlib.sha256(encoded).digest()
        sig = hmac.new(base64.b64decode(self._api_secret), msg, hashlib.sha512)
        headers = {
            "API-Key": self._api_key,
            "API-Sign": base64.b64encode(sig.digest()).decode(),
        }
        try:
            async with self._session.post(
                f"{KRAKEN_REST}{path}", data=params, headers=headers
            ) as resp:
                return await resp.json()
        except Exception as e:
            self.log.error("request failed: %s %s", path, e)
            return None


def _parse_kraken_order(result: dict, t0: int) -> FillResult:
    desc = result.get("descr", {})
    txids = result.get("txid", [])
    order_id = txids[0] if txids else ""

    # Kraken IOC orders don't return fill details immediately - need to query order status
    # For now assume filled if no error. TODO: poll order status for exact fill
    return FillResult(
        order_id=order_id,
        status=OrderStatus.FILLED if order_id else OrderStatus.CANCELLED,
        filled_qty=0.0,   # not available immediately from AddOrder response
        avg_price=0.0,
        fees_paid=0.0,
        latency_us=(time.monotonic_ns() - t0) // 1000,
        raw_response=result,
    )
