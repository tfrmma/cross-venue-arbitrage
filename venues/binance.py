"""
Binance connector. Perps on USDT-M, spot on the main exchange.

Notes:
- Use futures WS for perps: wss://fstream.binance.com
- Spot WS: wss://stream.binance.com:9443
- IOC orders via POST /fapi/v1/order with timeInForce=IOC
- Auth: HMAC-SHA256 on query string, X-MBX-APIKEY header
- Weight limits matter - don't hammer the REST endpoint
"""

import asyncio
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

log = logging.getLogger("arb.venue.binance")

FUTURES_WS = "wss://fstream.binance.com/ws"
FUTURES_REST = "https://fapi.binance.com"
SPOT_WS = "wss://stream.binance.com:9443/ws"
SPOT_REST = "https://api.binance.com"


class BinanceVenue(BaseVenue):
    def __init__(self, config: dict):
        super().__init__("binance", config)
        self._api_key = config["api_key"]
        self._api_secret = config["api_secret"]
        self._symbols: list[str] = config.get("symbols", [])
        self._is_futures: bool = config.get("futures", True)
        self._limiters = get_limiters()

        self._ws_base = FUTURES_WS if self._is_futures else SPOT_WS
        self._rest_base = FUTURES_REST if self._is_futures else SPOT_REST

        self._session: Optional[aiohttp.ClientSession] = None
        self._ws: Optional[websockets.WebSocketClientProtocol] = None

    async def connect(self) -> None:
        self._session = aiohttp.ClientSession(
            headers={"X-MBX-APIKEY": self._api_key},
            connector=aiohttp.TCPConnector(ssl=True, limit=20),
        )
        self._connected = True
        self.log.info("connected")

    async def close(self) -> None:
        if self._ws:
            await self._ws.close()
        if self._session:
            await self._session.close()
        self._connected = False

    async def stream_orderbook(self) -> None:
        streams = "/".join(f"{s.lower()}@depth@100ms" for s in self._symbols)
        url = f"{self._ws_base}/{streams}"

        async for ws in websockets.connect(url, ping_interval=20, ping_timeout=10):
            self._ws = ws
            self.log.info("book stream open: %s", streams)
            try:
                async for raw in ws:
                    self._handle_depth_msg(json.loads(raw))
            except websockets.ConnectionClosed as e:
                self.log.warning("WS closed: %s - reconnecting", e)

    def _handle_depth_msg(self, msg: dict) -> None:
        # combined stream wraps in {"stream": ..., "data": {...}}
        data = msg.get("data", msg)
        symbol = data.get("s", "")
        bids = [(float(p), float(q)) for p, q in data.get("b", [])]
        asks = [(float(p), float(q)) for p, q in data.get("a", [])]
        is_snapshot = "lastUpdateId" in data and "U" not in data  # snapshot has no U field

        if bids or asks:
            self._push_book_update(symbol, bids, asks, is_snapshot)

    async def place_ioc_order(
        self, symbol: str, side: str, qty: float, price: float
    ) -> Optional[FillResult]:
        t0 = time.monotonic_ns()
        endpoint = "/fapi/v1/order" if self._is_futures else "/api/v3/order"

        params = {
            "symbol": symbol,
            "side": side.upper(),
            "type": "LIMIT",
            "timeInForce": "IOC",
            "quantity": f"{qty:.6f}",
            "timestamp": int(time.time() * 1000),
        }
        if price > 0:
            params["price"] = f"{price:.2f}"

        params["signature"] = self._sign(params)

        try:
            async with self._limiters.order("binance"):
                async with self._session.post(
                    f"{self._rest_base}{endpoint}", data=params
                ) as resp:
                    data = await resp.json()

            if "code" in data:
                self.log.warning("order error: %s", data)
                return FillResult("", OrderStatus.FAILED, 0.0, 0.0, 0.0, 0, data)

            return _parse_order_response(data, t0)

        except Exception as e:
            self.log.error("place_ioc_order failed: %s", e)
            return None

    async def cancel_all_orders(self) -> None:
        endpoint = "/fapi/v1/allOpenOrders" if self._is_futures else "/api/v3/openOrders"
        for symbol in self._symbols:
            params = {"symbol": symbol, "timestamp": int(time.time() * 1000)}
            params["signature"] = self._sign(params)
            try:
                async with self._session.delete(
                    f"{self._rest_base}{endpoint}", params=params
                ) as resp:
                    await resp.json()
            except Exception as e:
                self.log.error("cancel_all failed for %s: %s", symbol, e)

    async def get_positions(self) -> list:
        if not self._is_futures:
            return []
        params = {"timestamp": int(time.time() * 1000)}
        params["signature"] = self._sign(params)
        async with self._session.get(
            f"{self._rest_base}/fapi/v2/positionRisk", params=params
        ) as resp:
            data = await resp.json()
        return [_parse_position(p) for p in data if float(p.get("positionAmt", 0)) != 0]

    async def get_balances(self) -> list[Balance]:
        endpoint = "/fapi/v2/balance" if self._is_futures else "/api/v3/account"
        params = {"timestamp": int(time.time() * 1000)}
        params["signature"] = self._sign(params)
        async with self._session.get(f"{self._rest_base}{endpoint}", params=params) as resp:
            data = await resp.json()

        if self._is_futures:
            return [
                Balance(b["asset"], float(b["availableBalance"]), float(b["balance"]) - float(b["availableBalance"]))
                for b in (data if isinstance(data, list) else [])
            ]
        balances_raw = data.get("balances", [])
        return [
            Balance(b["asset"], float(b["free"]), float(b["locked"]))
            for b in balances_raw
            if float(b["free"]) + float(b["locked"]) > 0
        ]

    def _sign(self, params: dict) -> str:
        query = urllib.parse.urlencode(params)
        return hmac.new(
            self._api_secret.encode(), query.encode(), hashlib.sha256
        ).hexdigest()


def _parse_order_response(data: dict, t0: int) -> FillResult:
    status_map = {
        "FILLED": OrderStatus.FILLED,
        "PARTIALLY_FILLED": OrderStatus.PARTIAL,
        "EXPIRED": OrderStatus.CANCELLED,
        "CANCELED": OrderStatus.CANCELLED,
    }
    status = status_map.get(data.get("status", ""), OrderStatus.FAILED)
    filled_qty = float(data.get("executedQty", 0))
    cum_quote = float(data.get("cummulativeQuoteQty", 0))
    avg_price = cum_quote / filled_qty if filled_qty > 0 else 0.0
    fees = sum(float(f.get("commission", 0)) for f in data.get("fills", []))
    latency_us = (time.monotonic_ns() - t0) // 1000

    return FillResult(
        order_id=str(data.get("orderId", "")),
        status=status,
        filled_qty=filled_qty,
        avg_price=avg_price,
        fees_paid=fees,
        latency_us=latency_us,
        raw_response=data,
    )


def _parse_position(p: dict):
    from risk.manager import Position
    return Position(
        venue="binance",
        symbol=p["symbol"],
        qty=float(p["positionAmt"]),
        avg_entry=float(p["entryPrice"]),
        unrealized_pnl=float(p["unrealizedProfit"]),
        is_perp=True,
    )
