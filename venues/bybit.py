"""
Bybit V5 API connector. Linear perps + spot.

V5 unified account - one endpoint for everything.
WS depth: use 'orderbook.200.BTCUSDT' for 200-level book.
Auth: HMAC-SHA256 on timestamp+api_key+recv_window+params.
"""

import asyncio
import hashlib
import hmac
import json
import logging
import time
from typing import Optional

import aiohttp
import websockets

from execution.router import FillResult, OrderStatus
from utils.rate_limiter import get_limiters
from venues.base import Balance, BaseVenue

log = logging.getLogger("arb.venue.bybit")

BYBIT_WS_LINEAR = "wss://stream.bybit.com/v5/public/linear"
BYBIT_WS_SPOT = "wss://stream.bybit.com/v5/public/spot"
BYBIT_REST = "https://api.bybit.com"

RECV_WINDOW = 5000


class BybitVenue(BaseVenue):
    def __init__(self, config: dict):
        super().__init__("bybit", config)
        self._api_key = config["api_key"]
        self._api_secret = config["api_secret"]
        self._symbols: list[str] = config.get("symbols", [])
        self._category: str = config.get("category", "linear")  # linear | spot
        self._session: Optional[aiohttp.ClientSession] = None

        self._ws_url = BYBIT_WS_LINEAR if self._category == "linear" else BYBIT_WS_SPOT
        self._limiters = get_limiters()

    async def connect(self) -> None:
        self._session = aiohttp.ClientSession(
            connector=aiohttp.TCPConnector(ssl=True, limit=20),
        )
        self._connected = True
        self.log.info("connected (%s)", self._category)

    async def close(self) -> None:
        if self._session:
            await self._session.close()

    async def stream_orderbook(self) -> None:
        args = [f"orderbook.200.{s}" for s in self._symbols]
        async for ws in websockets.connect(self._ws_url, ping_interval=20):
            try:
                await ws.send(json.dumps({"op": "subscribe", "args": args}))
                async for raw in ws:
                    self._handle_msg(json.loads(raw))
            except websockets.ConnectionClosed as e:
                self.log.warning("WS closed: %s - reconnecting", e)

    def _handle_msg(self, msg: dict) -> None:
        topic = msg.get("topic", "")
        if not topic.startswith("orderbook"):
            return

        data = msg.get("data", {})
        symbol = data.get("s", "")
        msg_type = msg.get("type", "delta")  # "snapshot" or "delta"

        bids = [(float(p), float(q)) for p, q in data.get("b", [])]
        asks = [(float(p), float(q)) for p, q in data.get("a", [])]

        self._push_book_update(symbol, bids, asks, is_snapshot=(msg_type == "snapshot"))

    async def place_ioc_order(
        self, symbol: str, side: str, qty: float, price: float
    ) -> Optional[FillResult]:
        t0 = time.monotonic_ns()
        params = {
            "category": self._category,
            "symbol": symbol,
            "side": "Buy" if side.lower() == "buy" else "Sell",
            "orderType": "Limit",
            "qty": str(qty),
            "price": str(price),
            "timeInForce": "IOC",
        }

        async with self._limiters.order("bybit"):
            resp = await self._signed_request("POST", "/v5/order/create", params)
        if resp is None:
            return None

        ret_code = resp.get("retCode", -1)
        if ret_code != 0:
            self.log.warning("order error %d: %s", ret_code, resp.get("retMsg"))
            return FillResult("", OrderStatus.FAILED, 0.0, 0.0, 0.0, 0, resp)

        result = resp.get("result", {})
        return FillResult(
            order_id=result.get("orderId", ""),
            status=OrderStatus.PENDING,  # need to poll for fill status
            filled_qty=0.0,
            avg_price=0.0,
            fees_paid=0.0,
            latency_us=(time.monotonic_ns() - t0) // 1000,
            raw_response=resp,
        )

    async def cancel_all_orders(self) -> None:
        for symbol in self._symbols:
            params = {"category": self._category, "symbol": symbol}
            await self._signed_request("POST", "/v5/order/cancel-all", params)

    async def get_positions(self) -> list:
        if self._category == "spot":
            return []
        params = {"category": self._category, "settleCoin": "USDT"}
        resp = await self._signed_request("GET", "/v5/position/list", params)
        if resp is None:
            return []
        return [_parse_bybit_position(p) for p in resp.get("result", {}).get("list", [])
                if float(p.get("size", 0)) != 0]

    async def get_balances(self) -> list[Balance]:
        params = {"accountType": "UNIFIED"}
        resp = await self._signed_request("GET", "/v5/account/wallet-balance", params)
        if resp is None:
            return []
        coins = resp.get("result", {}).get("list", [{}])[0].get("coin", [])
        return [
            Balance(c["coin"], float(c.get("availableToWithdraw", 0)), float(c.get("locked", 0)))
            for c in coins if float(c.get("walletBalance", 0)) > 0
        ]

    async def _signed_request(self, method: str, path: str, params: dict) -> Optional[dict]:
        ts = str(int(time.time() * 1000))
        payload = json.dumps(params) if method == "POST" else urllib.parse.urlencode(params)  # type: ignore
        sign_str = ts + self._api_key + str(RECV_WINDOW) + payload
        signature = hmac.new(
            self._api_secret.encode(), sign_str.encode(), hashlib.sha256
        ).hexdigest()

        headers = {
            "X-BAPI-API-KEY": self._api_key,
            "X-BAPI-TIMESTAMP": ts,
            "X-BAPI-SIGN": signature,
            "X-BAPI-RECV-WINDOW": str(RECV_WINDOW),
        }

        try:
            if method == "POST":
                async with self._session.post(
                    f"{BYBIT_REST}{path}", json=params, headers=headers
                ) as resp:
                    return await resp.json()
            else:
                async with self._session.get(
                    f"{BYBIT_REST}{path}", params=params, headers=headers
                ) as resp:
                    return await resp.json()
        except Exception as e:
            self.log.error("request failed: %s %s", path, e)
            return None


def _parse_bybit_position(p: dict):
    from risk.manager import Position
    return Position(
        venue="bybit",
        symbol=p["symbol"],
        qty=float(p["size"]) * (1 if p.get("side") == "Buy" else -1),
        avg_entry=float(p.get("avgPrice", 0)),
        unrealized_pnl=float(p.get("unrealisedPnl", 0)),
        is_perp=True,
    )


# missing import fix
import urllib.parse
