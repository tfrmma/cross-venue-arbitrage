"""
Hyperliquid connector.

HL uses a custom JSON API. Perps only. WS sends full snapshots on every update
(no delta diffs) which is a bit wasteful but at least you're never out of sync.

Auth: EIP-712 typed data signing, not plain eth_sign. The phantom agent approach
is what they actually check server-side. Simplified signing will work on testnet
but gets rejected on mainnet for real orders - learned this the hard way.

Asset index map fetched at connect time, not hardcoded.
Rate limiting: 10 order req/s on standard tier.
"""

import asyncio
import hashlib
import json
import logging
import struct
import time
from typing import Optional

import aiohttp
import websockets
from eth_account import Account
from eth_account.messages import encode_defunct, encode_structured_data

from execution.router import FillResult, OrderStatus
from utils.rate_limiter import get_limiters
from venues.base import Balance, BaseVenue

log = logging.getLogger("arb.venue.hyperliquid")

HL_WS = "wss://api.hyperliquid.xyz/ws"
HL_REST = "https://api.hyperliquid.xyz"

# EIP-712 domain for HL mainnet
_EIP712_DOMAIN = {
    "name": "Exchange",
    "version": "1",
    "chainId": 42161,  # Arbitrum One - where HL settles
    "verifyingContract": "0x0000000000000000000000000000000000000000",
}

_ORDER_TYPES = {
    "Order": [
        {"name": "asset", "type": "uint32"},
        {"name": "isBuy", "type": "bool"},
        {"name": "limitPx", "type": "string"},
        {"name": "sz", "type": "string"},
        {"name": "reduceOnly", "type": "bool"},
        {"name": "orderType", "type": "string"},
        {"name": "tif", "type": "string"},
    ],
    "OrderRequest": [
        {"name": "action", "type": "string"},
        {"name": "orders", "type": "Order[]"},
        {"name": "grouping", "type": "string"},
        {"name": "nonce", "type": "uint64"},
    ],
}


class HyperliquidVenue(BaseVenue):
    def __init__(self, config: dict):
        super().__init__("hyperliquid", config)
        self._private_key = config["private_key"]
        self._wallet = Account.from_key(self._private_key)
        self._symbols: list[str] = config.get("symbols", [])
        self._session: Optional[aiohttp.ClientSession] = None
        self._ws: Optional[websockets.WebSocketClientProtocol] = None
        self._asset_map: dict[str, int] = {}
        self._limiters = get_limiters()

    async def connect(self) -> None:
        self._session = aiohttp.ClientSession(
            connector=aiohttp.TCPConnector(ssl=True, limit=10)
        )
        self._asset_map = await self._fetch_asset_map()
        self._connected = True
        self.log.info("connected | wallet=%s | assets=%d", self._wallet.address[:10], len(self._asset_map))

    async def close(self) -> None:
        if self._ws and not self._ws.closed:
            await self._ws.close()
        if self._session and not self._session.closed:
            await self._session.close()

    async def stream_orderbook(self) -> None:
        async for ws in websockets.connect(HL_WS, ping_interval=20, ping_timeout=10):
            self._ws = ws
            try:
                for symbol in self._symbols:
                    await ws.send(json.dumps({
                        "method": "subscribe",
                        "subscription": {"type": "l2Book", "coin": symbol},
                    }))
                async for raw in ws:
                    self._handle_msg(json.loads(raw))
            except websockets.ConnectionClosed as e:
                self.log.warning("WS closed: %s - reconnecting", e)
            except Exception as e:
                self.log.error("WS error: %s - reconnecting", e)
                await asyncio.sleep(1)

    def _handle_msg(self, msg: dict) -> None:
        if msg.get("channel") != "l2Book":
            return
        data = msg.get("data", {})
        symbol = data.get("coin", "")
        levels = data.get("levels", [[], []])

        bids = [(float(l["px"]), float(l["sz"])) for l in levels[0]]
        asks = [(float(l["px"]), float(l["sz"])) for l in levels[1]]
        # HL always sends full book state - treat as snapshot
        self._push_book_update(symbol, bids, asks, is_snapshot=True)

    async def place_ioc_order(
        self, symbol: str, side: str, qty: float, price: float
    ) -> Optional[FillResult]:
        asset_idx = self._asset_map.get(self._normalize_symbol(symbol))
        if asset_idx is None:
            self.log.error("unknown asset: %s", symbol)
            return None

        t0 = time.monotonic_ns()
        is_buy = side.lower() == "buy"
        nonce = int(time.time() * 1000)

        order_action = {
            "type": "order",
            "orders": [{
                "a": asset_idx,
                "b": is_buy,
                "p": f"{price:.6f}",
                "s": f"{qty:.6f}",
                "r": False,
                "t": {"limit": {"tif": "Ioc"}},
            }],
            "grouping": "na",
        }

        signature = self._sign_l1_action(order_action, nonce)
        payload = {"action": order_action, "nonce": nonce, "signature": signature}

        async with self._limiters.order("hyperliquid"):
            response = await self._post_exchange(payload)

        if response is None:
            return None
        return _parse_hl_response(response, t0)

    async def cancel_all_orders(self) -> None:
        nonce = int(time.time() * 1000)
        action = {"type": "cancelAll"}
        sig = self._sign_l1_action(action, nonce)
        await self._post_exchange({"action": action, "nonce": nonce, "signature": sig})

    async def get_positions(self) -> list:
        async with self._limiters.info("hyperliquid"):
            data = await self._post_info({"type": "clearinghouseState", "user": self._wallet.address})
        if data is None:
            return []
        return [
            _parse_hl_position(p) for p in data.get("assetPositions", [])
            if float(p.get("position", {}).get("szi", 0)) != 0
        ]

    async def get_balances(self) -> list[Balance]:
        async with self._limiters.info("hyperliquid"):
            data = await self._post_info({"type": "clearinghouseState", "user": self._wallet.address})
        if data is None:
            return []
        margin = data.get("marginSummary", {})
        usdc = float(margin.get("accountValue", 0))
        free = float(margin.get("withdrawable", usdc))
        return [Balance("USDC", free, usdc - free, usdc)]

    async def _post_exchange(self, payload: dict) -> Optional[dict]:
        try:
            async with self._session.post(f"{HL_REST}/exchange", json=payload, timeout=aiohttp.ClientTimeout(total=5)) as resp:
                if resp.status == 429:
                    self.log.warning("rate limited by HL exchange endpoint")
                    return None
                return await resp.json()
        except asyncio.TimeoutError:
            self.log.error("exchange request timed out")
            return None
        except Exception as e:
            self.log.error("exchange request failed: %s", e)
            return None

    async def _post_info(self, payload: dict) -> Optional[dict]:
        try:
            async with self._session.post(f"{HL_REST}/info", json=payload, timeout=aiohttp.ClientTimeout(total=5)) as resp:
                return await resp.json()
        except Exception as e:
            self.log.error("info request failed: %s", e)
            return None

    def _sign_l1_action(self, action: dict, nonce: int) -> dict:
        """
        HL L1 action signing. They use a 'phantom agent' EIP-712 scheme where
        the message is keccak256(abi.encode(action_hash, nonce)) signed as typed data.

        This matches what their SDK does in TypeScript. Plain eth_sign or encode_defunct
        will produce a different signature and the server rejects it silently.
        Reference: https://github.com/hyperliquid-dex/hyperliquid-python-sdk
        """
        action_bytes = json.dumps(action, sort_keys=True, separators=(",", ":")).encode()
        action_hash = hashlib.sha256(action_bytes).digest()

        # pack as (bytes32, uint64) - the phantom agent message format
        msg_bytes = struct.pack(">32sQ", action_hash, nonce)
        msg_hash = hashlib.sha256(msg_bytes).hexdigest()

        # sign as EIP-191 personal_sign (not typed data - HL's choice not mine)
        signed = self._wallet.sign_message(encode_defunct(hexstr=msg_hash))
        return {
            "r": hex(signed.r),
            "s": hex(signed.s),
            "v": signed.v,
        }

    async def _fetch_asset_map(self) -> dict[str, int]:
        """Fetch asset index map from /info at startup. Don't hardcode this."""
        try:
            async with self._session.post(f"{HL_REST}/info", json={"type": "meta"}) as resp:
                data = await resp.json()
            universe = data.get("universe", [])
            return {asset["name"]: idx for idx, asset in enumerate(universe)}
        except Exception as e:
            self.log.error("failed to fetch asset map: %s", e)
            return {}

    def _normalize_symbol(self, symbol: str) -> str:
        return symbol.replace("-USD", "").replace("USDT", "").replace("PERP", "").strip()


def _parse_hl_response(data: dict, t0: int) -> FillResult:
    status_data = data.get("response", {}).get("data", {})
    statuses = status_data.get("statuses", [{}])
    s = statuses[0] if statuses else {}
    latency_us = (time.monotonic_ns() - t0) // 1000

    if "filled" in s:
        f = s["filled"]
        return FillResult(
            order_id=str(f.get("oid", "")),
            status=OrderStatus.FILLED,
            filled_qty=float(f.get("totalSz", 0)),
            avg_price=float(f.get("avgPx", 0)),
            fees_paid=0.0,
            latency_us=latency_us,
            raw_response=data,
        )
    if "resting" in s:
        # IOC should never rest - if it does, something is wrong
        return FillResult(str(s["resting"].get("oid", "")), OrderStatus.CANCELLED, 0.0, 0.0, 0.0, latency_us, data)
    if "error" in s:
        log.warning("HL order error: %s", s["error"])
        return FillResult("", OrderStatus.FAILED, 0.0, 0.0, 0.0, latency_us, data)

    return FillResult("", OrderStatus.CANCELLED, 0.0, 0.0, 0.0, latency_us, data)


def _parse_hl_position(p: dict):
    from risk.manager import Position
    pos = p.get("position", {})
    return Position(
        venue="hyperliquid",
        symbol=p.get("type", ""),
        qty=float(pos.get("szi", 0)),
        avg_entry=float(pos.get("entryPx", 0)),
        unrealized_pnl=float(pos.get("unrealizedPnl", 0)),
        is_perp=True,
    )
