"""
dYdX v4 connector. Cosmos/CometBFT-based chain, not EVM.

Their Python client is decent but heavy. We wrap the REST/WS directly.
Perps only. Order placement goes via their indexer or validator endpoint.

Auth: ED25519 key derived from mnemonic. Fun.
"""

import asyncio
import json
import logging
import time
from typing import Optional

import aiohttp
import websockets

from execution.router import FillResult, OrderStatus
from venues.base import Balance, BaseVenue

log = logging.getLogger("arb.venue.dydx")

DYDX_WS = "wss://indexer.dydx.trade/v4/ws"
DYDX_REST = "https://indexer.dydx.trade/v4"
DYDX_VALIDATOR = "https://dydx-ops-rpc.kingnodes.com"  # or your own node


class DydxVenue(BaseVenue):
    def __init__(self, config: dict):
        super().__init__("dydx", config)
        self._mnemonic = config.get("mnemonic", "")
        self._address = config.get("address", "")
        self._symbols: list[str] = config.get("symbols", [])
        self._session: Optional[aiohttp.ClientSession] = None
        self._subaccount_id = config.get("subaccount_id", 0)

        # lazy init - needs dydx-v4-client which is optional dep
        self._client = None

    async def connect(self) -> None:
        self._session = aiohttp.ClientSession(
            connector=aiohttp.TCPConnector(ssl=True)
        )
        # TODO: init dydx-v4-client here for order placement
        # from dydx_v4_client import Client
        # self._client = await Client.connect(...)
        self._connected = True
        self.log.info("connected (address: %s)", self._address[:12])

    async def close(self) -> None:
        if self._session:
            await self._session.close()

    async def stream_orderbook(self) -> None:
        async for ws in websockets.connect(DYDX_WS, ping_interval=20):
            try:
                for symbol in self._symbols:
                    await ws.send(json.dumps({
                        "type": "subscribe",
                        "channel": "v4_orderbook",
                        "id": symbol,
                    }))
                async for raw in ws:
                    self._handle_msg(json.loads(raw))
            except websockets.ConnectionClosed as e:
                self.log.warning("WS closed: %s - reconnecting", e)

    def _handle_msg(self, msg: dict) -> None:
        msg_type = msg.get("type")
        if msg_type not in ("channel_data", "channel_batch_data"):
            return

        channel = msg.get("channel", "")
        if channel != "v4_orderbook":
            return

        symbol = msg.get("id", "")
        contents = msg.get("contents", {})

        bids = [(float(p), float(q)) for p, q in (contents.get("bids") or [])]
        asks = [(float(p), float(q)) for p, q in (contents.get("asks") or [])]

        is_snapshot = msg_type == "channel_data" and "bids" in contents and "asks" in contents

        if bids or asks:
            self._push_book_update(symbol, bids, asks, is_snapshot=is_snapshot)

    async def place_ioc_order(
        self, symbol: str, side: str, qty: float, price: float
    ) -> Optional[FillResult]:
        """
        dYdX v4 orders go on-chain (Cosmos tx). This means ~300-500ms latency.
        IOC is supported via Good-Til-Block with immediate expiry.
        Realistically, dYdX is the slower leg in any CEX/dYdX arb.
        """
        if self._client is None:
            self.log.error("dydx client not initialized")
            return None

        t0 = time.monotonic_ns()
        # TODO: implement actual order placement via dydx-v4-client
        # Placeholder - this needs the full Cosmos tx signing flow
        self.log.warning("dydx order placement not fully implemented yet")
        return FillResult("", OrderStatus.FAILED, 0.0, 0.0, 0.0, 0, {})

    async def cancel_all_orders(self) -> None:
        # dYdX cancel requires individual order cancellation by order_id
        # batch cancel is theoretically possible via multiple msgs in one tx
        # TODO: track open orders and cancel them here
        self.log.warning("dydx cancel_all not implemented - orders will expire naturally")

    async def get_positions(self) -> list:
        url = f"{DYDX_REST}/addresses/{self._address}/subaccounts/{self._subaccount_id}"
        try:
            async with self._session.get(url) as resp:
                data = await resp.json()
        except Exception as e:
            self.log.error("get_positions failed: %s", e)
            return []

        sub = data.get("subaccount", {})
        return [_parse_dydx_position(p) for p in sub.get("openPerpetualPositions", {}).values()]

    async def get_balances(self) -> list[Balance]:
        url = f"{DYDX_REST}/addresses/{self._address}/subaccounts/{self._subaccount_id}"
        try:
            async with self._session.get(url) as resp:
                data = await resp.json()
        except Exception as e:
            self.log.error("get_balances failed: %s", e)
            return []

        sub = data.get("subaccount", {})
        equity = float(sub.get("equity", 0))
        free = float(sub.get("freeCollateral", 0))
        return [Balance("USDC", free, equity - free, equity)]


def _parse_dydx_position(p: dict):
    from risk.manager import Position
    side = p.get("side", "LONG")
    qty = float(p.get("size", 0))
    return Position(
        venue="dydx",
        symbol=p.get("market", ""),
        qty=qty if side == "LONG" else -qty,
        avg_entry=float(p.get("entryPrice", 0)),
        unrealized_pnl=float(p.get("unrealizedPnl", 0)),
        is_perp=True,
    )
