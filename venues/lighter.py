"""
Lighter connector. EVM-based CLOB DEX.

Lighter is interesting - on-chain orderbook, not AMM.
Order placement is an EVM tx. Feed comes from their indexer WS.
Use their SDK or call the contracts directly.

Atomic execution: orders placed via custom executor contract that
asserts min_price before executing. Reverts if price moved.
"""

import asyncio
import json
import logging
import time
from typing import Optional

import aiohttp
import websockets
from web3 import AsyncWeb3, AsyncHTTPProvider
from web3.exceptions import ContractLogicError

from execution.router import FillResult, OrderStatus
from venues.base import Balance, BaseVenue

log = logging.getLogger("arb.venue.lighter")

LIGHTER_WS = "wss://mainnet.zklighter.elliot.ai/stream"
LIGHTER_REST = "https://mainnet.zklighter.elliot.ai"
LIGHTER_RPC = "https://mainnet.zklighter.elliot.ai/rpc"


# Minimal ABI for our executor contract
# Full contract in contracts/LighterExecutor.sol
EXECUTOR_ABI = [
    {
        "name": "executeWithMinPrice",
        "type": "function",
        "inputs": [
            {"name": "marketId", "type": "uint32"},
            {"name": "isAsk", "type": "bool"},
            {"name": "amount", "type": "uint64"},
            {"name": "minPrice", "type": "uint64"},
        ],
        "outputs": [{"name": "filledAmount", "type": "uint64"}],
    }
]


class LighterVenue(BaseVenue):
    def __init__(self, config: dict):
        super().__init__("lighter", config)
        self._private_key = config["private_key"]
        self._symbols: list[str] = config.get("symbols", [])
        self._executor_address = config.get("executor_contract", "")
        self._market_ids: dict[str, int] = config.get("market_ids", {})
        self._session: Optional[aiohttp.ClientSession] = None
        self._w3: Optional[AsyncWeb3] = None
        self._executor = None
        self._account = None

    async def connect(self) -> None:
        self._session = aiohttp.ClientSession(
            connector=aiohttp.TCPConnector(ssl=True)
        )
        self._w3 = AsyncWeb3(AsyncHTTPProvider(LIGHTER_RPC))
        self._account = self._w3.eth.account.from_key(self._private_key)

        if self._executor_address:
            self._executor = self._w3.eth.contract(
                address=self._executor_address, abi=EXECUTOR_ABI
            )

        self._connected = True
        self.log.info("connected (wallet: %s)", self._account.address[:10])

    async def close(self) -> None:
        if self._session:
            await self._session.close()

    async def stream_orderbook(self) -> None:
        async for ws in websockets.connect(LIGHTER_WS, ping_interval=20):
            try:
                for symbol in self._symbols:
                    await ws.send(json.dumps({
                        "type": "subscribe",
                        "topic": "orderbook",
                        "market": symbol,
                    }))
                async for raw in ws:
                    self._handle_msg(json.loads(raw))
            except websockets.ConnectionClosed as e:
                self.log.warning("WS closed: %s - reconnecting", e)

    def _handle_msg(self, msg: dict) -> None:
        if msg.get("topic") != "orderbook":
            return
        symbol = msg.get("market", "")
        data = msg.get("data", {})

        bids = [(float(l["price"]), float(l["amount"])) for l in data.get("bids", [])]
        asks = [(float(l["price"]), float(l["amount"])) for l in data.get("asks", [])]
        is_snapshot = msg.get("type") == "snapshot"

        if bids or asks:
            self._push_book_update(symbol, bids, asks, is_snapshot)

    async def place_ioc_order(
        self, symbol: str, side: str, qty: float, price: float
    ) -> Optional[FillResult]:
        """Standard limit IOC via Lighter's order API."""
        t0 = time.monotonic_ns()
        payload = {
            "market": symbol,
            "side": side.lower(),
            "amount": str(qty),
            "price": str(price),
            "type": "ioc",
        }
        sig = self._sign_order(payload)

        try:
            async with self._session.post(
                f"{LIGHTER_REST}/order", json={**payload, "signature": sig}
            ) as resp:
                data = await resp.json()
        except Exception as e:
            self.log.error("place_ioc failed: %s", e)
            return None

        return _parse_lighter_response(data, t0)

    async def execute_atomic_swap(
        self, symbol: str, side: str, qty: float, min_price: float
    ) -> Optional[FillResult]:
        """
        Execute via our executor contract. Will revert if price < min_price.
        This is the whole point of having a custom contract.

        Gas price: use priority fee from the sequencer tip API.
        Don't cheap out on gas here - this is where latency = money.
        """
        if self._executor is None:
            self.log.error("executor contract not configured")
            return None

        t0 = time.monotonic_ns()
        market_id = self._market_ids.get(symbol, 0)
        is_ask = side.lower() == "sell"

        # scale to contract units (usually 1e6 or 1e8 depending on market)
        amount_raw = int(qty * 1e6)
        min_price_raw = int(min_price * 1e6)

        try:
            gas_price = await self._get_priority_gas_price()
            tx = await self._executor.functions.executeWithMinPrice(
                market_id, is_ask, amount_raw, min_price_raw
            ).build_transaction({
                "from": self._account.address,
                "maxFeePerGas": gas_price["max_fee"],
                "maxPriorityFeePerGas": gas_price["priority_fee"],
                "nonce": await self._w3.eth.get_transaction_count(self._account.address),
            })

            signed = self._account.sign_transaction(tx)
            tx_hash = await self._w3.eth.send_raw_transaction(signed.rawTransaction)
            receipt = await self._w3.eth.wait_for_transaction_receipt(tx_hash, timeout=30)

            latency_us = (time.monotonic_ns() - t0) // 1000

            if receipt["status"] == 1:
                self.log.info("atomic swap confirmed: %s (%dμs)", tx_hash.hex(), latency_us)
                return FillResult(
                    order_id=tx_hash.hex(),
                    status=OrderStatus.FILLED,
                    filled_qty=qty,  # assume full fill if contract didn't revert
                    avg_price=min_price,
                    fees_paid=0.0,  # gas cost tracked separately
                    latency_us=latency_us,
                    raw_response=dict(receipt),
                )
            else:
                return FillResult("", OrderStatus.FAILED, 0.0, 0.0, 0.0, latency_us, dict(receipt))

        except ContractLogicError as e:
            # expected - price assertion failed, tx reverted cleanly
            self.log.info("atomic swap reverted (price moved): %s", e)
            return FillResult("", OrderStatus.FAILED, 0.0, 0.0, 0.0, 0, {"revert": str(e)})
        except Exception as e:
            self.log.error("atomic swap failed unexpectedly: %s", e)
            return None

    async def cancel_all_orders(self) -> None:
        for symbol in self._symbols:
            try:
                sig = self._sign_order({"market": symbol, "type": "cancel_all"})
                async with self._session.delete(
                    f"{LIGHTER_REST}/orders/{symbol}", json={"signature": sig}
                ) as resp:
                    await resp.json()
            except Exception as e:
                self.log.error("cancel_all failed for %s: %s", symbol, e)

    async def get_positions(self) -> list:
        # Lighter is a DEX CLOB - "positions" are just open orders + token balances
        return []

    async def get_balances(self) -> list[Balance]:
        try:
            async with self._session.get(
                f"{LIGHTER_REST}/account/{self._account.address}"
            ) as resp:
                data = await resp.json()
        except Exception as e:
            self.log.error("get_balances failed: %s", e)
            return []

        balances = []
        for token, info in data.get("balances", {}).items():
            free = float(info.get("available", 0))
            locked = float(info.get("locked", 0))
            if free + locked > 0:
                balances.append(Balance(token, free, locked))
        return balances

    def _sign_order(self, payload: dict) -> str:
        # TODO: implement proper Lighter order signing
        # their signing scheme is documented at lighter.xyz/docs/api/auth
        import json as _json
        msg = _json.dumps(payload, sort_keys=True)
        from eth_account.messages import encode_defunct
        msg_hash = encode_defunct(text=msg)
        signed = self._account.sign_message(msg_hash)
        return signed.signature.hex()

    async def _get_priority_gas_price(self) -> dict:
        """Get current gas prices. Default to aggressive tip."""
        try:
            block = await self._w3.eth.get_block("latest")
            base_fee = block.get("baseFeePerGas", 10 ** 9)
            priority = 5 * 10 ** 8  # 0.5 gwei tip - adjust based on competition
            return {
                "max_fee": base_fee * 2 + priority,
                "priority_fee": priority,
            }
        except Exception:
            return {"max_fee": 3 * 10 ** 9, "priority_fee": 5 * 10 ** 8}


def _parse_lighter_response(data: dict, t0: int) -> FillResult:
    status_str = data.get("status", "")
    status_map = {"filled": OrderStatus.FILLED, "cancelled": OrderStatus.CANCELLED}
    status = status_map.get(status_str, OrderStatus.FAILED)

    return FillResult(
        order_id=data.get("order_id", ""),
        status=status,
        filled_qty=float(data.get("filled_amount", 0)),
        avg_price=float(data.get("avg_price", 0)),
        fees_paid=float(data.get("fees", 0)),
        latency_us=(time.monotonic_ns() - t0) // 1000,
        raw_response=data,
    )
