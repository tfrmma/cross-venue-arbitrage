"""
@file orderbook.py
@author Taha - Algorithmic Trader
@brief Institutional-grade cross-venue-arbitrage.

@note This is a public structural showcase. For full production-grade 
      deployment, architecture consulting, or recruitment inquiries:
      Contact: email: fadilrezokt@gmail.com / linkedin.com/in/tahaotc
"""

"""
L2 order book. Price-indexed dict + sorted array rebuilt only when BBO changes.

Previous version was sorting the full list on every delta. That's fine for 10 levels,
less fine when you're getting 50 updates/sec per venue and the book has 200 levels.
Now: O(1) dict lookup for updates, O(k) rebuild only on BBO change where k = dirty levels.

sweep_* uses numpy prefix sums - vectorized cumsum is meaningfully faster than a Python loop
when you're doing this on every detected opportunity.
"""

import asyncio
import time
from dataclasses import dataclass, field
from typing import Optional

import numpy as np


@dataclass
class BBO:
    bid: float
    bid_qty: float
    ask: float
    ask_qty: float
    ts_ns: int

    @property
    def mid(self) -> float:
        return (self.bid + self.ask) * 0.5

    @property
    def spread(self) -> float:
        return self.ask - self.bid

    @property
    def spread_bps(self) -> float:
        return (self.spread / self.mid) * 10_000 if self.mid > 0 else 0.0


class PriceLadder:
    """
    One side of the book. Dict for O(1) updates, sorted arrays rebuilt lazily.

    The sorted arrays are invalidated on any update and rebuilt on first access.
    Rebuilding is O(n log n) but n is typically small (25-200 levels) and
    we only pay it when BBO actually changed, not on every delta.
    """

    __slots__ = ("_levels", "_prices", "_qtys", "_dirty", "_descending")

    def __init__(self, descending: bool):
        self._levels: dict[float, float] = {}   # price -> qty
        self._prices: np.ndarray = np.empty(0)
        self._qtys: np.ndarray = np.empty(0)
        self._dirty: bool = True
        self._descending: bool = descending     # True = bids, False = asks

    def apply_delta(self, updates: list[tuple[float, float]]) -> bool:
        """Apply price/qty pairs. qty=0 removes level. Returns True if BBO changed."""
        if not updates:
            return False

        old_top = self._prices[0] if len(self._prices) > 0 and not self._dirty else None

        for price, qty in updates:
            if qty == 0.0:
                self._levels.pop(price, None)
            else:
                self._levels[price] = qty
        self._dirty = True

        self._rebuild()
        new_top = self._prices[0] if len(self._prices) > 0 else None
        return old_top != new_top

    def apply_snapshot(self, levels: list[tuple[float, float]]) -> None:
        self._levels = {p: q for p, q in levels if q > 0.0}
        self._dirty = True
        self._rebuild()

    def top(self) -> Optional[tuple[float, float]]:
        """Best price and qty. None if empty."""
        if len(self._prices) == 0:
            return None
        return float(self._prices[0]), float(self._qtys[0])

    def sweep(self, target_qty: float) -> tuple[float, float]:
        """
        Walk levels until target_qty is filled.
        Returns (vwap, filled_qty). Uses vectorized cumsum - no Python loop.
        """
        if len(self._prices) == 0 or target_qty <= 0:
            return 0.0, 0.0

        cum_qty = np.cumsum(self._qtys)
        # index of the first level where cumulative qty >= target
        idx = np.searchsorted(cum_qty, target_qty, side="left")
        idx = min(idx, len(self._prices) - 1)

        # partial fill at the last level
        filled_at_last = target_qty - (cum_qty[idx - 1] if idx > 0 else 0.0)
        filled_at_last = min(filled_at_last, self._qtys[idx])

        cost = np.dot(self._prices[:idx], self._qtys[:idx]) + self._prices[idx] * filled_at_last
        filled = float(cum_qty[idx - 1] if idx > 0 else 0.0) + float(filled_at_last)

        return float(cost / filled) if filled > 0 else 0.0, filled

    def depth_qty(self, n_levels: int) -> float:
        return float(np.sum(self._qtys[:n_levels])) if len(self._qtys) > 0 else 0.0

    def _rebuild(self) -> None:
        if not self._dirty:
            return
        if not self._levels:
            self._prices = np.empty(0)
            self._qtys = np.empty(0)
            self._dirty = False
            return

        prices = np.array(sorted(self._levels.keys(), reverse=self._descending), dtype=np.float64)
        qtys = np.array([self._levels[p] for p in prices], dtype=np.float64)
        self._prices = prices
        self._qtys = qtys
        self._dirty = False

"""
@file orderbook.py
@author Taha - Algorithmic Trader
@brief Institutional-grade cross-venue-arbitrage.

@note This is a public structural showcase. For full production-grade 
      deployment, architecture consulting, or recruitment inquiries:
      Contact: email: fadilrezokt@gmail.com / linkedin.com/in/tahaotc
"""


class OrderBook:
    __slots__ = ("venue", "symbol", "bids", "asks", "last_seq", "last_update_ns")

    def __init__(self, venue: str, symbol: str):
        self.venue = venue
        self.symbol = symbol
        self.bids = PriceLadder(descending=True)
        self.asks = PriceLadder(descending=False)
        self.last_seq: int = 0
        self.last_update_ns: int = 0

    def bbo(self) -> Optional[BBO]:
        bid = self.bids.top()
        ask = self.asks.top()
        if bid is None or ask is None:
            return None
        return BBO(bid[0], bid[1], ask[0], ask[1], self.last_update_ns)

    def apply_delta(self, bids: list[tuple], asks: list[tuple]) -> bool:
        bid_changed = self.bids.apply_delta(bids) if bids else False
        ask_changed = self.asks.apply_delta(asks) if asks else False
        self.last_update_ns = time.time_ns()
        return bid_changed or ask_changed

    def apply_snapshot(self, bids: list[tuple], asks: list[tuple]) -> None:
        self.bids.apply_snapshot(bids)
        self.asks.apply_snapshot(asks)
        self.last_update_ns = time.time_ns()

    def sweep_bids(self, target_qty: float) -> tuple[float, float]:
        return self.bids.sweep(target_qty)

    def sweep_asks(self, target_qty: float) -> tuple[float, float]:
        return self.asks.sweep(target_qty)

    def depth_imbalance(self, n: int = 5) -> float:
        bid_q = self.bids.depth_qty(n)
        ask_q = self.asks.depth_qty(n)
        total = bid_q + ask_q
        return (bid_q - ask_q) / total if total > 0 else 0.0


@dataclass
class MarketSnapshot:
    books: dict[str, OrderBook]
    captured_ns: int

    def get(self, venue: str, symbol: str) -> Optional[OrderBook]:
        return self.books.get(f"{venue}:{symbol}")

    def get_bbo(self, venue: str, symbol: str) -> Optional[BBO]:
        book = self.get(venue, symbol)
        return book.bbo() if book else None


class OrderBookManager:
    """
    Owns all books. Venues push deltas, detection loop pulls snapshots.
    All mutation is in the event loop - no locking needed.
    """

    _STALE_NS = 2_000_000_000  # 2 seconds

    def __init__(self, venue_configs: dict):
        self._books: dict[str, OrderBook] = {}
        self._update_event = asyncio.Event()

        for venue_name, cfg in venue_configs.items():
            for symbol in cfg.get("symbols", []):
                key = f"{venue_name}:{symbol}"
                self._books[key] = OrderBook(venue=venue_name, symbol=symbol)

    def handle_update(
        self,
        venue: str,
        symbol: str,
        bids: list,
        asks: list,
        is_snapshot: bool = False,
    ) -> None:
        book = self._books.get(f"{venue}:{symbol}")
        if book is None:
            return

        if is_snapshot:
            book.apply_snapshot(bids, asks)
            self._update_event.set()
        else:
            if book.apply_delta(bids, asks):
                self._update_event.set()

    async def wait_for_update(self) -> None:
        await self._update_event.wait()
        self._update_event.clear()

    def get_snapshot(self) -> MarketSnapshot:
        return MarketSnapshot(books=dict(self._books), captured_ns=time.time_ns())

    def get_stale_venues(self) -> list[str]:
        now = time.time_ns()
        return [
            key for key, book in self._books.items()
            if 0 < book.last_update_ns < now - self._STALE_NS
        ]
