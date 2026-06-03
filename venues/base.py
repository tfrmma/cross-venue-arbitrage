"""
Venue base. Every connector inherits this.

If you're adding a new venue, implement all abstract methods.
The rest of the engine doesn't care what's underneath.
"""

import asyncio
import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Callable, Optional

log = logging.getLogger("arb.venue")


@dataclass
class Balance:
    asset: str
    free: float
    locked: float
    usd_value: float = 0.0

    @property
    def total(self) -> float:
        return self.free + self.locked


class BaseVenue(ABC):
    def __init__(self, name: str, config: dict):
        self.name = name
        self.cfg = config
        self.on_book_update: Optional[Callable] = None
        self._connected = False
        self.log = logging.getLogger(f"arb.venue.{name}")

    @abstractmethod
    async def connect(self) -> None: ...

    @abstractmethod
    async def close(self) -> None: ...

    @abstractmethod
    async def stream_orderbook(self) -> None:
        """Runs forever, pushing updates via self.on_book_update."""
        ...

    @abstractmethod
    async def place_ioc_order(
        self,
        symbol: str,
        side: str,
        qty: float,
        price: float,
    ): ...

    @abstractmethod
    async def cancel_all_orders(self) -> None: ...

    @abstractmethod
    async def get_positions(self) -> list: ...

    @abstractmethod
    async def get_balances(self) -> list[Balance]: ...

    def _push_book_update(self, symbol: str, bids: list, asks: list, is_snapshot: bool = False) -> None:
        if self.on_book_update is not None:
            self.on_book_update(self.name, symbol, bids, asks, is_snapshot)

    async def _reconnect_loop(self, connect_fn: Callable, max_retries: int = 10) -> None:
        """Generic reconnect with exponential backoff."""
        retries = 0
        while retries < max_retries:
            try:
                await connect_fn()
                retries = 0
            except Exception as e:
                wait = min(2 ** retries, 60)
                self.log.warning("connection failed (%s), retry in %ds", e, wait)
                await asyncio.sleep(wait)
                retries += 1

        self.log.error("max retries exceeded for %s - giving up", self.name)
        raise RuntimeError(f"venue {self.name} unreachable after {max_retries} retries")
