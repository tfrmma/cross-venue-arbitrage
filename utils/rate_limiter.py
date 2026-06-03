"""
Token bucket rate limiter for REST endpoints.

Every venue has hard rate limits. Hitting them mid-arb is expensive
(you get a 429, the order doesn't go through, you're left with a naked leg).
This runs ahead of every REST call.

Using asyncio.Semaphore for the concurrency limit and a sliding window
for the per-second request count. Two separate constraints because venues
enforce both independently.
"""

import asyncio
import collections
import logging
import time
from dataclasses import dataclass
from typing import Optional

log = logging.getLogger("arb.ratelimit")


@dataclass
class RateLimitConfig:
    requests_per_second: float
    max_concurrent: int
    burst: int = 0          # extra tokens at startup; 0 = same as rps
    retry_on_429: bool = True
    max_retries: int = 3


class RateLimiter:
    """
    Sliding window (requests_per_second) + semaphore (max_concurrent).
    Both constraints must be satisfied before a request proceeds.

    The sliding window is a deque of timestamps, O(1) amortized.
    """

    def __init__(self, config: RateLimitConfig, name: str = ""):
        self._cfg = config
        self._name = name
        self._window_s = 1.0 / config.requests_per_second
        self._timestamps: collections.deque[float] = collections.deque()
        self._semaphore = asyncio.Semaphore(config.max_concurrent)
        self._lock = asyncio.Lock()

    async def acquire(self) -> None:
        async with self._semaphore:
            await self._wait_for_slot()

    async def __aenter__(self):
        await self.acquire()
        return self

    async def __aexit__(self, *_):
        pass

    async def _wait_for_slot(self) -> None:
        while True:
            async with self._lock:
                now = time.monotonic()
                window_start = now - 1.0  # 1-second rolling window

                # evict expired timestamps
                while self._timestamps and self._timestamps[0] < window_start:
                    self._timestamps.popleft()

                if len(self._timestamps) < self._cfg.requests_per_second + self._cfg.burst:
                    self._timestamps.append(now)
                    return

                # need to wait until the oldest request ages out
                wait = self._timestamps[0] - window_start
                log.debug("%s rate limit: waiting %.3fs", self._name, wait)

            await asyncio.sleep(max(wait, 0.001))


class VenueRateLimiters:
    """
    One RateLimiter per venue per endpoint category (order vs info).
    Order endpoints are more critical and get their own bucket.
    """

    # Default limits per venue. These are conservative - adjust to your tier.
    DEFAULTS: dict[str, dict] = {
        "binance": {
            "order": RateLimitConfig(requests_per_second=10, max_concurrent=5, burst=5),
            "info":  RateLimitConfig(requests_per_second=20, max_concurrent=10),
        },
        "bybit": {
            "order": RateLimitConfig(requests_per_second=10, max_concurrent=5),
            "info":  RateLimitConfig(requests_per_second=20, max_concurrent=10),
        },
        "kraken": {
            "order": RateLimitConfig(requests_per_second=1, max_concurrent=2),  # Kraken is strict
            "info":  RateLimitConfig(requests_per_second=5, max_concurrent=5),
        },
        "hyperliquid": {
            "order": RateLimitConfig(requests_per_second=10, max_concurrent=5),
            "info":  RateLimitConfig(requests_per_second=20, max_concurrent=10),
        },
        "dydx": {
            "order": RateLimitConfig(requests_per_second=5, max_concurrent=3),
            "info":  RateLimitConfig(requests_per_second=10, max_concurrent=5),
        },
        "lighter": {
            "order": RateLimitConfig(requests_per_second=5, max_concurrent=3),
            "info":  RateLimitConfig(requests_per_second=10, max_concurrent=5),
        },
    }

    def __init__(self, overrides: Optional[dict] = None):
        self._limiters: dict[str, dict[str, RateLimiter]] = {}
        cfg = {**self.DEFAULTS, **(overrides or {})}
        for venue, endpoints in cfg.items():
            self._limiters[venue] = {
                ep: RateLimiter(ep_cfg, name=f"{venue}.{ep}")
                for ep, ep_cfg in endpoints.items()
            }

    def get(self, venue: str, endpoint: str = "order") -> RateLimiter:
        venue_limiters = self._limiters.get(venue)
        if venue_limiters is None:
            # unknown venue - return a permissive default
            log.warning("no rate limit config for venue %s, using defaults", venue)
            return RateLimiter(RateLimitConfig(requests_per_second=5, max_concurrent=3), name=venue)
        return venue_limiters.get(endpoint, venue_limiters.get("order"))

    def order(self, venue: str) -> RateLimiter:
        return self.get(venue, "order")

    def info(self, venue: str) -> RateLimiter:
        return self.get(venue, "info")


# module-level singleton - venues import this directly
_global_limiters: Optional[VenueRateLimiters] = None


def get_limiters(overrides: Optional[dict] = None) -> VenueRateLimiters:
    global _global_limiters
    if _global_limiters is None:
        _global_limiters = VenueRateLimiters(overrides)
    return _global_limiters
