"""
Risk manager.

Keeps the engine from blowing up. Which it will try to do regularly.
"""

import logging
import time
from collections import deque
from dataclasses import dataclass, field
from enum import Enum
from typing import Optional

from core.opportunity import Opportunity

log = logging.getLogger("arb.risk")


class TradingState(str, Enum):
    ACTIVE = "active"
    PAUSED = "paused"       # soft stop - waiting for conditions to improve
    HALTED = "halted"       # hard stop - manual intervention required


@dataclass
class Position:
    venue: str
    symbol: str
    qty: float          # positive = long, negative = short
    avg_entry: float
    unrealized_pnl: float = 0.0
    is_perp: bool = False


@dataclass
class CircuitBreakerConfig:
    max_consecutive_failures: int = 5
    max_drawdown_usd: float = 1000.0
    max_slippage_bps: float = 20.0
    stale_feed_threshold_ms: float = 2000.0
    max_open_notional_usd: float = 500_000.0


@dataclass
class RiskState:
    state: TradingState = TradingState.ACTIVE
    consecutive_failures: int = 0
    realized_pnl: float = 0.0
    peak_pnl: float = 0.0
    current_drawdown: float = 0.0
    net_delta_usd: float = 0.0
    open_notional_usd: float = 0.0
    halt_reason: str = ""


class RiskManager:
    def __init__(self, config: dict):
        self.cfg = config
        self.cb = CircuitBreakerConfig(**config.get("circuit_breaker", {}))

        self._state = RiskState()
        self._recent_pnl: deque[float] = deque(maxlen=100)
        self._open_orders: dict[str, float] = {}  # order_id -> notional
        self._positions: dict[str, Position] = {}

        # funding rate state per perp symbol
        self._funding_rates: dict[str, float] = {}
        self._funding_ts: dict[str, int] = {}

        self._rebalance_threshold = config.get("rebalance_threshold_pct", 0.20)
        self._delta_threshold_usd = config.get("delta_threshold_usd", 5_000.0)

    # ---- state queries ----

    def is_trading_allowed(self) -> bool:
        return self._state.state == TradingState.ACTIVE

    def get_state(self) -> RiskState:
        return self._state

    # ---- opportunity gating ----

    def approve_opportunity(self, opp: Opportunity) -> bool:
        if not self.is_trading_allowed():
            return False

        notional = opp.size * opp.buy_leg.expected_price

        if self._state.open_notional_usd + notional > self.cb.max_open_notional_usd:
            log.debug("opp %s rejected: notional limit", opp.id)
            return False

        if opp.buy_leg.estimated_slippage * 10_000 > self.cb.max_slippage_bps:
            log.debug("opp %s rejected: slippage too high", opp.id)
            return False

        return True

    def register_order(self, order_id: str, notional: float) -> None:
        self._open_orders[order_id] = notional
        self._state.open_notional_usd += notional

    def release_order(self, order_id: str) -> None:
        notional = self._open_orders.pop(order_id, 0.0)
        self._state.open_notional_usd = max(0.0, self._state.open_notional_usd - notional)

    # ---- execution feedback ----

    def record_trade_result(self, success: bool, pnl: float = 0.0) -> None:
        if success:
            self._state.consecutive_failures = 0
            self._state.realized_pnl += pnl
            self._recent_pnl.append(pnl)
            self._update_drawdown()
        else:
            self._state.consecutive_failures += 1
            log.warning(
                "consecutive failures: %d/%d",
                self._state.consecutive_failures,
                self.cb.max_consecutive_failures,
            )
            self._check_circuit_breakers()

    def record_slippage(self, actual_bps: float) -> None:
        if actual_bps > self.cb.max_slippage_bps:
            log.warning("slippage breach: %.1f bps > %.1f bps limit", actual_bps, self.cb.max_slippage_bps)
            self._state.consecutive_failures += 1
            self._check_circuit_breakers()

    # ---- periodic audit ----

    def audit(self, positions: dict[str, list[Position]], recent_pnl: list[float]) -> None:
        self._sync_positions(positions)
        self._check_delta()
        self._check_funding_exposure()
        self._check_drawdown_limit()

        if self._state.state == TradingState.PAUSED:
            if self._conditions_recovered():
                log.info("conditions recovered, resuming trading")
                self._state.state = TradingState.ACTIVE
                self._state.consecutive_failures = 0

    def handle_feed_stale(self, venue: str, stale_ms: float) -> None:
        """Called by engine when a feed goes silent."""
        if stale_ms > self.cb.stale_feed_threshold_ms:
            log.error("feed stale: %s (%.0f ms) - halting", venue, stale_ms)
            self._halt(f"stale feed: {venue}")

    def update_funding_rate(self, symbol: str, rate: float) -> None:
        self._funding_rates[symbol] = rate
        self._funding_ts[symbol] = time.time_ns()

    # ---- rebalancing ----

    def check_rebalance_needed(self, balances: dict[str, dict]) -> list[dict]:
        """
        Check inventory skew. Returns list of rebalance ops if skew > threshold.
        Rebalance ops are just dicts for now - ugly but avoids circular imports.
        TODO: proper RebalanceOrder dataclass
        """
        ops = []
        total_usd = sum(
            sum(b.get("usd_value", 0) for b in venue_balances.values())
            for venue_balances in balances.values()
        )
        if total_usd == 0:
            return ops

        for venue, venue_balances in balances.items():
            venue_usd = sum(b.get("usd_value", 0) for b in venue_balances.values())
            share = venue_usd / total_usd
            target_share = self.cfg.get("target_venue_allocation", {}).get(venue, 1.0 / len(balances))

            skew = abs(share - target_share)
            if skew > self._rebalance_threshold:
                ops.append({
                    "venue": venue,
                    "current_share": share,
                    "target_share": target_share,
                    "delta_usd": (target_share - share) * total_usd,
                })
                log.info(
                    "rebalance needed for %s: %.1f%% -> %.1f%%",
                    venue, share * 100, target_share * 100,
                )

        return ops

    def get_recent_pnl(self) -> list[float]:
        return list(self._recent_pnl)

    # ---- internal ----

    def _sync_positions(self, raw_positions: dict[str, list]) -> None:
        self._positions.clear()
        for venue, pos_list in raw_positions.items():
            for p in pos_list:
                key = f"{venue}:{p.symbol}"
                self._positions[key] = p

    def _check_delta(self) -> None:
        net_delta = sum(
            p.qty * p.avg_entry * (1 if not p.is_perp else -1)
            for p in self._positions.values()
        )
        self._state.net_delta_usd = net_delta

        if abs(net_delta) > self._delta_threshold_usd:
            log.warning("delta exposure: $%.0f (limit $%.0f)", net_delta, self._delta_threshold_usd)
            # don't halt on this, just log - hedging is the execution router's job

    def _check_funding_exposure(self) -> None:
        """Warn if we're on the paying side of a large funding rate."""
        for symbol, rate in self._funding_rates.items():
            if abs(rate) > 0.001:  # 0.1% per 8h is getting spicy
                pos_key = next((k for k in self._positions if k.endswith(f":{symbol}")), None)
                if pos_key:
                    pos = self._positions[pos_key]
                    exposure = pos.qty * pos.avg_entry * rate
                    if exposure < -100:  # paying > $100 per funding tick
                        log.warning(
                            "funding cost exposure on %s: $%.2f/tick @ rate %.4f%%",
                            symbol, exposure, rate * 100,
                        )

    def _check_circuit_breakers(self) -> None:
        if self._state.consecutive_failures >= self.cb.max_consecutive_failures:
            self._pause(f"consecutive failures: {self._state.consecutive_failures}")

    def _check_drawdown_limit(self) -> None:
        if self._state.current_drawdown > self.cb.max_drawdown_usd:
            self._halt(f"max drawdown breached: ${self._state.current_drawdown:.2f}")

    def _update_drawdown(self) -> None:
        pnl = self._state.realized_pnl
        if pnl > self._state.peak_pnl:
            self._state.peak_pnl = pnl
        self._state.current_drawdown = self._state.peak_pnl - pnl

    def _pause(self, reason: str) -> None:
        if self._state.state == TradingState.ACTIVE:
            log.warning("pausing trading: %s", reason)
            self._state.state = TradingState.PAUSED

    def _halt(self, reason: str) -> None:
        log.error("HALTING trading: %s", reason)
        self._state.state = TradingState.HALTED
        self._state.halt_reason = reason

    def _conditions_recovered(self) -> bool:
        # simple: no failures in last 10s and drawdown is below 50% of limit
        return (
            self._state.consecutive_failures == 0
            and self._state.current_drawdown < self.cb.max_drawdown_usd * 0.5
        )
