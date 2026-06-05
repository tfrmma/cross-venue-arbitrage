"""
@file router.py
@author Taha - Algorithmic Trader
@brief Institutional-grade cross-venue-arbitrage.

@note This is a public structural showcase. For full production-grade 
      deployment, architecture consulting, or recruitment inquiries:
      Contact: email: fadilrezokt@gmail.com / linkedin.com/in/tahaotc
"""

"""
Execution router. The part where you actually lose money if you get it wrong.

Key invariant: never leave a naked leg open.
CEX/CEX: both IOC simultaneously, unwind whichever filled if the other didn't.
CEX→DEX: CEX first (cheap miss), DEX second (contract reverts if price moved).
DEX→CEX: contract atomicity on-chain, immediate CEX hedge after.

The unwind path is best-effort but tracked. If it also fails we halt immediately
rather than sitting on an unknown delta position. The risk manager needs to know.
"""

import asyncio
import logging
import time
from dataclasses import dataclass
from enum import Enum
from typing import Optional

from core.opportunity import Leg, Opportunity
from utils.metrics import EngineMetrics

log = logging.getLogger("arb.router")


class OrderStatus(str, Enum):
    PENDING = "pending"
    FILLED = "filled"
    PARTIAL = "partial"
    CANCELLED = "cancelled"
    FAILED = "failed"


@dataclass
class FillResult:
    order_id: str
    status: OrderStatus
    filled_qty: float
    avg_price: float
    fees_paid: float
    latency_us: int
    raw_response: dict

    @property
    def is_filled(self) -> bool:
        return self.status == OrderStatus.FILLED and self.filled_qty > 0


@dataclass
class ExecutionResult:
    opportunity_id: str
    success: bool
    buy_fill: Optional[FillResult]
    sell_fill: Optional[FillResult]
    realized_pnl: float
    total_fees: float
    actual_slippage_bps: float
    execution_time_us: int
    unwind_triggered: bool = False
    unwind_failed: bool = False     # if True, we have an open delta - halt


class ExecutionRouter:
    def __init__(self, config: dict, risk_manager, metrics: EngineMetrics):
        self.cfg = config
        self.risk = risk_manager
        self.metrics = metrics

        self._max_leg_wait_us: int = config.get("max_leg_wait_us", 500_000)
        self._dex_venues: set[str] = set(config.get("dex_venues", []))
        self._in_flight: set[str] = set()

    async def execute(self, opp: Opportunity, venues: dict) -> Optional[ExecutionResult]:
        if opp.id in self._in_flight:
            return None
        self._in_flight.add(opp.id)

        buy_venue = venues.get(opp.buy_leg.venue)
        sell_venue = venues.get(opp.sell_leg.venue)

        if buy_venue is None or sell_venue is None:
            log.error("missing venue for opp %s", opp.id)
            self._in_flight.discard(opp.id)
            return None

        notional = opp.size * opp.buy_leg.expected_price
        self.risk.register_order(opp.id, notional)
        start_ns = time.time_ns()

        try:
            result = await self._execute_legs(opp, buy_venue, sell_venue)
            result.execution_time_us = (time.time_ns() - start_ns) // 1000

            if result.unwind_failed:
                # unknown delta position - halt now, don't keep trading
                log.critical("unwind failed on opp %s - halting engine", opp.id)
                self.risk.record_slippage(9999.0)

            self.metrics.record_execution(result)
            self.risk.record_trade_result(result.success, result.realized_pnl)
            return result

        except Exception as e:
            log.exception("execution error on opp %s: %s", opp.id, e)
            self.risk.record_trade_result(False)
            return None
        finally:
            self.risk.release_order(opp.id)
            self._in_flight.discard(opp.id)

    async def _execute_legs(self, opp: Opportunity, buy_venue, sell_venue) -> ExecutionResult:
        buy_is_dex = opp.buy_leg.is_dex
        sell_is_dex = opp.sell_leg.is_dex

        if not buy_is_dex and not sell_is_dex:
            result = await self._execute_cex_cex(opp, buy_venue, sell_venue)
        elif not buy_is_dex and sell_is_dex:
            result = await self._execute_cex_then_dex(opp, buy_venue, sell_venue)
        else:
            result = await self._execute_dex_then_cex(opp, buy_venue, sell_venue)

        if result.success:
            log.info(
                "arb %s | pnl=$%.2f fees=$%.2f slip=%.1fbps",
                opp.id, result.realized_pnl, result.total_fees, result.actual_slippage_bps,
            )
        else:
            log.warning(
                "arb %s failed | unwind=%s unwind_failed=%s",
                opp.id, result.unwind_triggered, result.unwind_failed,
            )
        return result

    async def _execute_cex_cex(self, opp: Opportunity, buy_venue, sell_venue) -> ExecutionResult:
        buy_fill, sell_fill = await asyncio.gather(
            buy_venue.place_ioc_order(opp.buy_leg.symbol, "buy", opp.size, opp.buy_leg.expected_price),
            sell_venue.place_ioc_order(opp.sell_leg.symbol, "sell", opp.size, opp.sell_leg.expected_price),
            return_exceptions=True,
        )

        buy_fill = buy_fill if isinstance(buy_fill, FillResult) else None
        sell_fill = sell_fill if isinstance(sell_fill, FillResult) else None

        needs_unwind = _needs_unwind(buy_fill, sell_fill)
        unwind_failed = False

        if needs_unwind:
            unwind_failed = not await self._unwind(buy_fill, sell_fill, buy_venue, sell_venue, opp)

        if not needs_unwind and buy_fill and sell_fill and buy_fill.is_filled and sell_fill.is_filled:
            success, pnl, fees, slip = _assess_fills(opp, buy_fill, sell_fill)
            return ExecutionResult(
                opportunity_id=opp.id, success=success,
                buy_fill=buy_fill, sell_fill=sell_fill,
                realized_pnl=pnl, total_fees=fees, actual_slippage_bps=slip,
                execution_time_us=0,
            )

        return _failed_result(opp.id, buy_fill, sell_fill, unwind_triggered=needs_unwind, unwind_failed=unwind_failed)

    async def _execute_cex_then_dex(self, opp: Opportunity, cex_venue, dex_venue) -> ExecutionResult:
        buy_fill = await cex_venue.place_ioc_order(
            opp.buy_leg.symbol, "buy", opp.size, opp.buy_leg.expected_price
        )

        if buy_fill is None or not buy_fill.is_filled:
            return _failed_result(opp.id, buy_fill, None)

        # CEX filled - now committed. DEX contract reverts if price moved.
        sell_fill = await dex_venue.execute_atomic_swap(
            symbol=opp.sell_leg.symbol,
            side="sell",
            qty=buy_fill.filled_qty,
            min_price=opp.sell_leg.expected_price * 0.999,
        )

        if sell_fill is None or not sell_fill.is_filled:
            log.error("DEX leg failed for opp %s - unwinding CEX buy", opp.id)
            unwind_failed = not await self._unwind_single(
                cex_venue, opp.buy_leg.symbol, "sell", buy_fill.filled_qty
            )
            return _failed_result(opp.id, buy_fill, sell_fill, unwind_triggered=True, unwind_failed=unwind_failed)

        success, pnl, fees, slip = _assess_fills(opp, buy_fill, sell_fill)
        return ExecutionResult(
            opportunity_id=opp.id, success=success,
            buy_fill=buy_fill, sell_fill=sell_fill,
            realized_pnl=pnl, total_fees=fees, actual_slippage_bps=slip,
            execution_time_us=0,
        )

    async def _execute_dex_then_cex(self, opp: Opportunity, dex_venue, cex_venue) -> ExecutionResult:
        dex_fill = await dex_venue.execute_atomic_swap(
            symbol=opp.buy_leg.symbol,
            side="buy",
            qty=opp.size,
            min_price=opp.buy_leg.expected_price * 1.001,
        )

        if dex_fill is None or not dex_fill.is_filled:
            # contract reverted - no capital at risk
            return _failed_result(opp.id, dex_fill, None)

        hedge_fill = await cex_venue.place_ioc_order(
            opp.sell_leg.symbol, "sell", dex_fill.filled_qty, opp.sell_leg.expected_price
        )

        if hedge_fill is None or not hedge_fill.is_filled:
            # long on DEX, no hedge - this is a real position
            log.error("CEX hedge failed opp %s - delta %.4f %s OPEN", opp.id, dex_fill.filled_qty, opp.buy_leg.symbol)
            return _failed_result(opp.id, dex_fill, hedge_fill, unwind_triggered=True, unwind_failed=True)

        success, pnl, fees, slip = _assess_fills(opp, dex_fill, hedge_fill)
        return ExecutionResult(
            opportunity_id=opp.id, success=success,
            buy_fill=dex_fill, sell_fill=hedge_fill,
            realized_pnl=pnl, total_fees=fees, actual_slippage_bps=slip,
            execution_time_us=0,
        )

    async def _unwind(
        self,
        buy_fill: Optional[FillResult],
        sell_fill: Optional[FillResult],
        buy_venue,
        sell_venue,
        opp: Opportunity,
    ) -> bool:
        """
        Unwind both partial fills. Returns True if all unwinds succeeded.
        A failed unwind means unknown delta - caller should halt.
        """
        tasks = []
        if buy_fill and buy_fill.filled_qty > 0:
            tasks.append(("buy_unwind", self._unwind_single(buy_venue, opp.buy_leg.symbol, "sell", buy_fill.filled_qty)))
        if sell_fill and sell_fill.filled_qty > 0:
            tasks.append(("sell_unwind", self._unwind_single(sell_venue, opp.sell_leg.symbol, "buy", sell_fill.filled_qty)))

        if not tasks:
            return True

        results = await asyncio.gather(*[t for _, t in tasks], return_exceptions=True)
        for (name, _), result in zip(tasks, results):
            if isinstance(result, Exception) or result is False:
                log.error("unwind %s failed - open position possible: %s", name, result)
                return False
        return True

    async def _unwind_single(self, venue, symbol: str, side: str, qty: float) -> bool:
        """Market-out a single leg. Returns True if fill confirmed."""
        try:
            fill = await venue.place_ioc_order(symbol, side, qty, 0.0)
            if fill and fill.filled_qty > 0:
                log.info("unwind ok: %s %s %.4f @ %.4f", side, symbol, fill.filled_qty, fill.avg_price)
                return True
            log.error("unwind %s %s %.4f - zero fill", side, symbol, qty)
            return False
        except Exception as e:
            log.error("unwind exception %s %s: %s", side, symbol, e)
            return False

    async def cancel_all_open_orders(self, venues: dict) -> None:
        results = await asyncio.gather(
            *[venue.cancel_all_orders() for venue in venues.values()],
            return_exceptions=True,
        )
        for name, r in zip(venues.keys(), results):
            if isinstance(r, Exception):
                log.error("cancel_all failed on %s: %s", name, r)

    async def execute_rebalance(self, op: dict, venues: dict) -> None:
        # TODO: bridge routing - log for now, manual action required
        log.info("rebalance needed (manual): %s", op)


# ---- helpers ----

def _needs_unwind(buy_fill: Optional[FillResult], sell_fill: Optional[FillResult]) -> bool:
    """True if one leg filled but the other didn't - mismatched legs."""
    buy_ok = buy_fill is not None and buy_fill.is_filled
    sell_ok = sell_fill is not None and sell_fill.is_filled
    # one side filled and the other didn't, OR fills are wildly mismatched in qty
    if buy_ok and sell_ok:
        qty_skew = abs(buy_fill.filled_qty - sell_fill.filled_qty) / max(buy_fill.filled_qty, 1e-9)
        return qty_skew > 0.01  # more than 1% mismatch
    return buy_ok != sell_ok  # one filled, one didn't


def _assess_fills(
    opp: Opportunity,
    buy_fill: FillResult,
    sell_fill: FillResult,
) -> tuple[bool, float, float, float]:
    """Returns (success, pnl, fees, slippage_bps)."""
    gross = (sell_fill.avg_price - buy_fill.avg_price) * buy_fill.filled_qty
    fees = buy_fill.fees_paid + sell_fill.fees_paid
    pnl = gross - fees

    expected_mid = (opp.buy_leg.expected_price + opp.sell_leg.expected_price) * 0.5
    actual_mid = (buy_fill.avg_price + sell_fill.avg_price) * 0.5
    slip_bps = abs(actual_mid - expected_mid) / expected_mid * 10_000 if expected_mid > 0 else 0.0

    return pnl > 0, pnl, fees, slip_bps


def _failed_result(
    opp_id: str,
    buy_fill: Optional[FillResult],
    sell_fill: Optional[FillResult],
    unwind_triggered: bool = False,
    unwind_failed: bool = False,
) -> ExecutionResult:
    return ExecutionResult(
        opportunity_id=opp_id,
        success=False,
        buy_fill=buy_fill,
        sell_fill=sell_fill,
        realized_pnl=0.0,
        total_fees=0.0,
        actual_slippage_bps=0.0,
        execution_time_us=0,
        unwind_triggered=unwind_triggered,
        unwind_failed=unwind_failed,
    )
