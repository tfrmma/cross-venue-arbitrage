"""
Engine metrics. Latency histograms, PnL, fill rates.
Nothing fancy - just enough to tell if the engine is alive and printing.
"""

import logging
import time
from collections import deque
from dataclasses import dataclass, field

log = logging.getLogger("arb.metrics")


@dataclass
class ExecutionStats:
    total_attempts: int = 0
    total_fills: int = 0
    total_pnl: float = 0.0
    total_fees: float = 0.0
    avg_latency_us: float = 0.0
    fill_rate: float = 0.0


class EngineMetrics:
    def __init__(self, window: int = 1000):
        self._window = window
        self._executions: deque = deque(maxlen=window)
        self._latencies_us: deque[int] = deque(maxlen=window)
        self._pnl_history: deque[float] = deque(maxlen=window)

        self._start_time = time.time()
        self._total_attempts = 0
        self._total_fills = 0
        self._total_pnl = 0.0
        self._total_fees = 0.0

    def record_execution(self, result) -> None:
        self._total_attempts += 1
        self._executions.append(result)
        self._latencies_us.append(result.execution_time_us)

        if result.success:
            self._total_fills += 1
            self._total_pnl += result.realized_pnl
            self._total_fees += result.total_fees
            self._pnl_history.append(result.realized_pnl)

        if self._total_attempts % 100 == 0:
            self._log_summary()

    def get_recent_pnl(self) -> list[float]:
        return list(self._pnl_history)

    def stats(self) -> ExecutionStats:
        avg_lat = sum(self._latencies_us) / len(self._latencies_us) if self._latencies_us else 0.0
        fill_rate = self._total_fills / self._total_attempts if self._total_attempts > 0 else 0.0
        return ExecutionStats(
            total_attempts=self._total_attempts,
            total_fills=self._total_fills,
            total_pnl=self._total_pnl,
            total_fees=self._total_fees,
            avg_latency_us=avg_lat,
            fill_rate=fill_rate,
        )

    def _log_summary(self) -> None:
        s = self.stats()
        uptime = time.time() - self._start_time
        log.info(
            "stats | fills=%d/%d (%.1f%%) pnl=$%.2f fees=$%.2f avg_lat=%.0fμs uptime=%.0fs",
            s.total_fills,
            s.total_attempts,
            s.fill_rate * 100,
            s.total_pnl,
            s.total_fees,
            s.avg_latency_us,
            uptime,
        )
