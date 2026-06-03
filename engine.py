"""
Cross-venue arbitrage engine.
Hyperliquid / Binance / Kraken / Bybit / dYdX / Lighter.

Run with: python engine.py --config config.yaml
"""

import asyncio
import logging
import signal
import sys

from utils.config import EngineConfig, load as load_config
from core.orderbook import OrderBookManager
from core.opportunity import OpportunityDetector
from execution.router import ExecutionRouter
from risk.manager import RiskManager
from venues.factory import VenueFactory
from utils.metrics import EngineMetrics

log = logging.getLogger("arb.engine")


class ArbEngine:
    def __init__(self, config: EngineConfig):
        self.cfg = config
        self._running = False
        self._shutdown_event = asyncio.Event()

        self.metrics = EngineMetrics()

        # convert pydantic models back to dicts for the inner components
        # TODO: push pydantic models all the way down, this is a mild annoyance
        venues_raw = {k: v.model_dump() for k, v in config.venues.enabled_venues().items()}
        self.ob_manager = OrderBookManager(venues_raw)
        self.risk = RiskManager(config.risk.model_dump())
        self.detector = OpportunityDetector(config.strategy.model_dump(), self.risk)
        self.router = ExecutionRouter(config.execution.model_dump(), self.risk, self.metrics)

        self._venues = {}
        self._tasks: list[asyncio.Task] = []

    async def start(self) -> None:
        log.info("spinning up venues...")
        venues_raw = {k: v.model_dump() for k, v in self.cfg.venues.enabled_venues().items()}
        self._venues = await VenueFactory.create_all(venues_raw)

        for name, venue in self._venues.items():
            venue.on_book_update = self.ob_manager.handle_update

        self._tasks = [
            asyncio.create_task(self._run_feeds(), name="feeds"),
            asyncio.create_task(self._run_detection_loop(), name="detection"),
            asyncio.create_task(self._run_risk_loop(), name="risk"),
            asyncio.create_task(self._run_rebalancer(), name="rebalance"),
            asyncio.create_task(self._run_stale_feed_monitor(), name="stale_monitor"),
        ]

        self._running = True
        log.info("engine live | venues=%s", list(self._venues.keys()))

        try:
            await self._shutdown_event.wait()
        finally:
            await self._teardown()

    async def _run_feeds(self) -> None:
        feed_tasks = [
            asyncio.create_task(v.stream_orderbook(), name=f"feed_{n}")
            for n, v in self._venues.items()
        ]
        try:
            await asyncio.gather(*feed_tasks)
        except Exception as e:
            log.error("feed task crashed: %s - shutting down", e)
            self._shutdown_event.set()

    async def _run_detection_loop(self) -> None:
        # wakes on BBO change, not on timer - if you're polling you're already behind
        while self._running:
            try:
                await self.ob_manager.wait_for_update()
                if not self.risk.is_trading_allowed():
                    continue
                snapshot = self.ob_manager.get_snapshot()
                for opp in self.detector.scan(snapshot):
                    if self.risk.approve_opportunity(opp):
                        asyncio.create_task(
                            self.router.execute(opp, self._venues),
                            name=f"exec_{opp.id}",
                        )
            except asyncio.CancelledError:
                break
            except Exception as e:
                log.exception("detection loop error: %s", e)
                await asyncio.sleep(0.001)

    async def _run_risk_loop(self) -> None:
        interval = self.cfg.risk.audit_interval_s
        while self._running:
            try:
                await asyncio.sleep(interval)
                positions = await self._collect_positions()
                self.risk.audit(positions, self.metrics.get_recent_pnl())
            except asyncio.CancelledError:
                break
            except Exception as e:
                log.exception("risk loop error: %s", e)

    async def _run_rebalancer(self) -> None:
        interval = self.cfg.risk.rebalance_check_interval_s
        while self._running:
            try:
                await asyncio.sleep(interval)
                balances = await self._collect_balances()
                for op in self.risk.check_rebalance_needed(balances):
                    await self.router.execute_rebalance(op, self._venues)
            except asyncio.CancelledError:
                break
            except Exception as e:
                log.exception("rebalancer error: %s", e)

    async def _run_stale_feed_monitor(self) -> None:
        """Check every second for feeds that have gone silent."""
        while self._running:
            try:
                await asyncio.sleep(1.0)
                stale = self.ob_manager.get_stale_venues()
                for key in stale:
                    venue_name = key.split(":")[0]
                    self.risk.handle_feed_stale(venue_name, stale_ms=2001.0)
            except asyncio.CancelledError:
                break

    async def _collect_positions(self) -> dict:
        results = {}
        for name, venue in self._venues.items():
            try:
                results[name] = await venue.get_positions()
            except Exception as e:
                log.warning("get_positions failed for %s: %s", name, e)
        return results

    async def _collect_balances(self) -> dict:
        results = {}
        for name, venue in self._venues.items():
            try:
                results[name] = await venue.get_balances()
            except Exception as e:
                log.warning("get_balances failed for %s: %s", name, e)
        return results

    async def _teardown(self) -> None:
        log.info("shutting down...")
        self._running = False

        for task in self._tasks:
            task.cancel()

        # cancel orders before closing connections - order matters
        await self.router.cancel_all_open_orders(self._venues)

        for name, venue in self._venues.items():
            try:
                await venue.close()
            except Exception as e:
                log.warning("error closing %s: %s", name, e)

        log.info("shutdown complete")

    def handle_signal(self) -> None:
        log.info("signal received - shutting down")
        self._shutdown_event.set()


def setup_logging(level: str = "INFO") -> None:
    logging.basicConfig(
        level=getattr(logging, level, logging.INFO),
        format="%(asctime)s.%(msecs)03d %(levelname)-7s %(name)s: %(message)s",
        datefmt="%H:%M:%S",
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler("arb_engine.log"),
        ],
    )
    for noisy_lib in ("websockets", "aiohttp", "urllib3"):
        logging.getLogger(noisy_lib).setLevel(logging.WARNING)


async def main(config_path: str) -> None:
    try:
        config = load_config(config_path)
    except Exception as e:
        print(f"config error: {e}", file=sys.stderr)
        sys.exit(1)

    setup_logging(config.log_level)
    engine = ArbEngine(config)

    loop = asyncio.get_event_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, engine.handle_signal)

    await engine.start()


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", default="config.yaml")
    asyncio.run(main(parser.parse_args().config))
