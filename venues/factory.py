"""
Venue factory. Maps config keys to venue classes.
"""

import logging

from venues.base import BaseVenue
from venues.binance import BinanceVenue
from venues.bybit import BybitVenue
from venues.dydx import DydxVenue
from venues.hyperliquid import HyperliquidVenue
from venues.kraken import KrakenVenue
from venues.lighter import LighterVenue

log = logging.getLogger("arb.factory")

VENUE_REGISTRY: dict[str, type[BaseVenue]] = {
    "binance": BinanceVenue,
    "bybit": BybitVenue,
    "kraken": KrakenVenue,
    "hyperliquid": HyperliquidVenue,
    "dydx": DydxVenue,
    "lighter": LighterVenue,
}


class VenueFactory:
    @staticmethod
    async def create_all(venues_config: dict) -> dict[str, BaseVenue]:
        venues = {}
        for name, cfg in venues_config.items():
            if not cfg.get("enabled", True):
                log.info("venue %s is disabled, skipping", name)
                continue

            cls = VENUE_REGISTRY.get(name)
            if cls is None:
                log.error("unknown venue: %s - skipping", name)
                continue

            try:
                venue = cls(cfg)
                await venue.connect()
                venues[name] = venue
                log.info("venue %s ready", name)
            except Exception as e:
                log.error("failed to initialize venue %s: %s", name, e)
                if cfg.get("required", False):
                    raise

        return venues
