"""
Opportunity detection. This is where the money is (or isn't).

The math here needs to be exact. Approximate fee models have cost me sleep.
"""

import logging
import time
import uuid
from dataclasses import dataclass, field
from enum import Enum
from typing import Optional

from core.orderbook import MarketSnapshot, OrderBook

log = logging.getLogger("arb.detector")


class ArbType(str, Enum):
    CEX_CEX = "cex_cex"
    CEX_DEX = "cex_dex"
    PERP_PERP = "perp_perp"
    SPOT_PERP = "spot_perp"


@dataclass
class Leg:
    venue: str
    symbol: str
    side: str          # "buy" or "sell"
    size: float
    expected_price: float  # after fees and slippage
    raw_price: float       # BBO mid, for reference
    fee_rate: float
    estimated_slippage: float
    is_dex: bool = False


@dataclass
class Opportunity:
    id: str
    type: ArbType
    buy_leg: Leg
    sell_leg: Leg
    gross_pnl: float
    net_pnl: float       # after all fees + estimated slippage
    net_pnl_bps: float
    size: float
    detected_ns: int
    vpin_score: float = 0.0  # higher = more toxic flow, avoid

    @property
    def is_viable(self) -> bool:
        return self.net_pnl > 0 and self.vpin_score < 0.7


@dataclass
class FeeModel:
    """Per-venue fee config. Loaded from config.yaml."""
    maker_bps: float
    taker_bps: float
    min_size: float
    max_size: float
    # DEX extras
    gas_usd: float = 0.0
    lp_fee_bps: float = 0.0  # e.g. Uniswap pool fee

    def taker_cost(self, notional: float) -> float:
        return notional * (self.taker_bps / 10_000) + self.gas_usd

    def maker_cost(self, notional: float) -> float:
        return notional * (self.maker_bps / 10_000) + self.gas_usd


# VPIN state per venue:symbol pair
# TODO: proper rolling window with volume buckets, this is a cheap approximation
class VPINEstimator:
    """
    Volume-Synchronized Probability of Informed Trading.
    Tracks buy/sell volume imbalance across recent buckets.
    Cheap version - good enough to filter obvious toxic flow.
    """

    def __init__(self, window: int = 50):
        self._window = window
        self._buy_vol: list[float] = []
        self._sell_vol: list[float] = []

    def update(self, buy_vol: float, sell_vol: float) -> None:
        self._buy_vol.append(buy_vol)
        self._sell_vol.append(sell_vol)
        if len(self._buy_vol) > self._window:
            self._buy_vol.pop(0)
            self._sell_vol.pop(0)

    def score(self) -> float:
        """Returns 0-1. Above ~0.7 means flow is suspiciously one-directional."""
        if len(self._buy_vol) < 5:
            return 0.0
        total_buy = sum(self._buy_vol)
        total_sell = sum(self._sell_vol)
        total = total_buy + total_sell
        if total == 0:
            return 0.0
        return abs(total_buy - total_sell) / total


class OpportunityDetector:
    def __init__(self, config: dict, risk_manager):
        self.cfg = config
        self.risk = risk_manager

        self._min_net_pnl_bps: float = config.get("min_net_pnl_bps", 3.0)
        self._max_size_usd: float = config.get("max_size_usd", 50_000)

        self._fee_models: dict[str, FeeModel] = _load_fee_models(config.get("fees", {}))
        self._vpin: dict[str, VPINEstimator] = {}

        # pairs to scan: list of {buy_venue, sell_venue, symbol, type}
        self._pairs: list[dict] = config.get("arb_pairs", [])

    def scan(self, snapshot: MarketSnapshot) -> list[Opportunity]:
        opps = []
        for pair in self._pairs:
            opp = self._evaluate_pair(pair, snapshot)
            if opp and opp.is_viable and opp.net_pnl_bps >= self._min_net_pnl_bps:
                opps.append(opp)

        # best spread first
        opps.sort(key=lambda o: o.net_pnl_bps, reverse=True)
        return opps

    def _evaluate_pair(self, pair: dict, snapshot: MarketSnapshot) -> Optional[Opportunity]:
        buy_venue = pair["buy_venue"]
        sell_venue = pair["sell_venue"]
        symbol = pair["symbol"]
        arb_type = ArbType(pair.get("type", "cex_cex"))

        buy_book = snapshot.get(buy_venue, symbol)
        sell_book = snapshot.get(sell_venue, symbol)

        if buy_book is None or sell_book is None:
            return None

        buy_bbo = buy_book.bbo()
        sell_bbo = sell_book.bbo()
        if buy_bbo is None or sell_bbo is None:
            return None

        # raw spread check before doing the expensive sweep
        if sell_bbo.bid <= buy_bbo.ask:
            return None

        size = self._optimal_size(buy_book, sell_book, pair)
        if size <= 0:
            return None

        buy_leg = self._build_leg(buy_book, "buy", size, arb_type)
        sell_leg = self._build_leg(sell_book, "sell", size, arb_type)

        gross = (sell_leg.expected_price - buy_leg.expected_price) * size
        fees = (
            self._fee_models[buy_venue].taker_cost(buy_leg.expected_price * size)
            + self._fee_models[sell_venue].taker_cost(sell_leg.expected_price * size)
        )
        net = gross - fees
        net_bps = (net / (buy_leg.expected_price * size)) * 10_000

        vpin = self._get_vpin(buy_venue, symbol)

        return Opportunity(
            id=str(uuid.uuid4())[:8],
            type=arb_type,
            buy_leg=buy_leg,
            sell_leg=sell_leg,
            gross_pnl=gross,
            net_pnl=net,
            net_pnl_bps=net_bps,
            size=size,
            detected_ns=time.time_ns(),
            vpin_score=vpin,
        )

    def _build_leg(self, book: OrderBook, side: str, size: float, arb_type: ArbType) -> Leg:
        fee_model = self._fee_models.get(book.venue)
        is_dex = arb_type in (ArbType.CEX_DEX,) and book.venue in self.cfg.get("dex_venues", [])

        if side == "buy":
            raw_price, filled = book.sweep_asks(size)
        else:
            raw_price, filled = book.sweep_bids(size)

        slippage = abs(raw_price - book.bbo().mid) / book.bbo().mid

        if side == "buy":
            effective = raw_price * (1 + fee_model.taker_bps / 10_000)
            if is_dex:
                effective += fee_model.gas_usd / (size + 1e-9)
        else:
            effective = raw_price * (1 - fee_model.taker_bps / 10_000)
            if is_dex:
                effective -= fee_model.gas_usd / (size + 1e-9)

        return Leg(
            venue=book.venue,
            symbol=book.symbol,
            side=side,
            size=size,
            expected_price=effective,
            raw_price=raw_price,
            fee_rate=fee_model.taker_bps,
            estimated_slippage=slippage,
            is_dex=is_dex,
        )

    def _optimal_size(self, buy_book: OrderBook, sell_book: OrderBook, pair: dict) -> float:
        """
        Find the size where the edge starts to compress below threshold.
        Binary search would be cleaner but this is fast enough in practice.
        """
        max_size_usd = min(self._max_size_usd, pair.get("max_size_usd", self._max_size_usd))
        buy_fee = self._fee_models.get(buy_book.venue)
        sell_fee = self._fee_models.get(sell_book.venue)

        if buy_fee is None or sell_fee is None:
            return 0.0

        # approximate max size from book depth
        max_buy_qty = sum(l.qty for l in buy_book.asks[:10])
        max_sell_qty = sum(l.qty for l in sell_book.bids[:10])

        bbo_buy = buy_book.bbo()
        if bbo_buy is None:
            return 0.0

        qty_limit = min(max_buy_qty, max_sell_qty, max_size_usd / bbo_buy.ask)
        qty_limit = max(qty_limit, buy_fee.min_size)

        return min(qty_limit, buy_fee.max_size, sell_fee.max_size)

    def _get_vpin(self, venue: str, symbol: str) -> float:
        key = f"{venue}:{symbol}"
        estimator = self._vpin.get(key)
        if estimator is None:
            return 0.0
        return estimator.score()

    def update_vpin(self, venue: str, symbol: str, buy_vol: float, sell_vol: float) -> None:
        key = f"{venue}:{symbol}"
        if key not in self._vpin:
            self._vpin[key] = VPINEstimator()
        self._vpin[key].update(buy_vol, sell_vol)


def _load_fee_models(fees_cfg: dict) -> dict[str, FeeModel]:
    models = {}
    for venue, cfg in fees_cfg.items():
        models[venue] = FeeModel(
            maker_bps=cfg.get("maker_bps", 2.0),
            taker_bps=cfg.get("taker_bps", 5.0),
            min_size=cfg.get("min_size", 0.001),
            max_size=cfg.get("max_size", 1000.0),
            gas_usd=cfg.get("gas_usd", 0.0),
            lp_fee_bps=cfg.get("lp_fee_bps", 0.0),
        )
    return models
