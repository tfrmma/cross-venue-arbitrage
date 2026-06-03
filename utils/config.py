"""
Pydantic config models. If the YAML is malformed, fail at startup with a clear error.
Better than getting a KeyError at 3am when the first trade fires.

All fields have sensible defaults where possible. Required fields will raise
ValidationError immediately if missing.
"""

from __future__ import annotations

from enum import Enum
from pathlib import Path
from typing import Optional

import yaml
from pydantic import BaseModel, Field, field_validator, model_validator


class ArbType(str, Enum):
    CEX_CEX = "cex_cex"
    CEX_DEX = "cex_dex"
    PERP_PERP = "perp_perp"
    SPOT_PERP = "spot_perp"


# ---- venue configs ----

class BinanceConfig(BaseModel):
    enabled: bool = True
    required: bool = False
    futures: bool = True
    api_key: str
    api_secret: str
    symbols: list[str] = Field(default_factory=list)


class BybitConfig(BaseModel):
    enabled: bool = True
    required: bool = False
    category: str = "linear"
    api_key: str
    api_secret: str
    symbols: list[str] = Field(default_factory=list)

    @field_validator("category")
    @classmethod
    def valid_category(cls, v: str) -> str:
        if v not in ("linear", "spot", "inverse"):
            raise ValueError(f"bybit category must be linear/spot/inverse, got {v!r}")
        return v


class KrakenConfig(BaseModel):
    enabled: bool = True
    required: bool = False
    api_key: str
    api_secret: str
    symbols: list[str] = Field(default_factory=list)


class HyperliquidConfig(BaseModel):
    enabled: bool = True
    required: bool = False
    private_key: str
    symbols: list[str] = Field(default_factory=list)

    @field_validator("private_key")
    @classmethod
    def validate_key(cls, v: str) -> str:
        v = v.strip()
        if not v.startswith("0x") and len(v) == 64:
            v = "0x" + v
        if len(v) != 66:
            raise ValueError("hyperliquid private_key must be 32 bytes (64 hex chars, optionally 0x-prefixed)")
        return v


class DydxConfig(BaseModel):
    enabled: bool = False
    required: bool = False
    mnemonic: str = ""
    address: str = ""
    subaccount_id: int = 0
    symbols: list[str] = Field(default_factory=list)


class LighterConfig(BaseModel):
    enabled: bool = False
    required: bool = False
    private_key: str = ""
    executor_contract: str = ""
    market_ids: dict[str, int] = Field(default_factory=dict)
    symbols: list[str] = Field(default_factory=list)


class VenuesConfig(BaseModel):
    binance: Optional[BinanceConfig] = None
    bybit: Optional[BybitConfig] = None
    kraken: Optional[KrakenConfig] = None
    hyperliquid: Optional[HyperliquidConfig] = None
    dydx: Optional[DydxConfig] = None
    lighter: Optional[LighterConfig] = None

    def enabled_venues(self) -> dict[str, BaseModel]:
        result = {}
        for name in ("binance", "bybit", "kraken", "hyperliquid", "dydx", "lighter"):
            cfg = getattr(self, name)
            if cfg is not None and cfg.enabled:
                result[name] = cfg
        return result


# ---- fee model ----

class FeeModelConfig(BaseModel):
    maker_bps: float = 2.0
    taker_bps: float = 5.0
    min_size: float = 0.001
    max_size: float = 1000.0
    gas_usd: float = 0.0
    lp_fee_bps: float = 0.0

    @field_validator("taker_bps", "maker_bps")
    @classmethod
    def reasonable_fee(cls, v: float) -> float:
        if v < 0 or v > 100:
            raise ValueError(f"fee {v} bps looks wrong - should be 0-100 bps")
        return v


# ---- strategy ----

class ArbPairConfig(BaseModel):
    buy_venue: str
    sell_venue: str
    symbol: str
    type: ArbType = ArbType.PERP_PERP
    max_size_usd: float = 10_000.0

    @model_validator(mode="after")
    def venues_differ(self) -> ArbPairConfig:
        if self.buy_venue == self.sell_venue:
            raise ValueError(f"buy_venue and sell_venue must differ, both are {self.buy_venue!r}")
        return self


class StrategyConfig(BaseModel):
    min_net_pnl_bps: float = 3.0
    max_size_usd: float = 25_000.0
    dex_venues: list[str] = Field(default_factory=list)
    arb_pairs: list[ArbPairConfig] = Field(default_factory=list)
    fees: dict[str, FeeModelConfig] = Field(default_factory=dict)

    @field_validator("min_net_pnl_bps")
    @classmethod
    def sane_threshold(cls, v: float) -> float:
        if v < 0.5:
            raise ValueError(f"min_net_pnl_bps={v} is dangerously low - likely a config error")
        return v


# ---- risk ----

class CircuitBreakerConfig(BaseModel):
    max_consecutive_failures: int = 5
    max_drawdown_usd: float = 500.0
    max_slippage_bps: float = 15.0
    stale_feed_threshold_ms: float = 2000.0
    max_open_notional_usd: float = 200_000.0


class RiskConfig(BaseModel):
    audit_interval_s: float = 1.0
    rebalance_check_interval_s: float = 60.0
    rebalance_threshold_pct: float = 0.25
    delta_threshold_usd: float = 3_000.0
    target_venue_allocation: dict[str, float] = Field(default_factory=dict)
    circuit_breaker: CircuitBreakerConfig = Field(default_factory=CircuitBreakerConfig)

    @model_validator(mode="after")
    def allocations_sum_to_one(self) -> RiskConfig:
        if self.target_venue_allocation:
            total = sum(self.target_venue_allocation.values())
            if abs(total - 1.0) > 0.01:
                raise ValueError(f"target_venue_allocation sums to {total:.3f}, must be ~1.0")
        return self


# ---- execution ----

class ExecutionConfig(BaseModel):
    max_leg_wait_us: int = 500_000
    dex_venues: list[str] = Field(default_factory=list)


# ---- rate limits (optional override) ----

class RateLimitEndpointConfig(BaseModel):
    requests_per_second: float = 5.0
    max_concurrent: int = 3
    burst: int = 0


class RateLimitConfig(BaseModel):
    order: RateLimitEndpointConfig = Field(default_factory=RateLimitEndpointConfig)
    info: RateLimitEndpointConfig = Field(default_factory=lambda: RateLimitEndpointConfig(requests_per_second=10, max_concurrent=5))


# ---- root ----

class EngineConfig(BaseModel):
    log_level: str = "INFO"
    venues: VenuesConfig
    strategy: StrategyConfig
    risk: RiskConfig = Field(default_factory=RiskConfig)
    execution: ExecutionConfig = Field(default_factory=ExecutionConfig)
    rate_limits: dict[str, RateLimitConfig] = Field(default_factory=dict)

    @field_validator("log_level")
    @classmethod
    def valid_log_level(cls, v: str) -> str:
        valid = {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}
        if v.upper() not in valid:
            raise ValueError(f"log_level must be one of {valid}")
        return v.upper()

    @model_validator(mode="after")
    def pairs_reference_known_venues(self) -> EngineConfig:
        known = set(self.venues.enabled_venues().keys())
        for pair in self.strategy.arb_pairs:
            for venue in (pair.buy_venue, pair.sell_venue):
                if venue not in known:
                    raise ValueError(
                        f"arb pair references venue {venue!r} which is not enabled. "
                        f"Enabled venues: {sorted(known)}"
                    )
        return self


def load(path: str | Path) -> EngineConfig:
    """Load and validate config from YAML. Raises ValidationError with clear messages on failure."""
    with open(path) as f:
        raw = yaml.safe_load(f)
    return EngineConfig.model_validate(raw)
