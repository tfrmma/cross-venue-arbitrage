# cross-venue-arb-engine

Arbitrage engine across CEX perps and DEX CLOBs. Targets Binance, Bybit, Kraken, Hyperliquid, dYdX v4, and Lighter.

Python 3.11+. asyncio throughout. No Celery, no Kafka, no nonsense.

---

## what it does

Ingests L2 order books from all venues simultaneously over WebSocket. On every BBO change, scans configured arb pairs for net-positive spreads after fees, slippage, and gas. Fires both execution legs concurrently (or sequentially for CEX→DEX pairs). Manages inventory, delta, and circuit breakers in the background.

The detection loop wakes on BBO changes via `asyncio.Event`, not on a timer. If you're polling at 1ms you're wasting CPU and still losing to someone who isn't.

---

## project layout

```
arb_engine/
├── engine.py                   # entry point, main loops, signal handling
├── config.yaml                 # reference config with all fields documented
│
├── core/
│   ├── orderbook.py            # PriceLadder, BBO, delta/snapshot, numpy sweep
│   └── opportunity.py          # spread detection, fee model, VPIN, slippage sim
│
├── execution/
│   └── router.py               # leg coordination, IOC orders, unwind with halt
│
├── risk/
│   └── manager.py              # circuit breakers, delta audit, inventory rebalance
│
├── venues/
│   ├── base.py                 # abstract base, reconnect loop
│   ├── binance.py              # USDT-M futures + spot, WS depth + REST IOC
│   ├── bybit.py                # V5 linear/spot
│   ├── kraken.py               # WS v2, HMAC-SHA512 auth
│   ├── hyperliquid.py          # JSON API, phantom agent EIP-712 signing, asset map fetch
│   ├── dydx.py                 # indexer WS feed (order placement stub — see limitations)
│   ├── lighter.py              # CLOB DEX, executor contract for atomic swaps
│   └── factory.py              # instantiates venues from config
│
├── contracts/
│   └── LighterExecutor.sol     # price assertion + revert on stale arb
│
└── utils/
    ├── config.py               # Pydantic v2 models, validated at startup
    ├── rate_limiter.py         # sliding window + semaphore, per venue per endpoint
    └── metrics.py              # latency histograms, fill rate, PnL tracking
```

---

## setup

```bash
pip install -r requirements.txt
cp config.yaml config.local.yaml
# fill in API keys
python engine.py --config config.local.yaml
```

Python 3.11 minimum. Don't run this on 3.9.

---

## configuration

Config is validated at startup via Pydantic. If something is wrong — missing key, fees out of range, arb pair referencing a disabled venue, allocations not summing to 1.0 — the engine exits immediately with a clear error message before connecting to anything.

```yaml
log_level: INFO

venues:
  binance:
    enabled: true
    required: true       # engine won't start if this venue fails to connect
    futures: true        # false = spot
    api_key: "..."
    api_secret: "..."
    symbols: [BTCUSDT, ETHUSDT]

  hyperliquid:
    enabled: true
    private_key: "0x..."   # 32 bytes, 0x-prefixed or raw hex — both accepted
    symbols: [BTC, ETH]

strategy:
  min_net_pnl_bps: 3.0      # minimum edge after all costs. below 0.5 is rejected as misconfiguration
  max_size_usd: 25000.0

  arb_pairs:
    - buy_venue: binance
      sell_venue: hyperliquid
      symbol: BTCUSDT
      type: perp_perp         # cex_cex | cex_dex | perp_perp | spot_perp
      max_size_usd: 15000.0

  fees:
    binance:
      maker_bps: 2.0
      taker_bps: 4.0
      min_size: 0.001
      max_size: 10.0
    hyperliquid:
      taker_bps: 3.5
      # gas_usd: 0.0 for CEX; set non-zero for DEX venues

risk:
  audit_interval_s: 1.0
  rebalance_check_interval_s: 60.0
  rebalance_threshold_pct: 0.25
  delta_threshold_usd: 3000.0

  target_venue_allocation:        # must sum to 1.0
    binance: 0.50
    hyperliquid: 0.50

  circuit_breaker:
    max_consecutive_failures: 5
    max_drawdown_usd: 500.0
    max_slippage_bps: 15.0
    stale_feed_threshold_ms: 2000.0
    max_open_notional_usd: 200000.0
```

See `config.yaml` for the full reference with all venues and fields.

---

## order book

`PriceLadder` is the core data structure for each side of the book. It keeps a `dict[price, qty]` for O(1) updates and rebuilds the sorted numpy arrays lazily — only when the BBO actually changes, not on every delta message. Under 50 delta updates/sec per venue on a 200-level book, the old list+sort approach was spending most of its time in sort. The new version only pays the O(n log n) rebuild when it matters.

`sweep_asks` / `sweep_bids` use `np.cumsum` + `np.searchsorted` to walk the book for a target quantity. No Python loop. For the slippage calculation on every detected opportunity this is the hot path and the speedup is real (~10x over the loop version for 200 levels).

---

## execution logic

Three paths depending on the pair type:

**CEX / CEX** — both IOC orders fire simultaneously via `asyncio.gather`. If either comes back partial, the other gets unwound at market immediately. Unwind success/failure is tracked in `ExecutionResult.unwind_failed`. If the unwind itself fails, the engine halts — unknown delta is worse than no trading.

**CEX → DEX** — CEX IOC first. If it misses, abort — no gas spent. If it fills, we're committed; fire the DEX leg immediately. DEX contract reverts if price moved. If DEX fails after CEX filled, market out of the CEX position.

**DEX → CEX** — DEX transaction via `LighterExecutor` contract, which asserts `minPrice` before executing. If the arb is stale, the tx reverts and costs only gas. If it fills, hedge on CEX immediately. If the CEX hedge fails here we have an open long on DEX — `unwind_failed=True`, engine halts.

The unwind path returns a boolean now. A silent swallowed exception here used to mean unknown delta. That was the bug.

---

## the executor contract

`contracts/LighterExecutor.sol` wraps Lighter's order book with a price assertion:

```solidity
function executeWithMinPrice(
    uint32 marketId,
    bool isAsk,
    uint64 amount,
    uint64 minPrice
) external onlyOwner returns (uint64 filledAmount)
```

Reverts if fill price is below `minPrice` or if `filledAmount == 0`. Stale arbs cost gas, not capital. Deploy once per wallet. Approve token spending before use. The `rescueTokens` function is there for the inevitable moment something gets stuck.

---

## fee model

The detector never compares raw mid prices. For every candidate pair it sweeps actual book depth for the target size:

```
effective_buy  = sweep_ask_vwap × (1 + taker_bps/10000) + gas_usd/size
effective_sell = sweep_bid_vwap × (1 - taker_bps/10000) - gas_usd/size
net_pnl        = (effective_sell - effective_buy) × size
```

The pre-filter is a raw BBO check (`sell_bbo.bid > buy_bbo.ask`) before the expensive sweep. Most pairs fail that check immediately and never touch the book walk.

---

## rate limiting

Every REST call goes through `VenueRateLimiters` before it fires. Each venue has two independent buckets — `order` and `info` — because venues enforce them separately. The limiter is a sliding window (deque of timestamps) combined with an `asyncio.Semaphore` for max concurrency.

Default limits are conservative. Adjust them in `config.yaml` under `rate_limits` if your account tier supports higher throughput:

```yaml
rate_limits:
  binance:
    order:
      requests_per_second: 20
      max_concurrent: 10
```

Hitting a 429 during an active arb leg is expensive — you get a failed order with a potentially filled counterpart. The limiter runs ahead of the call and blocks rather than letting the exchange reject it.

---

## risk and circuit breakers

`RiskManager` runs on a 1s audit loop. It tracks:

- **Consecutive failures** — N failed executions in a row triggers a soft pause (`PAUSED`). Recovers automatically when conditions improve.
- **Max drawdown** — hard halt (`HALTED`) if realized PnL drops more than `max_drawdown_usd` from peak. Requires manual restart.
- **Net delta** — warns if the sum of all venue positions exceeds `delta_threshold_usd`. Doesn't halt on its own; the execution router is responsible for hedging.
- **Stale feeds** — a dedicated monitor task checks every second for books that haven't updated in >2s. If detected, `handle_feed_stale` is called and trading halts for that venue.
- **Funding rate exposure** — warns if a perp position is on the paying side of a funding tick above 0.1% (0.3% annualized per 8h tick starts adding up fast if you're holding overnight).
- **Unwind failure** — if a partial fill can't be unwound, `record_slippage(9999)` is called immediately, which trips the consecutive failure counter and halts.

Trading states: `ACTIVE → PAUSED → HALTED`. PAUSED self-recovers. HALTED requires manual restart and investigation.

---

## inventory rebalancing

An async loop runs every 60s and compares actual balances against `target_venue_allocation` in config. If any venue drifts more than `rebalance_threshold_pct` (default 25%) from its target share, it logs the required transfer.

Bridge execution is intentionally not implemented — cross-chain withdrawals and CEX→DEX transfers have enough edge cases (failed txs, gas spikes, bridge liquidity limits) that it warrants its own module with proper retry logic. For now: it detects, you execute.

---

## hyperliquid signing

The original signing used `encode_defunct` over JSON-serialized action data. That works on testnet. Mainnet rejects it silently for real orders — HL verifies a specific phantom agent EIP-712 scheme on the server side.

The current implementation does it correctly: `keccak256(abi.encode(action_hash, nonce))` packed as `(bytes32, uint64)`, signed as EIP-191 personal_sign. This matches what their TypeScript SDK produces. The asset index map is also fetched from `/info` at connect time instead of being hardcoded.

---

## known limitations

**dYdX order placement is a stub.** Feed (WS + positions + balances) works fine. Placing orders requires Cosmos transaction signing via `dydx-v4-client` — heavier dependency, their signing format has changed between versions. The stub returns `FAILED` and logs a warning. Enable `dydx` in config only if you're implementing the order leg yourself.

**Kraken IOC fill details require a follow-up query.** `AddOrder` doesn't return `filled_qty` or `avg_price` immediately — you need to poll `QueryOrders` after. The current implementation records `filled_qty=0.0` from the response, which makes PnL accounting inaccurate for Kraken legs. Acceptable for now; fix it when Kraken becomes a primary venue.

**Rebalancer logs, doesn't execute.** See inventory rebalancing section above.

**VPIN is approximate.** Rolling buy/sell volume imbalance, not proper volume-synchronized buckets. Good enough to filter obvious toxic flow. Not a substitute for real VPIN if you're running on sub-2bps spreads.

**Lighter signing is placeholder.** The `_sign_order` method in `venues/lighter.py` uses a generic EIP-191 hash over sorted JSON. Lighter's actual REST API signing scheme needs to be verified against their current docs before using the `place_ioc_order` path in production. The `execute_atomic_swap` path (on-chain via executor contract) is correct and doesn't use their REST API.

---

## deployment

Co-location matters more than code optimization past a certain point. Binance and Bybit are both in AWS Tokyo (`ap-northeast-1`). Hyperliquid's sequencer is in `us-east-1`. If you're running CEX/CEX Tokyo pairs and HL pairs from the same instance, you're adding ~140ms of cross-region latency to every HL leg.

For DEX legs, don't use public RPCs. They have rate limits, shared load, and will delay your tx at exactly the wrong moment. Run a dedicated node or use a private provider with SLA.

No Flashbots/MEV bundle integration. If you're seeing sandwich attacks on DEX legs, that's the next thing to add — submit the DEX tx as a private bundle through Flashbots or Jito (Solana) instead of the public mempool.

---

## adding a new venue

Subclass `BaseVenue` and implement:

```python
async def connect(self) -> None
async def close(self) -> None
async def stream_orderbook(self) -> None      # runs forever; call self._push_book_update()
async def place_ioc_order(...) -> Optional[FillResult]
async def cancel_all_orders(self) -> None
async def get_positions(self) -> list
async def get_balances(self) -> list[Balance]
```

Add a Pydantic config model in `utils/config.py`, a field in `VenuesConfig`, the class in `venues/factory.py` `VENUE_REGISTRY`, and a block in `config.yaml`. The validation in `EngineConfig.pairs_reference_known_venues` will catch any config mismatches at startup.

---

## license

MIT. Do what you want with it.
