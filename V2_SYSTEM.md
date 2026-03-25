# V2 System — Bilateral Accumulation

## 1. Overview

FaCaiBot v2 is a Polymarket market-making bot targeting BTC 5-minute prediction markets. Each market is a binary question — "Will BTC be above $X at expiry?" — with YES and NO tokens that resolve to $1.00 or $0.00.

**Core insight**: If you hold 1 YES share + 1 NO share, exactly one pays $1.00 at resolution. A matched pair bought for less than $1.00 total is guaranteed profit regardless of outcome.

**Strategy**: Continuously post maker orders on both YES and NO sides, accumulate shares throughout the 5-minute market, and pair them at resolution.

**Why bilateral beats 2-leg**: v1 required detecting a directional move first (Leg 1), then hedging on the opposite side (Leg 2). This was reactive — by the time the move was confirmed, the opposing side's price had often moved. v2 quotes both sides simultaneously, earning the bid-ask spread without needing to predict direction.

## 2. Profit Math

**Pair cost**: `yes_avg_price + no_avg_price`

If pair cost < $1.00, the difference is locked profit per paired share.

```
Example:
  50 YES shares @ $0.42 avg
  50 NO  shares @ $0.48 avg
  Pair cost = $0.90 per share
  Locked profit = (1.00 - 0.90) × 50 = $5.00
```

**Edge enforcement**: The quoting system targets prices at `fair_value - edge` on each side. With a $0.03 edge per side, the theoretical pair cost is `(fv - 0.03) + ((1 - fv) - 0.03) = 0.94`, yielding $0.06 per paired share.

**Unpaired risk**: Shares that don't have a matching opposite-side share carry directional risk — they're worth $1.00 or $0.00 at resolution. Risk limits cap unpaired exposure.

**Maker rebates**: Post-only orders earn a maker rebate (~20% of the taker fee rate). This adds to profitability on every fill.

## 3. Architecture

Three-layer lock-free pipeline connected by crossbeam SPSC channels:

```
Ingestor (gateway/)          Engine (engine/)              Executor (executor/)
├─ Binance Spot SBE WS      ├─ V2StrategyEngine           ├─ LiveExecutor
├─ Binance Futures JSON WS   │  ├─ FairValueEstimator      │  ├─ Post maker orders
├─ Polymarket Market WS      │  ├─ Quoter                  │  ├─ Cancel orders
├─ Polymarket User WS        │  ├─ BilateralPosition       │  ├─ Closing FOK
├─ Gamma API (rotation)      │                              │  └─ QuestDB analytics
└─ Heartbeat POST            └─ 3 metric + 2 vol trackers  └─ Telegram reports
```

**Data flow**:
1. Ingestor sends `IngestorEvent` variants to the engine via a bounded channel
2. Engine processes events, updates fair value, decides quoting actions
3. Engine sends `V2ExecutorCommand` variants to the executor
4. Executor sends `V2ExecutorFeedback` back (order confirmations, fill data)

**Threading**: Tokio 2-worker runtime. Ingestor tasks on async workers (cores 1-2). Engine runs on `spawn_blocking` (off the worker pool). Core 0 reserved for OS.

## 4. Fair Value Model

The `FairValueEstimator` continuously estimates P(BTC > strike at expiry), producing YES and NO fair values that sum to 1.0.

### Volatility

Realized volatility is measured by `RealizedVolTracker` — a ring buffer of log-returns with per-tick standard deviation (zero-mean assumption, valid for 5-minute windows). Two instances run in parallel:

| Instance | Capacity | Lookback | Purpose |
|----------|----------|----------|---------|
| **Short-window** | 600 samples | ~30s at 20 ticks/sec | Base model σ, edge sizing |
| **Session-window** | 6000 samples | Full 5-min market | Regime spike detection |

Both are fed every `BinanceTick` mid-price update. The short-window tracker's `scaled_vol(remaining_secs)` provides the σ used in the probability formula. If the short-window tracker is not fresh (no update within `vol_freshness_ms`), quoting is blocked (stale vol guard).

### Base Model

Uses a corrected log-displacement binary option formula:

1. **Displacement**: `ln(current_btc / strike_price)` (log-displacement, not linear)
2. **Scaled volatility**: `σ = realized_vol × √(ticks_per_sec × remaining_secs)` — wider uncertainty early, narrower late
3. **Z-score**: `d = displacement / max(σ, 0.0001)`
4. **Tail compression**: `d_adj = d × tail_compression_factor (0.85)` — fattens tails to avoid overconfident extremes
5. **Probability**: `P(YES) = Φ(d_adj)` where Φ is the standard normal CDF

### Strike Warmup

The strike price is set from the median of the first `strike_warmup_count` (default: 5) `BinanceTick` prices after market rotation, rather than snapping to the first tick. This avoids anomalous first-tick strikes.

### Momentum Adjustment

Three Binance metrics shift the fair value based on real-time order flow:

| Metric | Source | What it measures |
|--------|--------|-----------------|
| **Basis Delta** | Futures @bookTicker + Spot mid | Rate of change of futures-spot premium. Rising basis → futures leading up |
| **CVD Acceleration** | Futures @aggTrade | Fast EMA - slow EMA of signed trade volume. Accelerating buys → bullish |
| **OBI Velocity** | Spot @depth20 | Rate of change of order book imbalance. Accelerating bid-heavy → bullish |

Each metric produces a normalized [0,1] score with a directional sign. Weights are reduced relative to v1 — the corrected base model does the heavy lifting. The weighted sum is clamped to `±max_momentum_adj` (default: ±0.08) and added to the base fair value:

```
momentum = w_basis(0.10) × basis_signed + w_cvd(0.05) × cvd_signed + w_obi(0.05) × obi_signed
yes_fv = clamp(base_fv + momentum, 0.02, 0.98)
no_fv  = 1.0 - yes_fv
```

### Edge Sizing

The edge (minimum profit margin per share) scales dynamically with volatility, time, and data quality:

```
vol_normalized = min(realized_vol / baseline_vol, 2.0)
time_factor    = sqrt(time_remaining / 300s)

edge = min_edge
     + (vol_normalized × vol_edge_scale)       # vol-adaptive
     + (time_factor × time_edge_scale)          # time-adaptive
     + regime_spike_penalty (if triggered)       # short_vol / session_vol > threshold
     + stale_count × stale_data_edge_penalty     # per stale momentum feed
```

- **Vol-adaptive**: Higher realized vol → wider edge (more risk per share). Capped at 2× baseline.
- **Time-adaptive**: `sqrt(time_fraction)` — wider early, tighter late. Uses sqrt for convex decay.
- **Regime spike penalty**: Added when `short_vol / session_vol > regime_spike_threshold` (default: 2.0). Detects sudden vol regime changes.
- **Stale feed penalty**: Each stale momentum tracker (basis, CVD, OBI) adds `stale_data_edge_penalty` to the edge. Models uncertainty from missing data.

## 5. Quoting Logic

The `Quoter` manages one resting maker order per side (YES and NO).

### Global Guards

Before evaluating either side, these conditions must ALL be met — otherwise both sides are blocked:

1. **Fair value warm**: The estimator must have a strike price and current BTC price
2. **Vol data fresh**: Short-window vol tracker must have been updated within `vol_freshness_ms`
3. **Vol tracker warm**: Vol tracker must have `≥ vol_min_warmup` samples (belts-and-suspenders — Binance data will almost always warm it during the 5s quiet period, but blocks quoting if data lags)
4. **Heartbeat healthy**: Fewer than `heartbeat_dead_threshold` consecutive heartbeat failures. On healthy→unhealthy transition, all resting orders are automatically cancelled and a Telegram critical alert is sent
5. **Polymarket spread**: Neither YES nor NO book spread exceeds `max_entry_spread`
6. **Polymarket book fresh**: Both YES and NO books must have been updated within `stale_book_ms`
7. **FV extremity guard**: If YES fair value exceeds `max_fair_value_extremity` (default: 0.85) or is below `1 - max_fair_value_extremity` (0.15), skip quoting entirely. At extreme fair values, the cheap side cannot fill at any profitable price, making pairing structurally impossible. This is the deepest protection against one-sided accumulation

### Per-Side Evaluation

For each side, the quoter checks guards in order:

1. **Pending operation**: Skip if a post or cancel is in-flight for this side
2. **Unpaired limit**: Skip if `unpaired_shares(side) >= max_unpaired_shares`
3. **Pair-cost feasibility**: If the opposite side has fills (`other_avg > 0`), skip if `other_avg + target_price >= $1.00`. Prevents posting orders that would create unprofitable pairs (pair cost ≥ $1.00). Skipped when the opposite side has no fills yet (early market, guard inactive).
4. **Book-aware pair-cost check**: If the bot has unpaired shares on this side, check pairing viability using actual book data. If `opposite_best_ask` is `None` and there are no opposite fills (`other_avg == 0`), block (no pairing path exists). If `opposite_best_ask + target_price >= $1.00`, block (next pair unprofitable at current book prices).
5. **Dynamic sizing cap**: If `dynamic_order_size()` returns `None` (3 batches completed), stop trading this market.

### Size Computation (Dynamic)

Order size decreases as paired shares accumulate, halving at each pairing milestone:
- **Batch 1**: `max_order_size` (10) — trades until 10 shares paired
- **Batch 2**: `max(10/2, min_order_size)` = 5 — trades until 15 paired
- **Batch 3**: `max(5/2, min_order_size)` = 5 — trades until 20 paired
- **4th batch trigger**: **stop trading this market entirely**

The dynamic size is then further constrained:
```
dynamic_max      = dynamic_order_size(max_order_size, min_order_size, paired_shares)
max_by_imbalance = max_unpaired_shares - unpaired(side)
size             = min(dynamic_max, max_by_imbalance)
```

If `dynamic_order_size` returns None (pairing cap reached), skip posting. If size < `min_order_size` (default: 5 shares), skip posting. If `target_price × size < $1.00` (CLOB notional minimum), skip posting.

### Target Price

```
target_price = round_to_tick(fair_value - edge, tick_size)
```

### Zero-Edge Rebalance Posting

When the position is imbalanced, the **lagging side** (the side with fewer shares) posts its maker order with **zero edge** instead of the normal `min_edge`. This makes the lagging side's bid ~3 cents closer to the ask (since `min_edge` is typically 0.03), attracting fills to rebalance the position passively.

- **Long YES** (more YES than NO): NO side posts with edge = 0, YES side uses normal `base_edge`
- **Long NO** (more NO than YES): YES side posts with edge = 0, NO side uses normal `base_edge`
- **Balanced**: Both sides use `base_edge`

### Taker Rebalance

If zero-edge posting is insufficient and the imbalance exceeds `rebalance_threshold`, the engine sends a taker FOK on the lagging side. Rebalance attempts are sequential — each FOK is sent, the engine waits for the result, and immediately retries if the price is still favorable:

- **Pair cost guard**: `other_side_avg + best_ask < rebalance_max_pair_cost` (0.97)
- **Size**: `min(rebalance_size, abs_imbalance)`, minimum 5 shares
- **Sequential**: No interval between attempts — the natural throttle is waiting for the FOK result. If cancelled or partially filled, the next `quote_tick` iteration re-triggers if conditions are still met

### Requoting

A resting order is cancelled and replaced when fair value has drifted by `≥ requote_threshold` since the order was posted. The cancel→confirm→repost cycle (~1.2s round-trip) is the natural throttle — no artificial interval. Both sides independently handle requotes, so fast BTC moves are handled reactively on each side.

## 6. Fill Detection

Fills are detected via two channels:

1. **User WS** (`TradeStatusUpdate`): Real-time fill notifications from Polymarket. The engine matches the `order_id` to a resting order and records the fill immediately.

2. **Executor Feedback**: When the executor posts, cancels, or sends a FOK, the result includes fill information:
   - `OrderPosted { already_filled }` — rare for post-only but handled
   - `CancelResult { order_id, size_matched }` — reveals fills that occurred before the cancel
   - `RebalanceResult { filled, size_matched }` — rebalance taker fills

Each fill records: side, price, size, timestamp, was_taker flag, and fee (maker rebate or taker fee).

### Fill Deduplication

User WS and CancelResult can both report the same fills (e.g., shares filled during a cancel round-trip). To prevent double-counting, each `ManagedOrder` tracks `size_filled` — the cumulative filled size already recorded in the position. Additionally, the quoter retains a `ClearedOrder` snapshot per side (`last_cleared`) so that late-arriving CancelResults can still dedup after a full WS fill has cleared the order.

- **User WS path**: Calls `quoter.record_ws_fill(side, cumulative_matched)` which computes `delta = cumulative - size_filled`, updates the tracker, and returns the delta. Only the delta is recorded in the position. On full fill, `on_fill(was_full=true)` clears the order but saves its `(order_id, price, size_filled)` to `last_cleared`.
- **CancelResult path**: Uses `quoter.lookup_order_for_cancel(side, order_id)` to find the order by ID — checking the current resting order first, then `last_cleared`. Computes `delta = size_matched - already_filled`. Only records if delta > 0. If the order_id is not found (stale), the CancelResult is silently ignored.

This ensures fills are counted exactly once regardless of which path arrives first, if both arrive, or if the WS fill clears the order before the CancelResult. Correct fill tracking is a prerequisite for accurate repost sizing after a cancel-requote cycle.

## 7. Market Rotation

When the Gamma API discovers a new market:

1. **Report** the outgoing market (if any fills occurred): Telegram message with shares, pairing, locked profit, rebates
2. **Reset** all state: position, quoter, fair value estimator
3. **Forward** `MarketRotation` command to executor (resets its SDK caches)
4. **Enter QUIET phase**: Wait `rotation_quiet_ms` before quoting (lets books populate and strike price settle)
5. **Strike snapshot**: Median of first `strike_warmup_count` (default: 5) Binance ticks after rotation — this is the reference price for the fair value model

## 8. State Machine

```
                    MarketRotation
    ┌─────────────────────┐
    │                     ▼
  IDLE ──────────── ▶ QUIET ──────────── ▶ QUOTING
                     (wait quiet_ms)      (post maker orders)
                                          ◄─── requote loop ──►
```

| Phase | Entry Condition | Behavior |
|-------|----------------|----------|
| **IDLE** | Boot (no market yet) | No quoting, waiting for first rotation |
| **QUIET** | MarketRotation received | Wait for books to populate, set strike price |
| **QUOTING** | `now >= quiet_until_ms` | Evaluate and post/requote on both sides. Blocked by: stale vol data, vol tracker not warm, unhealthy heartbeat, fair value estimator not warm, wide Polymarket spread (`> max_entry_spread` on either book), stale Polymarket book (`> stale_book_ms` since last update). Also: taker rebalance if imbalance `≥ rebalance_threshold` |

All phases revert to QUIET on the next MarketRotation. The bot quotes continuously until market rotation — there is no closing phase.

## 9. Config Reference

### `[fair_value]` — Fair Value Model

| Param | Default | Description |
|-------|---------|-------------|
| `momentum_weight_basis` | 0.10 | Weight of basis delta in momentum adjustment |
| `momentum_weight_cvd` | 0.05 | Weight of CVD acceleration in momentum adjustment |
| `momentum_weight_obi` | 0.05 | Weight of OBI velocity in momentum adjustment |
| `max_momentum_adj` | 0.08 | Maximum absolute momentum shift to fair value |
| `vol_edge_scale` | 0.5 | How much realized vol widens the edge |
| `time_edge_scale` | 0.02 | How much time remaining widens the edge (sqrt scaling) |
| `baseline_vol` | 0.00003 | Reference vol for normalization (vol_normalized = realized / baseline) |
| `vol_ring_capacity` | 600 | Short-window ring buffer size (~30s at 20 ticks/sec) |
| `vol_session_capacity` | 6000 | Session-window ring buffer size (full 5-min market) |
| `vol_freshness_ms` | 500 | Vol tracker staleness threshold — blocks quoting if exceeded |
| `vol_min_warmup` | 10 | Minimum samples before vol tracker returns valid data |
| `vol_default` | 0.00003 | Default vol returned before warmup |
| `vol_ticks_per_sec` | 20.0 | Estimated BinanceTick cadence for time-scaling vol |
| `tail_compression_factor` | 0.85 | Multiplier on z-score to fatten tails |
| `stale_data_edge_penalty` | 0.01 | Extra edge added per stale momentum feed |
| `regime_spike_threshold` | 2.0 | short_vol / session_vol ratio that triggers spike penalty |
| `regime_spike_penalty` | 0.01 | Extra edge during a vol regime spike |
| `strike_warmup_count` | 5 | Number of BinanceTick prices to median for strike |
| `basis_halflife_ms` | 250 | EMA half-life for basis delta tracker |
| `basis_freshness_ms` | 150 | Staleness threshold for basis data |
| `basis_min` | 0.0 | Minimum threshold for basis normalization |
| `basis_saturation` | 0.05 | Saturation point for basis normalization |
| `cvd_fast_halflife_ms` | 200 | Fast EMA half-life for CVD tracker |
| `cvd_slow_halflife_ms` | 500 | Slow EMA half-life for CVD tracker |
| `cvd_freshness_ms` | 150 | Staleness threshold for CVD data |
| `cvd_min` | 0.0 | Minimum threshold for CVD normalization |
| `cvd_saturation` | 0.6 | Saturation point for CVD normalization |
| `obi_halflife_ms` | 250 | EMA half-life for OBI velocity tracker |
| `obi_freshness_ms` | 120 | Staleness threshold for OBI data |
| `obi_min` | 0.0 | Minimum threshold for OBI normalization |
| `obi_saturation` | 0.2 | Saturation point for OBI normalization |

### `[quoting]` — Order Management

| Param | Default | Description |
|-------|---------|-------------|
| `min_edge` | 0.04 | Minimum profit margin per share ($0.04 = 4 cents). Raised from 0.03 to account for 1.4s latency |
| `requote_threshold` | 0.01 | Fair value drift that triggers a requote (cancel then repost) |
| `max_order_size` | 100 | Maximum shares per order |
| `min_order_size` | 5 | Minimum shares per order (below this, skip posting) |
| `max_fair_value_extremity` | 0.85 | Don't quote when either side's FV exceeds this (or is below 1 - this). At extremes, the cheap side can't fill, making pairing impossible |

### `[risk_v2]` — Risk Limits

| Param | Default | Description |
|-------|---------|-------------|
| `max_unpaired_shares` | 30 | Maximum unpaired shares per side (lowered from 50 to reduce directional exposure) |
| `rotation_quiet_ms` | 8000 | Quiet period after market rotation (ms) |
| `rebalance_threshold` | 15 | Shares imbalance to trigger taker rebalance |
| `rebalance_size` | 10 | Shares per rebalance FOK order |
| `rebalance_max_pair_cost` | 0.97 | Max pair cost for rebalance (only rebalance when profitable) |
| `stale_book_ms` | 850 | Polymarket book staleness threshold — blocks quoting if either book is older than this |
| `max_entry_spread` | 0.08 | Maximum Polymarket book spread — blocks both sides if either book spread exceeds this |
| `heartbeat_dead_threshold` | 5 | Consecutive heartbeat failures before unhealthy. On transition, all resting orders auto-cancelled + Telegram alert |
| `binance_stale_event_ms` | 150 | Binance event staleness threshold |

### `[rotation]` — Market Discovery

| Param | Default | Description |
|-------|---------|-------------|
| `prewarm_lead_secs` | 20 | Seconds before market end to start searching for next market |

## 10. Operational Layer (Telegram + QuestDB)

### 10.1 Telegram Notifications

Three notification categories, each gated by a `/trades`, `/summary`, or `/diag` toggle:

| Category | Toggle | Pattern | Content |
|----------|--------|---------|---------|
| Fill notifications | `trades_enabled` | Per fill, non-critical | `MAKER YES 10@$0.470 \| YES:30 NO:20 \| paired:20 locked:$2.00` |
| Market reports | `summary_enabled` | Per market rotation, critical | Full market summary (shares, pairing, locked profit, fees) |
| Session summary | `summary_enabled` | On shutdown/restart, critical | Uptime, markets traded, total fills, final position |
| Diagnostics | `diagnostics_enabled` | Every 60s, non-critical | Phase, fills, position, fair value, edge |

**Fill message format**: `{MAKER|TAKER} {YES|NO} {size}@${price} | YES:{total} NO:{total} | paired:{n} locked:${amt}`

All notifications are stored as pending messages in the engine and dispatched in the main loop. Fill and diagnostic messages use `post_telegram_message` (rate-limited). Market reports and session summaries use `fire_critical` (bypasses rate limit).

Messages are always logged via `info!()` regardless of toggle state.

### 10.2 QuestDB Analytics

Three tables, all written via ILP over TCP:

**`binance_ticks`** — 1/sec downsampled Binance ticker snapshots (batched, 1000 rows or 60s).

**`v2_fills`** — Individual fill events (flushed immediately):
- `condition_id` (symbol), `side` (symbol), `price`, `size`, `was_taker`, `fee`
- Running totals: `pair_cost`, `paired`, `locked_profit`
- Model snapshot: `fair_value_yes` (f64), `strike` (f64)
- `timestamp` (designated ts)

**`v2_market_summaries`** — End-of-market summaries (flushed immediately):
- `condition_id` (symbol), `yes_shares`, `no_shares`, `yes_avg`, `no_avg`
- `paired`, `pair_cost`, `locked_profit`, `taker_fees`, `fill_count`
- Model context: `strike` (f64), `final_fv_yes` (f64)
- Unpaired breakdown: `unpaired_yes` (f64), `unpaired_no` (f64), `rebalance_count` (i64)
- `timestamp` (designated ts)

Fill records and market summaries are produced alongside fill notifications and market reports in the engine, then drained and written in the main loop.

### 11.3 QuestDB Retention

A background task runs every 24h, dropping partitions older than 7 days for all three tables via QuestDB's HTTP `/exec` endpoint. Estimated disk usage: ~30 MB/week at 7-day retention.

### 11.4 `/status` Output (v2)

```
Uptime: 1h 23m 45s
Mode: v2-live [QUOTING]
Heartbeat: OK (42ms)
Market: ...abc12
Position: YES:30 NO:20
Paired: paired:20 locked:$2.00
Unpaired: YES:10 | NO:0
Markets: 5 | Fills: 47
Trades notify: on | Summary notify: on
```

## 11. Source File Map

| File | Purpose |
|------|---------|
| `engine/strategy.rs` | `V2StrategyEngine`: event routing, state machine, quote ticks |
| `engine/fair_value.rs` | `FairValueEstimator`: BTC probability model, momentum adjustment, edge sizing |
| `engine/quoter.rs` | `Quoter`: per-side order management, requoting, inventory skewing |
| `engine/position.rs` | `BilateralPosition`: share tracking, pairing math, PnL computation |
| `engine/buildup/metrics.rs` | Metric trackers: CVD, OBI velocity, basis delta, realized vol |
| `executor/live.rs` | `LiveExecutor`: CLOB order placement, cancel, FOK, SDK caching |
| `executor/fill_engine.rs` | `compute_taker_fee`, `round_to_tick` |
| `types/market.rs` | `IngestorEvent`, `MarketState`, `OrderBook`, Binance structs |
| `types/order.rs` | `V2ExecutorCommand`, `V2ExecutorFeedback`, `OrderRequest` |
| `config.rs` | `Config`, `BotConfig`, TOML sub-configs, `.env` loading |
| `gateway/polymarket/rotation.rs` | Gamma API polling, market discovery, rotation events |
| `gateway/polymarket/rest.rs` | CLOB REST: order placement, cancellation |
| `gateway/polymarket/market_ws.rs` | Public Market WS: book, price, tick events |
| `gateway/polymarket/user_ws.rs` | Authenticated User WS: fill detection |
| `gateway/binance/ws.rs` | Spot SBE WS: depth20 + bestBidAsk + @trade |
| `gateway/binance/futures_ws.rs` | Futures JSON WS: @aggTrade, @bookTicker, @forceOrder |
| `reporting/telegram.rs` | Telegram Bot API (fire-and-forget, rate-limited) |
| `storage/cold.rs` | QuestDB ILP ingestion: binance_ticks, v2_fills, v2_market_summaries, retention |
| `control/listener.rs` | Telegram command listener (getUpdates polling) |
| `control/handlers.rs` | Command handlers (pure logic) |
| `control/config_editor.rs` | TOML read/write, param allowlist with ranges |
| `control/wallet.rs` | `/balance`, `/polybalance`, `/redeem` |
