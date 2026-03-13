# Signal Detection & Entry (Sections 1-5)

---

## 1. Core Trade Model

FaCaiBot exploits the repricing lag between Binance (source of truth) and Polymarket's CLOB (5-minute BTC prediction markets). The bot uses a **predictive entry model**: a composite 6-metric buildup detector identifies directional momentum building across futures and spot markets, and the bot places a maker post-only order on the Polymarket CLOB **before** the price reprices. After Leg 1 fills, it hedges with the opposite side:

```
Buildup detected -> Buy YES (Leg 1, maker post-only at best ask)
                 -> CLOB reprices, maker fills
                 -> Buy NO (Leg 2, post-only maker, $0 fee)
                 -> Paired position: e.g. $0.48 + $0.495 = $0.975
                 -> Market resolves -> pays $1.00 -> 2.5% profit
```

**Key economics:**
- Leg 1 uses a **maker post-only** order -- earns maker rebate (negative fee) instead of paying taker fees
- Leg 2 targets post-only execution (maker, zero fee) where possible
- Additional taker fees apply to Leg 2 FOK emergency exits (Phase 1 breach, break-even breach, Phase 2 timeout, whipsaw reversal, flow reversal, flow collapse, market expiry) and favorable taker fills
- Taker fee formula: `C * 0.25 * (p*(1-p))^2`, max 1.56% at p=0.50
- Maker rebate estimate: `taker_fee * 0.20` (upper-bound of Polymarket daily maker rebate)
- Unfilled Leg 1 makers are cancelled (sustain failure or timeout) at zero cost (post-only)

**One trade at a time.** The engine self-gates after emitting a Leg 1 signal: no new buildups are evaluated until the current trade completes or resets.

---

## 2. Signal Detection Pipeline

The signal detection system uses the **composite BuildupDetector** as its sole entry mechanism. It runs inline in the engine, fed by data from the ingestor (dedicated OS thread, CPU-pinned core 0).

### 2a. BuildupDetector (Composite 6-Metric)

The `BuildupDetector` (`engine/buildup/detector.rs`) combines 6 real-time metrics from Binance futures and spot streams into a single composite score. It runs inline in the engine, evaluated after every relevant ingestor event (futures trade, futures book ticker, futures liquidation, spot trade, spot depth).

#### Data sources

| Stream | Protocol | Feeds metric |
|--------|----------|-------------|
| Binance Futures `@aggTrade` | JSON WS | CVD acceleration |
| Binance Futures `@bookTicker` | JSON WS | Basis delta |
| Binance Futures `@forceOrder` | JSON WS | Liquidation pressure |
| Binance Spot `@trade` | SBE WS | Spot trade flow |
| Binance Spot `@depth20` (50ms) | SBE WS | OBI velocity, ATR displacement |
| Binance Spot `@bestBidAsk` | SBE WS | Spot mid (basis computation) |

#### 6 metrics

| # | Metric | Source | Category | What it measures | Weight | Academic Backing |
|---|--------|--------|----------|-----------------|--------|------------------|
| 1 | **Basis delta** | Futures `@bookTicker` + Spot | Leading | Futures-spot basis change (EMA of `futures_mid - spot_mid`) | **0.35** | Strong (CME, CFB, Aleti et al. 2021) |
| 2 | **CVD acceleration** | Futures `@aggTrade` | Supporting | Net aggressive buying/selling acceleration (fast EMA - slow EMA of volume delta) | **0.20** | Empirical only (no peer review) |
| 3 | **OBI velocity** | Spot `@depth20` | Confirming | Rate of change of Order Book Imbalance (EMA-smoothed) | **0.20** | Strong (PSU, arXiv:2507.22712) |
| 4 | **Spot trade flow** | Spot `@trade` | Confirming | EMA of net aggressive buy vs sell flow on spot | **0.15** | Strong (VPIN, J. Financial Markets 2025) |
| 5 | **Liquidation pressure** | Futures `@forceOrder` | Risk gauge | Exponentially-decaying sum of forced liquidation volume (half-life 2s) | **0.05** | Reactive, not predictive |
| 6 | **ATR displacement** | Spot `@depth20` | Volatility | Current spot displacement from EMA, normalized by EMA-ATR | **0.05** | No direction prediction (Chen & Liao 2009) |

**Reweighting rationale (2026-03-11c):**
- **Basis -> 0.35** (was 0.20): CME and Binance Futures consistently lead spot by 100-500ms. Basis expansion predicts spot rallies with ~70% accuracy at 5-min horizon. Strongest empirical signal.
- **CVD -> 0.20** (was 0.30): Order flow toxicity (VPIN) is academically validated, but CVD acceleration lacks peer-reviewed validation. Reduced to supporting role pending empirical backtest results.
- **OBI -> 0.20** (unchanged): Confirmed microstructure predictor, but signal degrades in high-volatility regimes (when FaCaiBot trades). Keep as co-confirming signal with spot flow.
- **Spot flow -> 0.15** (unchanged): VPIN (order flow toxicity) is well-validated, but lags futures by 100-500ms. Useful for hedge confirmation, not entry prediction.
- **ATR -> 0.05** (was 0.10): Academic literature (Chen & Liao 2009, Hansen & Lunde) confirms ATR predicts volatility magnitude, NOT direction. Only valid for position sizing/risk management.
- **Liquidations -> 0.05** (unchanged): Cascades are reactive (amplify existing moves), not predictive. Kept as risk gauge (avoid entries near liquidation clusters).

**Sources:**
- CME OpenMarkets, CFB Benchmarks: Bitcoin basis dynamics as leading indicator
- "Bitcoin spot and futures market microstructure" (Aleti et al., J. Futures Markets 2021)
- "Bitcoin wild moves: Evidence from order flow toxicity and price jumps" (ScienceDirect 2025) -- VPIN validation in crypto
- "Order Book Filtration and Directional Signal Extraction at High Frequency" (arXiv:2507.22712, 2025)
- "Microstructure and Market Dynamics in Crypto Markets" (Easley et al., Cornell/SSRN-4814346)

Each metric is independently normalized to [0,1] using configurable `(min, saturation)` bounds and a freshness window (stale metrics return 0). Each metric also tracks a direction (Up/Down).

#### Future Improvement: Futures Order Flow Imbalance (Recommended)

**Current status:** CVD acceleration is empirical; VPIN (Volume-Synchronized Probability of Informed Trading) is the academic standard for order flow toxicity. Binance does not directly expose VPIN, but **Futures Order Flow Imbalance (OFI)** can be computed simply:

```
FuturesOFI = (buy_volume - sell_volume) / (buy_volume + sell_volume)
```

Where buy/sell volume is the 1-min or 30-sec rolling sum of aggressive trade volumes on Binance Futures `@aggTrade`. This is:
- **Simpler** than CVD acceleration (one EMA instead of two)
- **More validated** (aligns with VPIN literature)
- **Earlier signal** (no slow EMA lag)

**Recommendation:** Replace CVD acceleration with Futures OFI in a future iteration. Architecture:
1. Add `FuturesOrderFlowTracker` in `src/engine/buildup/metrics.rs` (similar to `SpotFlowTracker`, but on futures trades)
2. Feed `FuturesAggTrade` events to it in `strategy.rs::on_event()`
3. Set weight to 0.20-0.25 (currently assigned to CVD)
4. Remove or reduce CVD weight

This change would align the system fully with academic literature on order flow toxicity.

#### Evaluation pipeline

1. **Normalize**: Compute [0,1] value for each metric (0 if stale beyond freshness window)
2. **Direction consensus**: At least 3 of the 5 directional metrics (basis delta, CVD, OBI velocity, spot flow, liquidation pressure) must agree, with at most `max_dissenters` (default: 1) disagreeing. If `minority > max_dissenters`, the signal is **vetoed** (score = 0). ATR displacement is not a directional metric and does not participate in the vote. Set `max_dissenters = 0` for unanimous agreement among all active directional metrics
3. **Causal ordering**: At least 1 **leading** metric (basis delta or CVD) AND 1 **confirming** metric (spot flow or OBI velocity) must be fresh and non-zero. Vetoed otherwise
4. **Weighted sum**: `composite = sum(weight_i * normalized_i)`
5. **Entry check**: If `composite >= entry_threshold` (default 0.40), emit `BuildupInfo` with full metric breakdown. The `above_threshold` flag is set on the first crossing and cleared when the score drops below threshold. **Direction flips while above threshold also reset `above_threshold`**, allowing a new signal to fire immediately on direction change without requiring the score to dip below threshold first. `last_direction: Option<Direction>` tracks the previous composite direction for this comparison

#### Expected behavior changes with new weights (2026-03-11c)

**Will improve:**
- **Faster entries when basis widens** -- basis now worth 35% instead of 20%, so basis-only spikes + confirming spot flow will trigger sooner
- **Fewer false positives on CVD noise** -- CVD reduced from 30% to 20%, so noisy CVD acceleration without basis/OBI backing will score lower
- **More stable profit targets** -- higher basis weight = more stable repricing estimates (basis is more validated than CVD)

**May hurt (if basis lags):**
- **On Binance-specific venues** -- if Binance Futures basis lags CME, higher basis weight could delay entries. Backtest and compare.
- **On sudden reversals** -- if reversal happens before basis contracts, ATR-only signals now score even lower (0.05 weight). Rely on directional consensus veto instead.

**Tuning guidance for live trading:**
- **If entries are too sparse:** Lower `entry_threshold` from 0.40 to 0.35, or decrease `basis_halflife_ms` from 300 to 200 for faster response
- **If entries over-allocate:** Reduce `max_alloc_per_trade` or lower `reprice_scale` -- don't change weights without backtest data
- **If basis is stale at your latency:** Increase `freshness_basis_ms` back to 300ms and re-run diagnostics

#### Thresholds

| Threshold | Default | Purpose |
|-----------|---------|---------|
| `entry_threshold` | 0.40 | Composite score must exceed this to trigger Leg 1 entry |
| `cancel_threshold` | 0.25 | Composite below this cancels unfilled Leg 1 maker (sustain failure) or triggers emergency FOK during hedge |

Config: `[buildup]` section with all weights, thresholds, per-metric halflife/freshness/min/saturation values.

### 2b. Signal delivery

- **`BuildupConfirmed(BuildupInfo)`**: Composite 6-metric detector exceeded `entry_threshold`. Engine sets `buildup_detected = true`, `last_buildup = Some(info)`. Processed via `handle_buildup_confirmed()`

Normal `BinanceTick` events update `binance_price` and feed the buildup detector's spot BBA tracker but do not directly trigger entry evaluation. Futures events (`FuturesAggTrade`, `FuturesBookTicker`, `FuturesForceOrder`) and spot events (`SpotTrade`, `BinanceDepth`) each feed their respective buildup metrics and trigger `check_buildup_entry()`.

### 2c. OBI (Order Book Imbalance)

`BuildupInfo` carries an `obi` field: Order Book Imbalance from the Binance `@depth20` snapshot.

```
obi = (bid_depth - ask_depth) / (bid_depth + ask_depth)
```

Range [-1, +1]: positive = bid-heavy (bullish), negative = ask-heavy (bearish). Computed by `BinanceDepth::obi()` and stored by the buildup detector (`last_obi`). Used as an entry guard: signals whose OBI contradicts the buildup direction (e.g., Up signal with strongly bearish OBI) are rejected (`ObiMismatch`). Config: `min_obi_alignment` (default 0.2).

---

## 3. Entry Validation (Leg 1 Guards)

When `buildup_detected = true`, the evaluator checks every guard in sequence. **The ordering matters** -- cheaper/faster checks run first, and the `ActiveTrade` check is deliberately placed after book checks.

| # | Guard | Condition | Rejection | Why this order |
|---|-------|-----------|-----------|----------------|
| 1 | **No buildup** | `!buildup_detected` | `Skipped` | Fast path -- most events have no buildup |
| 2 | **No market** | `active_condition_id` absent | `Other` | Rare edge case after rotation |
| 3 | **Direction book** | Book for YES (Up) or NO (Down) token exists with bid+ask | `NoBook` | Can't price without book |
| 4 | **Binance price** | `binance_price` exists | `NoBinance` | Reference price needed |
| 5 | **Stale book** | `book_age_ms > stale_book_ms` (500ms) | `StaleBook` | Stale data = unreliable pricing |
| 6 | **Hard skew cap** | YES mid > `hard_skew_cap` or < `1 - hard_skew_cap` (default 0.90) | `PriceSkewed` | Extreme markets have negligible repricing capacity |
| 7 | **Active trade** | `leg1_state != None` | `ActiveTrade` | **After book** -- `rej_busy` counts only signals that had a valid book |
| 8 | **Entry cutoff** | `time_remaining_secs < entry_cutoff_secs` (see config.toml) | `Other` | Defence-in-depth |
| 9 | **Repricing model** | `expected_pct < min_reprice_pct` (2.0%) | `InsufficientRepricing` | Model output too low to justify entry |
| 10 | **OBI alignment** | Binance book imbalance contradicts buildup direction | `ObiMismatch` | Signal quality confirmation from order flow |
| 11 | **Min size clamp** | Entry size clamped to `max(5, ceil($1/price))` | n/a (clamped, not rejected) | CLOB minimum: 5 shares for maker, $1.00 notional for FOK |

### Direction-aware book selection

- **Buildup Up** -> buying YES -> use YES book (fallback: generic book)
- **Buildup Down** -> buying NO -> use NO book (fallback: derive from YES complement -- `no_bid = 1 - yes_ask`, `no_ask = 1 - yes_bid`)

### Self-gating

`buildup_detected` is cleared on both signal emission AND rejection. Each buildup gets exactly one evaluation attempt, preventing re-evaluation on subsequent events and signal flooding.

---

## 4. Repricing Model & Allocation

### Repricing model

The repricing model runs once per trade lifecycle at signal time:

- **Entry repricing**: Computed in `Leg1Evaluator::evaluate()` using the composite buildup score as signal strength. The result (`expected_pct`) drives the entry gate, allocation, and becomes the Phase 1 profit target. `init_leg2()` stores this value directly in `HedgeState.expected_pct`.

### Expected repricing formula

```
expected_reprice_pct = norm_signal x adjusted_sensitivity x time_factor x reprice_scale
```

Output is a decimal fraction (e.g., 0.015 = 1.5%). The raw output drives entry gate (min_reprice_pct) and allocation (alloc_fraction). The Phase 1 profit target is **dampened**: `round_to_tick(expected_pct x phase1_target_dampen, tick)` -- separating "is this worth trading?" from "what target is achievable?". Default `phase1_target_dampen = 0.8`.

**Component 1: `norm_signal` [0,1]** -- signal quality:

The composite score is already [0,1]-normalized by the buildup detector, so no re-normalization is needed:
```
norm_signal = clamp((composite_score - 0) / (1 - 0), 0, 1) = composite_score
```

**Component 2: `adjusted_sensitivity`** -- binary option delta proxy:
```
base = 4P(1-P)                    // P = yes_mid
distance = |yes_mid - 0.5|
with_consensus = (Up AND yes_mid > 0.5) OR (Down AND yes_mid < 0.5)
alignment = with_consensus ? (1.0 + distance) : (1.0 - distance)
adjusted_sensitivity = base x alignment
```

**Component 3: `time_factor`** -- expiry amplification:
```
time_factor = min((300.0 / max(time_remaining_secs, 10)) ^ time_exponent, max_time_factor)
```
Default `time_exponent = 0.5` (sqrt). Set to 0 to disable. `max_time_factor` (default 2.0) caps the amplification -- at default settings, time can at most double the output (kicks in at ~75s remaining).

**Component 4: `reprice_scale`** -- calibration ceiling (default 0.015 = 1.5%).

### Three-layer entry guard

1. **Hard skew cap** (`hard_skew_cap`, default 0.90): Reject if YES mid > 0.90 or < 0.10.
2. **Minimum expected repricing** (`min_reprice_pct`, default 0.02 = 2.0%): Model output must exceed this floor. Rejection reason: `InsufficientRepricing`.
3. **Dynamic allocation**: `alloc_fraction = clamp(model_output / reprice_scale, min_alloc_pct, 1.0)`.

### Allocation

`alloc = max(round(max_alloc_per_trade x alloc_fraction, 2dp), $0.01)`. Dynamic -- better signals get more capital.

Entry size: `round_dp(alloc / ask_price, 2)`, then clamped to `max(raw, 5, ceil($1/price))`. The 5-share floor satisfies the CLOB maker minimum; the `ceil($1/price)` floor satisfies the FOK $1.00 notional minimum. If entry_size rounds to 0 before clamping, the signal is rejected.

### ProfitTier (display-only label)

| Expected Repricing | Tier |
|--------------------|------|
| >= `reprice_scale` (1.5%) | HIGH |
| >= `reprice_scale / 2` (0.75%) | MED |
| < `reprice_scale / 2` | LOW |

`ProfitTier::from_expected_reprice()` -- used for Telegram labels and diagnostics only. Does not affect allocation or targets.

---

## 5. Leg 1 Execution

### Pricing logic

The evaluator sets `signal.price = best_ask` -- the best ask on the directional book (YES for Up buildups, NO for Down). This is the price at which the maker post-only order is placed. Size: `(alloc / ask_price).round_dp(2)`.

### Fee model

Leg 1 is a **maker post-only** order. The fee is **negative** (rebate): `signal.leg1_fee = -compute_maker_rebate(ask_price, entry_size)`. This rebate improves the effective breakeven for the hedge: `breakeven = 1.0 - fill_price - leg1_fee` (where `leg1_fee < 0`, so breakeven widens).

### Maker post-only execution

Leg 1 uses a single **post-only GTC** (Good-Til-Cancelled) maker order placed at `best_ask`:

1. **Order construction:** `OrderRequest::post_only_gtc(token_id, Buy, price, size)`
2. **Placement:** Single `place_order()` HTTP call to CLOB
3. **Resting:** Order sits on the book waiting for a market maker to sell into it (or a taker to lift the ask)
4. **Feedback:** `OrderPosted { already_filled, price, size }` -- if `already_filled=true` (rare for post-only), engine transitions directly to `Filled`. Otherwise, order rests and fill is detected via User WS

If the CLOB rejects the order (`Rejected` status -- e.g., price crosses the book), `OrderFailed` feedback is sent and the slot is freed. For SDK/network errors, same `OrderFailed` path.

### Leg 1 sustain (flow-based cancel)

After Leg 1 is posted (maker resting on book), the engine monitors the composite flow score on every event. Two cancel triggers:

1. **Flow fade:** `current_composite_score < cancel_threshold` (default 0.25) -- the buildup that triggered entry has dissipated. Cancel the unfilled maker
2. **Timeout:** `cancel_window_ms` elapsed since posting (configurable) -- prevent indefinite resting

Both triggers dispatch `CancelLeg1Order { order_id }` to the executor. The cancel is a fire-and-confirm operation:
- **Cancel confirmed** (`was_cancelled = true`): Engine resets `leg1_state = None`, `leg1_direction = None`, `pending_leg1_signal = None`, `buildup_detected = false`. Ready for next signal
- **Cancel NOT confirmed** (`was_cancelled = false`): Order may have filled before cancel reached CLOB. Engine keeps `leg1_state = Posted` and waits for User WS fill notification. If User WS sends MATCHED, Leg 1 transitions to `Filled` and hedge begins normally

Guards:
- `pending_leg1_cancel.is_none()` -- prevents duplicate cancel dispatch while waiting for CLOB response
- **Provisional ID guard**: Sustain checks (timeout, flow fade, and ask-drift repost) are skipped while the order ID starts with `"sim-leg1-"` (provisional). The cancel window and flow fade only activate after `on_order_posted()` replaces the provisional ID with the real CLOB order ID and resets `timestamp_ms` to the confirmation time. This prevents futile cancel attempts against a non-existent CLOB order during the ~1.3s SDK round-trip.

Diagnostic counters: `diag_sustain_cancels` (flow fade), `diag_sustain_timeouts` (timeout).

**Immediate drift check on order confirmation**: `on_order_posted()` installs the real CLOB order ID and immediately runs a drift check. If the ask has moved ≥ `leg1_repost_tick_threshold` ticks since evaluation time, the order is cancelled for repost right away — without waiting for the next flow update. This catches the case where the ask drifted significantly during the ~1.3s SDK round-trip and the sustain checks were suppressed (provisional ID guard) throughout that window.

### 5a. Adaptive Leg 1 Repost

After Leg 1 is posted at `best_ask`, the market maker's ask price may drift upward while the buildup composite score remains strong (`> cancel_threshold`). If the ask moves ≥ `leg1_repost_tick_threshold` ticks away, the order sits stranded below the market. A repost at the new ask recovers this case — but is bounded by two hard limits to prevent the chase loop that destroys entry advantage.

**Repost trigger logic** (runs on every event in sustain block + on_order_posted):
```
if leg1_state == Posted AND composite_score > cancel_threshold:
    if current_ask - posted_ask >= leg1_repost_tick_threshold * tick_size:
        repost_allowed = (max_repost_count == 0) OR (repost_count < max_repost_count)
        chase_allowed  = (max_chase_ticks == 0)  OR (current_ask - original_signal_price <= max_chase_ticks * tick_size)
        if repost_allowed AND chase_allowed:
            cancel(repost: true)
        else:
            log "repost CAPPED, order stays resting"
```

**Repost flow:**
1. Engine detects ask drift ≥ threshold while composite live
2. Checks two guards: `repost_count < max_repost_count` AND `chase_distance ≤ max_chase_ticks`
3. If either guard fails: logs "repost CAPPED", increments `diag_repost_capped`, order stays resting
4. If both pass: dispatches `CancelLeg1Order` to executor, increments `diag_repost_attempts`
5. Executor cancels the order on CLOB
6. Engine receives cancel confirmation with `was_cancelled = true`
7. **Repost path**: increments `leg1_repost_count`, re-arms `buildup_detected = true`, keeping `last_buildup`, `current_composite_score`, and `leg1_original_signal_price` live
8. Main loop calls `evaluate()` on the next event
9. `evaluate()` re-runs all entry guards (book, Binance price, skew, repricing) and emits a fresh Leg 1 signal at the new best ask
10. If any guard rejects the repost (e.g., buildup faded between cancel dispatch and confirm), full state reset as normal

**Chase anchor**: `leg1_original_signal_price` is captured on the first Leg 1 post (in `evaluate()` and `on_order_posted()`). All subsequent reposts measure chase distance from this anchor, not from the most recent posted price. This prevents incremental drift from bypassing the cap.

**Config:**
- `leg1_repost_tick_threshold` (default: 1) — minimum ask drift in ticks to trigger repost. Set to 0 to disable
- `max_repost_count` (default: 2) — maximum reposts per buildup episode. Set to 0 for unlimited
- `max_chase_ticks` (default: 4) — maximum chase distance in ticks from original signal price. Set to 0 for unlimited

**State resets**: `leg1_repost_count` and `leg1_original_signal_price` are cleared in: `on_trade_complete()`, MarketRotation handler, `reset_leg1_state()`, and the "composite faded" / non-repost branches of `on_cancel_result()`.

**Why no sustain window on reversal?**
Repost is only triggered when composite **remains live** (`> cancel_threshold`). It does NOT require a sustain window for composite decay — if the composite fades, the cancel defaults to `repost: false` and state is fully reset. This allows rapid adaptive reposting as long as the buildup signal is hot, but immediately stops when the signal dies.

**Scenario examples:**
- **Ask drifts 1 tick (within cap):** Composite = 0.45 (still live), ask moves 1 tick up, repost_count=0 → cancel + repost. Entry at original+1tick.
- **Ask drifts 5 ticks (chase cap hit):** Composite = 0.60 (very hot), ask moves 5 ticks from original, max_chase_ticks=4 → repost CAPPED. Order stays resting at current price.
- **3rd repost attempt (count cap hit):** After 2 successful reposts, ask drifts again, max_repost_count=2 → repost CAPPED. No further chasing.
- **Ask drifts + buildup fades:** Composite drops below cancel_threshold → cancel with `repost: false` → full reset including repost counters.

Diagnostic counters: `diag_repost_attempts` (reposts issued), `diag_repost_capped` (reposts blocked by limits). Both logged every 60s.

### Fill models

**Simulation:** The engine simulates maker fills. On each loop iteration, it checks if `ask <= fill_price` (our limit) and `near_ask_depth > 0` within 2 ticks. If both pass, transitions to `Filled` instantly and initializes the hedge.

**Live:** The executor places a single post-only GTC order. Sends `OrderPosted { already_filled }` feedback. If `already_filled=true` (rare -- synchronous fill), engine transitions Leg 1 to `Filled`. Otherwise, the order rests on the book. Fill is detected via User WS `TradeStatusUpdate` (MATCHED event with matching hex order hash). On fill detection, `on_order_posted()` (or User WS handler) transitions Leg 1 to `Filled`, calls `init_leg2()`, and sends the Telegram opportunity alert.
