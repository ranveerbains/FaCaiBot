# Trading Logic — Complete Reference

This document covers every aspect of FaCaiBot's trading logic: signal detection, entry validation, execution, hedge management, emergency exits, and state machines — for both live and simulation modes.

---

## Table of Contents

1. [Core Trade Model](#1-core-trade-model)
2. [Signal Detection Pipeline](#2-signal-detection-pipeline)
3. [Entry Validation (Leg 1 Guards)](#3-entry-validation-leg-1-guards)
4. [Confidence Scoring & Allocation](#4-confidence-scoring--allocation)
5. [Leg 1 Execution](#5-leg-1-execution)
6. [Leg 2 Erosion Cascade](#6-leg-2-erosion-cascade)
7. [Emergency Exits](#7-emergency-exits)
8. [Post-Only First Strategy](#8-post-only-first-strategy)
9. [Favorable Taker Exits](#9-favorable-taker-exits)
10. [Trade Completion & State Reset](#10-trade-completion--state-reset)
11. [Market Rotation](#11-market-rotation)
12. [Cutoff Window](#12-cutoff-window)
13. [Capital Management](#13-capital-management)
14. [State Machines & Transitions](#14-state-machines--transitions)
15. [Edge Cases & Race Conditions](#15-edge-cases--race-conditions)
16. [Diagnostic Logging](#16-diagnostic-logging)
17. [Complete Trade Example](#17-complete-trade-example)
18. [Simulation vs Live Differences](#18-simulation-vs-live-differences)

---

## 1. Core Trade Model

FaCaiBot exploits the repricing lag between Binance (source of truth) and Polymarket's CLOB (15-minute BTC/ETH prediction markets). When BTC spikes on Binance, Polymarket market makers take seconds to adjust quotes. The bot enters before repricing and hedges with the opposite side:

```
Binance spike UP → Buy YES cheap (Leg 1, post-only, $0 fee)
                 → CLOB reprices
                 → Buy NO (Leg 2, post-only, $0 fee)
                 → Paired position: e.g. $0.48 + $0.495 = $0.975
                 → Market resolves → pays $1.00 → 2.5% profit
```

**Key economics:**
- Both legs target post-only execution (maker, zero fee)
- Taker fees apply only to FOK emergency exits: `C * 0.25 * (p*(1-p))^2`, max 1.56% at p=0.50
- At p=0.50 and 50 shares: ~$0.78 taker fee per FOK fill
- Unfilled post-only orders cost nothing — failed signals are free

**One trade at a time.** The engine self-gates after emitting a Leg 1 signal: no new spikes are evaluated until the current trade completes or resets.

---

## 2. Signal Detection Pipeline

**Source file:** `src/gateway/binance/spike.rs`

The spike detector runs on a dedicated OS thread (CPU-pinned core 0) processing Binance `@depth20@100ms` and `@ticker` WebSocket streams via `fastwebsockets`.

### Mid-price computation
```
mid = (best_bid + best_ask) / 2
```
Updated every ~100ms from `@depth20` orderbook snapshots.

### Rolling EMA-ATR
```
atr_alpha = 0.01 (~200-tick / ~20s memory)
atr(t) = alpha * |delta| + (1 - alpha) * atr(t-1)
```

No spikes are emitted for the first 10 ticks (~1 second) while the ATR warms up. This prevents false positives during initialization when the ATR is unrepresentative.

### 6-Gate Spike Confirmation Pipeline

Each gate is checked in sequence. A candidate is discarded the moment any gate fails:

| # | Gate | Threshold | What it filters |
|---|------|-----------|-----------------|
| 1 | **ATR warmup** | 10 samples | Initialization noise |
| 2 | **Per-tick threshold** | `\|delta\| > multiplier * ATR` (2x) | Normal volatility |
| 3 | **Window timeout** | 350ms | Transient liquidity gaps (candidate dies if not sustained) |
| 4 | **Sustain duration** | 300ms | Short-lived noise (must hold displacement for this long) |
| 5 | **Momentum ratio** | displacement/peak >= 0.50 | Fading spikes (price already retracing) |
| 6 | **Magnitude gate** | `displacement/origin_price >= min_magnitude_pct` (0.01 = 1%) | Marginal moves too small to trade |

**Timing:** The "catch zone" is `window_ms - sustain_ms` = 50ms. This is the tolerance for network jitter between sustain confirmation and window expiry.

**Magnitude uses sustain-time displacement**, not peak overshoot. This ensures we're measuring the sustained move, not a momentary spike that's already fading.

### Spike Delivery
Confirmed spikes emit `IngestorEvent::SpikeConfirmed(SpikeInfo)` carrying:
- `direction`: Up or Down
- `magnitude`: as Decimal fraction (e.g., 0.005 = 0.5%)
- `sustained_ms`: how long displacement held
- `timestamp_ms`: when spike was first detected

This is a dedicated event variant — normal `BinanceTick` events only update `binance_price` and never trigger spike evaluation.

---

## 3. Entry Validation (Leg 1 Guards)

**Source file:** `src/engine/evaluator.rs` — `Leg1Evaluator::evaluate()`

When `spike_detected = true`, the evaluator checks every guard in sequence. **The ordering matters** — cheaper/faster checks run first, and the `ActiveTrade` check is deliberately placed after book/spread checks (see rationale below).

### Guard ordering and rationale

| # | Guard | Condition | Rejection | Why this order |
|---|-------|-----------|-----------|----------------|
| 1 | **No spike** | `!spike_detected` | `Skipped` | Fast path — most events have no spike |
| 2 | **No market** | `active_condition_id` absent | `Other` | Rare edge case after rotation |
| 3 | **Direction book** | Book for YES (Up) or NO (Down) token exists with bid+ask | `NoBook` | Can't price without book |
| 4 | **Binance price** | `binance_price` exists | `NoBinance` | Reference price needed |
| 5 | **Stale book** | `book_age_ms > stale_book_ms` (500ms) | `StaleBook` | Stale data = unreliable pricing |
| 6 | **Price skew** | YES mid > `max_price_skew` (0.80) or < 0.20 | `PriceSkewed` | Near-certain markets have illiquid sides |
| 7 | **Spread** | `(ask - bid) / mid > max_spread_pct` (0.02 = 2%) | `SpreadWide` | Book too thin for reliable entry |
| 8 | **Active trade** | `leg1_state != None` | `ActiveTrade` | **After spread** — `rej_busy` counts only spikes that had a valid book. Separates "good signal, executor busy" from "spike on bad book" |
| 9 | **Entry cutoff** | `time_remaining_secs < entry_cutoff_secs` (300s) | `Other` | Defence-in-depth (normally caught upstream) |
| 10 | **Depth** | `book_bid_depth < required_depth * depth_min_pct` (0.20) | `InsufficientDepth` | Not enough liquidity to absorb our order |

### Direction-aware book selection

The evaluator uses the book for the token being bought:
- **Spike Up** → buying YES → use `poly_yes_book` (fallback: `poly_book`)
- **Spike Down** → buying NO → use `poly_no_book` (fallback: derive from YES complement)

If the NO book is unavailable (common early in market lifecycle), NO prices are derived as complement of YES: `no_bid = 1 - yes_ask`, `no_ask = 1 - yes_bid`.

### Self-gating

`spike_detected` is cleared on both `Signal` AND `Rejected` outcomes. Each spike gets exactly 1 evaluation attempt. This prevents:
- Re-evaluating the same spike on every subsequent event
- Signal flooding if multiple events arrive during a single spike

---

## 4. Confidence Scoring & Allocation

**Source file:** `src/engine/confidence.rs`

### Confidence formula
```
confidence = 0.4 * min(spike_magnitude / ATR, 1.0)    // spike quality vs recent volatility
           + 0.2 * min(total_book_depth / avg_depth, 1.0)  // book quality
           + 0.2 * (time_remaining_secs / 900.0)       // time value (900s = 15 min)

Max possible: 0.8
```

**Why 3 factors, not 4:** The sustain factor was removed — all confirmed spikes already passed the sustain gate, so it contributed a constant offset with zero discriminative value.

### Tier thresholds

| Confidence | Tier | Profit Target | Default `tier_pct` | Example ($20 max) |
|------------|------|---------------|--------------------|-------------------|
| >= 0.6 | HIGH | 2.5% | 100% | $20 |
| >= 0.3 | MED | 1.5% | 50% | $10 |
| < 0.3 | LOW | 1.0% | 25% | $5 |

### Allocation computation
```
alloc = max(round(max_alloc_per_trade * tier_pct), $1)
```
The `$1` floor ensures we always trade at least the minimum, even at the lowest tier.

### Entry sizing
```
entry_size = round_dp(alloc / bid_price, 2)  // Polymarket min precision: 0.01 shares
```
If `entry_size <= 0` after rounding, the signal is rejected.

---

## 5. Leg 1 Execution

### Pricing logic

```
bid_price = round_to_tick(best_bid + tick, tick)  // one tick above current best bid
```

**Capping:** If `bid_price >= best_ask`, cap at `best_ask - tick` (post-only constraint — must not cross spread).

**Smart outbidding:** If a depth wall is detected (single level with > `depth_wall_multiplier` (4x) the average depth of other levels), outbid it by 1 tick. This wins queue priority against competing bots. Only outbid if the resulting price stays within break-even and below the ask.

**Break-even cap:** Final check ensures `bid_price` won't cause pair cost > 1.0 even at the initial profit target:
```
est_leg2 = 1.0 - target_pct - bid_price
be_cap = 1.0 - est_leg2 - tick
if bid_price > be_cap: bid_price = round_to_tick(be_cap, tick)
```

### Simulation mode

**Source:** `src/engine/strategy.rs` — `advance_simulation()`

The engine acts as the simulated CLOB. On each event loop iteration:

```rust
// Post-only validity check
post_only_valid = best_ask > fill_price  // our bid is below the ask (no spread crossing)

// Depth check within 2 ticks of our bid
near_ask_depth = sum(asks where price <= fill_price + 2*tick)

should_fill = post_only_valid && near_ask_depth > 0
```

If `should_fill`:
1. Transition `leg1_state` from `Posted` to `Filled`
2. Initialize erosion state
3. Emit confirmed fill signal (`sim_confirmed_fill = true`) → `SimulationExecutor` records it

The `SimulationExecutor` receives two types of Leg 1 signals:
- `sim_confirmed_fill = false` (from `evaluate()`): Records signal detection only (counter, QuestDB)
- `sim_confirmed_fill = true` (from `advance_simulation()`): Records confirmed fill, creates `SimPosition`, deducts from `virtual_balance`, sends Telegram opportunity alert

### Live mode

**Source:** `src/executor/live.rs` — `handle_leg1()`

1. Build `OrderRequest::post_only_gtc(token_id, side, price, size)`
2. Submit via `PolymarketGateway::place_order()`
3. Handle response:
   - **Rejected** (post-only would cross spread): Send `ExecutorFeedback::OrderFailed`, increment `orders_failed`
   - **Placed**: Send `ExecutorFeedback::OrderPosted { order_id, price, size }`, increment `orders_placed`
   - **API error**: Send `OrderFailed`, log to QuestDB as "failed"
4. Log signal to QuestDB

The engine receives feedback via the reverse channel and updates `leg1_state` with the real CLOB order ID. This is needed for matching User WS fill notifications later.

---

## 6. Leg 2 Erosion Cascade

**Source files:** `src/engine/erosion.rs`, `src/engine/evaluator.rs` — `Leg2Evaluator::evaluate_leg2()`

### Erosion state initialization

When Leg 1 fills, `init_erosion()` captures:
```rust
ErosionState {
    leg1_fill_ms,                       // fill timestamp
    leg1_fill_price,                    // entry price
    leg1_fill_size,                     // shares
    initial_profit_target,              // tier target (0.025/0.015/0.010)
    steps_applied: 0,
    tier,                               // HIGH/MED/LOW
    direction,                          // Up/Down
    spike_info,                         // original spike details
    confidence,                         // original confidence score
    binance_at_fill: Some(binance_mid), // Binance mid at Leg 1 fill time
    opposing_ask_at_fill: Some(ask),    // hedge book best ask at fill time
    emergency_submitted: false,
    exit_reason: None,
}
```

The `opposing_ask_at_fill` baseline is critical — break-even breach detection compares current opposing ask against this baseline to detect deterioration.

### Step sizing — triangle weights

Five steps with front-loaded weights `[5, 4, 3, 2, 1]` (sum = 15):

| Step | Weight | % of margin | HIGH (2.5%) | MED (1.5%) | LOW (1.0%) |
|------|--------|-------------|-------------|------------|------------|
| 0 | 5/15 | 33.3% | 0.833% | 0.500% | 0.333% |
| 1 | 4/15 | 26.7% | 0.667% | 0.400% | 0.267% |
| 2 | 3/15 | 20.0% | 0.500% | 0.300% | 0.200% |
| 3 | 2/15 | 13.3% | 0.333% | 0.200% | 0.133% |
| 4 | 1/15 | 6.7% | 0.167% | 0.100% | 0.067% |

**Rationale:** Early steps give up more margin (higher chance of fill at a good price); later steps give up less and fire faster (urgency increases).

After all 5 steps: 100% of margin eroded → price is at break-even.

### Interval timing — exponential decay

```
interval(step) = max(base_ms * decay^step, 200ms)
```

Default: `base=3500ms`, `decay=0.5`:

| Step | Interval | Cumulative |
|------|----------|-----------|
| 0 | 3500ms | 3.5s |
| 1 | 1750ms | 5.25s |
| 2 | 875ms | 6.125s |
| 3 | 437ms | 6.562s |
| 4 | 218ms | 6.780s |

Early steps wait longer (market has time to fill at best price). Later steps fire rapidly (urgency).

Steps are capped at `MAX_EROSION_STEPS` (5). After step 5, the cascade is exhausted and auto-escalates to a `BreakEvenBreach` emergency (see Section 7d).

### Erosion evaluation flow

On every engine event, if `erosion` exists and `leg1_state == Filled`:

```
1. Is emergency already submitted? → repost at interval (see Section 8)
   After emergency_max_maker_attempts post-only reposts → FOK at best_ask
2. Compute hedge book data (direction-aware)
3. Check adverse movement (Binance reversal) → emergency post-only
4. Check break-even breach (opposing ask worsened)
5. Check erosion exhausted (steps_applied >= 5) → emergency post-only
6. Check quick reversal (within 100ms of fill)
7. Check erosion interval gate (time since last signal)
8. Determine whether to advance step (capped at MAX_EROSION_STEPS)
9. Compute new target price
10. Apply constraints (don't cross ask, break-even floor, smart outbid)
11. Emit erosion signal
```

### Target price computation

```
current_profit = initial_target - cumulative_erosion(steps)
target_price = round_to_tick(1.0 - current_profit - leg1_price, tick)
```

**Constraints applied in order:**
1. **Don't cross ask:** If `target >= best_ask`, clamp to `best_ask - tick`
2. **Break-even floor:** If `target > 1.0 - leg1_price`, clamp to break-even
3. **Smart outbid:** If depth wall detected on the ask side and wall price <= our target, outbid by 1 tick (wall_price - tick)

### Quick reversal protection

Within the first 100ms after Leg 1 fill, if Binance reverses by more than `quick_reversal_threshold` (0.03%), the evaluator holds off on emitting any Leg 2 signal. This gives the market a moment to settle before committing to a hedge direction.

---

## 7. Emergency Exits

All emergency exits set `erosion.emergency_submitted = true` and `erosion.exit_reason = Some(reason)`. Once set, the evaluator switches from erosion mode to emergency repost mode (see Section 8).

### 7a. Adverse Movement

**Trigger:** Binance reversal from `binance_at_fill` exceeds `adverse_threshold` (0.1% = 0.001)

```rust
change = |current_binance - binance_at_fill| / binance_at_fill
adverse = match direction {
    Up   => current < fill_price,  // price dropped
    Down => current > fill_price,  // price rose
};
if adverse && change >= adverse_threshold → EMERGENCY
```

**Timing:** Checked on every evaluation, zero grace period. The spike thesis is invalidated by the source (Binance) itself.

**Price:** First signal is **post-only** at `best_ask - 1 tick` (not taker). Subsequent reposts follow the standard emergency repost loop (Section 8) with FOK fallback after `emergency_max_maker_attempts` post-only attempts.

**FOK size:** `min(leg1_size, ask_depth_within_2_ticks)` — cap at available liquidity to avoid reject.

**Exit reason:** `ExitReason::AdverseMovement`

### 7b. Break-Even Breach

**Trigger:** Opposing ask has worsened beyond tolerance after first erosion step.

**Gates (all must be true):**
1. `steps_applied >= 1` (at least one erosion step completed, ~3.5s after fill)
2. `current_opposing_ask > opposing_ask_at_fill + break_even_tolerance_ticks * tick` (2 ticks)
3. `leg1_price + current_opposing_ask >= 1.0` (pair cost exceeds $1.00)

**FOK price cap:** `opposing_ask_at_fill + max_loss_ticks * tick` (3 ticks). If the ask has gapped beyond this cap, defer to erosion — don't crystallize a catastrophic loss.

```rust
max_fok_price = opposing_ask_at_fill + max_loss_ticks * tick
if ask > max_fok_price → defer to erosion (log and continue)
```

**Exit reason:** `ExitReason::BreakEvenBreach`

### 7c. Market Expiry

**Trigger:** `MarketRotation` event arrives while `leg1_state == Filled` and `leg2_state != Filled`.

This is handled specially in `on_event(MarketRotation)` — NOT in the evaluator:

```rust
// Build emergency signal BEFORE state reset (using OLD market's token IDs)
let signal = make_leg2_signal(
    hedge_token_id,    // OLD market's opposing token
    condition_id,      // OLD market's condition
    best_ask,          // current opposing ask
    leg1_size,
    ...,
    exit_reason: Some(ExitReason::MarketExpiry),
);
rotation_emergency_buffer.push(signal);
```

The main loop drains this buffer BEFORE sending `ExecutorCommand::MarketRotation` to the executor, ensuring the emergency FOK is attempted before the executor resets state.

**Exit reason:** `ExitReason::MarketExpiry`

### 7d. Erosion Exhausted

**Trigger:** `steps_applied >= MAX_EROSION_STEPS (5)` — the full erosion cascade completed without filling. Profit target is zero (break-even).

**Timing:** Checked after break-even breach, before quick reversal. Fires on the first evaluation after all 5 steps have been applied.

**Price:** Post-only at `best_ask - 1 tick`. FOK fallback after `emergency_max_maker_attempts` post-only attempts (via Section 8 repost loop).

**Exit reason:** `ExitReason::BreakEvenBreach` (same as break-even breach — semantically identical: the cascade reached break-even without filling).

**Step cap safety net:** In `strategy.rs`, `steps_applied` is also guarded against incrementing beyond `MAX_EROSION_STEPS`. In `evaluator.rs`, `advance_step` is gated with `steps_applied < MAX_EROSION_STEPS`. These are belt-and-suspenders — the exhaustion check fires before the step could overflow.

---

## 8. Post-Only First Strategy

Emergency exits use a **post-only first, FOK fallback** strategy to minimize taker fees. After `emergency_max_maker_attempts` (default 3) post-only attempts without a fill, the engine escalates to a FOK taker at `best_ask` to guarantee execution.

### Engine-side: emergency repost loop

**Source:** `src/engine/evaluator.rs` — the `emergency_submitted` branch

Once `emergency_submitted = true`, the evaluator switches from normal erosion to interval-gated repost mode:

```rust
if snap.emergency_submitted {
    // Rate-limit reposts
    time_since_last = now_ms - last_erosion_ms;
    if time_since_last < emergency_repost_interval_ms (500ms) {
        return None;  // too soon
    }

    best_ask = hedge_book.best_ask();
    fok_fallback = emergency_repost_count >= emergency_max_maker_attempts;

    if fok_fallback {
        // FOK taker — post at best_ask to cross the spread and guarantee fill
        repost_price = best_ask;
    } else {
        // Post-only at top of book
        repost_price = round_to_tick(best_ask - tick, tick);
    }

    // Emit emergency signal with same exit_reason, updated price
    return Emergency { signal, price: repost_price, size: leg1_size };
}
```

The `emergency_repost_count` is tracked in `ErosionState` and incremented in `strategy.rs` on each subsequent emergency decision (first emergency leaves count at 0). With `emergency_max_maker_attempts = 3`:

```
Attempt 0: initial emergency → post-only (best_ask - tick)
Attempt 1: first repost (500ms) → post-only
Attempt 2: second repost (1000ms) → post-only
Attempt 3: third repost (1500ms) → FOK at best_ask (crosses spread)
```

This means the engine tries post-only for 1.5s, then guarantees fill with FOK. The executor handles each signal identically — cancel old + post new.

### Live executor: two-step execution

**Source:** `src/executor/live.rs` — `handle_leg2_emergency()`

```
1. Cancel existing resting Leg 2 order (if any)
2. Compute aggressive post-only price: signal.price - tick
3. Place OrderRequest::aggressive_post_only() at that price
   ├─ CLOB accepts → Track as active_leg2_order_id
   │                  Send OrderPosted feedback
   │                  Send Telegram alert
   │                  Increment emergency_maker_posts
   │                  (Zero fee — maker)
   │
   ├─ CLOB rejects (would cross spread) → emergency_fok_fallback()
   │   └─ Place OrderRequest::emergency_fok() at original signal.price
   │      ├─ Accepted → Track, feedback, Telegram alert (emergency_foks++)
   │      └─ Failed → OrderFailed feedback, CRITICAL Telegram alert
   │                   "POSITION EXPOSED"
   │
   └─ API error → emergency_fok_fallback()
```

### Simulation model: book-based taker determination

**Source:** `src/engine/strategy.rs` — `advance_simulation()` emergency branch

```rust
if is_emergency {
    best_ask = opposing_book.best_ask();
    if best_ask <= posted_price - tick {
        // Would cross spread → FOK fallback (taker)
        sim_was_taker = true;  // taker fee charged
        fill_price = best_ask;
    } else {
        // Post-only rests at top of book → fills as maker
        sim_was_taker = false;  // zero fee
        fill_price = posted_price;
    }
}
```

The `sim_was_taker` flag propagates through `TradeSignal` to the `SimulationExecutor`, which uses it to determine fee treatment:
- `sim_was_taker = true` → `is_taker = true` → taker fee computed and charged
- `sim_was_taker = false` → `is_taker = false` → zero fee (recorded via `record_emergency_maker()`)

### Fee savings

At p=0.50 and 50 shares, the taker fee is ~$0.78. The post-only-first strategy avoids this fee entirely when the aggressive post-only is accepted (book hasn't crossed). Over many emergency exits, this saves significant capital.

---

## 9. Favorable Taker Exits

**Trigger:** During normal erosion, the opposing ask drops strictly below the posted Leg 2 bid. A post-only order at this price would be rejected by the CLOB (would cross spread). Instead of leaving Leg 1 unhedged, the bot market-takes.

### Simulation detection

In `advance_simulation()`, the non-emergency Leg 2 fill check:
```rust
match best_ask {
    Some(ask) if ask < posted_price => (true, true, ask, false)   // favorable taker at ask
    Some(ask) if ask <= posted_price => (true, false, posted_price, false)  // normal maker
    _ => (false, false, posted_price, false)  // no fill
}
```

When `is_favorable_taker = true`:
- `exit_reason` set to `FavorableTaker`
- Fee treatment depends on whether post-only would have been rejected (always taker in non-emergency context since ask < bid)

### Live detection

In `handle_leg2_erosion()`, when the CLOB returns `Rejected` for a post-only order:
```
Post-only rejected → ask is below our bid → attempt_favorable_exit()
  ├─ Try aggressive post-only (best_ask - tick)
  │   ├─ Accepted → emergency_maker_posts++ (maker fill)
  │   └─ Rejected → favorable_exit_fok_fallback()
  │       └─ FOK at eroded price (automatic price improvement from CLOB)
  └─ API error → favorable_exit_fok_fallback()
```

The CLOB fills FOK orders at the actual best ask (which is below our limit), giving automatic price improvement.

### Tracking

- `favorable_taker_fills` counter in `SimulationState`, `MarketSummary`, `SessionSummary`, `LiveExecutor`
- `[FAVORABLE POST-ONLY]` or `[FAVORABLE FOK FALLBACK]` tags in Telegram trade completions

---

## 10. Trade Completion & State Reset

### Detection

**Simulation:** `advance_simulation()` checks after Leg 2 fill:
```rust
if leg1_state == Filled && leg2_state == Filled {
    // Reset everything
}
```

**Live:** The main engine loop checks after processing each event:
```rust
if leg1_filled && leg2_filled {
    engine.on_trade_complete();
}
```

### State reset

```rust
leg1_state = None;
leg2_state = None;
erosion = None;
last_erosion_signal_ms = 0;
leg1_direction = None;
pending_leg1_signal = None;
// cumulative_used is NOT reset — capital stays allocated within this market
```

After reset, the engine can immediately accept a new spike signal.

### PnL computation (simulation)

```rust
pair_cost = leg1.price + leg2.price;           // per-share
gross_profit = (1.0 - pair_cost) * size;       // USDC
taker_fee = leg2.taker_fee;                     // USDC (0 for maker)
net_profit = gross_profit - taker_fee;          // USDC
profit_pct = net_profit / (pair_cost * size) * 100;
```

Virtual balance update:
```rust
// On Leg 1 fill: virtual_balance -= leg1.price * size
// On close: virtual_balance += leg1.price * size + net_profit
```

---

## 11. Market Rotation

### Timeline

```
T-300s   Cutoff window: no new Leg 1 entries (spikes dropped)
T-180s   Pre-warm: discover Market B via Gamma API, fetch books
T-0      Instant switch: emit pre-warmed MarketRotation + books
         Market WS resubscribes to new token IDs in parallel
```

### Market discovery

**Source:** `src/gateway/polymarket/rotation.rs`

Gamma API: `GET /events?tag_id=102467&active=true&closed=false&limit=10`
- Tag 102467 = "15M" markets
- Filter by slug prefix `btc-updown-15m-` or `eth-updown-15m-`
- `clobTokenIds` is a JSON-encoded string (not a JSON array): index 0 = YES, index 1 = NO

Pre-warming at T-180s: `discover_market_after(current_end_ms)` queries for markets ending after the current one, skipping Market A to find Market B. Pre-fetches both YES and NO books via REST.

### Rotation emergency protection

If Leg 1 is filled but Leg 2 incomplete when rotation arrives:

```rust
// BEFORE state reset:
if leg1_filled && !leg2_filled {
    // Build emergency using OLD market's token IDs/books
    signal = make_leg2_signal(
        old_hedge_token_id,
        old_condition_id,
        current_opposing_ask,
        leg1_fill_size,
        ExitReason::MarketExpiry,
    );
    rotation_emergency_buffer.push(signal);
}
```

**Main loop drainage order:**
1. Drain `rotation_emergency_buffer` → send emergency signals to executor
2. Send `ExecutorCommand::MarketRotation` → executor resets

This guarantees the emergency FOK is attempted before the executor wipes state.

### Engine state reset on rotation

```rust
active_condition_id = new_id;
active_yes_token_id = new_yes;
active_no_token_id = new_no;
market_end_timestamp_ms = new_end;
poly_book = None;           // old books cleared
poly_yes_book = None;
poly_no_book = None;
spike_detected = false;
last_spike = None;
leg1_state = None;
leg2_state = None;
cumulative_used = 0;        // RESET for new market
erosion = None;
last_erosion_signal_ms = 0;
leg1_direction = None;
in_cutoff_window = false;   // RESET for new market
```

### Executor cleanup

**Live:** `cancel_all()` all open CLOB orders, reset `active_leg2_order_id`

**Simulation:** `on_market_rotation()`:
1. Force-close any open positions (`status == Open`) — record as full loss
2. Lock `AwaitingResolution` positions for UMA resolution tracking
3. Reset per-market counters (`cumulative_used`, `current_market_signals`, `current_market_walls`)
4. Increment `markets_observed`

---

## 12. Cutoff Window

**Source:** `src/engine/strategy.rs` — checked on every event

### Detection

```rust
if !in_cutoff_window && active_condition_id.is_some() {
    time_remaining_secs = (market_end_ms - now_ms) / 1000;
    if time_remaining_secs < entry_cutoff_secs (300) {
        in_cutoff_window = true;
        cutoff_trigger_pending = true;
        cutoff_market_end_ms = market_end_ms;
    }
}
```

**Runs on every event**, not just spikes. This ensures the cutoff is detected promptly regardless of event type.

### Effects

| Action | During cutoff? |
|--------|---------------|
| New Leg 1 entries | **Blocked** — spikes dropped, `diag_spikes_dropped_cutoff++` |
| Existing Leg 2 erosion | **Continues** — no cutoff check in `evaluate_leg2()` |
| Emergency exits | **Continue** — adverse, break-even, favorable all active |
| Market summary | **Sent** — `ExecutorCommand::MarketCutoff` triggers Telegram summary (sim) |

### Cutoff alert (sim)

When first entering cutoff with an open position:
```
"open position detected at cutoff — Leg 2 will continue until rotation"
```
This is informational only — no special action is taken.

---

## 13. Capital Management

### Per-trade allocation

```
alloc = max(round(max_alloc_per_trade * tier_pct), $1)
```

`max_alloc_per_trade` is the sole capital control. The wallet balance is the real constraint in live trading.

### Per-market budget

`cumulative_used` tracks total USDC allocated in the current 15-minute market. Reset to 0 on rotation. There is no explicit per-market cap guard — the single-trade-at-a-time constraint plus `max_alloc_per_trade` naturally bound exposure.

### Session tracking (simulation only)

| Field | Updates |
|-------|---------|
| `virtual_balance` | -cost on Leg 1 fill, +(cost + net_profit) on trade close |
| `locked_in_resolution` | +cost when position locked for UMA |
| `total_pnl` | +net_profit on each trade close |
| `total_taker_fees_paid` | +fee on each taker fill |
| `total_maker_rebates_earned` | estimated 20% of taker fees as maker rebate |

### Live capital

In live mode, the wallet USDC.e balance is the real constraint. No virtual balance tracking — the CLOB itself rejects orders that exceed available funds.

---

## 14. State Machines & Transitions

### OrderState

```
         evaluate()
None ──────────────► Posted ─────────────► Filled
  ▲                    │                      │
  │   OrderFailed      │   UserWS/advanceSim  │
  └────────────────────┘                      │
  ▲                                           │
  │         on_trade_complete() / rotation     │
  └───────────────────────────────────────────┘
```

**Triggers:**
- `None → Posted`: Engine emits signal, executor posts order
- `Posted → Filled`: User WS `TradeStatusUpdate::Confirmed` (live) or `advance_simulation()` (sim)
- `Posted → None`: `ExecutorFeedback::OrderFailed` (CLOB rejected/error)
- `Filled → None`: `on_trade_complete()` (both legs done) or `MarketRotation`

### ErosionState

```
                init_erosion()
   None ────────────────────► Active (steps=0, emergency=false)
                                  │
                       ┌──────────┼──────────┐
                       ▼          ▼          ▼
                 Erosion step  Adverse   Break-even
                 (steps++)     movement   breach
                       │          │          │
                       │          ▼          ▼
                       │    emergency_submitted = true
                       │    exit_reason = Some(...)
                       │          │
                       ▼          ▼
                    Leg 2 fill detected
                       │
                       ▼
              on_trade_complete()
                 erosion = None
```

### SimPosition status

```
record_leg1_fill()                record_leg2_fill() / record_emergency_*()
      None ──────► Open ──────────────────► Hedged
                     │                         │
                     │ lock_for_resolution()    │ close_trade()
                     ▼                         ▼
              AwaitingResolution          [closed_trades]
```

### Full trade lifecycle

```
1. SpikeConfirmed ─► spike_detected = true

2. evaluate() passes all guards ─► Leg 1 signal emitted
   spike_detected = false
   leg1_state = Posted
   pending_leg1_signal stored (sim)

3a. [SIM] advance_simulation() Leg 1 check passes ─► leg1_state = Filled
    init_erosion()
    emit confirmed fill signal → SimulationExecutor records

3b. [LIVE] Executor places post-only GTC
    ExecutorFeedback::OrderPosted → engine stores order_id
    User WS TradeStatusUpdate::Confirmed → leg1_state = Filled
    init_erosion()

4. evaluate_leg2() runs on each event:
   - Normal: emit erosion signal → executor cancel+repost
   - Emergency: set emergency_submitted → executor attempts post-only first

5a. [SIM] advance_simulation() Leg 2 fill detected
    leg2_state = Filled
    emit confirmed fill signal → SimulationExecutor records, closes trade

5b. [LIVE] User WS TradeStatusUpdate::Confirmed for Leg 2
    leg2_state = Filled
    Main loop detects both filled → on_trade_complete()

6. State reset → ready for next spike
```

---

## 15. Edge Cases & Race Conditions

### Rotation while Leg 2 is posting

**Scenario:** `MarketRotation` arrives while `leg1_state = Filled`, `leg2_state = Posted`.

**Handle:** The engine builds an emergency FOK signal using the OLD market's token IDs and books BEFORE resetting state. The main loop sends this emergency to the executor before the `MarketRotation` command.

**Guarantee:** Open position is hedged (or best-effort FOK attempted) before state wipe.

### User WS fill notification delay (live)

**Scenario:** CLOB fills an order, but the User WS notification arrives 200-500ms later.

**Handle:** The feedback channel sends the real CLOB order ID back to the engine in ~50-100ms (REST round-trip). The engine stores this ID immediately. When the User WS notification arrives later, the engine can match it.

**Sequence:**
1. Executor places order → CLOB accepts → executor sends `OrderPosted` feedback (~50ms)
2. Engine receives feedback → stores order_id in `leg_state`
3. User WS notifies fill (~200-500ms) → engine matches order_id → updates to `Filled`

**Safety:** Feedback channel is drained BEFORE `on_event()` in each main loop iteration.

### Stale events

Events older than `stale_event_threshold_ms` (from `stale_book_ms` config, default 500ms) are not explicitly discarded at the ingestor level, but the evaluator's stale book check rejects signals based on old data.

### Adverse movement false positives

The `adverse_threshold` (0.1%) is designed to filter normal market noise. At BTC $65K, this requires a $65 reversal — well above normal tick-to-tick noise but catching genuine spike reversals.

### Break-even breach at extreme prices

When the opposing ask has gapped beyond `max_loss_ticks` (3 ticks) above the baseline, the break-even FOK is suppressed:
```
if ask > opposing_ask_at_fill + max_loss_ticks * tick:
    defer to erosion (don't crystallize catastrophic loss)
```
This prevents the bot from panic-buying the hedge at a terrible price during a liquidity gap.

### Leg 2 with zero ask depth

If the hedge book has no ask depth within 2 ticks, the emergency FOK size is capped to zero:
```rust
fok_size = min(leg1_size, ask_depth_2tick).round_dp(2)
if fok_size <= 0 → return None (log warning, wait for depth to appear)
```
The evaluator returns `None` and will re-evaluate on the next event.

### Double emergency submission

Once `emergency_submitted = true`, the evaluator switches to repost mode (not re-triggering adverses). The `exit_reason` is preserved from the original trigger, so even though the evaluator reposts at 500ms intervals, the reason stays consistent.

### Book bootstrapping

If no full orderbook has been received yet but a `BestBidAsk` event arrives, the engine bootstraps a synthetic book:
```rust
poly_book = OrderBook {
    bids: [PriceLevel { price: best_bid, size: 500 }],
    asks: [PriceLevel { price: best_ask, size: 500 }],
}
```
This allows evaluation to proceed before the first full book snapshot arrives.

### Spike during existing trade

The `ActiveTrade` guard rejects the spike, incrementing `diag_rej_busy`. The spike is consumed (cleared) and cannot be re-evaluated. This is by design — the engine only runs one trade at a time. The diagnostic counter tracks how many valid-book spikes were lost to executor busyness, informing parameter tuning.

### Decimal precision

All pricing uses `rust_decimal::Decimal` — never `f32`/`f64`. This prevents floating-point rounding errors in pair cost calculations where $0.001 differences matter. The `round_to_tick()` function ensures all prices are exact multiples of the tick size (typically 0.01).

---

## 16. Diagnostic Logging

### Engine diagnostics (every 60s, cumulative from app start)

```
"engine 60s" {
    markets_rotated,
    spikes_received,
    spikes_dropped_cutoff,
    rej_busy,           // ActiveTrade: valid spike, executor occupied
    rej_no_book,        // NoBook / NoBinance
    rej_stale,          // StaleBook
    rej_skew,           // PriceSkewed
    rej_spread,         // SpreadWide
    rej_depth,          // InsufficientDepth
    rej_other,          // Catchall (no market, bid cap, zero size)
    leg1_signals,       // Signals emitted
    leg1_fills,         // Fills confirmed
    leg2_erosion_steps, // Erosion steps emitted
    leg2_fills,         // Leg 2 fills confirmed
    emergencies,        // Emergency signals emitted
}
```

### Spike detector diagnostics (every 60s)

```
"spike 60s" {
    atr,                    // Current ATR value
    threshold,              // multiplier * ATR
    mid,                    // Current Binance mid
    candidates_started,     // Spike candidates initiated
    expired_window,         // Timed out before sustain
    fading_momentum,        // Failed momentum ratio
    below_magnitude,        // Below min magnitude
    confirmed,              // Successfully emitted
}
```

### Simulation executor diagnostics (every 60s)

```
"session 60s" {
    uptime_min,
    markets,
    signals,
    leg1_fills,
    hedged,
    emergency,
    emergency_maker,    // Post-only emergency fills (zero fee)
    adverse_fok,
    be_fok,
    pnl,
    win_rate,
    open,               // Currently open positions
}
```

### Live executor diagnostics (every 60s)

```
"live executor 60s" {
    placed,
    cancelled,
    failed,
    emergency_fok,          // FOK fallbacks
    emergency_maker,        // Post-only emergency fills (zero fee)
    favorable_taker,        // Favorable exit fills
}
```

---

## 17. Complete Trade Example

**Scenario:** BTC spikes up $400 (0.77% at $52,000). 15-minute market has 10 minutes remaining.

### Step 1: Spike Detection (T+0ms)

```
Binance @depth20: mid jumps from $51,800 to $52,200
ATR = $12.50, threshold = 2 * $12.50 = $25.00
|delta| = $400 >> $25 → candidate started
```

### Step 2: Spike Confirmation (T+300ms)

```
Sustain: displacement held for 300ms ✓
Momentum: 380/400 = 0.95 ≥ 0.50 ✓
Magnitude: 400/52000 = 0.0077 ≥ 0.01 ✓
→ SpikeConfirmed { direction: Up, magnitude: 0.0077, sustained_ms: 300 }
```

### Step 3: Leg 1 Evaluation (T+301ms)

```
Guards:
  Active trade? None ✓
  Binance price? $52,200 ✓
  YES book? bid=0.49, ask=0.51 ✓
  Stale? 50ms old ✓
  Skew? YES mid=0.50 ✓
  Spread? (0.51-0.49)/0.50 = 4% > 2% → REJECTED (SpreadWide)
```

**Wait** — in this example the spread is too wide. The spike is consumed and no trade is placed. Let's retry with a tighter book:

```
YES book? bid=0.495, ask=0.505 ✓
Spread? (0.505-0.495)/0.50 = 2.0% ≤ 2% ✓
Depth? 200 shares on bid ✓

Confidence = 0.4*min(0.0077/0.001,1) + 0.2*min(200/150,1) + 0.2*(600/900)
           = 0.4*1.0 + 0.2*1.0 + 0.2*0.667
           = 0.4 + 0.2 + 0.133 = 0.733 → HIGH tier

Alloc = round(20 * 1.0) = $20
Bid = round_to_tick(0.495 + 0.01, 0.01) = $0.50
Cap: 0.50 < 0.505 ✓ (post-only valid)
Size = round_dp(20 / 0.50, 2) = 40 shares

→ Leg 1 signal: Buy YES @ $0.50 × 40sh = $20.00
```

### Step 4: Leg 1 Fill (T+350ms)

**Sim:** `advance_simulation()` checks: ask=0.505 > bid=0.50 ✓, near depth > 0 ✓ → fill
**Live:** CLOB accepts post-only GTC, rests on book. User WS notifies fill.

```
leg1_state = Filled { price: 0.50, size: 40 }
init_erosion(0.50, 40, now_ms)
  opposing_ask_at_fill = 0.505 (NO book best ask)
  binance_at_fill = 52200
  initial_profit_target = 0.025 (HIGH)
```

### Step 5: Erosion Step 0 (T+3850ms, 3.5s later)

```
current_profit = 0.025 - 0 = 0.025 (no erosion yet)
target = 1.0 - 0.025 - 0.50 = 0.475
Leg 2 target: Buy NO @ $0.475

→ Emit erosion signal
```

### Step 6: Erosion Step 1 (T+5600ms, 1.75s later)

```
erosion = 0.025 * 5/15 = 0.00833
current_profit = 0.025 - 0.00833 = 0.01667
target = 1.0 - 0.01667 - 0.50 = 0.48333 → round to 0.48
→ Emit erosion signal: Buy NO @ $0.48
```

### Step 7: Leg 2 Fill (T+5800ms)

**Sim:** NO book best_ask = 0.48 ≤ posted 0.48 → fill as maker
**Live:** CLOB fills post-only at 0.48

```
Trade complete:
  Leg 1: YES @ $0.50 × 40sh = $20.00
  Leg 2: NO  @ $0.48 × 40sh = $19.20
  Pair cost: $0.98/sh
  Gross profit: (1.0 - 0.98) × 40 = $0.80
  Taker fee: $0 (both legs maker)
  Net profit: $0.80 (4.0% return on $20 deployed)
```

### Alternative: Emergency adverse movement (T+5000ms)

If instead BTC reverses at T+5000ms (1.5s after fill):
```
Binance drops to $51,950
Change = |51950 - 52200| / 52200 = 0.00479 ≥ 0.001 → ADVERSE MOVEMENT

Emergency signal: Buy NO @ best_ask (0.51)
emergency_submitted = true
exit_reason = AdverseMovement

Post-only first: place at 0.51 - 0.01 = 0.50
[SIM] best_ask=0.51 > 0.50 → post-only rests → maker fill, zero fee
[LIVE] CLOB accepts post-only → zero fee; if rejected → FOK at 0.51 (taker fee)

Pair: 0.50 + 0.50 = 1.00 → $0.00 gross profit (break-even, but no fee)
      OR if FOK: 0.50 + 0.51 = 1.01 → -$0.40 gross - ~$0.63 fee = -$1.03 net
```

---

## 18. Simulation vs Live Differences

| Aspect | Simulation | Live |
|--------|-----------|------|
| **Fill authority** | Engine (`advance_simulation()`) | CLOB (User WS fills) |
| **Leg 1 fill model** | Post-only check: bid < ask AND near depth > 0 | Real CLOB matching engine |
| **Leg 2 fill model** | Book-based: ask ≤ posted → fill | Real CLOB matching engine |
| **Emergency fill model** | Book-based taker determination (`sim_was_taker`) | Post-only first, FOK fallback on CLOB rejection |
| **Feedback channel** | Not used (engine is fill authority) | `ExecutorFeedback` carries order IDs back |
| **Fill latency** | Instant (on next event loop) | CLOB matching + network RTT (~50-100ms) |
| **Order tracking** | Synthetic IDs (`sim-leg1-{ts}`) | Real CLOB order IDs |
| **Capital** | Virtual balance (starts at `max_alloc_per_trade`) | Real wallet USDC.e balance |
| **Telegram** | Full reporting: opportunity, completion, market, session | Alerts only (emergencies, critical) |
| **QuestDB** | `simulated_trades` table | `executed_trades` table |
| **Heartbeat** | Not needed | 5s POST to `/heartbeat` (keep API session alive) |
| **MarketRotation** | Force-close open positions, lock for resolution | `cancel_all()` CLOB orders |
| **Trade detection** | `advance_simulation()` sees both filled | Main loop checks both `leg_state == Filled` |

### Key simulation simplifications

1. **Instant fills:** No fill latency. If the book supports the order, it fills on the next event. This over-estimates fill rates vs live.
2. **Full depth available:** Sim assumes our entire order fills at posted price. In reality, partial fills may occur.
3. **No queue position:** Sim doesn't model time priority in the CLOB queue. Real-world fill probability depends on queue position.
4. **Deterministic emergency fees:** Sim uses book state to determine maker vs taker. Live depends on actual CLOB acceptance/rejection.

These simplifications mean simulation PnL is an optimistic estimate. Live trading will likely see lower fill rates, occasional partial fills, and more FOK fallbacks.
