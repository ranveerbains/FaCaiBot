# Leg 2 Hedge & Emergency Exits (Sections 6-8)

---

## 6. Leg 2 Hedge System (2-Phase)

### Hedge state initialization

When Leg 1 fills, the engine calls `init_leg2()` which captures: fill timestamp, fill price, fill size, initial profit target (from tier), and computes the Phase 1 target price. The hedge begins in `HedgePhase::Phase1`.

### 2-Phase design

The hedge system uses two phases with a single cancel/repost at the transition -- reducing off-book time from the old multi-step erosion cascade (~1s per cancel/repost) to ~200ms total.

**Phase 1 -- Profit target rest** (`phase1_timeout_ms`, default 2000ms):
- Leg 2 is posted at the repricing-model profit target price
- The order rests on the book for up to `phase1_timeout_ms` waiting for a fill
- If filled during Phase 1 -> trade completes at the profit target (best outcome)
- If `phase1_timeout_ms` elapses without fill -> transition to Phase 2

**Phase 2 -- Break-even pursuit / Dual-order** (`phase2_timeout_ms`, default 2000ms):
- Post Phase 2 order at `best_ask - 1 tick` **without cancelling Phase 1** -- two maker orders rest simultaneously (dual-order)
- **No reposts** on either order -- preserve FIFO queue priority
- Whichever order fills first -> cancel the other -> trade complete
- Phase 2 timeout (`phase2_timeout_ms` elapsed) -> immediate FOK taker at best ask
- If `ask > phase2_posted_price` -> Phase 2 breach -> immediate FOK taker at best ask (Section 7)
- **Phase 2 entry guard**: If `ask - 1tick > breakeven_hedge_price` (`$1.00 - leg1_price`), even the best maker fill would give pair cost > $1.00 -> skip posting Phase 2, FOK immediately

### Phase 1 target price computation

`target_price = round_to_tick(1.0 - initial_profit_target - leg1_price, tick)` where `initial_profit_target = round_to_tick(expected_pct x phase1_target_dampen, tick)` -- the dampened target from the repricing model. `expected_pct` is the composite-score-based entry repricing estimate (default dampening 80% of raw expected_pct).

The raw target is sent to the executor as-is (no don't-cross-ask clamping, no smart outbid). If the price would cross the book, the CLOB rejects the post-only order or returns "crosses book", and the executor routes to `attempt_favorable_maker_then_fok()` -- which is the correct path for that scenario.

### Phase 1 post-once-and-wait

Phase 1 posts once at the profit target, then holds. The evaluator returns `None` if `leg2_state` is already `Posted` or `Filled`. If `OrderFailed` feedback resets `leg2_state` to `None`, the evaluator will re-emit a Phase 1 post signal on the next cycle (retry on failure). No reposts, no outbidding, no price adjustments during Phase 1.

### Phase 2 behavior (dual-order)

Phase 2 posts at `best_ask - 1 tick` **without cancelling the Phase 1 order** -- two maker orders rest simultaneously (dual-order). No reposts on either order, preserving FIFO queue priority on both. Whichever fills first wins; the engine cancels the other and completes the trade. Three exit triggers: (1) `phase2_timeout_ms` elapsed -> FOK taker; (2) Phase 2 breach (`ask > phase2_posted_price`) -> immediate FOK taker; (3) Phase 2 entry breach (ask - tick > breakeven) -> skip posting, immediate FOK. `HedgeState.phase2_start_ms` tracks when Phase 2 began for timeout calculation. `HedgeState.phase2_posted_price` records the Phase 2 order price for breach detection.

### Double-fill rebalance

Rare race condition: both Phase 1 and Phase 2 orders fill before the cancel reaches the CLOB (~3ms window). Result: N shares Leg 1 + 2N shares Leg 2 (excess). Fix: FOK taker buy N shares on Leg 1 side (same direction as original entry) to form a second pair. Uses the standard `emergency_fok_fallback` price-escalation loop, capped at $1.00. FOK is atomic -> no recursive overfill risk. If FOK fails -> critical Telegram alert (naked directional exposure, manual intervention needed). Tracked via `post_trade_orphan` on `StrategyEngine` -- persists across `on_trade_complete()` so the orphan fill can be detected after the trade resets.

### Flow-based graduated hedge response

During Phase 1, the composite flow score from the buildup detector drives a graduated response that fires **faster** than time-based backstops. The engine propagates the current composite score and direction to `HedgeState` on every event. Three flow-based triggers (checked in order of severity):

1. **Flow reversal** (`flow_direction != hedge_direction AND score > cancel_threshold`): The buildup has reversed direction -- immediate emergency FOK at ask. `ExitReason::WhipsawReversal`
2. **Flow collapse** (`score < cancel_threshold`): The buildup has dissipated below the cancel threshold -- immediate emergency FOK at ask. `ExitReason::FlowCollapse`. Exits before the book reprices against us
3. **Flow weakening** (`score < entry_threshold`): The buildup has weakened below the entry threshold but hasn't collapsed -- transition to Phase 2 alongside Phase 1 (dual-order at `ask - 1tick`). `TransitionReason::FlowWeakening`. Tightens the hedge without abandoning the profit target. If `ask - tick > breakeven` at transition time, entry guard fires and FOK immediately instead

These flow-based triggers only fire when `flow_monitoring_active = true` (composite score > 0, meaning the detector has fresh data). When flow data goes stale (all metrics beyond freshness window), the system falls back to time-based Phase 1 timeout and breach checks.

### Hedge evaluation flow

On every engine event, if hedge exists and `leg1_state == Filled`:

1. Is emergency already submitted? -> return None (FOK already dispatched, executor handles it)
2. Compute hedge book data (direction-aware)
3. **Phase 1 path** (if `phase == Phase1`):
   - Check Phase 1 breach (pair cost > `phase1_breach_threshold`) -> immediate FOK taker at ask
   - Check flow reversal -> immediate FOK (Section 7f)
   - Check flow collapse -> immediate FOK (Section 7f)
   - Check flow weakening -> transition to Phase 2 alongside
   - Check Phase 1 timeout (`elapsed >= phase1_timeout_ms`) -> transition to Phase 2
   - If already posted or filled -> hold (post-once-and-wait)
   - Emit Phase 1 post signal (only when `leg2_state == None`)
4. **Phase 2 path** (if `phase == Phase2`):
   - Check Phase 2 timeout (`elapsed >= phase2_timeout_ms`) -> FOK taker at best ask
   - Check Phase 2 breach (`ask > phase2_posted_price`) -> immediate FOK taker at ask
   - No breach/timeout -> hold (preserve queue priority, no reposts on either order)

---

## 7. Emergency Exits

All emergency exits set `emergency_submitted = true` and record an `exit_reason`. Once set, the evaluator returns None on subsequent evaluations -- the executor handles the FOK. All emergency exits are **immediate FOK taker at ask** -- no post-only chase, no deadline, no reposts. The ~4s hedge window (Phase 1 + Phase 2) is short enough that any meaningful Binance reversal reprices the Polymarket book and triggers Phase 2 BE breach anyway. Whipsaw catches violent reversals (opposite buildup detected after fill).

### 7a. Phase 1 Breach

**Trigger:** Pair cost has exceeded `phase1_breach_threshold` during Phase 1. Catches fast book repricing that pushes the pair well above break-even.

**Gates (all must be true):**
1. `phase == Phase1` (only during Phase 1 -- Phase 2 has its own break-even breach check)
2. `leg1_price + current_opposing_ask > phase1_breach_threshold` (stricter threshold than break-even)

**Action:** Immediate FOK taker at `round_to_tick(best_ask, tick)`. `sim_was_taker = true`.

**Exit reason:** `Phase1Breach`

### 7b. Phase 2 Breach (was Break-Even Breach)

**Trigger:** Current ask has risen above the Phase 2 posted price -- the Phase 2 order is deep in the book and unlikely to fill.

**Gates (all must be true):**
1. `phase == Phase2`
2. `current_opposing_ask > phase2_posted_price` (ask moved above our Phase 2 order)

**Action:** Immediate FOK taker at `round_to_tick(best_ask, tick)`. `sim_was_taker = true`.

**Exit reason:** `Phase2PriceBreach`

**Note:** This replaces the old `leg1_price + ask > $1.00` check. The Phase 2 posted price (ask-1tick at posting) is the worst maker price we'll accept. If ask rises above it, exit via FOK.

### 7c. Phase 2 Timeout

**Trigger:** Phase 2 has been active for `phase2_timeout_ms` (default 2000ms) without a fill.

**Action:** Immediate FOK taker at `round_to_tick(best_ask, tick)`. `sim_was_taker = true`.

**Exit reason:** `Phase2Timeout`

### 7d. Rotation Emergency (Market Expiry)

**Trigger:** `MarketRotation` arrives while Leg 1 is Filled and Leg 2 is not Filled.

Given the entry cutoff (`entry_cutoff_secs`), any Leg 1 fill has at least that time for the 2-phase hedge system, so this only fires when all other exit paths failed before rotation.

Handled in the MarketRotation event handler -- the engine builds an emergency FOK signal using the OLD market's token IDs and books BEFORE resetting state. The main loop sends this emergency to the executor before the rotation command, ensuring the position is hedged (or best-effort attempted) before state wipe.

**Exit reason:** `MarketExpiry`

### 7e. Whipsaw Reversal

**Trigger:** `BuildupConfirmed` arrives with the OPPOSITE direction to `leg1_direction` while Leg 1 is active.

**Two sub-cases:**
1. **Leg 1 Posted (unfilled):** Send `CancelLeg1Order { order_id }` to the executor (maker orders must be explicitly cancelled -- they rest on the book until cancelled or filled). Cancel-not-confirmed path preserves Posted state for User WS fill detection. Resets buildup detection state and returns early from the handler.
2. **Leg 1 Filled:** Set `whipsaw_fok_pending = true`, triggering immediate emergency exit on the next `evaluate_leg2()` cycle. `emit_whipsaw_fok()` builds the emergency signal with `ExitReason::WhipsawReversal` and `sim_was_taker = true` -- the executor goes straight to `emergency_fok_fallback()` (direct FOK, no post-only attempt). This exits within one CLOB round-trip (~200ms) rather than waiting 2-3s for Phase 1 timeout + Phase 2 transition + BE breach detection. Taker fee (~$0.015/sh) is negligible vs the $0.05-0.07/sh saved by exiting faster.

**Exit reason:** `WhipsawReversal`

**Diagnostic counter:** `diag_whipsaw_foks` -- incremented when Leg 1 Filled whipsaw detected.

### 7f. Flow-Based Emergency Exits (Phase 1)

During Phase 1, the composite flow score provides **faster-acting** emergency triggers than time-based backstops. These require `flow_monitoring_active = true` on the hedge state.

**7f-i. Flow Reversal**

**Trigger:** Composite direction has flipped to the opposite of the hedge direction, AND the reversed score exceeds `cancel_threshold`.

**Action:** **IMMEDIATE** FOK taker at `round_to_tick(best_ask, tick)` — no sustain window.

**Exit reason:** `WhipsawReversal`

**Rationale:** The composite detector sees the directional shift in futures metrics (CVD, basis) before it manifests as a spot price move. This is the entire point of using Binance futures as a leading indicator: exit Polymarket BEFORE it reprices using the Binance 100-500ms lead (see Section 2a, Basis delta). A sustain window delays the exit and destroys that advantage — Polymarket may have already repriced against us during the wait. Immediate exit is intentional. Flash crash exits (false reversals) are an acceptable small cost (~$0.015/sh taker fee) vs the risk of being caught in a genuine reversal where the book has moved.

**Relationship to Whipsaw (BuildupConfirmed reversal):**
- **Whipsaw**: `BuildupConfirmed` event arrives with opposite direction BEFORE Leg 1 fill → cancel unfilled Leg 1 maker (`CancelLeg1Order { repost: false }`)
- **Flow Reversal**: Composite direction monitoring AFTER Leg 1 is filled → immediate Leg 2 FOK exit (`WhipsawReversal`)

These are the same underlying event (market has flipped direction), detected via different paths:
- Whipsaw detection catches reversals during the Leg 1 posting window (before fill)
- Flow reversal catches reversals during the Leg 2 resting window (after Leg 1 fills)

Both result in immediate exits and are correctly classified in session/market summaries.

**7f-ii. Flow Collapse**

**Trigger:** Composite score has dropped below `cancel_threshold` (default 0.25) during Phase 1 with Leg 1 filled.

**Action:** Immediate FOK taker at `round_to_tick(best_ask, tick)`.

**Exit reason:** `FlowCollapse`

**Rationale:** The buildup that justified entry has dissipated. Even if the book hasn't repriced against us yet, the lack of follow-through makes the profit target unrealistic. Exit early before the book catches up.

**7f-iii. Flow Weakening (Phase 2 transition)**

**Trigger:** Composite score has dropped below `entry_threshold` (default 0.40) but remains above `cancel_threshold` (0.25).

**Action:** Post Phase 2 order at `ask - 1tick` alongside Phase 1 (dual-order). If `ask - tick > breakeven`, the Phase 2 entry guard fires and FOK immediately instead.

**This is a Phase 2 transition, not an emergency exit.** `TransitionReason::FlowWeakening`. It tightens the hedge without abandoning the profit target -- the Phase 1 order still rests at the original target price.

---

## 8. Favorable Exits (Try Maker First)

**Trigger:** During normal hedge (Phase 1 or Phase 2), the opposing ask drops strictly below the posted Leg 2 bid. A post-only order at this price would be rejected by the CLOB (would cross spread).

**Simulation:** `advance_simulation()` detects `ask < posted_price` on each book update across all direction branches. Fills at the ask price with `ExitReason::FavorableTaker`.

**Live -- Try Maker First:** When the CLOB rejects a post-only hedge order (price would cross, including "crosses book" SDK errors), the executor uses `attempt_favorable_maker_then_fok()`:

1. Post maker at `best_ask - 1tick` (rests below current ask)
2. Poll for fill up to `favorable_maker_timeout_ms` (default 1000ms)
3. **Breakeven breach guard:** Each polling iteration also queries `GET /book` for the current best ask. If `current_ask - tick > breakeven` (ask snapped back above breakeven), the maker is cancelled early and a FOK fallback executes immediately -- prevents the maker from sitting while the book deteriorates
4. If filled -> maker fill (no taker fee + rebate = ~$0.37 savings). `fill_method=FavorableMaker`
5. If not filled -> cancel -> FOK taker fallback (existing path). `fill_method=FavorableTaker`
6. If cancel NOT confirmed (order may have filled) -> send `OrderPosted` with `already_filled=false`, let User WS determine outcome

Guards: `clob_safe_fok_size()` zero-size check and `price x size >= $1` notional minimum. `already_filled` set if sync fill. Emergency FOK paths (`emergency_fok_fallback`) send `fill_method=EmergencyTaker` -- distinct from favorable exits. The `FillMethod` metadata allows the engine to set the correct `LiveTradeMeta` flags even though the executor autonomously converted the signal.

**Leg 2 cancel-not-confirmed meta reset:** When a Leg 2 cancel returns `was_cancelled = false` and the order's Posted state is restored, `LiveTradeMeta` is reset and hedge emergency state (`emergency_submitted`, `exit_reason`) is cleared. This prevents a successful maker fill from being mislabeled as `[EMERGENCY POST-ONLY]`.

**Tracking:** `diag_favorable_exits` counter (total), `diag_favorable_maker_fills` (maker try succeeded), `diag_favorable_maker_timeouts` (fell back to FOK). Telegram tags: `[FAVORABLE MAKER]` (maker try succeeded), `[FAVORABLE FOK FALLBACK]` (FOK fallback), `[FAVORABLE POST-ONLY]` (emergency maker fill).

### 8a. Entry Size Clamping

Leg 1 entry size is clamped to `max(raw_size, 5, ceil($1/price))` in the evaluator. This ensures all trades meet both the CLOB 5-share maker minimum and the $1.00 FOK notional floor. Since Leg 2 inherits Leg 1's filled size, it always satisfies these minimums too.

### 8b. FOK Emergency Loop Constraints

The `emergency_fok_fallback()` price-escalation loop enforces several safety constraints:

- **$1.00 minimum notional floor:** `clob_safe_fok_size()` increases the FOK size if `price × size < $1.00`. Sizes are rounded up to meet the CLOB notional minimum — the executor never sends a sub-dollar FOK.
- **Maximum 10 attempts:** The loop is capped at 10 price-escalation attempts. If the position is still unhedged after 10 FOK attempts, the loop exits and fires a Telegram orphaned-position alert ("ORPHANED POSITION — FOK loop exhausted without fill") for manual intervention.
- **Non-retryable errors (immediate abort):** Two error classes cause the loop to abort immediately rather than retrying at a higher price:
  - `"too old"` — the market has expired or the order timestamp is stale. Retrying at a higher price cannot resolve this; further attempts are futile.
  - `"min size"` — the computed FOK size is below the exchange minimum even after the $1.00 notional adjustment. Escalating price would only reduce the computed size further, so the loop aborts.
- **Other non-transient errors** (e.g., `"balance"`, `"allowance"`, `"decimal places"`, `"Validation"`) also abort the loop immediately (existing behavior).
