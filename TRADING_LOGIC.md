# Trading Logic Reference

This document covers FaCaiBot's trading logic: signal detection, entry validation, execution, hedge management, emergency exits, and state machines — for both live and simulation modes.

---

## Table of Contents

1. [Core Trade Model](#1-core-trade-model)
2. [Signal Detection Pipeline](#2-signal-detection-pipeline)
3. [Entry Validation (Leg 1 Guards)](#3-entry-validation-leg-1-guards)
4. [Confidence Scoring & Allocation](#4-confidence-scoring--allocation)
5. [Leg 1 Execution](#5-leg-1-execution)
6. [Leg 2 Hedge System (2-Phase)](#6-leg-2-hedge-system-2-phase)
7. [Emergency Exits](#7-emergency-exits)
8. [Price-Improvement Chase Strategy](#8-price-improvement-chase-strategy)
9. [Favorable Taker Exits](#9-favorable-taker-exits)
10. [Trade Completion & State Reset](#10-trade-completion--state-reset)
11. [Market Rotation](#11-market-rotation)
12. [Cutoff Window](#12-cutoff-window)
13. [Capital Management](#13-capital-management)
14. [State Machines & Transitions](#14-state-machines--transitions)
15. [Edge Cases & Race Conditions](#15-edge-cases--race-conditions)
16. [Complete Trade Example](#16-complete-trade-example)
17. [Simulation vs Live Differences](#17-simulation-vs-live-differences)

---

## 1. Core Trade Model

FaCaiBot exploits the repricing lag between Binance (source of truth) and Polymarket's CLOB (5-minute BTC prediction markets). When BTC spikes on Binance, Polymarket market makers take seconds to adjust quotes. The bot enters before repricing and hedges with the opposite side:

```
Binance spike UP → Buy YES cheap (Leg 1, post-only, $0 fee)
                 → CLOB reprices
                 → Buy NO (Leg 2, post-only, $0 fee)
                 → Paired position: e.g. $0.48 + $0.495 = $0.975
                 → Market resolves → pays $1.00 → 2.5% profit
```

**Key economics:**
- Both legs target post-only execution (maker, zero fee)
- Taker fees apply only to FOK emergency exits (adverse, Phase 1 breach, break-even breach, whipsaw reversal, market expiry) — max 1.56% at p=0.50; at 50 shares ~$0.78 per FOK fill
- Unfilled post-only orders cost nothing — failed signals are free

**One trade at a time.** The engine self-gates after emitting a Leg 1 signal: no new spikes are evaluated until the current trade completes or resets.

---

## 2. Signal Detection Pipeline

The spike detector runs on a dedicated OS thread (CPU-pinned core 0) processing Binance SBE `@depth20` (50ms cadence) and `@bestBidAsk` (real-time) binary WebSocket streams.

### Mid-price

Computed as `(best_bid + best_ask) / 2` from SBE `@depth20` orderbook snapshots, updated every ~50ms.

### Rolling EMA-ATR

Uses `atr_alpha = 0.01` (~200-tick / ~10s memory at 50ms/tick). No spikes are emitted for the first 10 ticks (~0.5 seconds) while the ATR warms up, preventing false positives during initialization.

### Speculative Spike Detection Pipeline

The detector uses a **"post then cancel if unconfirmed"** model. Leg 1 is posted speculatively on the initial ATR+magnitude trigger, ~300ms before sustain confirmation. This gains CLOB queue priority at zero cost (post-only orders can be cancelled for free).

**Phase 1 — Immediate candidate (T+0):**

| # | Check | Threshold | What it filters |
|---|-------|-----------|-----------------|
| 1 | **ATR warmup** | 10 samples | Initialization noise |
| 2 | **Per-tick threshold** | `|delta| > multiplier × ATR` (2x) | Normal volatility |
| 3 | **Magnitude gate** | `displacement/origin_price >= 0.015%` (1.5 bp) | Marginal moves too small to trade |

If all pass → emit `SpikeCandidate` immediately → engine posts speculative Leg 1.

**Phase 2 — Sustain confirmation (T+300ms):**

| # | Check | Threshold | What it filters |
|---|-------|-----------|-----------------|
| 4 | **Sustain duration** | 300ms | Short-lived noise |
| 5 | **Displacement held** | Price still above threshold | Spike already reverted |
| 6 | **Momentum ratio** | displacement/peak >= 0.65 | Fading spikes |

If all pass → emit `SpikeConfirmed` → sim fills unblocked. If any fail → emit `SpikeFailed` → speculative Leg 1 cancelled.

### Spike Delivery

Three event variants carry spike lifecycle signals:

- **`SpikeCandidate(SpikeInfo)`**: ATR + magnitude passed → triggers speculative Leg 1 posting. Engine sets `spike_detected = true`, `speculative_awaiting_sustain = true`
- **`SpikeConfirmed(SpikeInfo)`**: Sustain + momentum passed → clears `speculative_awaiting_sustain` (sim fills allowed). Live mode: no-op (fills come from REST poll / User WS)
- **`SpikeFailed { timestamp_ms }`**: Spike faded → engine cancels speculative Leg 1 if `Posted`, resets spike state. If already `Filled`: no-op, Leg 2 proceeds normally

Normal `BinanceTick` events only update `binance_price` and never trigger spike evaluation.

---

## 3. Entry Validation (Leg 1 Guards)

When `spike_detected = true`, the evaluator checks every guard in sequence. **The ordering matters** — cheaper/faster checks run first, and the `ActiveTrade` check is deliberately placed after book/spread checks.

| # | Guard | Condition | Rejection | Why this order |
|---|-------|-----------|-----------|----------------|
| 1 | **No spike** | `!spike_detected` | `Skipped` | Fast path — most events have no spike |
| 2 | **No market** | `active_condition_id` absent | `Other` | Rare edge case after rotation |
| 3 | **Direction book** | Book for YES (Up) or NO (Down) token exists with bid+ask | `NoBook` | Can't price without book |
| 4 | **Binance price** | `binance_price` exists | `NoBinance` | Reference price needed |
| 5 | **Stale book** | `book_age_ms > stale_book_ms` (500ms) | `StaleBook` | Stale data = unreliable pricing |
| 6 | **Price skew** | YES mid > 0.80 or < 0.20 | `PriceSkewed` | Near-certain markets have illiquid sides |
| 7 | **Spread** | `(ask - bid) > max_spread` ($0.02) | `SpreadWide` | Book too thin for reliable entry |
| 8 | **Active trade** | `leg1_state != None` | `ActiveTrade` | **After spread** — `rej_busy` counts only spikes that had a valid book |
| 9 | **Entry cutoff** | `time_remaining_secs < entry_cutoff_secs` (see config.toml) | `Other` | Defence-in-depth |
| 10 | **Depth** | `book_bid_depth < required_depth × depth_min_pct` (0.20) | `InsufficientDepth` | Not enough liquidity |

### Direction-aware book selection

- **Spike Up** → buying YES → use YES book (fallback: generic book)
- **Spike Down** → buying NO → use NO book (fallback: derive from YES complement — `no_bid = 1 - yes_ask`, `no_ask = 1 - yes_bid`)

### Self-gating

`spike_detected` is cleared on both signal emission AND rejection. Each spike gets exactly 1 evaluation attempt, preventing re-evaluation on subsequent events and signal flooding.

---

## 4. Confidence Scoring & Allocation

### Confidence formula

```
f1 = clamp((atr_ratio - min_spike_atr_ratio) / (strong_spike_atr_ratio - min_spike_atr_ratio), 0, 1)
f2 = min(total_book_depth / avg_depth, 1.0)
f3 = time_remaining_secs / 300.0

confidence = 0.4 × f1 + 0.2 × f2 + 0.2 × f3
```

Max possible: **0.8**. The spike quality factor (f1) uses ATR-relative scoring: `atr_ratio = abs_displacement / ema_atr` (dimensionless, computed in the spike detector). Spikes at `min_spike_atr_ratio` (config, default 25) score f1 = 0; spikes at `strong_spike_atr_ratio` (config, default 75) score f1 = 1.0. The ATR ratio automatically adapts to volatility regime — no unit mismatch possible. `SpikeInfo` carries `atr_ratio` from the spike detector through to the evaluator.

### Tier thresholds

| Confidence | Tier | Profit Target | Default `tier_pct` | Example ($9 max) |
|------------|------|---------------|--------------------|-------------------|
| >= 0.5 | HIGH | 3% | 100% | $9 |
| >= 0.30 | MED | 2% | 80% | $7.20 |
| < 0.30 | LOW | 2% | 50% | $4.50 |

### Allocation

**Zero-alloc guard:** If `tier_pct == 0` for any tier, the signal is rejected immediately with `Leg1RejectReason::Other` before the allocation calculation. This allows disabling entire tiers via config by setting their `*_alloc_pct = 0`. Currently all tiers are enabled: `high_alloc_pct = 1.0`, `med_alloc_pct = 0.8`, `low_alloc_pct = 0.5`.

`alloc = max(round(max_alloc_per_trade × tier_pct), $1)`. The $1 floor ensures we always trade at least the minimum.

Entry size: `round_dp(alloc / bid_price, 2)`. Polymarket min precision is 0.01 shares. If entry_size rounds to 0, the signal is rejected.

---

## 5. Leg 1 Execution

### Pricing logic

1. **Base bid:** `round_to_tick(best_bid + tick, tick)` — one tick above current best bid
2. **Post-only cap:** If `bid_price >= best_ask`, cap at `best_ask - tick` (must not cross spread)
3. **Smart outbidding:** If a depth wall is detected (single level with > 4x average depth), outbid it by 1 tick, as long as the outbid price is below the ask

### Fill models

**Speculative posting**: Leg 1 is posted on `SpikeCandidate` (before sustain confirmation), gaining ~300ms of CLOB queue priority. If `SpikeFailed` arrives before fill, the order is cancelled at zero cost (post-only). If `SpikeFailed` arrives after fill, the fill is valid and Leg 2 proceeds normally.

**Simulation:** The engine acts as the simulated CLOB. On each event loop iteration, it checks post-only validity (bid < ask) and near-ask depth within 2 ticks. If both pass, transitions `leg1_state` from `Posted` to `Filled`, initializes the hedge (`init_leg2()`), and emits a confirmed fill signal. **Speculative fill gate**: fills are blocked while `speculative_awaiting_sustain = true` (set on `SpikeCandidate`, cleared on `SpikeConfirmed`). This ensures sim fills only happen after the spike is confirmed (~300ms).

**Live:** The executor builds a post-only GTC order and submits via the `polymarket-client-sdk` (which handles EIP-712 signing, fee rate lookup, and tick size validation internally). On acceptance, it sends `OrderPosted` feedback (with the CLOB order ID) back to the engine, then enters a REST fill polling loop (`poll_leg1_fill()`): every 200ms it calls `GET /data/order/{id}` via the SDK's `order()` method. On detecting `Filled` status, it sends `RestFillDetected` feedback — the engine transitions Leg 1 to `Filled` and starts the hedge system (~200ms deterministic latency). The User WS remains as backup: if the REST poll doesn't detect the fill (network error, order cancelled externally, max polls reached), the User WS `TradeStatusUpdate` with MATCHED status will handle it. If REST detects the fill first, the later User WS event is deduped (`leg1_state` is already `Filled`, not `Posted`). During polling, the executor checks for incoming commands via `rx.try_recv()`: `CancelLeg1` is executed inline, other commands (MarketRotation, etc.) are buffered in `deferred_cmd` and processed in the next `run()` loop iteration.

### Leg 1 staleness timeout

If a posted Leg 1 order is not filled within `leg1_timeout_ms` (default 2500ms) of actual book resting time, the engine cancels it and frees the slot for the next spike. Without this, an unfilled Leg 1 blocks all subsequent spikes until market rotation.

**Sim mode:** Checked in `advance_simulation()` before fill check. Rarely fires (instant fills). Uses the provisional `timestamp_ms` from `evaluate()`, which is correct since there's no CLOB round-trip.

**Live mode:** `check_leg1_staleness()` runs each engine loop iteration. It **skips provisional order IDs** (`"sim-..."`) — the CLOB round-trip (~1.2s) would consume the entire timeout before the order reaches the book. The timer starts when `on_order_posted()` resets `timestamp_ms` with the real CLOB ID. This ensures the full `leg1_timeout_ms` is actual book resting time. If `SpikeFailed` or staleness fires while the ID is still provisional, a `cancel_leg1_on_feedback` flag defers the cancel until the real CLOB ID arrives via `ExecutorFeedback::OrderPosted`.

---

## 6. Leg 2 Hedge System (2-Phase)

### Hedge state initialization

When Leg 1 fills, the engine calls `init_leg2()` which captures: fill timestamp, fill price, fill size, initial profit target (from tier), the Binance mid at fill time (`binance_at_fill`), and computes the Phase 1 target price. The hedge begins in `HedgePhase::Phase1`.

### 2-Phase design

The hedge system uses two phases with a single cancel/repost at the transition — reducing off-book time from the old multi-step erosion cascade (~1s per cancel/repost) to ~200ms total.

**Phase 1 — Profit target rest** (`phase1_timeout_ms`, default 2000ms):
- Leg 2 is posted at the confidence-scaled profit target price (same as old Step 0)
- The order rests on the book for up to `phase1_timeout_ms` waiting for a fill
- If filled during Phase 1 → trade completes at the profit target (best outcome)
- If `phase1_timeout_ms` elapses without fill → transition to Phase 2

**Phase 2 — Break-even pursuit**:
- Single cancel/repost at `best_ask - 1 tick` (aggressive maker)
- Only repost when the book offers a strictly better price (preserves FIFO queue priority)
- If `leg1_price + ask > $1.00` → break-even breach → emergency exit (Section 7/8)

### Phase 1 target price computation

`target_price = round_to_tick(1.0 - initial_profit_target - leg1_price, tick)`

**Constraints applied in order:**
1. **Don't cross ask:** If target >= best_ask, clamp to `best_ask - tick`
2. **Smart outbid:** If depth wall detected on the ask side and wall price <= our target, outbid by 1 tick (must keep `leg1_price + outbid < $1.00` and `outbid < ask`)

### Phase 1 skip guard

If the current resting Leg 2 order is already at a price equal to or better than the Phase 1 target, the evaluator returns None — preserving the favorable position.

### Phase 2 price tracking

`HedgeState.phase2_posted_price` tracks the currently resting Phase 2 price. Reposts only occur when `round_to_tick(best_ask - tick, tick) > phase2_posted_price` — strictly better price available. This preserves FIFO queue priority and avoids unnecessary cancel/repost churn.

### Hedge evaluation flow

On every engine event, if hedge exists and `leg1_state == Filled`:

1. Is emergency already submitted? → price-improvement chase (see Section 8). After `emergency_deadline_ms` → FOK at best_ask
2. Compute hedge book data (direction-aware)
3. Check adverse movement (Binance reversal) → emergency post-only
4. **Phase 1 path** (if `phase == Phase1`):
   - Check Phase 1 breach (pair cost > `phase1_breach_threshold`) → transition to Phase 2
   - Check Phase 1 timeout (`elapsed >= phase1_timeout_ms`) → transition to Phase 2
   - Skip guard: if posted price <= target → hold
   - Emit Phase 1 post signal
5. **Phase 2 path** (if `phase == Phase2`):
   - Check break-even breach (pair cost > $1.00) → emergency
   - Check price improvement → repost at `ask - 1 tick`
   - No improvement → hold (preserve queue priority)

---

## 7. Emergency Exits

All emergency exits set `emergency_submitted = true` and record an `exit_reason`. Once set, the evaluator switches from hedge mode to emergency repost mode (see Section 8). Exception: `WhipsawReversal` bypasses the hedge system and emergency repost entirely — it emits an immediate FOK at best ask.

### 7a. Adverse Movement

**Trigger:** Binance reversal from `binance_at_fill` exceeds `adverse_threshold` (0.1%). Checked on every evaluation with zero grace period. The spike thesis is invalidated by the source (Binance) itself.

- Direction-aware: Up spike → adverse if price dropped; Down spike → adverse if price rose
- **Price:** First signal is post-only at `best_ask - 1 tick`. Subsequent reposts only on price improvement (Section 8), FOK fallback at `emergency_deadline_ms`
- **FOK size:** `min(leg1_size, ask_depth_within_2_ticks)` — capped at available liquidity
- **Exit reason:** `AdverseMovement`

### 7b. Phase 1 Breach

**Trigger:** Pair cost has exceeded `phase1_breach_threshold` ($1.05) during Phase 1. Catches fast book repricing that pushes the pair well above break-even. Rather than waiting for the Phase 1 timeout to expire, this triggers an immediate transition to Phase 2 (break-even pursuit).

**Gates (all must be true):**
1. `phase == Phase1` (only during Phase 1 — Phase 2 has its own break-even breach check)
2. `leg1_price + current_opposing_ask > phase1_breach_threshold` (stricter threshold than break-even)

**Action:** Transition to Phase 2 — cancel and repost at `best_ask - 1 tick`.

**Exit reason:** `Phase1Breach`

### 7c. Break-Even Breach

**Trigger:** Pair cost has exceeded $1.00 during Phase 2.

**Gates (all must be true):**
1. `phase == Phase2` (only during Phase 2 — Phase 1 has its own breach check at a stricter threshold)
2. `leg1_price + current_opposing_ask > 1.0` (pair cost exceeds $1.00, strict — at exactly $1.00 the emergency exit often fills worse)

**Price:** Post-only at `best_ask - 1 tick`, price-improvement chase, FOK fallback at deadline (Section 8).

**Exit reason:** `BreakEvenBreach`

### 7d. Rotation Emergency (Market Expiry)

**Trigger:** `MarketRotation` arrives while Leg 1 is Filled and Leg 2 is not Filled.

Given the entry cutoff (`entry_cutoff_secs`), any Leg 1 fill has at least that time for the 2-phase hedge system, so this only fires when all other exit paths failed before rotation.

Handled in the MarketRotation event handler — the engine builds an emergency FOK signal using the OLD market's token IDs and books BEFORE resetting state. The main loop sends this emergency to the executor before the rotation command, ensuring the position is hedged (or best-effort attempted) before state wipe.

**Exit reason:** `MarketExpiry`

### 7e. Whipsaw Reversal

**Trigger:** `SpikeConfirmed` arrives with the OPPOSITE direction to `leg1_direction` while Leg 1 is active.

**Two sub-cases:**
1. **Leg 1 Posted (unfilled):** Cancel immediately. Reuses the SpikeFailed cancel pattern, including provisional ID deferral (`cancel_leg1_on_feedback`). Resets all Leg 1 state and returns early from the SpikeConfirmed handler.
2. **Leg 1 Filled:** Set `whipsaw_fok_pending = true`, triggering immediate emergency exit on the next `evaluate_leg2()` cycle. `emit_whipsaw_fok()` builds the emergency signal with `ExitReason::WhipsawReversal` and `sim_was_taker = true` — the executor goes straight to `emergency_fok_fallback()` (direct FOK, no post-only attempt). This exits within one CLOB round-trip (~200ms) rather than waiting 2-3s for Phase 1 timeout + Phase 2 transition + BE breach detection. Taker fee (~$0.015/sh) is negligible vs the $0.05-0.07/sh saved by exiting faster.

**Exit reason:** `WhipsawReversal`

**Diagnostic counter:** `diag_whipsaw_foks` — incremented when Leg 1 Filled whipsaw detected.

---

## 8. Price-Improvement Chase Strategy

Emergency exits use a **price-improvement chase with hard deadline** strategy to minimize taker fees while preserving FIFO queue priority.

### How it works

Once `emergency_submitted = true`, the engine posts an aggressive post-only limit at `best_ask - 1 tick` and records `emergency_first_post_ms` (deadline clock start) and `emergency_posted_price` (current resting price). From that point, on each Polymarket book update:

1. **Deadline check**: If `now - emergency_first_post_ms >= emergency_deadline_ms` (default 2000ms) → FOK at `round_to_tick(best_ask, tick)` (guaranteed fill, taker fee). The `exit_reason` is re-evaluated at deadline time: if the FOK price makes `pair_cost >= $1.00`, a stale `FavorableTaker` is overridden to `BreakEvenBreach` (prevents favorable labeling on losing trades). The price is rounded to tick size to prevent SDK validation errors from raw book prices (e.g., 16-decimal-place prices)
2. **Price improvement check**: If `best_ask - 1 tick > emergency_posted_price` → cancel and repost at the improved price (price-chase)
3. **No change**: Hold current order — preserve FIFO queue priority (no blind reposts)

**Key insight**: Binance tick events are irrelevant during emergency exit (only Polymarket book changes affect exit pricing). The engine skips evaluation on Binance events when in emergency mode, only re-evaluating on Polymarket book/price updates or when the deadline may have expired.

### Example timeline

| Time | Book state | Action |
|------|-----------|--------|
| T+0 | ask=0.52 | Emergency trigger → post-only at 0.51 (`best_ask - tick`) |
| T+800ms | ask=0.52 | Book update, no improvement → hold (preserve queue) |
| T+1200ms | ask=0.54 | Book update, 0.53 > 0.51 → cancel and repost at 0.53 |
| T+2000ms | ask=0.54 | Book update, no improvement → hold |
| T+2000ms | — | Deadline expired → FOK at `best_ask` |

### Live executor flow

The evaluator communicates intent via the `sim_was_taker` flag on `TradeSignal`:

- `sim_was_taker = true` (deadline expired): Cancel existing → FOK at `signal.price` (the evaluator set this to `round_to_tick(best_ask, tick)`). On liquidity failure (Rejected or non-transient error), price escalates +1 tick per attempt up to `$1.00` cap (~23 ticks max, ~2.3s to sweep). `clob_safe_fok_size()` recomputed each iteration. Aborts immediately on SDK validation errors ("decimal places", "Validation", "balance", "allowance") or zero safe size
- `sim_was_taker = false` (price-chase): Cancel existing → aggressive post-only at `signal.price` (evaluator already computed `best_ask - 1 tick`). If CLOB rejects (would cross spread) → FOK fallback at `signal.price + tick` with same price-escalating sweep

### Simulation model

Emergency fills wait the full deadline window. On each book update: if `best_ask <= posted_price` → maker fill (zero fee, market came to our bid). If deadline expires without fill → taker FOK at `best_ask` (taker fee applies). The `sim_was_taker` flag propagates to the executor for fee treatment.

### Fee savings

At p=0.50 and 50 shares, the taker fee is ~$0.78. The price-improvement chase avoids this fee entirely when the market moves to our posted price within the deadline. Queue priority preservation means our resting order is ahead of later arrivals at the same price level.

---

## 9. Favorable Taker Exits

**Trigger:** During normal hedge (Phase 1 or Phase 2), the opposing ask drops strictly below the posted Leg 2 bid. A post-only order at this price would be rejected by the CLOB (would cross spread). Instead of leaving Leg 1 unhedged, the bot market-takes at the ask — taker fee is acceptable insurance vs the risk of an open position.

**Simulation:** `advance_simulation()` detects `ask < posted_price` on each book update across all direction branches. Fills at the ask price with `ExitReason::FavorableTaker`.

**Live:** When the CLOB rejects a post-only hedge order (price would cross, including "crosses book" SDK errors), the executor calls `attempt_favorable_exit()` which performs a **walk-down**: up to 4 post-only attempts at exponential tick offsets `[1, 2, 4, 8]` from `signal.price` (i.e., `price - 1*tick`, `price - 2*tick`, `price - 4*tick`, `price - 8*tick`). Each attempt is ~100ms (CLOB HTTP round-trip) and checks `price > 0` and `price × size >= $1` before trying. First accepted placement rests as maker (`fill_method=FavorableMaker`). If all 4 cross or fail → FOK fallback at `signal.price` (`fill_method=FavorableTaker` + `already_filled` if sync fill). Non-crossing errors (balance, etc.) skip remaining walk-down attempts and go straight to FOK. The CLOB fills FOK at the actual best ask (below our limit), giving automatic price improvement. Emergency FOK paths (`emergency_fok_fallback` and `emergency_fok_at_price`) send `fill_method=EmergencyTaker` — distinct from favorable exits so the engine correctly classifies the trade. The `FillMethod` metadata allows the engine to set the correct `LiveTradeMeta` flags (`favorable_taker`, `emergency_maker`, `leg2_was_taker`) even though the executor autonomously converted the signal.

**Leg 2 cancel-not-confirmed meta reset:** When a Leg 2 cancel returns `was_cancelled = false` and the order's Posted state is restored, `LiveTradeMeta` is reset (preserving `leg1_cancel_race`) and hedge emergency state (`emergency_submitted`, `exit_reason`) is cleared. This prevents a successful maker fill from being mislabeled as `[EMERGENCY POST-ONLY]` when the emergency dispatch happened before the cancel race resolved.

**Tracking:** `favorable_taker_fills` counter across all reporting contexts. Telegram tags: `[FAVORABLE POST-ONLY]` or `[FAVORABLE FOK FALLBACK]`.

---

## 10. Trade Completion & State Reset

### Detection

**Simulation:** `advance_simulation()` detects both legs Filled after Leg 2 fill.

**Live:** Two detection paths:
1. **User WS fill or REST fill**: The main engine loop checks after processing each event — if both `leg1_state` and `leg2_state` are Filled, calls `on_trade_complete()`. Leg 1 fills may arrive via `RestFillDetected` (primary, ~200ms) or User WS `TradeStatusUpdate` (backup).
2. **Sync FOK fill**: When `OrderPosted` feedback has `already_filled=true` (FOK returned `Filled` synchronously from REST), the engine transitions Leg 2 directly to `OrderState::Filled` in `on_order_posted()`. The main loop detects both legs filled immediately in the feedback drain iteration and triggers `on_trade_complete()`. This prevents the double-fill bug where the engine keeps evaluating and dispatching additional FOK signals while waiting for a User WS MATCHED event that never arrives for synchronous FOK fills.

### State reset

On completion: `leg1_state`, `leg2_state`, and `hedge` all reset to None. `cumulative_used` persists (capital stays allocated within this market). After reset, the engine can immediately accept a new spike signal.

### PnL computation (simulation)

- **Pair cost** = leg1_price + leg2_price (per share)
- **Gross profit** = (1.0 - pair_cost) × size
- **Maker rebate** = sum of both legs' `compute_maker_rebate(price, size)` (= `compute_taker_fee() × 0.20`; zero for taker fills)
- **Net profit** = gross_profit - taker_fee + maker_rebate
- **Profit %** = net_profit / (pair_cost × size) × 100

---

## 11. Market Rotation

### Timeline

```
T-180s   Cutoff window: no new Leg 1 entries (spikes dropped)
T-180s   Pre-warm: discover Market B via Gamma API, fetch books
T-0      Instant switch: emit pre-warmed MarketRotation + books
         Market WS resubscribes to new token IDs in parallel
         Quiet period starts (rotation_quiet_ms = 30s, no new entries)
T+30s    Quiet period ends — trading enabled
```

### Market discovery

Gamma API `GET /events?tag_id=102892&closed=false&order=endDate&ascending=true&limit=100`. Tag 102892 = "5M" markets. Filter by slug prefix `btc-updown-5m-`. Note: `clobTokenIds` is a JSON-encoded string (not an array) — index 0 = YES, index 1 = NO.

Pre-warming at T-180s: query for markets ending after the current one, skipping Market A to find Market B. Pre-fetch both YES and NO books via REST.

**Fallback:** If pre-warming failed, falls back to immediate Gamma poll within 5s of expiry.

**Delivery:** `MarketRotation` uses blocking `send()` to guarantee delivery. Book events use `try_send()` (expendable — WS will provide updates).

### Rotation emergency protection

If Leg 1 is Filled but Leg 2 incomplete when rotation arrives, the engine builds an emergency FOK signal using the OLD market's token IDs and books BEFORE resetting state. The main loop drains this buffer before sending the rotation command to the executor.

**Drainage order:**
1. Drain `rotation_emergency_buffer` → send emergency signals to executor
2. Send `MarketRotation` → executor resets

**Telegram alert (live mode):** When `leg1_filled && !leg2_filled` at rotation, the engine sends a `fire_critical()` Telegram alert with position details (direction, entry price, size, whether a FOK was submitted). This ensures abandoned positions are never silent — you always know when a trade was open at market expiry.

### Engine state reset on rotation

All market-specific state resets: active token IDs updated, books cleared, spike state cleared, leg states cleared, `cumulative_used` reset to 0, `in_cutoff_window` reset to false, `whipsaw_fok_pending` reset to false, `in_quiet_period` set to true (starts `rotation_quiet_ms` quiet period).

### Executor cleanup

**Live:** `cancel_all()` open CLOB orders, reset active order tracking.

**Simulation:** Force-close any open positions (record as full loss), lock `AwaitingResolution` positions for UMA resolution tracking, reset per-market counters, increment `markets_observed`.

---

## 12. Cutoff Window

### Detection

Checked on every event (not just spikes) — the cutoff is detected promptly regardless of event type. When `time_remaining_secs < entry_cutoff_secs` (`entry_cutoff_secs`), the engine sets `in_cutoff_window = true`.

### Effects

| Action | During cutoff? |
|--------|---------------|
| New Leg 1 entries | **Blocked** — spikes dropped |
| Existing Leg 2 hedge | **Continues** — no cutoff check in Leg 2 evaluation |
| Emergency exits | **Continue** — adverse, Phase 1 breach, break-even, whipsaw, favorable all active |
| Market summary | **Sent** — `MarketCutoff` triggers Telegram summary (sim) |

When first entering cutoff with an open position, a log notes "Leg 2 will continue until rotation" — informational only, no special action taken.

---

## 12b. Rotation Quiet Period

### Detection

Checked on every event (not just spikes). When `MarketRotation` fires, `in_quiet_period = true` and `rotation_ms = now_ms`. On each subsequent event, if `now_ms - rotation_ms >= rotation_quiet_ms` (default 30000ms), `in_quiet_period` clears.

### Effects

| Action | During quiet period? |
|--------|---------------------|
| New Leg 1 entries | **Blocked** — spike candidates dropped |
| Existing Leg 2 hedge | N/A — no position exists at rotation start |
| Emergency exits | N/A |

### Rationale

After market rotation, the Polymarket book takes ~20-30s to fully reprice. Entries during this window have stale reference prices, leading to losses clustering near rotation boundaries. The quiet period prevents this by waiting for market makers to establish fresh liquidity.

### Diagnostic

`diag_spikes_dropped_quiet` counter, visible in `/diag` Telegram output.

---

## 13. Capital Management

### Per-trade allocation

`alloc = max(round(max_alloc_per_trade × tier_pct), $1)`. `max_alloc_per_trade` is the sole capital control.

### Per-market budget

`cumulative_used` tracks total USDC allocated in the current 5-minute market. Reset to 0 on rotation. No explicit per-market cap guard — the single-trade-at-a-time constraint plus `max_alloc_per_trade` naturally bound exposure.

### Session tracking (simulation only)

| Field | Updates |
|-------|---------|
| `virtual_balance` | -cost on Leg 1 fill, +(cost + net_profit) on trade close |
| `locked_in_resolution` | +cost when position locked for UMA |
| `total_pnl` | +net_profit on each trade close |
| `total_taker_fees_paid` | +fee on each taker fill |
| `total_maker_rebates_earned` | +maker_rebate on each trade close |

### Live capital

In live mode, the wallet USDC.e balance is the real constraint. No virtual balance tracking — the CLOB rejects orders that exceed available funds.

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
- `None → Posted`: Engine emits signal on `SpikeCandidate`, executor posts order (speculative)
- `Posted → Filled`: REST poll fill or User WS fill (live) or `advance_simulation()` after `SpikeConfirmed` (sim)
- `Posted → None`: CLOB rejection/error OR Leg 1 staleness timeout OR `SpikeFailed` cancel
- `Filled → None`: Trade completion (both legs done) or MarketRotation

### HedgeState

```
                  init_leg2()
   None ────────────────────► Phase1 (emergency=false)
                                  │
                       ┌──────────┼──────────┐
                       ▼          ▼          ▼
                Phase1 breach  Adverse   Phase1 timeout
                or timeout     movement
                       │          │          │
                       ▼          │          ▼
                    Phase2        │     Phase2 (ask-1tick)
                       │          │          │
                       ▼          ▼          ▼
                  BE breach  emergency_submitted = true
                       │     exit_reason = Some(...)
                       │          │
                       ▼          ▼
                    Leg 2 fill detected
                       │
                       ▼
              on_trade_complete()
                  hedge = None
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
1. SpikeCandidate → spike_detected = true, speculative_awaiting_sustain = true

2. evaluate() passes all guards → Leg 1 signal emitted (speculative)
   spike_detected = false, leg1_state = Posted

3. SpikeConfirmed (T+300ms) → speculative_awaiting_sustain = false
   OR SpikeFailed → cancel Leg 1 if Posted, reset state → STOP

4a. [SIM] advance_simulation() fill check passes (gate cleared) → leg1_state = Filled
    init_leg2(), emit confirmed fill signal

4b. [LIVE] Executor places post-only GTC (at T+0, gains ~300ms queue priority)
    OrderPosted feedback → engine stores order_id
    REST poll fill (~200ms) or User WS fill → leg1_state = Filled, init_leg2()

5. evaluate_leg2() runs on each event:
   - Phase 1: post at profit target, hold until fill or timeout
   - Phase 2: post at ask-1tick, repost on improvement only
   - Emergency: set emergency_submitted → post-only first, FOK fallback

6a. [SIM] advance_simulation() Leg 2 fill detected
    leg2_state = Filled, emit confirmed fill signal

6b. [LIVE] User WS fill for Leg 2
    leg2_state = Filled, main loop detects both filled → on_trade_complete()

7. State reset → ready for next spike
```

---

## 15. Edge Cases & Race Conditions

### Rotation while Leg 2 is posting

**Scenario:** MarketRotation arrives while Leg 1 is Filled and Leg 2 is Posted.

**Handle:** The engine builds an emergency FOK using the OLD market's token IDs and books BEFORE resetting state. The main loop sends this emergency before the rotation command.

**Guarantee:** Open position is hedged (or best-effort FOK attempted) before state wipe.

### User WS fill notification delay (live)

**Scenario:** CLOB fills an order, but the User WS notification has variable latency (50-100ms typical, can spike to seconds during reconnects).

**Handle:** REST fill polling is the primary detection path: after placing Leg 1, the executor polls `GET /data/order/{id}` every 200ms for deterministic fill detection. On `Filled` status, `RestFillDetected` feedback triggers immediate state transition + hedge init (~200ms latency). The User WS remains as backup — if it arrives first, it processes normally; if it arrives after REST detection, it's deduped (`leg1_state` already `Filled`).

**Safety:** Feedback channel is drained BEFORE `on_event()` in each main loop iteration. `on_rest_fill_detected()` guards on `leg1_state == Posted` with matching `order_id`.

### Stale events

The evaluator's stale book check (500ms threshold) rejects signals based on old data.

### Adverse movement false positives

The `adverse_threshold` (0.1%) is designed to filter normal market noise. At BTC $65K, this requires a $65 reversal — well above tick-to-tick noise but catching genuine spike reversals.

### Leg 2 with zero ask depth

If the hedge book has no ask depth within 2 ticks, the emergency FOK size caps to zero. The evaluator returns None and will re-evaluate on the next event.

### Double emergency submission

Once `emergency_submitted = true`, the evaluator switches to price-improvement chase mode (not re-triggering). The `exit_reason` is preserved from the original trigger. Reposts only happen on Polymarket book price improvement; otherwise the order holds its FIFO queue position. This applies regardless of which phase triggered the emergency.

### SpikeFailed races with fill (live)

**Scenario:** CLOB fills the speculative Leg 1 order before `SpikeFailed` arrives (~300ms later).

**Handle:** `SpikeFailed` checks `leg1_state`. If `Filled` → no-op, Leg 2 proceeds normally. The fill is valid because the initial ATR+magnitude signal was genuine; only the sustain check failed. If `Posted` → cancel order, reset state.

**Safety:** Same race semantics as existing Leg 1 staleness timeout. User WS fill overrides state regardless.

### Provisional order ID race (live)

**Scenario:** `evaluate()` sets `leg1_state = Posted { order_id: "sim-leg1-{ts}" }` immediately (self-gating). The real CLOB ID arrives ~1.2s later via `ExecutorFeedback::OrderPosted`. During this window, `SpikeFailed` or staleness timeout could try to cancel using the provisional ID.

**Handle:** If the order ID is provisional when a cancel is needed, the engine sets `cancel_leg1_on_feedback = true` instead of sending a `CancelLeg1` command. When `on_order_posted()` receives the real CLOB ID, it checks this flag and immediately returns `Some(CancelLeg1)` with the real ID. The state is NOT resurrected — `leg1_state` stays `None`.

**Safety:** Prevents (1) sending invalid provisional IDs to the CLOB, (2) ghost orders from `on_order_posted()` resurrecting a cancelled trade. The flag is cleared in `on_order_failed()`, `on_trade_complete()`, and `MarketRotation`.

### Emergency signal stacking (live)

**Scenario:** Engine evaluates every ~2-50ms, executor takes ~1-2s per CLOB call. During Phase 2 or emergency, multiple signals queue in the executor channel. A later signal cancels a FOK that was already filled by an earlier signal → cancel not confirmed → engine restores "Posted" state → no User WS MATCHED arrives → trade stuck.

**Handle:** `emergency_signal_in_flight` flag on the engine. Set when an emergency signal is dispatched, cleared on any executor feedback (OrderPosted, OrderFailed, CancelResult for leg2, trade complete, rotation). `evaluate_leg2()` returns None while the flag is set. Additionally, the executor sets `active_leg2_order_id = None` (instead of `Some(...)`) when a FOK returns `Filled` — even if a stale signal sneaks through, it can't cancel a filled order.

**Safety:** Two-layer defense: Layer A (engine) prevents most stacking; Layer B (executor) prevents damage from any that slip through.

### Stale Leg 2 command contamination (live)

**Scenario:** While the executor processes a favorable exit (3 sequential HTTP calls, ~3.6s total), the engine's hedge evaluator fires and queues a SECOND hedge command. Trade 1 completes and resets. The stale command executes, fills, and `on_order_posted()` blindly sets `leg2_state = Filled` with stale data. When Trade 2's Leg 1 fills, both legs appear Filled → wrong trade completion with mismatched sizes.

**Handle:** Three-layer defense:
- **Layer A (root cause):** `leg2_command_pending` flag gates `evaluate_leg2()` while ANY Leg 2 command is in the executor pipeline. Set on dispatch (live mode only), cleared on any Leg 2 feedback (OrderPosted, OrderFailed, CancelResult). Prevents new hedge/emergency commands from queuing during multi-step executor operations.
- **Layer B (stale feedback guard):** `on_order_posted()` and `on_order_failed()` check if `leg1_state` is `Filled` before processing Leg 2 feedback. If the trade has already been reset (leg1 is `None`), the feedback is silently discarded with a warning log.
- **Layer C (executor):** `active_leg2_order_id = None` on filled FOK prevents stale cancel of already-filled orders.

### Balance exhaustion (live)

**Scenario:** "Not enough balance / allowance" errors during Leg 2 placement cause the executor to burn futile FOK attempts across hedge phases and emergency rounds.

**Handle:** `balance_exhausted` flag on `LiveExecutor`. Set on first "balance"/"allowance" error during Leg 2 hedge placement. All subsequent Leg 2 commands (hedge, emergency) immediately return `OrderFailed` without calling CLOB. Cleared on `MarketRotation`. FOK retry loops also abort immediately on "balance"/"allowance" errors (added to non-transient error list alongside "decimal places" and "Validation"). Executor sends `BalanceExhausted` feedback → engine fires `fire_critical()` Telegram alert with Leg 1 position details.

### Spike during existing trade

The `ActiveTrade` guard rejects the spike, incrementing `rej_busy`. The spike is consumed (cleared) and cannot be re-evaluated. The diagnostic counter tracks how many valid-book spikes were lost to executor busyness, informing parameter tuning.

### New spike during speculative window

**Scenario:** A second spike arrives while the first is in the speculative window (Posted, awaiting sustain).

**Handle:** The `ActiveTrade` guard blocks it — `leg1_state == Posted`. After `SpikeFailed` cancels and resets state to `None`, the next spike is accepted normally.

---

## 16. Complete Trade Example

**Scenario:** BTC spikes up $400 (0.77% at $52,000). 5-minute market has 3 minutes remaining.

### Step 1: Spike Candidate (T+0ms)

Binance mid jumps from $51,800 to $52,200. ATR = $12.50, threshold = 2 × $12.50 = $25.00. |delta| = $400 >> $25 → ATR check passed. Magnitude: 400/52000 = 0.0077 >= 0.0001 → magnitude check passed.

Result: `SpikeCandidate { direction: Up, magnitude: 0.0077, sustained_ms: 0 }` — emitted immediately.

### Step 2: Leg 1 Evaluation (T+1ms, speculative)

Guards pass: no active trade, Binance price present, YES book bid=0.495/ask=0.505, book age 50ms, YES mid=0.50 (no skew), spread = 1 tick, 200 shares depth.

Confidence = 0.4×1.0 + 0.2×1.0 + 0.2×0.667 = 0.733 → **HIGH** tier.

Allocation = round(20 × 1.0) = $20. Bid = round_to_tick(0.495 + 0.01) = $0.50. Size = round_dp(20 / 0.50) = 40 shares.

**Signal:** Buy YES @ $0.50 × 40 shares = $20.00 (speculative — posted before sustain confirmation)

### Step 3: Spike Confirmation (T+300ms)

- Sustain: displacement held 300ms
- Momentum: 380/400 = 0.95 >= 0.65
- Magnitude reconfirmed: 0.0077 >= 0.0001

Result: `SpikeConfirmed` — sim fill gate cleared, order has been resting on CLOB for ~300ms already.

### Step 4: Leg 1 Fill (T+350ms)

Sim: gate cleared by SpikeConfirmed, ask=0.505 > bid=0.50, near depth > 0 → fill. Live: CLOB accepts post-only GTC at T+0 (300ms queue priority), User WS notifies fill.

Result: `leg1_state = Filled`, hedge initialized (`init_leg2()`). `binance_at_fill` = 52200, `initial_profit_target` = 0.025 (HIGH).

### Step 5: Phase 1 Post (T+350ms, immediately after fill)

target = 1.0 - 0.025 - 0.50 = **$0.475**. Emit Phase 1 post signal: Buy NO @ $0.475. Order rests on book at profit target.

### Step 6: Phase 1 Fill (T+1800ms)

NO book best_ask = 0.475 <= posted 0.475 → fill as maker during Phase 1 (before timeout).

Alternatively, if not filled by T+2350ms (2000ms after fill): Phase 1 timeout → transition to Phase 2 at `ask - 1 tick`.

### Step 7: Trade Complete

| | Price | Shares | Cost |
|---|-------|--------|------|
| Leg 1 (YES) | $0.50 | 40 | $20.00 |
| Leg 2 (NO) | $0.475 | 40 | $19.00 |
| **Pair cost** | $0.975/sh | | |
| **Gross profit** | | | $1.00 |
| **Taker fee** | | | $0.00 |
| **Net profit** | | | **$1.00 (2.56%)** |

### Alternative: Emergency adverse movement (T+5000ms)

If BTC reverses to $51,950 at T+5s: change = |51950-52200|/52200 = 0.48% >= 0.1% → **ADVERSE MOVEMENT**.

Post-only first at `best_ask - tick` = $0.50. If accepted → maker fill, zero fee, pair = $1.00 (break-even). If rejected → FOK at $0.51 → pair = $1.01, loss = $0.40 gross + ~$0.63 fee = -$1.03 net.

---

## 17. Simulation vs Live Differences

| Aspect | Simulation | Live |
|--------|-----------|------|
| **Fill authority** | Engine (`advance_simulation()`) | CLOB (REST poll primary + User WS backup) |
| **Speculative fill gate** | Blocked until `SpikeConfirmed` clears `speculative_awaiting_sustain` | No gate — CLOB decides fill timing |
| **Leg 1 fill model** | Post-only check: bid < ask AND near depth > 0 (after gate clears) | Real CLOB matching engine |
| **Leg 2 fill model** | Book-based: ask <= posted → fill | Real CLOB matching engine |
| **Leg 2 hedge model** | 2-phase: Phase 1 at profit target, Phase 2 at ask-1tick | 2-phase: same logic, real CLOB matching |
| **Emergency fill model** | Deadline-aware: maker if ask <= posted, taker FOK at deadline | Price-improvement chase, FOK at deadline or CLOB rejection |
| **Feedback channel** | Not used (engine is fill authority) | Executor → engine order IDs |
| **Spike cancel** | `SpikeFailed` cancels Posted Leg 1 (no fill ever) | `CancelLeg1` sent to executor; if already filled, no-op |
| **Fill latency** | After sustain confirmation (~300ms + next event loop) | CLOB + network RTT (order posted ~300ms earlier than old model) |
| **Order tracking** | Synthetic IDs | Real CLOB order IDs |
| **Capital** | Virtual balance | Real wallet USDC.e |
| **Telegram** | Full: opportunity, completion, market, session | Alerts: emergencies, critical only |
| **QuestDB** | `simulated_trades` table | `executed_trades` table |
| **Heartbeat** | Not needed | 5s POST to `/heartbeat` |
| **MarketRotation** | Force-close open, lock for resolution | `cancel_all()` CLOB orders |
| **Trade detection** | `advance_simulation()` sees both filled | Main loop checks both legs Filled |
| **Leg 1 timeout** | In `advance_simulation()` (rarely fires — instant fills) | `check_leg1_staleness()` skips provisional IDs — timer starts from real CLOB confirmation |
| **Order signing** | N/A (no real orders) | SDK handles EIP-712 signing, fee rate caching, L2 HMAC auth |

### Key simulation simplifications

1. **Fills after sustain:** Sim fills only happen after `SpikeConfirmed` (~300ms); live mode may fill earlier since the order is on the CLOB from T+0
2. **Full depth available:** Assumes entire order fills at posted price; real CLOB may partially fill
3. **No queue position:** Doesn't model time priority in the CLOB queue
4. **Deterministic emergency fees:** Uses book state for maker/taker; live depends on actual CLOB acceptance

These simplifications mean simulation PnL is an optimistic estimate. Live trading will likely see lower fill rates, occasional partial fills, and more FOK fallbacks.
