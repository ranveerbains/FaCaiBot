# Trading Logic Reference

This document covers FaCaiBot's trading logic: signal detection, entry validation, execution, hedge management, emergency exits, and state machines — for both live and simulation modes.

---

## Table of Contents

1. [Core Trade Model](#1-core-trade-model)
2. [Signal Detection Pipeline](#2-signal-detection-pipeline)
3. [Entry Validation (Leg 1 Guards)](#3-entry-validation-leg-1-guards)
4. [Confidence Scoring & Allocation](#4-confidence-scoring--allocation)
5. [Leg 1 Execution](#5-leg-1-execution)
6. [Leg 2 Erosion Cascade](#6-leg-2-erosion-cascade)
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
- Taker fees apply only to FOK emergency exits — max 1.56% at p=0.50; at 50 shares ~$0.78 per FOK fill
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
| 3 | **Magnitude gate** | `displacement/origin_price >= 0.01%` (1 bp) | Marginal moves too small to trade |

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
- **`SpikeConfirmed(SpikeInfo)`**: Sustain + momentum passed → clears `speculative_awaiting_sustain` (sim fills allowed). Live mode: no-op (fills come from User WS)
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

`confidence = 0.4 × min(spike_magnitude / ATR, 1.0) + 0.2 × min(total_book_depth / avg_depth, 1.0) + 0.2 × (time_remaining_secs / 300.0)`

Max possible: **0.8**. The sustain factor was removed — all confirmed spikes already passed the sustain gate, so it contributed a constant offset with zero discriminative value.

### Tier thresholds

| Confidence | Tier | Profit Target | Default `tier_pct` | Example ($20 max) |
|------------|------|---------------|--------------------|-------------------|
| >= 0.5 | HIGH | 4% | 100% | $20 |
| >= 0.35 | MED | 3% | 50% | $10 |
| < 0.35 | LOW | 1% | 25% | $5 |

### Allocation

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

**Simulation:** The engine acts as the simulated CLOB. On each event loop iteration, it checks post-only validity (bid < ask) and near-ask depth within 2 ticks. If both pass, transitions `leg1_state` from `Posted` to `Filled`, initializes erosion, and emits a confirmed fill signal. **Speculative fill gate**: fills are blocked while `speculative_awaiting_sustain = true` (set on `SpikeCandidate`, cleared on `SpikeConfirmed`). This ensures sim fills only happen after the spike is confirmed (~300ms).

**Live:** The executor builds a post-only GTC order and submits via the `polymarket-client-sdk` (which handles EIP-712 signing, fee rate lookup, and tick size validation internally). On acceptance, it sends `OrderPosted` feedback (with the CLOB order ID) back to the engine. Fills arrive via the authenticated User WebSocket as `TradeStatusUpdate` events, matched by order ID. No fill gate needed — the CLOB decides fill timing.

### Leg 1 staleness timeout

If a posted Leg 1 order is not filled within `leg1_timeout_ms` (default 5000ms) of actual book resting time, the engine cancels it and frees the slot for the next spike. Without this, an unfilled Leg 1 blocks all subsequent spikes until market rotation.

**Sim mode:** Checked in `advance_simulation()` before fill check. Rarely fires (instant fills). Uses the provisional `timestamp_ms` from `evaluate()`, which is correct since there's no CLOB round-trip.

**Live mode:** `check_leg1_staleness()` runs each engine loop iteration. It **skips provisional order IDs** (`"sim-..."`) — the CLOB round-trip (~1.2s) would consume the entire timeout before the order reaches the book. The timer starts when `on_order_posted()` resets `timestamp_ms` with the real CLOB ID. This ensures the full `leg1_timeout_ms` is actual book resting time. If `SpikeFailed` or staleness fires while the ID is still provisional, a `cancel_leg1_on_feedback` flag defers the cancel until the real CLOB ID arrives via `ExecutorFeedback::OrderPosted`.

---

## 6. Leg 2 Erosion Cascade

### Erosion state initialization

When Leg 1 fills, the engine captures: fill timestamp, fill price, fill size, initial profit target (from tier), and the Binance mid at fill time (`binance_at_fill`).

### Step sizing — triangle weights

Five steps with front-loaded weights `[5, 4, 3, 2, 1]` (sum = 15). Early steps give up more margin (higher chance of fill at a good price); later steps give up less:

| Step | Weight | % of margin | HIGH (4%) | MED (3%) | LOW (1%) |
|------|--------|-------------|-----------|----------|----------|
| 0 | 5/15 | 33.3% | 1.333% | 1.000% | 0.333% |
| 1 | 4/15 | 26.7% | 1.067% | 0.800% | 0.267% |
| 2 | 3/15 | 20.0% | 0.800% | 0.600% | 0.200% |
| 3 | 2/15 | 13.3% | 0.533% | 0.400% | 0.133% |
| 4 | 1/15 | 6.7% | 0.267% | 0.200% | 0.067% |

After all 5 steps: 100% of margin eroded → price is at break-even.

### Interval timing — exponential decay

`interval(step) = max(base_ms × decay^step, 200ms)`. Default: base=3000ms, decay=0.5:

| Step | Interval | Cumulative |
|------|----------|-----------|
| 0 | 3000ms | 3.0s |
| 1 | 1500ms | 4.5s |
| 2 | 750ms | 5.25s |
| 3 | 375ms | 5.625s |
| 4 | 200ms | 5.825s |

Early steps wait longer (market has time to fill). Later steps fire rapidly (urgency). Steps are capped at `MAX_EROSION_STEPS` (5). After step 5, the cascade is exhausted and auto-escalates to a `BreakEvenBreach` emergency.

### Erosion evaluation flow

On every engine event, if erosion exists and `leg1_state == Filled`:

1. Is emergency already submitted? → price-improvement chase (see Section 8). After `emergency_deadline_ms` → FOK at best_ask
2. Compute hedge book data (direction-aware)
3. Check adverse movement (Binance reversal) → emergency post-only
4. Check break-even breach (opposing ask worsened) → emergency post-only
5. Check erosion exhausted (steps_applied >= 5) → emergency post-only
6. Check erosion interval gate (time since last signal)
7. Determine whether to advance step (capped at MAX_EROSION_STEPS)
8. Compute new target price
9. Apply constraints (don't cross ask, break-even floor, smart outbid)
10. Skip guard: if posted Leg 2 price <= computed target, return None (keep existing order)
11. Emit erosion signal

### Target price computation

`current_profit = initial_target - cumulative_erosion(steps)`, then `target_price = round_to_tick(1.0 - current_profit - leg1_price, tick)`.

**Constraints applied in order:**
1. **Don't cross ask:** If target >= best_ask, clamp to `best_ask - tick`
2. **Break-even floor:** If target > `1.0 - leg1_price`, clamp to break-even
3. **Smart outbid:** If depth wall detected on the ask side and wall price <= our target, outbid by 1 tick

### Erosion skip guard

After all price constraints, the evaluator checks if the current resting Leg 2 order is already at a price equal to or better than the new target. If so, it returns None — the cancel-and-repost is skipped, preserving a favorable position. This prevents erosion from overwriting a good price with a worse one.

**Monotonically safe:** Erosion lowers the profit target over time, which raises `target_price`. If the posted price is already below the current target, it will be below all future targets too.

---

## 7. Emergency Exits

All emergency exits set `emergency_submitted = true` and record an `exit_reason`. Once set, the evaluator switches from erosion mode to emergency repost mode (see Section 8).

### 7a. Adverse Movement

**Trigger:** Binance reversal from `binance_at_fill` exceeds `adverse_threshold` (0.1%). Checked on every evaluation with zero grace period. The spike thesis is invalidated by the source (Binance) itself.

- Direction-aware: Up spike → adverse if price dropped; Down spike → adverse if price rose
- **Price:** First signal is post-only at `best_ask - 1 tick`. Subsequent reposts only on price improvement (Section 8), FOK fallback at `emergency_deadline_ms`
- **FOK size:** `min(leg1_size, ask_depth_within_2_ticks)` — capped at available liquidity
- **Exit reason:** `AdverseMovement`

### 7b. Break-Even Breach

**Trigger:** Pair cost has exceeded $1.00 after first erosion step.

**Gates (all must be true):**
1. `steps_applied >= 1` (at least one erosion step completed, ~3s after fill)
2. `leg1_price + current_opposing_ask > 1.0` (pair cost exceeds $1.00, strict — at exactly $1.00 the emergency exit often fills worse)

**Price:** Post-only at `best_ask - 1 tick`, price-improvement chase, FOK fallback at deadline (Section 8).

**Exit reason:** `BreakEvenBreach`

### 7c. Rotation Emergency (Market Expiry)

**Trigger:** `MarketRotation` arrives while Leg 1 is Filled and Leg 2 is not Filled.

Given the entry cutoff (`entry_cutoff_secs`), any Leg 1 fill has at least that time for the erosion cascade (~5.8s total), so this only fires when all other exit paths failed before rotation.

Handled in the MarketRotation event handler — the engine builds an emergency FOK signal using the OLD market's token IDs and books BEFORE resetting state. The main loop sends this emergency to the executor before the rotation command, ensuring the position is hedged (or best-effort attempted) before state wipe.

**Exit reason:** `MarketExpiry`

### 7d. Erosion Exhausted

**Trigger:** `steps_applied >= MAX_EROSION_STEPS (5)` — the full cascade completed without filling. Profit target is zero (break-even).

Checked after break-even breach, before the erosion interval gate. Auto-escalates as a `BreakEvenBreach` emergency (semantically identical — the cascade reached break-even without filling).

**Exit reason:** `BreakEvenBreach`

---

## 8. Price-Improvement Chase Strategy

Emergency exits use a **price-improvement chase with hard deadline** strategy to minimize taker fees while preserving FIFO queue priority.

### How it works

Once `emergency_submitted = true`, the engine posts an aggressive post-only limit at `best_ask - 1 tick` and records `emergency_first_post_ms` (deadline clock start) and `emergency_posted_price` (current resting price). From that point, on each Polymarket book update:

1. **Deadline check**: If `now - emergency_first_post_ms >= emergency_deadline_ms` (default 2500ms) → FOK at `best_ask` (guaranteed fill, taker fee)
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
| T+2500ms | — | Deadline expired → FOK at `best_ask` |

### Live executor flow

The evaluator communicates intent via the `sim_was_taker` flag on `TradeSignal`:

- `sim_was_taker = true` (deadline expired): Cancel existing → direct FOK at `signal.price` (the evaluator set this to `best_ask`)
- `sim_was_taker = false` (price-chase): Cancel existing → aggressive post-only at `signal.price` (evaluator already computed `best_ask - 1 tick`). If CLOB rejects (would cross spread) → FOK fallback at `signal.price + tick`

### Simulation model

Emergency fills wait the full deadline window. On each book update: if `best_ask <= posted_price` → maker fill (zero fee, market came to our bid). If deadline expires without fill → taker FOK at `best_ask` (taker fee applies). The `sim_was_taker` flag propagates to the executor for fee treatment.

### Fee savings

At p=0.50 and 50 shares, the taker fee is ~$0.78. The price-improvement chase avoids this fee entirely when the market moves to our posted price within the deadline. Queue priority preservation means our resting order is ahead of later arrivals at the same price level.

---

## 9. Favorable Taker Exits

**Trigger:** During normal erosion, the opposing ask drops strictly below the posted Leg 2 bid. A post-only order at this price would be rejected by the CLOB (would cross spread). Instead of leaving Leg 1 unhedged, the bot market-takes at the ask — taker fee is acceptable insurance vs the risk of an open position.

**Simulation:** `advance_simulation()` detects `ask < posted_price` on each book update across all direction branches. Fills at the ask price with `ExitReason::FavorableTaker`.

**Live:** When the CLOB rejects a post-only erosion order (price would cross), the executor calls `attempt_favorable_exit()` — first tries aggressive post-only at `best_ask - 1 tick`, then FOK fallback if rejected. The CLOB fills FOK at the actual best ask (below our limit), giving automatic price improvement.

**Tracking:** `favorable_taker_fills` counter across all reporting contexts. Telegram tags: `[FAVORABLE POST-ONLY]` or `[FAVORABLE FOK FALLBACK]`.

---

## 10. Trade Completion & State Reset

### Detection

**Simulation:** `advance_simulation()` detects both legs Filled after Leg 2 fill.

**Live:** The main engine loop checks after processing each event — if both `leg1_state` and `leg2_state` are Filled, calls `on_trade_complete()`.

### State reset

On completion: `leg1_state`, `leg2_state`, and `erosion` all reset to None. `cumulative_used` persists (capital stays allocated within this market). After reset, the engine can immediately accept a new spike signal.

### PnL computation (simulation)

- **Pair cost** = leg1_price + leg2_price (per share)
- **Gross profit** = (1.0 - pair_cost) × size
- **Net profit** = gross_profit - taker_fee (zero for maker fills)
- **Profit %** = net_profit / (pair_cost × size) × 100

---

## 11. Market Rotation

### Timeline

```
T-180s   Cutoff window: no new Leg 1 entries (spikes dropped)
T-180s   Pre-warm: discover Market B via Gamma API, fetch books
T-0      Instant switch: emit pre-warmed MarketRotation + books
         Market WS resubscribes to new token IDs in parallel
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

### Engine state reset on rotation

All market-specific state resets: active token IDs updated, books cleared, spike state cleared, leg states cleared, `cumulative_used` reset to 0, `in_cutoff_window` reset to false.

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
| Existing Leg 2 erosion | **Continues** — no cutoff check in Leg 2 evaluation |
| Emergency exits | **Continue** — adverse, break-even, favorable all active |
| Market summary | **Sent** — `MarketCutoff` triggers Telegram summary (sim) |

When first entering cutoff with an open position, a log notes "Leg 2 will continue until rotation" — informational only, no special action taken.

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
- `Posted → Filled`: User WS fill (live) or `advance_simulation()` after `SpikeConfirmed` (sim)
- `Posted → None`: CLOB rejection/error OR Leg 1 staleness timeout OR `SpikeFailed` cancel
- `Filled → None`: Trade completion (both legs done) or MarketRotation

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
1. SpikeCandidate → spike_detected = true, speculative_awaiting_sustain = true

2. evaluate() passes all guards → Leg 1 signal emitted (speculative)
   spike_detected = false, leg1_state = Posted

3. SpikeConfirmed (T+300ms) → speculative_awaiting_sustain = false
   OR SpikeFailed → cancel Leg 1 if Posted, reset state → STOP

4a. [SIM] advance_simulation() fill check passes (gate cleared) → leg1_state = Filled
    init_erosion(), emit confirmed fill signal

4b. [LIVE] Executor places post-only GTC (at T+0, gains ~300ms queue priority)
    OrderPosted feedback → engine stores order_id
    User WS fill → leg1_state = Filled, init_erosion()

5. evaluate_leg2() runs on each event:
   - Normal: emit erosion signal → executor cancel+repost
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

**Scenario:** CLOB fills an order, but the User WS notification arrives 200-500ms later.

**Handle:** The feedback channel sends the real CLOB order ID back in ~50-100ms (REST round-trip). The engine stores this ID immediately. When the User WS notification arrives later, the engine matches it.

**Safety:** Feedback channel is drained BEFORE `on_event()` in each main loop iteration.

### Stale events

The evaluator's stale book check (500ms threshold) rejects signals based on old data.

### Adverse movement false positives

The `adverse_threshold` (0.1%) is designed to filter normal market noise. At BTC $65K, this requires a $65 reversal — well above tick-to-tick noise but catching genuine spike reversals.

### Leg 2 with zero ask depth

If the hedge book has no ask depth within 2 ticks, the emergency FOK size caps to zero. The evaluator returns None and will re-evaluate on the next event.

### Double emergency submission

Once `emergency_submitted = true`, the evaluator switches to price-improvement chase mode (not re-triggering). The `exit_reason` is preserved from the original trigger. Reposts only happen on Polymarket book price improvement; otherwise the order holds its FIFO queue position.

### SpikeFailed races with fill (live)

**Scenario:** CLOB fills the speculative Leg 1 order before `SpikeFailed` arrives (~300ms later).

**Handle:** `SpikeFailed` checks `leg1_state`. If `Filled` → no-op, Leg 2 proceeds normally. The fill is valid because the initial ATR+magnitude signal was genuine; only the sustain check failed. If `Posted` → cancel order, reset state.

**Safety:** Same race semantics as existing Leg 1 staleness timeout. User WS fill overrides state regardless.

### Provisional order ID race (live)

**Scenario:** `evaluate()` sets `leg1_state = Posted { order_id: "sim-leg1-{ts}" }` immediately (self-gating). The real CLOB ID arrives ~1.2s later via `ExecutorFeedback::OrderPosted`. During this window, `SpikeFailed` or staleness timeout could try to cancel using the provisional ID.

**Handle:** If the order ID is provisional when a cancel is needed, the engine sets `cancel_leg1_on_feedback = true` instead of sending a `CancelLeg1` command. When `on_order_posted()` receives the real CLOB ID, it checks this flag and immediately returns `Some(CancelLeg1)` with the real ID. The state is NOT resurrected — `leg1_state` stays `None`.

**Safety:** Prevents (1) sending invalid provisional IDs to the CLOB, (2) ghost orders from `on_order_posted()` resurrecting a cancelled trade. The flag is cleared in `on_order_failed()`, `on_trade_complete()`, and `MarketRotation`.

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

Result: `leg1_state = Filled`, erosion initialized. `binance_at_fill` = 52200, `initial_profit_target` = 0.025 (HIGH).

### Step 5: Erosion Step 0 (T+3350ms)

current_profit = 0.025, target = 1.0 - 0.025 - 0.50 = **$0.475**. Emit erosion signal: Buy NO @ $0.475.

### Step 6: Erosion Step 1 (T+4850ms)

Erosion = 0.025 × 5/15 = 0.00833. current_profit = 0.01667. target = 1.0 - 0.01667 - 0.50 = 0.48333 → round to **$0.48**. Emit erosion signal: Buy NO @ $0.48.

### Step 7: Leg 2 Fill (T+5000ms)

NO book best_ask = 0.48 <= posted 0.48 → fill as maker.

| | Price | Shares | Cost |
|---|-------|--------|------|
| Leg 1 (YES) | $0.50 | 40 | $20.00 |
| Leg 2 (NO) | $0.48 | 40 | $19.20 |
| **Pair cost** | $0.98/sh | | |
| **Gross profit** | | | $0.80 |
| **Taker fee** | | | $0.00 |
| **Net profit** | | | **$0.80 (4.0%)** |

### Alternative: Emergency adverse movement (T+5000ms)

If BTC reverses to $51,950 at T+5s: change = |51950-52200|/52200 = 0.48% >= 0.1% → **ADVERSE MOVEMENT**.

Post-only first at `best_ask - tick` = $0.50. If accepted → maker fill, zero fee, pair = $1.00 (break-even). If rejected → FOK at $0.51 → pair = $1.01, loss = $0.40 gross + ~$0.63 fee = -$1.03 net.

---

## 17. Simulation vs Live Differences

| Aspect | Simulation | Live |
|--------|-----------|------|
| **Fill authority** | Engine (`advance_simulation()`) | CLOB (User WS fills) |
| **Speculative fill gate** | Blocked until `SpikeConfirmed` clears `speculative_awaiting_sustain` | No gate — CLOB decides fill timing |
| **Leg 1 fill model** | Post-only check: bid < ask AND near depth > 0 (after gate clears) | Real CLOB matching engine |
| **Leg 2 fill model** | Book-based: ask <= posted → fill | Real CLOB matching engine |
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
