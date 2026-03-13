# Edge Cases, Examples & Sim/Live (Sections 14-16)

---

## 14. Edge Cases & Race Conditions

### Rotation while Leg 2 is posting

**Scenario:** MarketRotation arrives while Leg 1 is Filled and Leg 2 is Posted.

**Handle:** The engine builds an emergency FOK using the OLD market's token IDs and books BEFORE resetting state. The main loop sends this emergency before the rotation command.

**Guarantee:** Open position is hedged (or best-effort FOK attempted) before state wipe.

### User WS fill notification delay (live)

**Scenario:** CLOB fills an order, but the User WS notification has variable latency (50-100ms typical, can spike to seconds during reconnects).

**Handle:** Leg 1 uses a maker post-only order. The primary fill detection path is the User WS MATCHED event (hex order hash). If the executor's `place_order()` returns `already_filled=true` (rare for post-only -- would require the book to cross immediately), the engine transitions Leg 1 directly to `Filled` via `on_order_posted()`. If a User WS event arrives after this, it's deduped (`leg1_state` already `Filled`).

**Safety:** Feedback channel is drained BEFORE `on_event()` in each main loop iteration.

### Stale events

The evaluator's stale book check (500ms threshold) rejects signals based on old data.

### Leg 2 with zero ask depth

Emergency FOK sizing uses full `leg1_size` (no depth cap). The executor's price-escalation loop (`emergency_fok_fallback`) sweeps cumulative depth across multiple price levels -- a FOK at $0.60 fills all asks at $0.59 AND $0.60. If `leg1_size` rounds to zero (shouldn't happen in practice), the evaluator returns `None` as a safety net.

### Double emergency submission

Once `emergency_submitted = true`, the evaluator returns None on subsequent evaluations (not re-triggering). The `exit_reason` is preserved from the original trigger. The executor's FOK retry loop handles the exit -- no further signals are needed from the engine.

### Sustain cancel race with fill (live)

**Scenario:** CLOB fills the Leg 1 maker order at the same moment the engine dispatches a `CancelLeg1Order` (sustain failure or timeout).

**Handle:** Cancel is fire-and-confirm. If `was_cancelled = true`, the order was cancelled before fill — engine resets state. If `was_cancelled = false`, the order already filled — engine keeps `leg1_state = Posted` and waits for User WS MATCHED event. When MATCHED arrives, it transitions to `Filled` and hedge begins normally.

**Critical invariant:** `was_cancelled = false` MUST NOT clear `leg1_cancel_inflight`. If the flag is cleared, the sustain monitor fires again immediately, producing a cancel retry loop (repeated dispatches of a cancel for an order that no longer exists on the CLOB). `leg1_cancel_inflight` is only cleared when `was_cancelled = true` (in `on_cancel_result`) OR when `on_trade_complete()` runs (trade ended regardless of how cancel resolved).

**Safety:** `pending_leg1_cancel.is_none()` guard prevents duplicate cancel dispatch.

### Provisional order ID race (live)

**Scenario:** `evaluate()` sets `leg1_state = Posted { order_id: "sim-leg1-{ts}" }` immediately (self-gating). The real CLOB ID arrives later via `ExecutorFeedback::OrderPosted` (~1.3s round-trip). During this window, all three sustain cancel triggers (sustain timeout, flow/composite fade, ask-drift repost) could fire and send the placeholder ID to the CLOB cancel endpoint, causing "Invalid orderID" 400 errors — there is no CLOB order under that ID yet.

**Handle:** `is_provisional_order()` helper gates all 3 cancel dispatch points: if `order_id.starts_with("sim-leg1-")`, cancel commands are suppressed entirely. Once `on_order_posted()` arrives (~1.3s later), it replaces the provisional ID with the real CLOB hex hash and resets `timestamp_ms` to the confirmation time, activating all cancel checks against the real order. Additionally, `on_order_posted()` runs an immediate drift check: if the ask has moved ≥ `leg1_repost_tick_threshold` ticks since evaluation time, it cancels for repost right away rather than waiting for the next flow update.

**Safety:** No futile cancel attempts against non-existent CLOB orders. The cancel window starts from CLOB confirmation, not signal emission. Stale orders (ask drifted during the round-trip) are caught immediately on confirmation.

### Hedge not initialized after Leg 1 fill

**Scenario:** Leg 1 fills (User WS MATCHED) but `init_leg2()` returns early because `self.state.last_buildup` is `None`, so `HedgeState` is never created. `evaluate_leg2()` returns `None` (requires `self.hedge.is_some()`). Leg 2 never starts — trade stalls indefinitely with Leg 1 Filled.

**Root cause:** `evaluate()` previously cleared `self.state.last_buildup = None` when consuming the buildup to emit the Leg 1 signal (self-gating). `init_leg2()` is called from `on_order_filled()` which fires ~1s later (CLOB round-trip). By then, `last_buildup` was already gone.

**Fix:** `evaluate()` only clears `buildup_detected = false` (self-gating). `last_buildup` is preserved through signal emission and is only cleared in:
- `on_trade_complete()` (normal trade completion)
- `MarketRotation` handler (market switch)
- Non-repost cancel confirmed path (sustain fade / timeout cancel)

**Safety:** `buildup_detected = false` still provides the one-evaluation-per-buildup guarantee. `last_buildup` is safe to read by `init_leg2()` because fills arrive ~1s after evaluation on the live path.

### Non-repost cancel confirmed: full Leg 1 reset required

**Scenario:** A sustain-fade or timeout cancel is confirmed (`was_cancelled = true`, `leg1_last_cancel_repost = false`). Previously only `leg1_posted_ask` was cleared — leaving `leg1_state = Posted`, `leg1_direction`, `pending_leg1_signal`, and `last_buildup` populated. Next event: `evaluate()` sees `leg1_state != None` and blocks (correctly), but `last_buildup` leaks into the next trade cycle.

**Fix:** The non-repost cancel confirmed path now does a full Leg 1 reset: `leg1_state = None`, `leg1_posted_ask = None`, `leg1_direction = None`, `last_buildup = None`, `pending_leg1_signal = None`, `leg1_cancel_inflight = false`, `leg1_last_cancel_repost = false`. Matches the reset done in rotation and rejection paths.

### Emergency signal stacking (live)

**Scenario:** Engine evaluates every ~2-50ms, executor takes ~1-2s per CLOB call. During Phase 2 or emergency, multiple signals queue in the executor channel. A later signal cancels a FOK that was already filled by an earlier signal -> cancel not confirmed -> engine restores "Posted" state -> no User WS MATCHED arrives -> trade stuck.

**Handle:** `emergency_signal_in_flight` flag on the engine. Set when an emergency signal is dispatched, cleared on any executor feedback (OrderPosted, OrderFailed, CancelResult for leg2, trade complete, rotation). `evaluate_leg2()` returns None while the flag is set. Additionally, the executor sets `active_leg2_order_id = None` (instead of `Some(...)`) when a FOK returns `Filled` -- even if a stale signal sneaks through, it can't cancel a filled order.

**Safety:** Two-layer defense: Layer A (engine) prevents most stacking; Layer B (executor) prevents damage from any that slip through.

### Stale Leg 2 command contamination (live)

**Scenario:** While the executor processes a favorable exit (3 sequential HTTP calls, ~3.6s total), the engine's hedge evaluator fires and queues a SECOND hedge command. Trade 1 completes and resets. The stale command executes, fills, and `on_order_posted()` blindly sets `leg2_state = Filled` with stale data. When Trade 2's Leg 1 fills, both legs appear Filled -> wrong trade completion with mismatched sizes.

**Handle:** Three-layer defense:
- **Layer A (root cause):** `leg2_command_pending` flag gates `evaluate_leg2()` while ANY Leg 2 command is in the executor pipeline. Set on dispatch (live mode only), cleared on any Leg 2 feedback (OrderPosted, OrderFailed, CancelResult). Prevents new hedge/emergency commands from queuing during multi-step executor operations.
- **Layer B (stale feedback guard):** `on_order_posted()` and `on_order_failed()` check if `leg1_state` is `Filled` before processing Leg 2 feedback. If the trade has already been reset (leg1 is `None`), the feedback is silently discarded with a warning log.
- **Layer C (executor):** `active_leg2_order_id = None` on filled FOK prevents stale cancel of already-filled orders.

### Balance exhaustion (live)

**Scenario:** "Not enough balance / allowance" errors during Leg 2 placement cause the executor to burn futile FOK attempts across hedge phases and emergency rounds.

**Handle:** `balance_exhausted` flag on `LiveExecutor`. Set on first "balance"/"allowance" error during Leg 2 hedge placement. All subsequent Leg 2 commands (hedge, emergency) immediately return `OrderFailed` without calling CLOB. Cleared on `MarketRotation`. FOK retry loops also abort immediately on "balance"/"allowance" errors (added to non-transient error list alongside "decimal places" and "Validation"). Executor sends `BalanceExhausted` feedback -> engine fires `fire_critical()` Telegram alert with Leg 1 position details.

### Signal during existing trade

The `ActiveTrade` guard rejects the buildup signal, incrementing `rej_busy`. The signal is consumed (cleared) and cannot be re-evaluated. The diagnostic counter tracks how many valid-book signals were lost to executor busyness, informing parameter tuning.

### New signal during sustain window

**Scenario:** A second buildup signal arrives while the first Leg 1 maker is resting (Posted, awaiting fill).

**Handle:** The `ActiveTrade` guard blocks it -- `leg1_state == Posted`. After sustain cancel or timeout resets state to `None`, the next signal is accepted normally.

### Partial maker fill (live)

**Scenario:** Leg 1 maker is partially filled before sustain cancel is dispatched.

**Handle:** The CLOB does not allow partial cancel of maker orders -- if the order was partially filled before the cancel reached the CLOB, `was_cancelled = false`. The engine keeps Posted state and waits for User WS events. If the remaining size fills, User WS sends MATCHED events that the engine processes normally. The paired size PnL logic (`min(l1_size, l2_size)`) handles size mismatches naturally.

### User WS CANCELED race with cancel inflight (live)

**Scenario:** A drift-repost or sustain cancel is dispatched (`leg1_cancel_inflight = true`). Before the cancel result arrives, the CLOB sends a `TradeStatus::Canceled` event via the User WS (e.g., CLOB self-cancels due to post-only constraint violation or TTL). The engine's old handler unconditionally reset `leg1_state = None`. If the order then filled (CLOB processed fill before cancel), the `TradeStatus::Matched` event arrives after the state reset — `is_leg1` is `false`, fill is buffered/missed, and the open position is orphaned.

**Handle:** The `TradeStatus::Canceled` handler checks `leg1_cancel_inflight`:
- **`leg1_cancel_inflight = true`** — Our cancel is in flight; the cancel result is the authoritative cleanup path. State is NOT reset here. If the order filled, the later `Matched` event finds `leg1_state = Posted` and processes the fill normally, starting Leg 2.
- **`leg1_cancel_inflight = false`** — Unexpected CLOB cancellation (e.g., TTL expiry, post-only rejection) with no cancel from our side. Full reset: `leg1_state = None`, `leg1_posted_ask = None`, `last_buildup = None`, `leg1_direction = None`, `pending_leg1_signal = None`.

The same conditional logic applies to the `replay_pending_fills` buffer handler.

**Manual trace (orphan prevention):**
1. Drift repost cancel dispatched → `leg1_cancel_inflight = true`
2. CLOB sends `Canceled` via User WS → handler defers (no state change)
3. CLOB sends `Matched` (order filled before cancel) → `is_leg1 = true` (state still Posted) → fill processed → Leg 2 starts normally
4. Cancel result arrives: `was_cancelled = false` → `leg1_cancel_inflight` stays `true`, `leg1_last_cancel_repost = false`
5. `on_trade_complete()` clears `leg1_cancel_inflight = false`

**Safety:** No orphaned positions. Cancel result is always the authoritative Leg 1 cancel path when `leg1_cancel_inflight = true`.

### Maker rejected (live)

**Scenario:** Post-only order is rejected because the price would cross the book (e.g., our bid is at or above the current ask).

**Handle:** Executor detects `OrderStatus::Rejected`, sends `OrderFailed`. Engine resets `leg1_state = None` via `on_order_failed()`. The signal is consumed -- next buildup can enter fresh.

### Whipsaw guard removal (dead code)

**Background:** `handle_buildup_confirmed()` previously contained a whipsaw guard block (approx. lines 1257-1290) that was intended to cancel a Posted Leg 1 order and emit a FOK taker in the opposite direction when a new opposite-direction buildup fired during a live order.

**Why it was unreachable:** The early-return guard at the top of `handle_buildup_confirmed()` — `if !matches!(self.state.leg1_state, OrderState::None) { return; }` — exits immediately for any non-None `leg1_state`, including `Posted`. The whipsaw block could never be reached because the function had already returned.

**Removed:** `whipsaw_fok_pending` field, `emit_whipsaw_fok()` function, `diag_whipsaw_cancels` and `diag_whipsaw_foks` diagnostic counters.

**Kept:** `ExitReason::WhipsawReversal` (still used by the evaluator's flow reversal path in `evaluate_leg2()`), `whipsaw_reversal` field on `LiveTradeReport` (used for Telegram and QuestDB tagging of flow-reversal emergency exits).

### Flow data stale during hedge

**Scenario:** All buildup metrics go stale (beyond their freshness windows) during Phase 1.

**Handle:** `flow_monitoring_active` is set to `false` when the composite score drops to 0 (all stale). The flow-based graduated response (reversal, collapse, weakening) is skipped entirely. The system falls back to time-based Phase 1 timeout and breach checks. No false emergency exits from stale data.

### Flow collapse vs Phase 1 breach priority

**Scenario:** Both flow collapse and Phase 1 breach conditions are true simultaneously.

**Handle:** In `evaluate_leg2()`, Phase 1 breach is checked BEFORE flow-based triggers. This ensures the higher-severity price-based emergency fires first. Flow collapse would be redundant if Phase 1 breach already triggered.

---

## 15. Complete Trade Example

**Scenario:** BTC building directional momentum upward at $52,000. 5-minute market has 3 minutes remaining.

### Step 1: Buildup Detection (T+0ms)

Composite 6-metric detector evaluates on Binance depth event (using 2026-03-11c weights):
- Basis delta: +0.55 (futures premium expanding -- **leading**)
- CVD acceleration: +0.72 (strong aggressive buying on futures -- **supporting**)
- OBI velocity: +0.45 (bid depth growing relative to ask -- **confirming**)
- Spot flow: +0.38 (net aggressive spot buying -- **confirming**)
- ATR displacement: +0.30 (spot price rising within ATR bands)
- Liquidation pressure: +0.10 (minor short liquidations)

Direction consensus: 6/6 Up, 0 minority. Causal ordering: Basis (leading, fresh) + OBI (confirming, fresh) both present. Weighted sum (new weights):
```
composite = 0.35*0.55 + 0.20*0.72 + 0.20*0.45 + 0.15*0.38 + 0.05*0.30 + 0.05*0.10
          = 0.1925 + 0.1440 + 0.0900 + 0.0570 + 0.0150 + 0.0050
          = 0.504
```
Score 0.504 >= entry_threshold 0.40 -> `BuildupConfirmed`.

**Compare to old weights:** Old calculation would have been `0.30*0.72 + 0.20*0.55 + 0.20*0.45 + 0.15*0.38 + 0.10*0.30 + 0.05*0.10 = 0.508` (nearly identical in this scenario, but basis is now "pulling weight" instead of CVD).

### Step 2: Leg 1 Evaluation (T+1ms)

Guards pass: no active trade, Binance price present, YES book bid=0.495/ask=0.505, book age 50ms, YES mid=0.50 (no skew).

Repricing model: composite_score 0.508 (already [0,1]) x `4P(1-P)` x alignment x time_factor x reprice_scale -> expected_pct. Used as Phase 1 profit target (dampened by phase1_target_dampen).

Allocation = round(max_alloc x alloc_fraction) based on dynamic sizing. Ask = best_ask = $0.505. Size = round_dp(alloc / ask_price).

**Signal:** Buy YES @ $0.505 x 40 shares = $20.20 (maker post-only at best ask)

### Step 3: Leg 1 Sustain (T+1ms to T+500ms)

Maker order rests on book. Engine monitors composite score on every event. Score stays at 0.48 -> above cancel_threshold (0.25) -> order holds. No whipsaw signals arrive.

### Step 4: Leg 1 Fill (T+500ms)

CLOB market maker sells into our resting bid at $0.505. User WS sends MATCHED event.

Result: `leg1_state = Filled`. Hedge initialized (`init_leg2()`). `initial_profit_target` = dampened entry expected_pct (0.508 composite score → dampened by phase1_target_dampen).

### Step 5: Phase 1 Post (T+500ms, immediately after fill)

target = 1.0 - 0.025 - 0.505 = **$0.47**. Emit Phase 1 post signal: Buy NO @ $0.47. Order rests on book at profit target.

### Step 6: Phase 1 Fill (T+1800ms)

NO book best_ask = 0.47 <= posted 0.47 -> fill as maker during Phase 1 (before timeout).

Alternatively, if composite flow weakens below entry_threshold (0.40) at T+1200ms: transition to Phase 2 alongside, posting at `ask - 1 tick`.

### Step 7: Trade Complete

| | Price | Shares | Cost |
|---|-------|--------|------|
| Leg 1 (YES) | $0.505 | 40 | $20.20 |
| Leg 2 (NO) | $0.47 | 40 | $18.80 |
| **Pair cost** | $0.975/sh | | |
| **Gross profit** | | | $1.00 |
| **Taker fee** | | | $0.00 |
| **Maker rebate** | | | ~$0.06 |
| **Net profit** | | | **$1.06 (2.72%)** |

### Alternative: Flow collapse emergency

If composite score drops below cancel_threshold (0.25) at T+800ms during Phase 1: immediate FOK taker at best ask $0.51. Pair = $0.505 + $0.51 = $1.015, loss = $0.60 gross + ~$0.63 taker fee = -$1.23 net. But the early flow-based exit avoids the worse outcome of Phase 2 timeout when the book has repriced further.

### Alternative: Phase 2 break-even breach

If Phase 1 times out and Phase 2 posts at `ask - 1 tick = $0.505`. Book reprices further: `ask = $0.51 > phase2_posted_price $0.505` -> **PHASE 2 PRICE BREACH**.

Immediate FOK taker at `round_to_tick(0.51, tick)` = $0.51. Pair = $1.015, loss = $0.60 gross + ~$0.63 taker fee = -$1.23 net.

---

## 16. Simulation vs Live Differences

| Aspect | Simulation | Live |
|--------|-----------|------|
| **Fill authority** | Engine (`advance_simulation()`) | CLOB (User WS primary, sync fill backup) |
| **Leg 1 entry model** | Maker post-only: bid < ask AND near depth > 0 check | Maker post-only GTC via `place_order()` |
| **Leg 1 fill model** | Book-based: ask <= fill_price + 2×tick AND near_ask_depth > 0 | Real CLOB matching engine, User WS notification |
| **Leg 1 cancel model** | Direct state reset (no real order to cancel) | `CancelLeg1Order` to executor, fire-and-confirm |
| **Leg 2 fill model** | Book-based: ask <= posted -> fill | Real CLOB matching engine |
| **Leg 2 hedge model** | 2-phase: Phase 1 at profit target, Phase 2 at ask-1tick | 2-phase: same logic, real CLOB matching |
| **Flow monitoring** | Same composite score evaluation | Same, plus hedge flow propagation |
| **Emergency fill model** | Immediate FOK taker at ask (uses `fok_emitted` as discriminator) | Immediate FOK taker at ask, price-escalating retry on liquidity failure |
| **Feedback channel** | Not used (engine is fill authority) | Executor -> engine order IDs, cancel results |
| **Sustain cancel** | Direct state reset | `CancelLeg1Order` -> fire-and-confirm (cancel-not-confirmed keeps Posted) |
| **Fill latency** | Next event loop after buildup detection | CLOB maker fill latency (variable -- depends on market activity) |
| **Order tracking** | Synthetic IDs (`sim-leg1-{ts}`) | Real CLOB order IDs (hex hashes) |
| **Capital** | Virtual balance | Real wallet USDC.e |
| **Telegram** | Full: opportunity, completion, market, session | Alerts: emergencies, critical only |
| **QuestDB** | `simulated_trades` table | `executed_trades` table |
| **Heartbeat** | Not needed | 5s POST to `/heartbeat` |
| **MarketRotation** | Force-close open, lock for resolution | `cancel_all()` CLOB orders |
| **Trade detection** | `advance_simulation()` sees both filled | Main loop checks both legs Filled |
| **Leg 1 sustain timeout** | Direct state reset (no real order) | `CancelLeg1Order` -> CLOB cancel, fire-and-confirm |
| **Order signing** | N/A (no real orders) | SDK handles EIP-712 signing, fee rate caching, L2 HMAC auth |

### Key simulation simplifications

1. **Instant maker fills:** Sim fills whenever `ask <= fill_price + 2×tick` AND depth > 0 on the next event (models the spread collapsing to our bid within 2 ticks); live fills depend on real CLOB queue position and market activity
2. **No sustain cancel latency:** Sim sustain cancel is a direct state reset; live requires CLOB round-trip with cancel-not-confirmed race
3. **Full depth available:** Assumes entire order fills at posted price; real CLOB may partially fill
4. **No queue position:** Doesn't model time priority in the CLOB queue
5. **Deterministic emergency fees:** All emergency exits are taker FOK; live depends on actual CLOB acceptance and price escalation

These simplifications mean simulation PnL is an optimistic estimate. Live trading will likely see lower fill rates, occasional partial fills, and more FOK fallbacks.

---

## 15. Ask Moves Away During Leg 1 Posting

**Scenario:** Leg 1 is posted at best ask = $0.48, composite score = 0.45 (hot buildup). The market maker's ask drifts up to $0.50 while the buildup remains live (composite still > 0.25). Our $0.48 limit order sits stranded below the market, waiting for someone to sell at $0.48 when the current ask is $0.50.

**Three possible explanations:**

1. **Genuine market maker repricing** (most common): Market maker is hedging longer-dated futures or adjusting inventory. Not related to the spike. Our order is in the wrong place.
2. **Spike is happening** (bullish): Buyers are aggressively lifting the ask, so it rises. Our maker sits at $0.48, likely to fill from continued buying pressure. Adaptive repost would post at the new ask ($0.50), but the spike will probably bring more buyers — no need to repost.
3. **Buildup faded** (bearish): The composite that drove our entry has dissipated. The ask drifted up because the market cooled and the buildup is dead. No point following the ask down; cancel and reset.

**Adaptive Leg 1 Repost handles (1) and avoids (2) and (3):**

| Scenario | Ask drift | Composite | Repost behavior | Outcome |
|----------|-----------|-----------|-----------------|---------|
| (1) MM repricing | ≥ N ticks | remains > 0.25 | YES — cancel + post at new ask | Catches next seller at better price |
| (2) Spike happening | ≥ N ticks | remains high (>0.40) | YES — cancel + post at new ask | BUT: new order faces same gap, fills from buying pressure anyway. Repost is safe; order will fill either way |
| (3) Buildup faded | ≥ N ticks | drops < 0.25 | NO — full reset | Avoids chasing; wait for next buildup |

**Why repost only on case (1)?**

Repost is conditioned on `composite > cancel_threshold` (default 0.25). The condition gates reposts to genuine market microstructure noise (ask drift with a live signal), not fundamental reversals or buildup collapse. If composite drops below the threshold between cancel dispatch and confirmation, the cancel is treated as `repost: false` and state is fully reset.

**Config:** `leg1_repost_tick_threshold` (default: 1 tick). Set to 0 to disable. Large values (>= 3 ticks) make repost rare.

---

## 15a. Transient Binance Reversal During Active Spike (No Sustain Window)

**Scenario:** Leg 1 filled at $0.48 (UP spike). Phase 1 hedge posted at $0.495 (profit target). Composite score = 0.55 (very hot buildup, sustained buying). Suddenly, a large futures liquidation cascade reverses the composite to the opposite direction (DOWN, score = 0.30) for 100ms, then it swings back UP to 0.50. Did we miss a whipsaw exit?

**Answer: No. And that's intentional.**

The whole value of flow reversal detection is that it exits **before Polymarket reprices**. Polymarket lags Binance by 100-500ms. If we add a sustain window (e.g., "reversal confirmed if score stays opposite for 200ms"), we are:
1. **Waiting 200ms** while the reversal signal is true
2. During that 200ms, **Polymarket reprices** using Binance data that is already 100-500ms old
3. By the time the sustain window expires and we FOK, **the market has already moved** — we lose the lead time

**Flash crash scenario:** A brief liquidation cascade reverses the composite for 50ms, then the real move continues UP. With a sustain window, we:
- Exit on false reversal at $0.50 (FOK taker)
- Pay ~$0.016 taker fee per share
- Later session trades on the correct UP direction at better prices
- Small cost, manageable

**Alternative (sustain window):** Keep waiting through the 100ms reversal flutter, then exit AFTER Polymarket has already repriced and our exit price is worse.

**Conclusion:** The phase-based multi-phase hedge (Phase 1 profit target, Phase 2 break-even) is the price-based backstop for false reversals. Phase 1 breach and Phase 2 price breach FOK provide a secondary safeguard if the market truly moves against us. Flow reversal is optimized for capturing Binance's lead, and flash crash exits are an acceptable cost of that optimization.

**Diagnostic:** Count flash crash (true) reversals vs false reversals in session summaries and `whipsaw_reversal` field on `LiveTradeReport` (cross-reference with later UP trades). Sustained negative sessions suggest the reversal sustain window should be reconsidered.

---

## 17. Session Fixes — 2026-03-12

The following changes were made to address real-money edge cases discovered in live operation.

### 17a. Skew Guard Tightened (hard_skew_cap 0.93 → 0.90)

`hard_skew_cap` was reduced from 0.93 to 0.90. Markets priced above $0.90 or below $0.10 (YES mid) have insufficient repricing capacity for a profitable trade. The tighter cap reduces false entries in near-binary markets.

### 17b. FOK $1.00 Minimum Notional Fix

The `emergency_fok_fallback()` loop now enforces a $1.00 minimum notional via `clob_safe_fok_size()`. Previously, price escalation could reduce the computed size to a sub-dollar notional that the CLOB rejects. The fix increases size to meet the floor before each FOK submission. See Section 8b for full loop constraints.

### 17c. Entry Size Clamping (5 Shares / $1.00 Notional)

Leg 1 entry size is clamped to `max(raw_size, 5, ceil($1/price))` instead of being rejected. The 5-share floor satisfies the CLOB maker minimum; the `ceil($1/price)` floor satisfies the FOK $1.00 notional minimum. Since Leg 2 inherits Leg 1's filled size, it always meets these minimums. See Section 8a.

### 17d. FOK Escalation Loop: "Too Old" Abort

A `"too old"` error response from the CLOB during the FOK escalation loop now causes an immediate non-retryable abort. This error indicates the market has expired or the order timestamp is stale; retrying at a higher price cannot resolve it and burns attempts unnecessarily. The loop fires an orphaned-position Telegram alert and exits.

### 17e. FOK Attempt Cap (10)

The `emergency_fok_fallback()` price-escalation loop is now capped at a maximum of 10 attempts. Previously the loop could run indefinitely if the book was thin or the price kept moving. After 10 failed attempts, the loop exits and fires a Telegram orphaned-position alert for manual intervention.
