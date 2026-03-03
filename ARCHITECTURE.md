# FaCaiBot — Architecture & Operations

## 1. Core Concept

FaCaiBot exploits the repricing lag on Polymarket's CLOB. When Binance BTC/USDT spikes, Polymarket market makers take seconds to adjust quotes. The bot detects the spike, enters before repricing, and hedges with the opposite side:

```
Binance spike → Buy directional shares cheap (Leg 1, post-only, $0 fee)
             → CLOB reprices
             → Buy opposite shares (Leg 2, post-only, $0 fee)
             → Paired position: e.g. $0.48 + $0.495 = $0.975 → pays $1.00 → 2.5% profit
```

Both legs are `post_only=true` (maker, zero fee). Taker fees (`C * 0.25 * (p*(1-p))^2`, max 1.56% at p=0.50) apply only to FOK fills: emergency exits (adverse movement, break-even breach, market expiry) and favorable taker fills (opposing ask dropped below posted bid).

**Why it works**: Binance is the largest liquidity venue. >95% correlation with Chainlink for moves >1%. Post-only entry preserves the full spread at the cost of lower fill rate (~30-50%). Unfilled signals cost nothing.

**Modes** (`MODE` env var):
- **`live`**: Submits real orders via polymarket-client-sdk (EIP-712 signing handled internally), monitors fills via User WS
- **`simulation`**: Full pipeline against live data, but engine simulates fills internally. Reports via Telegram + QuestDB

---

## 2. Pipeline

Three-layer lock-free pipeline connected by crossbeam SPSC bounded(8192) channels:

```
Ingestor (gateway/)        Engine (engine/)           Executor (executor/)
─────────────────          ──────────────             ─────────────────────
Dedicated OS thread        spawn_blocking thread      tokio task (worker thread)
CPU-pinned core 0          (off worker pool)          Manual runtime (2 workers)
                           Pinned cores 1-2            Pinned cores 1-2

Binance SBE WS ──┐
  @depth20 (50ms)│
  @bestBidAsk    ├──► IngestorEvent ──► MarketState update
Polymarket WS ───┤         ▲              Spike detection
  Market channel │         │              Signal evaluation
  User channel   │    Shutdown /          Erosion cascade
Gamma API ───────┘    DrainAndRestart     advance_simulation() (sim)
Heartbeat (5s) ──┘         │                    │
                    CommandListener         TradeSignal / ExecutorCommand
                    (control/)                   │
                    Telegram getUpdates          ▼
                    ◄──► AtomicBool flags   Live: LiveExecutor ──► CLOB REST
                         (NotifyFlags)           ◄── ExecutorFeedback (order IDs)
                         watch<BotStatus>   Sim: SimulationExecutor ──► Telegram+QuestDB
                         watch<DrainStatus>
```

### Simulation Engine Loop

**Single-authority fill model**: the engine is the sole fill authority in sim mode (mirrors live where the CLOB decides fills). `advance_simulation()` acts as the simulated CLOB — the executor only records what the engine tells it via the `sim_confirmed_fill` flag on `TradeSignal`.

**Speculative fill gate**: After a `SpikeCandidate`, `advance_simulation()` is blocked from filling Leg 1 until `SpikeConfirmed` clears the gate (`speculative_awaiting_sustain`). If `SpikeFailed` arrives instead, the speculative Leg 1 is cancelled — no fill ever happens.

```
on_event()           → update book/price state
                       SpikeCandidate: set spike_detected, speculative_awaiting_sustain=true
                       SpikeConfirmed: clear speculative_awaiting_sustain (allow sim fills)
                       SpikeFailed: cancel speculative Leg 1 if Posted
take_spike_cancel()  → drain pending CancelLeg1 from SpikeFailed
advance_simulation() → simulate fills: Posted→Filled→Complete→Reset
                       BLOCKED while speculative_awaiting_sustain=true
                       emits confirmed fill signals (sim_confirmed_fill=true)
evaluate()           → Leg 1 signal (self-gates after emitting)
                       emits detection signal (sim_confirmed_fill=false)
                       records rejection reason if spike blocked
evaluate_leg2()      → Leg 2 erosion/emergency signal (self-gates after emitting)
                       erosion steps sent to executor (sim_confirmed_fill=false)
                       emergency signals handled internally by advance_simulation()
```

### Live Engine Loop

In live mode, the CLOB is the fill authority. Fills arrive via the authenticated User WebSocket as `TradeStatusUpdate` events. The engine matches these against posted order IDs to transition `OrderState::Posted → Filled`.

Three feedback types flow from executor → engine via the `ExecutorFeedback` channel:

| Feedback | When | Engine action |
|----------|------|---------------|
| `OrderPosted { order_id, price, size, is_leg2 }` | CLOB accepted the order | Overwrite provisional `"sim-..."` ID with real hex hash. If `cancel_leg1_on_feedback` is set, immediately return `CancelLeg1` instead. Replay `pending_fills` buffer |
| `OrderFailed { is_leg2 }` | CLOB rejected or network error (non-emergency only — emergency FOKs retry internally, never send this) | Reset leg state to `None`. If `cancel_leg1_on_feedback` is set, clear the flag (nothing to cancel) |
| `CancelResult { order_id, was_cancelled, is_leg2 }` | Executor received CLOB cancel response | If confirmed: clear saved order info. If NOT confirmed: restore `OrderState::Posted` from saved info, replay `pending_fills` buffer |

**User WS event routing**: The Polymarket User WS sends two event types: `"order"` events (hex order hash, e.g. `0x13828d75...`) and `"trade"` events (UUID trade ID, e.g. `89f124e7-...`). Only `"order"` events are forwarded to the engine as `TradeStatusUpdate` — their `id` field matches the hex hash stored from `OrderPosted` feedback. `"trade"` UUIDs never match and are harmlessly ignored. Actionable statuses forwarded: MATCHED, MINED, CONFIRMED, FAILED, RETRYING, CANCELED. Non-actionable statuses (LIVE) are silently skipped by `parse_trade_status()`.

**Speculative posting**: Leg 1 is posted to the CLOB immediately on `SpikeCandidate` (gaining ~300ms queue priority). If `SpikeFailed` arrives before the order fills, a `CancelLeg1` is sent. If it already filled, Leg 2 proceeds normally. The engine saves order info before clearing state, so a `CancelResult { was_cancelled: false }` can restore it.

```
Engine loop (one iteration per IngestorEvent):
┌─────────────────────────────────────────────────────────────────┐
│ 1. DRAIN FEEDBACK (non-blocking)                                │
│    while feedback_rx.try_recv():                                │
│      OrderPosted  → set real CLOB ID, replay pending_fills      │
│      OrderFailed  → reset leg to None                           │
│      CancelResult → restore or clear saved order info           │
│      DiagSnapshot → store for Telegram forwarding               │
│                                                                 │
│ 2. CONTROL EVENTS                                               │
│    Shutdown / DrainAndRestart / PauseTrading / ResumeTrading     │
│    (cancel unfilled Leg 1 with save-before-clear, set flags)     │
│                                                                 │
│ 3. ON_EVENT (state update)                                      │
│    Book/price updates, spike lifecycle, rotation, User WS fills  │
│    TradeStatusUpdate: match order_id against leg1/leg2 state     │
│      → Matched/Mined/Confirmed → transition to Filled           │
│      → Failed → reset to None                                   │
│      → Canceled → reset to None (CLOB auto-cancel)              │
│      → unmatched → buffer in pending_fills (cap 8)              │
│                                                                 │
│ 4. SIGNAL GENERATION                                            │
│    take_spike_cancel() → drain CancelLeg1 from SpikeFailed      │
│    check_leg1_staleness() → cancel stale orders (save-then-clear)│
│    evaluate() → Leg 1 signal (self-gates after emitting)         │
│    evaluate_leg2() → Leg 2 erosion/emergency (save-then-overwrite)│
│                                                                 │
│ 5. TRADE COMPLETION                                             │
│    Both legs Filled → record to QuestDB → on_trade_complete()    │
│    (clears all state including saved order info + pending_fills)  │
└─────────────────────────────────────────────────────────────────┘
```

**Key differences from sim**:
- `advance_simulation()` never runs — no speculative fill gate needed (CLOB is fill authority)
- Emergency signals are NOT filtered — the executor places FOK orders on the CLOB
- Trade completion detected in the main loop (both `leg1_state` and `leg2_state` are `Filled`)
- Feedback is drained before `on_event()` — CLOB round-trip (~1.2s) completes before User WS notification (~1.5-2s), so order IDs are typically set before fill events arrive
- Cancel operations are fire-and-confirm — the executor checks the CLOB DELETE response and sends `CancelResult` feedback so the engine can detect orders that filled before the cancel

### LiveExecutor (`executor/live.rs`)

Handles the full trade lifecycle via the SDK-backed `PolymarketGateway`:

| Signal | Action |
|--------|--------|
| Leg 1 | Post-only GTC → feedback `OrderPosted` to engine |
| Leg 1 rejected | CLOB returns `Rejected` → feedback `OrderFailed` to engine |
| CancelLeg1 | Send cancel → `CancelResult { was_cancelled }` feedback to engine |
| Leg 2 erosion | Cancel previous → confirmed: repost at eroded price. NOT confirmed: `CancelResult` feedback, skip replacement |
| Leg 2 erosion rejected | Post-only rejected (ask < bid) → attempt favorable exit (post-only first, FOK fallback) |
| Leg 2 emergency | Cancel resting → confirmed: place emergency order. NOT confirmed: `CancelResult` feedback, skip replacement |
| Leg 2 emergency rejected | Post-only rejected → FOK fallback at `best_ask + 1 tick` (retries internally until CLOB accepts — never sends `OrderFailed`) |
| Market rotation | `cancel_all()` → reset state → pre-warm SDK caches → `caches_warm = true` |

On network error during cancel, the executor cannot determine state — it proceeds with the replacement (cancel counted, no `CancelResult` sent). The User WS is the final authority: if the old order filled, the MATCHED event will arrive and be matched by the engine.

On placement failure or CLOB rejection, sends `OrderFailed` feedback so the engine resets the leg state to `None`. **Exception**: Emergency FOK orders (`emergency_fok_fallback` and `emergency_fok_at_price`) retry internally until the CLOB accepts — they never send `OrderFailed`. After Leg 1 fills, the position must be hedged; the ~1.2s HTTP round-trip per attempt is the natural rate limiter.

---

## 3. Signal Detection

### Spike Detection (`gateway/binance/spike.rs`)

**Speculative posting model**: The detector emits a `SpikeCandidate` immediately when ATR + magnitude pass, allowing the engine to post a Leg 1 order ~300ms before confirmation. If the spike fails to sustain, the speculative order is cancelled (post-only = zero cost). This gains ~300ms of CLOB queue priority vs waiting for full confirmation.

1. **Mid-price**: `(best_bid + best_ask) / 2` from `@depth20` updates (SBE binary, 50ms cadence)
2. **Rolling EMA-ATR**: alpha=0.01 (~100 ticks / ~5s memory at 50ms/tick). No spikes emitted for first 10 ticks while ATR warms up
3. **Spike trigger + magnitude gate** (immediate): `|delta| > multiplier * ATR` (2x) AND `displacement / origin_price >= min_magnitude_pct` (1 bp). If both pass → emit `SpikeCandidate` immediately → engine posts speculative Leg 1
4. **Sustain + momentum check** (at `sustain_ms` = 300ms):
   - **Displacement held**: price must still be displaced above threshold at sustain time
   - **Momentum ratio**: displacement at sustain must be ≥65% of peak (filters fading spikes)
   - If both pass → emit `SpikeConfirmed` (gates sim fills)
   - If either fails → emit `SpikeFailed` (engine cancels speculative Leg 1)
5. **One attempt**: Each spike evaluates exactly once. `spike_detected` cleared regardless of outcome

### Spike Signal Delivery

Three event variants carry spike lifecycle signals:

- **`SpikeCandidate(SpikeInfo)`**: ATR + magnitude passed → triggers speculative Leg 1 posting. Engine sets `spike_detected = true`, stores `last_spike`, and sets `speculative_awaiting_sustain = true`
- **`SpikeConfirmed(SpikeInfo)`**: Sustain + momentum passed → clears `speculative_awaiting_sustain`, allowing sim fills. Live mode: no-op (fills come from User WS)
- **`SpikeFailed { timestamp_ms }`**: Spike faded before sustain time → engine cancels speculative Leg 1 if still `Posted`, resets spike state. If already `Filled`: no-op, Leg 2 proceeds normally

Normal `BinanceTick` events only update `binance_price` — they never trigger spike evaluation.

---

## 4. Trade Lifecycle (Live Mode)

The diagram below shows the complete live-mode trade lifecycle. Every state transition, cancel path, and edge case is covered. Simulation mode is simpler (engine is sole fill authority via `advance_simulation()`; no CLOB round-trips, no cancel races).

```
                    ┌──────────────────────────────────────────────────────────────────┐
                    │                    LIVE TRADE STATE MACHINE                       │
                    └──────────────────────────────────────────────────────────────────┘

     IDLE                                      LEG 1                                          LEG 2
    ═════                          ════════════════════════                        ════════════════════════

                 SpikeCandidate
    leg1=None ─────────────────► evaluate() passes guards
                                  │
                                  │  Engine: leg1_state = Posted("sim-leg1-{ts}")
                                  │          pending_leg1_signal = Some(signal)
                                  │          spike_detected cleared (self-gate)
                                  │          send Signal to executor
                                  ▼
                            ┌───────────┐
                            │  Leg 1    │
                            │  POSTED   │◄──────────────────────────────────┐
                            │(provisional)                                  │
                            └─────┬─────┘                                   │
                                  │                                         │
                    ┌─────────────┼──────────────┐                          │
                    │             │              │                          │
               SpikeFailed   OrderPosted    OrderFailed                     │
               (before fill)  feedback      feedback                       │
                    │             │              │                          │
                    ▼             ▼              ▼                          │
              provisional?   overwrite ID    reset to None                  │
              ┌───┴───┐      with real hex   (slot freed)                   │
              │       │      CLOB hash                                     │
            YES      NO         │                                          │
              │       │         │                                          │
  set defer   │  save info     ┌┴──────────┐                               │
  cancel flag │  send Cancel   │  Leg 1    │                               │
  leg1=None   │  leg1=None     │  POSTED   │                               │
              │       │        │ (real ID) │                               │
              │       │        └─────┬─────┘                               │
              │       │              │                                     │
              │       │    ┌─────────┼──────────┬──────────┐               │
              │       │    │         │          │          │               │
              │       │  Staleness  User WS   User WS    SpikeFailed      │
              │       │  timeout    MATCHED   CANCELED   (already filled)  │
              │       │    │         │          │          │               │
              │       │    ▼         │          ▼         no-op            │
              │       │  save info  │       reset to None                  │
              │       │  send Cancel│       (CLOB killed it)               │
              │       │  leg1=None  │                                      │
              │       │    │        │                                      │
              │       ▼    ▼        ▼                                      │
              │   ┌─────────────────────────┐                              │
              │   │    CANCEL RESULT        │                              │
              │   │  (from executor)        │                              │
              │   │                         │                              │
              │   │  was_cancelled = true   │                              │
              │   │    → clear saved info   │                              │
              │   │    → slot freed         │                              │
              │   │                         │                              │
              │   │  was_cancelled = false  │                              │
              │   │    → RESTORE leg1_state │                              │
              │   │      from saved info    │──── User WS MATCHED ──┐     │
              │   │    → replay pending_fills                       │     │
              │   └─────────────────────────┘                       │     │
              │                                                     │     │
              │   OrderPosted arrives (deferred cancel flag set)     │     │
              │     → cancel_leg1_on_feedback = false                │     │
              │     → send CancelLeg1 with real ID ─────────────────┤     │
              │                                                     │     │
              │                                                     ▼     │
              │                                              ┌────────────┴──┐
              │                                              │   Leg 1       │
              │                                              │   FILLED      │
              │                                              │               │
              │                                              │ init_erosion()│
              │                                              │ Telegram alert│
              │                                              └───────┬───────┘
              │                                                      │
              │                                           evaluate_leg2()
              │                                                      │
              │                    Engine: save prev Leg 2 info (if real CLOB ID)
              │                           leg2_state = Posted("sim-leg2-{type}-{ts}")
              │                           send Signal to executor
              │                                                      │
              │                                                      ▼
              │                                              ┌───────────────┐
              │                                              │   Leg 2       │
              │                                              │   POSTED      │
              │                                              │ (provisional) │
              │                                              └───────┬───────┘
              │                                                      │
              │                               ┌──────────────────────┼──────────────────────┐
              │                               │                      │                      │
              │                          OrderPosted              Erosion step         Emergency trigger
              │                          feedback                 (timer elapsed)      (adverse/BE/exhausted)
              │                               │                      │                      │
              │                               ▼                      │                      │
              │                        ┌──────────────┐              │                      │
              │                        │   Leg 2      │              │                      │
              │                        │   POSTED     │              │                      │
              │                        │  (real ID)   │              │                      │
              │                        └──────┬───────┘              │                      │
              │                               │                      │                      │
              │              ┌────────────────┼──────────────────┐   │                      │
              │              │                │                  │   │                      │
              │         User WS          User WS           User WS  │                      │
              │         MATCHED          CANCELED          FAILED   │                      │
              │              │                │                  │   │                      │
              │              ▼                ▼                  ▼   │                      │
              │        ┌──────────┐    reset to None      reset to None                    │
              │        │  Leg 2   │    clear saved info   (evaluate_leg2                   │
              │        │  FILLED  │    (CLOB killed it)    will regenerate)                 │
              │        │          │                             │                           │
              │        └────┬─────┘                             │                           │
              │             │                                   │                           │
              │             ▼                                   │                           │
              │     ┌───────────────┐                           │                           │
              │     │ TRADE         │                           │                           │
              │     │ COMPLETE      │                           │                           │
              │     │               │                           │                           │
              │     │ Both Filled   │                           │                           │
              │     │ Record QuestDB│                           │                           │
              │     │ Telegram msg  │                           │                           │
              │     │ Clear all     │                           │                           │
              │     │ saved state   │                           │                           │
              │     └───────┬───────┘                           │                           │
              │             │                                   │                           │
              └─────────────┴───────────────────────────────────┘                           │
                     back to IDLE                                                           │
                                                                                            │
                     ┌──────────────────────────────────────────────────────────────────────┘
                     │
                     ▼
              ┌──────────────────────────────────────────────────────────┐
              │              LEG 2 CANCEL-REPLACE FLOW                  │
              │                                                         │
              │  Engine: evaluate_leg2() returns Erosion or Emergency    │
              │          save current Leg 2 info (prev_leg2_order)       │
              │          overwrite leg2_state with provisional ID        │
              │          send Signal to executor                         │
              │                                                         │
              │  Executor receives Signal:                               │
              │    1. Cancel previous resting order via CLOB DELETE      │
              │                                                         │
              │    ┌─── cancel confirmed (order was in canceled list) ───┐
              │    │                                                     │
              │    │  2a. Post replacement order                         │
              │    │      → Accepted: OrderPosted feedback               │
              │    │      → Rejected (post-only would cross):            │
              │    │          Erosion: attempt_favorable_exit()           │
              │    │          Emergency: FOK fallback (retries internally) │
              │    │      → Network error:                               │
              │    │          Emergency FOK: retry (never sends feedback) │
              │    │          Other orders: OrderFailed feedback          │
              │    │                                                     │
              │    └────────────────────────────────────────────────────┘
              │                                                         │
              │    ┌─── cancel NOT confirmed (not in canceled list) ────┐
              │    │                                                     │
              │    │  The order may have filled before the cancel.       │
              │    │  2b. DO NOT post replacement                        │
              │    │      Send CancelResult { was_cancelled: false }     │
              │    │      Engine restores prev Leg 2 Posted state        │
              │    │      User WS MATCHED event arrives → Leg 2 Filled   │
              │    │      → trade complete                               │
              │    │                                                     │
              │    └────────────────────────────────────────────────────┘
              │                                                         │
              │    ┌─── cancel network error ──────────────────────────┐
              │    │                                                     │
              │    │  Cannot determine state.                            │
              │    │  2c. Post replacement anyway (best effort)          │
              │    │      No CancelResult sent.                          │
              │    │      User WS is final authority — if old order      │
              │    │      filled, MATCHED arrives and engine matches it  │
              │    │      via pending_fills buffer (old ID won't match   │
              │    │      current leg state, gets buffered, replayed     │
              │    │      if state changes).                             │
              │    │                                                     │
              │    └────────────────────────────────────────────────────┘
              └──────────────────────────────────────────────────────────┘
```

### Leg 1: Entry

**Speculative entry**: Leg 1 is posted speculatively on `SpikeCandidate` (before sustain confirmation). `evaluate(&mut self)` clears `spike_detected`, sets `leg1_state = Posted` with a provisional `"sim-leg1-{ts}"` ID, and increments `cumulative_used`. One trade at a time. If `SpikeFailed` arrives before fill, the order is cancelled and state reset (post-only = zero cost).

**Pre-entry guards** (abort if any fail):

| Guard | Threshold | Notes |
|-------|-----------|-------|
| No market | `active_condition_id` absent | Rare — awaiting rotation |
| No book | book or bid/ask missing | — |
| No Binance price | `binance_price` absent | — |
| Stale book | book age > `stale_book_ms` (500ms) | — |
| Price skew | YES mid > 0.80 or < 0.20 | Near-certain-resolution, Leg 2 fill collapses |
| Spread | > `max_spread` ($0.02) | Dollar-based, consistent across tick sizes |
| Active trade | `leg1_state != None` | Checked **after** spread — `rej_busy` counts only valid-book spikes lost to a busy executor |
| Expiry | < `entry_cutoff_secs` (see config.toml) | Defence-in-depth; normally caught upstream |
| Depth | < `depth_min_pct` (20%) of required | — |
| Paused/Draining | `paused` or `draining` flag set | `/stop` or `/shutdown` in effect |

**Bidding**: `round_to_tick(best_bid + tick, tick)` → smart outbid walls by 1 tick (>4x avg depth). Submit GTC, post_only=true.

**Sizing**: Confidence-weighted allocation (see Section 5).

**Provisional ID lifecycle** (live mode only):

```
evaluate() sets:      leg1_state = Posted("sim-leg1-{ts}")    ← provisional
Executor places:      POST /order → CLOB returns hex hash
OrderPosted feedback: leg1_state = Posted("0x13828d75...")     ← real
```

Three things can happen to the provisional ID before `OrderPosted` arrives:
1. **SpikeFailed**: Set `cancel_leg1_on_feedback` flag. When `OrderPosted` arrives later, immediately return `CancelLeg1` with the real ID instead of resurrecting state.
2. **Staleness timeout**: Same as SpikeFailed — `check_leg1_staleness()` skips provisional IDs, so this can't fire until the real ID is set. But if the engine code path reaches it while provisional, the flag mechanism applies.
3. **OrderFailed**: Placement failed at the CLOB — clear the flag (nothing to cancel), state already `None`.

### Leg 1 Cancel Edge Cases

Every Leg 1 cancel path (SpikeFailed, staleness, /stop, /shutdown, /set) follows the **save-then-clear** pattern:

1. If the order has a real CLOB ID: save `(order_id, price, size)` in `cancelled_leg1_info`, send `CancelLeg1` to executor, set `leg1_state = None`
2. If the order has a provisional ID: set `cancel_leg1_on_feedback = true`, set `leg1_state = None` (cancel deferred until real ID arrives)
3. Executor sends cancel to CLOB, receives response, sends `CancelResult` feedback
4. Engine `on_cancel_result()`:
   - `was_cancelled = true`: order was actually on the book and is now gone. Clear `cancelled_leg1_info`. Slot freed.
   - `was_cancelled = false`: order filled before the cancel reached CLOB. Restore `leg1_state = Posted` from saved info. User WS MATCHED event will arrive and transition to `Filled`. Leg 2 proceeds normally.

**Why not just ignore the CancelResult?** Without restore, the MATCHED event arrives to `leg1_state = None` → unmatched → the fill is silently lost. The bot has an untracked position with no hedge.

### Leg 1 Staleness Timeout

If a posted Leg 1 order is not filled within `leg1_timeout_ms` (default 5000ms) of actual book resting time, the engine cancels it and frees the slot for the next spike.

- **Timer start**: Begins when `on_order_posted()` sets the real CLOB ID (resets `timestamp_ms`). The ~1.2s CLOB round-trip does NOT count.
- **Provisional skip**: `check_leg1_staleness()` returns `None` for `"sim-..."` IDs — the order hasn't reached the book yet.
- **Save-then-clear**: Order info saved in `cancelled_leg1_info` before clearing state.
- **Sim mode**: `advance_simulation()` handles staleness directly (no CLOB round-trip, provisional timing is correct).

### Leg 2: Hedge

Triggered when Leg 1 fills. In live mode, fills arrive via User WS `"order"` events with MATCHED status. In sim mode, `advance_simulation()` transitions `Posted → Filled` internally (only after `SpikeConfirmed` clears the speculative fill gate).

**Target price**: `round_to_tick(1.0 - target_profit - leg1_price, tick)`

**Erosion cascade** (all post-only until emergency, cancel-replace on each step):

**Step sizes** — front-loaded using triangle weights `[5, 4, 3, 2, 1]` (sum=15). Early steps give up more margin (higher chance of fill at a good price); later steps give up less:

| Step | Weight | % of margin | HIGH (4%) | MED (3%) | LOW (1%) |
|------|--------|-------------|-----------|----------|----------|
| 1 | 5/15 | 33.3% | 1.33% | 1.00% | 0.33% |
| 2 | 4/15 | 26.7% | 1.07% | 0.80% | 0.27% |
| 3 | 3/15 | 20.0% | 0.80% | 0.60% | 0.20% |
| 4 | 2/15 | 13.3% | 0.53% | 0.40% | 0.13% |
| 5 | 1/15 | 6.7% | 0.27% | 0.20% | 0.07% |

**Intervals** — exponential decay: `interval_i = base × decay^i`. Early steps wait longer (market has time to fill); later steps fire faster (urgency):

```
Config: erosion_base_interval_ms = 3000, erosion_interval_decay = 0.5
Step 0: 3000ms → Step 1: 1500ms → Step 2: 750ms → Step 3: 375ms → Step 4: 200ms
Total: ~5.8s to break-even
```

**Cancel-replace with confirmation** (live mode): Each erosion step generates a new signal. The executor cancels the previous resting order and checks the CLOB response:
- **Confirmed**: Post replacement at the eroded price.
- **Not confirmed**: The old order may have filled. Skip replacement, send `CancelResult`. Engine restores old order state, User WS MATCHED arrives → trade complete.
- **Network error**: Post replacement anyway (best effort). User WS is final authority.

Steps are capped at `MAX_EROSION_STEPS` (5). After step 5, the cascade is exhausted (profit target = 0) and auto-escalates to a `BreakEvenBreach` emergency.

**Skip guard**: Before reposting, the evaluator checks if the current resting Leg 2 order is already at a price equal to or better than the new erosion target. If so, the cancel-and-repost is skipped — preserving a favorable position. This prevents erosion from overwriting a good price with a worse one.

### Emergency Triggers (price-improvement chase with hard deadline)

Four independent exit paths. All use a **price-improvement chase with hard deadline** strategy: the engine posts an aggressive post-only limit at `best_ask - 1 tick` (zero fee) and preserves FIFO queue priority. Only cancels and reposts when the Polymarket book offers a strictly better price. After `emergency_deadline_ms` (default 2500ms) from the first emergency post without a fill, the engine escalates to a FOK taker at `best_ask`.

| Trigger | Condition | Timing |
|---------|-----------|--------|
| **Adverse movement** | Binance reversal > `adverse_threshold` (0.1%) from Binance price at Leg 1 fill | **Immediate** — zero grace period |
| **Break-even breach** | Pair cost (leg1 + opposing ask) > $1.00 | **After first erosion step** (~3s) |
| **Erosion exhausted** | All 5 erosion steps applied without fill | **After step 5** (~5.8s). Auto-escalates as `BreakEvenBreach` |
| **Market expiry** | `MarketRotation` while Leg 1 Filled, Leg 2 incomplete | **At rotation** — last-resort FOK before state reset |

**Price-improvement chase flow**: The evaluator tracks `emergency_first_post_ms` (deadline clock start) and `emergency_posted_price` (current resting price). On each Polymarket book update:
1. `emergency_deadline_ms` elapsed? → FOK at `best_ask` (`sim_was_taker=true`)
2. `best_ask - 1 tick` better than posted price? → cancel and repost (price-chase)
3. Neither? → hold current order, preserve FIFO queue priority

Binance ticks are skipped during emergency mode (`hedge_book_changed` gate) — only Polymarket book changes matter.

**Live executor emergency cancel**: Same fire-and-confirm pattern as erosion. If the cancel is not confirmed (order filled mid-cancel), the executor skips the replacement and sends `CancelResult`. Engine restores state, User WS MATCHED confirms the fill → trade complete.

### Favorable Taker (Sim + Live)

When the opposing ask drops strictly below the posted Leg 2 bid, a post-only order would be rejected by the CLOB. The bot market-takes at the ask price. Taker fee is acceptable insurance vs an open position.

- **Sim mode**: `advance_simulation()` detects `ask < posted_price` on book update. Fills at ask with `ExitReason::FavorableTaker`.
- **Live mode**: `handle_leg2_erosion()` receives `Rejected` → `attempt_favorable_exit()`: aggressive post-only at `best_ask - 1 tick` first, FOK fallback if rejected.
- **Tracking**: `favorable_taker_fills` counter. `[FAVORABLE TAKER]` / `[FAVORABLE POST-ONLY]` / `[FAVORABLE FOK FALLBACK]` tags in Telegram.

### CLOB Auto-Cancel (Heartbeat Failure)

If the CLOB cancels all orders (heartbeat failure, admin action), User WS sends `"order"` events with status `CANCELED`. The engine handles these:
- **Leg 1 CANCELED**: Reset `leg1_state = None`, clear `cancelled_leg1_info`. Slot freed for next spike.
- **Leg 2 CANCELED**: Reset `leg2_state = None`, clear `prev_leg2_order`. Erosion continues — `evaluate_leg2()` will generate a new signal on the next iteration.

### Unmatched Event Buffer (`pending_fills`)

When a `TradeStatusUpdate` arrives but `order_id` doesn't match either `leg1_state` or `leg2_state`, the event is buffered in `pending_fills` (VecDeque, capacity 8). This catches two race conditions:

1. **FOK fills before OrderPosted**: User WS MATCHED arrives before the executor's `OrderPosted` feedback updates the provisional ID. The event is buffered, then replayed when `on_order_posted()` sets the real ID.

2. **Cancel-then-fill race**: Engine clears state for a cancel. MATCHED event arrives for the now-cleared order ID. Event buffered. `CancelResult { was_cancelled: false }` restores the state. `replay_pending_fills()` finds the match → `Filled`.

Buffer is cleared on `on_trade_complete()` and `MarketRotation`.

### Trade Completion

Both legs `Filled` (detected in the main engine loop) → `record_live_trade()` writes to QuestDB → `on_trade_complete()` resets all state:
- `leg1_state`, `leg2_state` → `None`
- `erosion` → `None`
- `cancelled_leg1_info`, `prev_leg2_order` → `None`
- `pending_fills` → cleared
- `cancel_leg1_on_feedback` → `false`
- `cumulative_used` persists (capital cap per market window, reset on rotation)

---

## 5. Confidence & Allocation

**Scoring** (`engine/confidence.rs`):
```
confidence = 0.4 * min(spike_magnitude / ATR, 1.0)   [spike quality vs. recent volatility]
           + 0.2 * min(poly_book_depth / avg_book_depth, 1.0)
           + 0.2 * (time_remaining / 300.0)

Max score: 0.8 (sustain removed — all confirmed spikes already passed the gate,
               so it contributed a constant offset with no discriminative value)
```

**Allocation**: `alloc = max(round(max_alloc_per_trade × tier_pct), $1)`
| Confidence | Tier | Profit Target | Default tier_pct | Example ($10 max) |
|------------|------|---------------|-------------------|-------------------|
| ≥ 0.5 | HIGH | 4% | 100% | $10 |
| ≥ 0.35 | MED | 3% | 50% | $5 |
| < 0.35 | LOW | 1% | 25% | $3 |

Minimum allocation is $1 regardless of tier. `max_alloc_per_trade` is the sole capital control; the wallet balance is the real constraint in live trading.

---

## 6. Market Rotation

- **Discovery**: Gamma API `GET /events?tag_id=102892&closed=false&order=endDate&ascending=true&limit=100` every 10 min
  - Tag 102892 = "5M". Filter by slug prefix `btc-updown-5m-`
  - `clobTokenIds` is a JSON-encoded string (index 0 = YES, index 1 = NO)
- **Anticipatory pre-warming** (T-180s = 3 min before current market expires):
  - `discover_market_after(current_end_ms)` queries Gamma for markets ending **after** the current one, skipping the still-active Market A to find Market B
  - Pre-fetches YES and NO order books for Market B via REST
  - Caches `MarketInfo` + books in memory, ready for instant switch
- **Instant switch on expiry**: When `remaining_ms == 0`, emits the pre-warmed `MarketRotation` + book events immediately (zero gap). Market WS resubscribes to new token IDs in parallel
- **Fallback**: If pre-warming failed (Market B not yet on Gamma, REST error, etc.), falls back to immediate Gamma poll within 5s of expiry
- **`MarketRotation`** uses blocking `send()` (not `try_send()`) to guarantee delivery. Book events use `try_send()` (expendable — WS will provide updates)
- **Rotation emergency**: If Leg 1 is Filled but Leg 2 is incomplete when rotation arrives, the engine builds an emergency FOK signal at the opposing ask BEFORE resetting state. The main loop sends this signal to the executor before `ExecutorCommand::MarketRotation`, so the position is hedged (or best-effort attempted) instead of force-closed at full loss. Exit reason: `ExitReason::MarketExpiry`
- **Cutoff window**: After `entry_cutoff_secs` (`entry_cutoff_secs` before expiry), no new Leg 1 entries are allowed (spikes dropped, evaluate() blocked). However, existing open positions continue their Leg 2 erosion cascade and emergency exit paths unimpeded until rotation

```
Timeline:
  T-180s  Cutoff window — no new Leg 1 entries
  T-180s  Pre-warm: discover Market B, fetch books
  T-0     Instant switch: emit pre-warmed MarketRotation + books
          Market WS resubscribes to new tokens
```

### Resolution

- **UMA Optimistic Oracle**: Proposer submits outcome → 2-hour challenge period → resolved
- Capital locked between expiry and resolution (~2h minimum)
- Redeem via `redeemPositions()` on CTF contract. EOA pays POL gas (capped at `MAX_GAS_PRICE` 100 gwei)

---

## 7. Risk Controls

| Risk | Mitigation |
|------|------------|
| **Legging** | Three independent exit paths, all using **price-improvement chase with hard deadline** (`emergency_deadline_ms`=2500ms): (1) Adverse movement — immediate post-only on Binance reversal >0.1%, price-chase on book improvement, FOK at deadline; (2) Break-even breach — after first erosion step, post-only at `best_ask - 1 tick`, price-chase then FOK; (3) Favorable taker — when opposing ask drops below posted bid, post-only then FOK if rejected. FIFO queue priority preserved (no blind reposts). Triangle-weighted erosion cascade (front-loaded steps, accelerating pace, ~5.8s to break-even) |
| **False positive spikes** | Speculative posting with cancel-on-failure: post Leg 1 immediately on ATR+magnitude, cancel if sustain/momentum fails (~300ms). Post-only = zero cost on cancel. Sustain filter (300ms hold above 2×ATR) + momentum ratio (≥65% of peak) + magnitude gate (1 bp). Post-fill reversals caught by adverse_threshold |
| **Signal flooding** | Self-gating on both success and failure. One trade at a time |
| **Taker fees** | Both legs post-only ($0 fee). Emergency exits try aggressive post-only first (zero fee); FOK taker only as fallback when post-only would cross spread. Fee: `C × 0.25 × (p×(1-p))²`, max 1.56% at p=0.50 |
| **Competing bots** | Smart outbid walls by 1 tick (capped at break-even). Post-only = unfilled orders cost nothing |
| **Stale data** | Discard events where `now_ms - timestamp > 500ms` |
| **Heartbeat failure** (live) | Dedicated async task, 5s interval. 2 consecutive failures → reset executor state + Telegram alert |
| **Matching engine restart** | Monday 20:00 ET. Pre-cancel at 19:55 ET. HTTP 425 → exponential backoff |
| **Drawdown** (live) | WARN 3% → PAUSE 5% (30m cooldown) → HALT 8% (manual restart) |
| **Operator latency** (live) | Kill switch if MATCHED→CONFIRMED P99 > 1s |
| **Telegram flooding** | 5s rate limiter (AtomicU64). `fire_critical()` bypasses for trade completions |

---

## 8. Configuration

### `config.toml` — Tuning Parameters

```toml
[spike_detection]      # 5 params
multiplier, atr_alpha, sustain_ms,
min_magnitude_pct, momentum_ratio_min
# NOTE: no spikes emitted for first ~0.5s (10 ticks at 50ms) while ATR warms up

[entry_guards]         # 6 params
max_spread, depth_min_pct, entry_cutoff_secs, stale_book_ms, max_price_skew,
leg1_timeout_ms

[capital]              # 4 params
max_alloc_per_trade, high_alloc_pct, med_alloc_pct, low_alloc_pct

[confidence]           # 5 params
high_threshold, med_threshold,
high_target_pct, med_target_pct, low_target_pct

[risk]                 # 5 params
adverse_threshold,
erosion_base_interval_ms, erosion_interval_decay,
depth_wall_multiplier,
emergency_deadline_ms
```

All have `#[serde(default)]` with production defaults. `Config::test_defaults()` uses `BotConfig::default()` (same defaults as config.toml).

### `.env` — Secrets & Infrastructure

| Variable | Required | Description |
|----------|----------|-------------|
| `MODE` | Yes | `simulation` or `live` |
| `PRIVATE_KEY` | Live only | Hex private key — passed to SDK via `build_signer()` for EIP-712 signing |
| `POLYMARKET_API_KEY/SECRET/PASSPHRASE` | Live only | L2 HMAC auth credentials |
| `BINANCE_ED25519_API_KEY` | Yes | Binance Ed25519 API key for SBE binary market data streams |
| `TELEGRAM_BOT_TOKEN/CHAT_ID` | Sim only | Telegram reporting |
| `TELEGRAM_ALLOWED_USER_ID` | No | Enable Telegram bot control (get from `@userinfobot`) |
| `QUESTDB_URL` | No (default localhost) | Cold storage (analytics only) |
| `BINANCE_SBE_WS_URL` | No (default stream-sbe.binance.com) | Binance SBE WS endpoint |

---

## 9. Storage

All execution state lives in-memory (no database on the hot path). QuestDB is used exclusively for write-only analytics — fire-and-forget, non-blocking, never read during trading.

### QuestDB (Cold Analytics)
5 ILP tables, partitioned by DAY, 7-day rolling retention (hourly pruning via Postgres wire):

| Table | Key Columns | Write Source |
|-------|-------------|--------------|
| `binance_ticks` | symbol, bid, ask, mid | Engine loop (batch flush every 1000 rows) |
| `poly_book_snapshots` | token_id, best_bid/ask, depth, spread | Engine loop (every 5s) |
| `trade_signals` | market_id, direction, action, confidence, spike_magnitude | SimulationExecutor |
| `executed_trades` | market_id, leg1/2 price+size, pair_cost, profit | LiveExecutor |
| `simulated_trades` | Full trade record: tier, erosion_steps, taker_fee, confidence, exit_reason, spike_magnitude, etc. | SimulationExecutor |

`binance_ticks` and `poly_book_snapshots` are recorded continuously in the engine loop for backtesting and parameter tuning. Trade tables are written by the executor on fill events.

### Tuning Analytics

`simulated_trades` carries full context for outcome correlation:
- `exit_reason` (symbol): `NormalErosion`, `AdverseMovement`, `BreakEvenBreach`, `MarketExpiry`, `FavorableTaker` — loss attribution
- `spike_magnitude` (f64): spike quality vs. outcome correlation
- `favorable_taker`, `emergency_maker` (bool): exit type flags

See `queries.sql` for 15 analytics queries (7 operational + 8 tuning). Tuning queries map loss causes directly to config parameters:

| Query | Answers | Tune |
|-------|---------|------|
| Loss attribution by exit reason | What's the #1 cause of losses? | `adverse_threshold`, `entry_cutoff_secs` |
| Spike quality vs outcome | Am I trading weak spikes? | `min_magnitude_pct`, `multiplier` |
| Confidence threshold calibration | Where should HIGH/MED/LOW cutoffs be? | `high_threshold`, `med_threshold` |
| Erosion step analysis | Are trades filling too late? | `*_target_pct`, `erosion_base_interval_ms` |
| Taker fee impact | How much are emergency fees eating profits? | `emergency_deadline_ms` |

---

## 10. Telegram Reporting & Control

### Reporting (Outbound)

Four tiers via `hyper` + `tokio-rustls` (fire-and-forget, no teloxide):

1. **Opportunity Alert**: Per signal — spike info, confidence, allocation, Leg 1 entry, Leg 2 target
2. **Trade Completed**: Per trade — "Buy YES"/"Buy NO" labels, pair cost, profit (USDC), erosion steps
3. **Market Summary**: Per 5-min expiry — fill rate, trades, PnL
4. **Session Summary**: Hourly + shutdown — aggregate stats, win rate, balance

Rate limited at 5s intervals. Critical messages (trade completions) bypass the limiter.

Opportunity alerts and trade completions are gated by `NotifyFlags::trades_enabled`; market summaries by `NotifyFlags::summary_enabled`. Both default to `true`, toggled via `/trades` and `/summary` commands.

### Bot Control (Inbound)

Bidirectional Telegram control via `getUpdates` long-polling (30s timeout, 45s outer timeout to detect dropped connections). Enabled when `TELEGRAM_ALLOWED_USER_ID` is set. Runs as a separate tokio task — zero overhead on the hot path.

| Command | Action |
|---------|--------|
| `/trades on\|off` | Toggle opportunity + trade-completed notifications |
| `/summary on\|off` | Toggle market summary notifications |
| `/diag on\|off` | Toggle 60s diagnostic forwarding to Telegram |
| `/stop` | Pause trading — block new entries, keep connections alive for `/balance`, `/status`, etc. |
| `/resume` | Resume trading after `/stop` pause |
| `/shutdown` | Graceful shutdown — drain open position, exit (exit 0, no systemd restart) |
| `/set <param> <value>` | Validate + write config.toml → drain → restart (exit 42) |
| `/config [section]` | Show all params, or just one section (e.g. `/config risk`) |
| `/status` | Uptime, mode, current market, leg states, trade counters, toggle states. Shows `[PAUSED]` when stopped |
| `/balance` | Wallet USDC.e + POL balance on Polygon |
| `/polybalance` | Polymarket positions and total value |
| `/redeem` | Redeem resolved positions to USDC.e |
| `/help` | List commands with usage |

**Security**: Every message verified against `TELEGRAM_ALLOWED_USER_ID` + `TELEGRAM_CHAT_ID`. 2s rate limit between commands. `/set` uses a strict allowlist of 27 params with min/max ranges. No shell execution.

**Notification toggles**: `AtomicBool` flags (`Relaxed` ordering) shared between the command listener and `TelegramReporter`. One CPU instruction per check — zero hot-path impact.

### Pause vs Shutdown

**`/stop` (pause)**: Sets `engine.paused = true`, blocking new Leg 1 entries. Cancels any unfilled Leg 1 (with provisional ID race safety via `cancel_leg1_on_feedback`). Existing Leg 2 continues through erosion/emergency. The bot stays alive — `/balance`, `/status`, `/redeem`, `/polybalance` all remain functional. Use `/resume` to unpause.

**`/shutdown` (full exit)**: Triggers drain mode then exits. The bot never abandons an open position:

| Current State | Behavior |
|---|---|
| No open position | Immediate exit |
| Leg 1 posted, unfilled | Cancel Leg 1 → immediate exit |
| Leg 1 filled, Leg 2 in progress | Block new entries, let Leg 2 continue through erosion/emergency → exit after resolution |

Drain progress is published via `tokio::sync::watch<DrainStatus>` (Idle → Draining → Complete). The command listener watches the channel and sends real-time Telegram updates:

```
User: /shutdown
Bot:  "Shutting down..."
Bot:  "Drain mode activated (shutdown) — Leg 2 in progress, waiting for position to close"
Bot:  "Position closed. Bot stopped."
```

**Exit codes**: `/shutdown` exits with code 0 (success — systemd does not restart). `/set` exits with code 42 (on-failure — systemd restarts in 5s with new config).

### Control Architecture (`src/control/`)

```
src/control/
├── mod.rs              # Module declarations
├── listener.rs         # TelegramCommandListener: getUpdates polling, auth, dispatch
├── handlers.rs         # Command handlers (pure logic, returns reply strings)
├── config_editor.rs    # TOML read/write, param allowlist with min/max ranges
└── types.rs            # NotifyFlags, BotStatus, DrainStatus
```

**Data flow**: Commands inject `IngestorEvent` variants into the ingestor channel: `PauseTrading` (from `/stop`), `ResumeTrading` (from `/resume`), `Shutdown` (from `/shutdown`), or `DrainAndRestart` (from `/set`). The engine handles these in its main loop — pause sets `paused = true`, shutdown sets `draining = true` and publishes `DrainStatus` updates.

---

## 11. Polymarket API Reference

### Endpoints Used

| Endpoint | Purpose |
|----------|---------|
| `wss://ws-subscriptions-clob.polymarket.com/ws/market` | Public book/price/tick events |
| `wss://ws-subscriptions-clob.polymarket.com/ws/user` | Authenticated fill tracking (live). Two event types: `"order"` (hex hash — forwarded to engine) and `"trade"` (UUID — ignored for matching) |
| `https://clob.polymarket.com/order` | Order submission (via SDK — EIP-712 signing, fee rate, tick size) |
| `https://clob.polymarket.com/heartbeat` | Keep-alive (5s, live) |
| `https://gamma-api.polymarket.com/events` | Market discovery |

### Contracts (Polygon, Chain ID 137)

| Contract | Address |
|----------|---------|
| CTF Exchange | `0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E` |
| Neg Risk CTF Exchange | `0xC5d563A36AE78145C45a50134d48A1215220f80a` |
| Conditional Tokens (CTF) | `0x4D97DCd97eC945f40cF65F87097ACe5EA0476045` |
| USDC.e | `0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174` |

**Auth**: EOA (signature type 0). Signer = maker = funder. L2 credentials (`POLYMARKET_API_KEY/SECRET/PASSPHRASE`) passed to SDK `Credentials` struct at init; SDK handles all authenticated HTTP headers internally.

### SDK Integration (`polymarket-client-sdk` v0.4)

Order placement, signing, and cancellation use the official Rust SDK (v0.4, `clob` feature). The SDK replaced manual EIP-712 signing, hardcoded fee rates, and broken cancel endpoints.

**Gateway struct** (`PolymarketGateway` in `rest.rs`):
```
PolymarketGateway {
    signer: Option<PrivateKeySigner>,                     // Built via build_signer(), chain_id=137
    sdk_client: Option<SdkClient<Authenticated<Normal>>>, // Authenticated CLOB client (None in sim)
}
```

**Initialization** (async — `PolymarketGateway::new().await` in `main.rs`):
1. `build_signer()` parses hex private key → `PrivateKeySigner` with `chain_id=137` (Polygon)
2. `init_sdk_client()` creates `SdkClient`, passes pre-existing L2 credentials (`Credentials::new(uuid, secret, passphrase)`), calls `.authenticate().await` — validates without `create_or_derive_api_key` network call
3. Logs signer address at `info!` level — user should verify it matches their Polymarket wallet. On failure, logs address + hint to check `PRIVATE_KEY` matches API key wallet
4. If either step fails → read-only mode (`sdk_client = None`, order placement unavailable)

**Order flow** (`place_order`):
```
OrderRequest
  → sdk.limit_order().token_id().side().price().size().order_type().post_only().build().await
    (SDK auto-fetches tick_size, fee_rate, neg_risk per token — DashMap cache, one CLOB call each then cached)
  → debug! log: signer_address, token_id, side, price, size (diagnostic for signature issues)
  → sdk.sign(signer, signable).await
    (EIP-712 typed data — auto-detects neg_risk for correct exchange contract domain separator)
  → sdk.post_order(signed).await
    (L2 HMAC auth headers constructed internally, POST /order)
    (error includes token/price/size for diagnosing 400 rejections)
  → PostOrderResponse mapped to OrderResponse { order_id, status, timestamp_ms }
```

**Cancel flow**:
- `cancel_order(id) → Result<bool>` → `sdk.cancel_order(id).await` → `DELETE /order` → returns `true` if order was in `canceled` list, `false` if it was not (may have filled before cancel reached CLOB)
- `cancel_all()` → `sdk.cancel_all_orders().await` → `DELETE /cancel-all`

**SDK cache pre-warm (hard gate)**: On every `MarketRotation`, `LiveExecutor` calls `sdk.tick_size(token_id)`, `sdk.neg_risk(token_id)`, and `sdk.fee_rate_bps(token_id)` for both YES and NO tokens, populating the SDK's internal `DashMap` caches with real CLOB values. A `caches_warm: bool` field gates all order placement — if any pre-warm fetch fails, ALL signals are rejected with `OrderFailed` feedback until the next rotation succeeds. This eliminates the ~150ms first-order latency from auto-fetch while guaranteeing correctness (no hardcoded values that could cause "invalid signature" or 400 errors).

`signing.rs` contains only `build_signer()` — hex private key parsing to `PrivateKeySigner`.

---

## 12. Deployment

### Runtime Optimizations

- **jemalloc**: Global allocator (`tikv-jemallocator`) eliminates glibc malloc latency spikes. Conditional on `cfg(not(target_env = "msvc"))` — active on both macOS (local dev) and Linux (production)
- **Manual tokio runtime**: 2 worker threads pinned to cores 1-2 via `on_thread_start` + `core_affinity`. Engine loop runs on `tokio::task::spawn_blocking` (off worker pool) to prevent thread starvation — the live executor blocks one worker with synchronous `crossbeam recv()`, so the engine must not also block a worker. Core 0 reserved for ingestor (dedicated OS thread)
- **Release profile**: `opt-level=3`, `lto="fat"`, `codegen-units=1`, `strip=true`. Production builds add `RUSTFLAGS="-C target-cpu=native"` for AVX-512 on c7i

### Stage 1: Local Simulation
```bash
docker-compose up -d
cp .env.example .env  # Set MODE=simulation, Telegram creds
cargo build && cargo run
```
Verify: WS connections, spike detection, simulated trades, Telegram alerts.

### Stage 2: AWS Production

**Infrastructure**: `c7i.xlarge` in `eu-west-2` (London) — co-located with Polymarket CLOB servers. Amazon Linux 2023. QuestDB on same instance (Docker, pinned to core 3).

**Core allocation**: core 0 = ingestor, cores 1-2 = engine+executor (tokio), core 3 = QuestDB + system processes

```bash
# One-time setup (installs Rust, Docker, QuestDB, kernel tuning, systemd service)
sudo bash deploy/setup.sh

# Build with native CPU optimizations
RUSTFLAGS="-C target-cpu=native" cargo build --release

# Deploy
cp target/release/facaibot /opt/facaibot/
cp config.toml /opt/facaibot/
# Create /opt/facaibot/.env with production secrets (chmod 600)
sudo systemctl enable --now facaibot
```

**OS-level tuning** (applied by `setup.sh`):
- TCP low-latency mode, increased socket buffers, TCP fast open
- ENA NIC: ring buffers 4096, interrupt coalescing disabled
- Transparent Huge Pages disabled (prevents compaction latency spikes)
- IRQ affinity: network interrupts moved to cores 2-3
- CPU frequency locked to max (`performance` governor)
- Clock sync: Amazon Time Sync Service (sub-microsecond via Nitro hypervisor)

**Optional kernel boot parameters** (maximum latency reduction):
```
isolcpus=0,1 nohz_full=0,1 rcu_nocbs=0,1 intel_pstate=disable processor.max_cstate=1 idle=poll
```

**systemd restart policy**: `Restart=on-failure` with `RestartSec=5s`. Exit 0 (`/shutdown`) = success → no restart. Exit 42 (`/set` config change) = failure → restart in 5s with new config. Crashes = failure → restart in 5s.

See `README.md` for step-by-step instructions and `deploy/` for all scripts.

### Stage 3: Live Trading
Set `MODE=live`, fill CLOB credentials, fund EOA wallet (USDC.e + POL). Start with reduced allocation (`max_alloc_per_trade=$1`). Approve Exchange contract for spending.

**Go-live gate**: Leg 1 fill rate >25%, win rate >80% over 200+ trades, average net >1.0%, emergency taker <15% of Leg 2 fills.

### Monitoring

- **Logs**: `journalctl -u facaibot -f` (live), `--since "1 hour ago"` (history)
- **Telegram**: trade signals, fills, emergencies, market summaries (built-in)
- **Health check**: `deploy/healthcheck.sh` via cron (Telegram alert if service down)
- **QuestDB**: `http://<ip>:9000` for analytics dashboard (restrict to your IP)
- **Updates**: `bash deploy/deploy.sh` (git pull, build, restart)

---

## 13. Performance Targets

| Metric | Target |
|--------|--------|
| Spike-to-CLOB (internal, eu-west-2) | <50ms |
| Leg 1 fill rate | 30-50% of signals |
| Win rate (hedged trades) | 85-95% |
| Avg net profit per trade | >1.0% |
| Emergency taker fills | <15% of Leg 2 |
| System uptime | >99.5% |
