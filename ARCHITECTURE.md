# FaCaiBot — Architecture & Operations

## 1. Core Concept

FaCaiBot exploits the repricing lag on Polymarket's CLOB. A composite buildup detector monitors 6 real-time metrics across Binance futures and spot to predict imminent BTC/USDT spikes. The bot enters before the spike materializes, gaining queue priority on the CLOB, then hedges with the opposite side:

```
Buildup detected → Post maker at best ask (Leg 1, post-only resting order)
                 → Spike arrives, CLOB reprices, maker fills
                 → Buy opposite shares (Leg 2, post-only maker, $0 fee)
                 → Paired position: e.g. $0.48 + $0.495 = $0.975 → pays $1.00 → 2.5% profit
```

Leg 1 uses a single post-only maker order at the best ask price. The order rests on the book and fills when liquidity arrives (spike repricing). Unfilled makers are cancelled if the composite flow score drops below `cancel_threshold` or `cancel_window_ms` elapses (flow-based sustain). Leg 2 targets `post_only=true` (maker, zero fee). Both legs earn an estimated maker rebate of 20% of the fee-equivalent — computed per fill via `compute_maker_rebate()` and included in net PnL calculations and Telegram messages. Taker fees (`C * 0.25 * (p*(1-p))^2`, max 1.56% at p=0.50) apply only to Leg 2 FOK emergency exits (Phase 1 breach, break-even breach, Phase 2 timeout, market expiry, whipsaw reversal, flow collapse) and favorable taker fills.

**Why it works**: Binance is the largest liquidity venue. >95% correlation with Chainlink for moves >1%. Predictive entry via composite buildup detection places the maker order before the spike, gaining FIFO queue priority at zero cost (unfilled makers are free to cancel). Maker-maker trades eliminate taker fees on both legs.

**Modes** (`MODE` env var):
- **`live`**: Submits real orders via polymarket-client-sdk (EIP-712 signing handled internally), Leg 1 fills detected via User WS, Leg 2 fills detected via User WS
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

Binance Spot SBE WS ─┐
  @depth20 (50ms)     │
  @bestBidAsk         │
  @trade              │
Binance Futures WS ───┤
  @aggTrade           │
  @bookTicker         ├──► IngestorEvent ──► MarketState update
  @forceOrder         │         ▲              BuildupDetector (6 metrics)
Polymarket WS ────────┤         │
  Market channel      │         │              Signal evaluation
  User channel        │    Shutdown /          2-phase hedge
Gamma API ────────────┘    DrainAndRestart     advance_simulation() (sim)
Heartbeat (5s) ───────┘         │                    │
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

```
on_event()           → update book/price state
                       Feed BuildupDetector on each spot/futures event
                       check_buildup_entry(): composite > entry_threshold → buildup_detected
                       Flow-based Leg 1 sustain: cancel unfilled maker if composite drops
advance_simulation() → simulate fills: Posted→Filled→Complete→Reset
                       emits confirmed fill signals (sim_confirmed_fill=true)
evaluate()           → Leg 1 signal (self-gates after emitting)
                       emits detection signal (sim_confirmed_fill=false)
                       records rejection reason if buildup blocked
evaluate_leg2()      → Leg 2 hedge/emergency signal (self-gates after emitting)
                       flow-based graduated response (reversal/collapse/weakening)
                       phase transitions sent to executor (sim_confirmed_fill=false)
                       emergency signals handled internally by advance_simulation()
```

### Live Engine Loop

In live mode, the CLOB is the fill authority. Fills arrive via the authenticated User WebSocket as `TradeStatusUpdate` events. The engine matches these against posted order IDs to transition `OrderState::Posted → Filled`.

Three feedback types flow from executor → engine via the `ExecutorFeedback` channel:

| Feedback | When | Engine action |
|----------|------|---------------|
| `OrderPosted { order_id, price, size, is_leg2, fill_method, already_filled }` | CLOB accepted the order | Overwrite provisional `"sim-..."` ID with real hex hash. For Leg 2: apply `fill_method` metadata to `LiveTradeMeta` (favorable exit tags). If `already_filled` (FOK returned `Filled` synchronously), transition directly to `OrderState::Filled` and trigger trade completion — do NOT wait for User WS MATCHED. For Leg 1 maker: if `already_filled` (rare for post-only), transition directly to `Filled` and init Leg 2. Replay `pending_fills` buffer |
| `OrderFailed { is_leg2 }` | CLOB rejected or network error (non-emergency only — emergency FOKs retry internally, never send this) | Reset leg state to `None` |
| `CancelResult { order_id, was_cancelled, is_leg2 }` | Executor received CLOB cancel response | If confirmed: clear saved order info. If NOT confirmed: restore `OrderState::Posted` from saved info, replay `pending_fills` buffer. For Leg 1 cancel-not-confirmed: order may have filled before cancel reached CLOB — User WS MATCHED event will resolve. For Leg 2 cancel-not-confirmed: restore Posted state, reset `LiveTradeMeta` and clear hedge emergency state |

**User WS event routing**: The Polymarket User WS sends two event types: `"order"` events (hex order hash, e.g. `0x13828d75...`) and `"trade"` events (UUID trade ID, e.g. `89f124e7-...`). Only `"order"` events are forwarded to the engine as `TradeStatusUpdate` — their `id` field matches the hex hash stored from `OrderPosted` feedback. `"trade"` UUIDs never match and are harmlessly ignored. Actionable statuses forwarded: MATCHED, MINED, CONFIRMED, FAILED, RETRYING, CANCELED. Non-actionable statuses (LIVE) are silently skipped by `parse_trade_status()`.

**Predictive maker entry**: Leg 1 is posted as a post-only maker at best ask when the composite buildup detector exceeds `entry_threshold`. The order rests on the book, gaining FIFO queue priority. If the composite score drops below `cancel_threshold` or `cancel_window_ms` elapses without a fill, a `CancelLeg1Order` is sent. If the order fills (via User WS MATCHED), Leg 2 proceeds normally. Cancel-not-confirmed (order filled before cancel reached CLOB) is handled via state restoration and User WS reconciliation.

```
Engine loop (one iteration per IngestorEvent):
┌─────────────────────────────────────────────────────────────────┐
│ 1. DRAIN FEEDBACK (non-blocking)                                │
│    while feedback_rx.try_recv():                                │
│      OrderPosted  → set real CLOB ID, apply fill_method meta,   │
│                     if already_filled: direct Filled + complete  │
│                     else: replay pending_fills                   │
│      OrderFailed  → reset leg to None                           │
│      CancelResult → restore or clear saved order info           │
│                                                                 │
│ 2. CONTROL EVENTS                                               │
│    Shutdown / DrainAndRestart / PauseTrading / ResumeTrading     │
│    (cancel unfilled Leg 1 maker, set flags)                      │
│                                                                 │
│ 3. ON_EVENT (state update)                                      │
│    Book/price updates, buildup lifecycle, rotation, fills        │
│    Feed BuildupDetector on spot/futures events                   │
│    check_buildup_entry() on each feed                            │
│    Flow-based Leg 1 sustain (cancel unfilled maker)              │
│    TradeStatusUpdate: match order_id against leg1/leg2 state     │
│      → Matched/Mined/Confirmed → transition to Filled           │
│      → Failed → reset to None                                   │
│      → Canceled → reset to None (CLOB auto-cancel)              │
│      → unmatched → buffer in pending_fills (cap 8)              │
│                                                                 │
│ 4. SIGNAL GENERATION                                            │
│    take_pending_leg1_cancel() → drain CancelLeg1Order            │
│    evaluate() → Leg 1 signal (self-gates after emitting)         │
│    evaluate_leg2() → Leg 2 hedge/emergency (save-then-overwrite)  │
│                                                                 │
│ 5. TRADE COMPLETION                                             │
│    Both legs Filled → record to QuestDB → on_trade_complete()    │
│    (clears all state including saved order info + pending_fills)  │
└─────────────────────────────────────────────────────────────────┘
```

**Key differences from sim**:
- `advance_simulation()` never runs — CLOB is fill authority. Leg 1 fills arrive via User WS MATCHED events (maker orders rest on book)
- Emergency signals are gated by `emergency_signal_in_flight` — only one emergency signal in the executor channel at a time (cleared on feedback). Prevents stale signals from cancelling already-filled FOKs
- Trade completion detected in the main loop (both `leg1_state` and `leg2_state` are `Filled`), OR immediately when `already_filled` FOK feedback transitions Leg 2 directly to `Filled`
- Feedback is drained before `on_event()` — CLOB round-trip (~1.2s) completes before User WS notification (~1.5-2s), so order IDs are typically set before fill events arrive
- Cancel operations are fire-and-confirm — the executor checks the CLOB DELETE response and sends `CancelResult` feedback so the engine can detect orders that filled before the cancel. Applies to both Leg 1 sustain cancels and Leg 2 phase transitions
- `OrderPosted` carries `fill_method` (executor-initiated favorable exit metadata) and `already_filled` (sync FOK fill detection). The engine applies these to `LiveTradeMeta` for Telegram tags and immediate trade completion

### LiveExecutor (`executor/live.rs`)

Handles the full trade lifecycle via the SDK-backed `PolymarketGateway`:

| Signal | Action |
|--------|--------|
| Leg 1 | Maker post-only GTC at `best_ask` → `OrderPosted { already_filled: false }` to engine (order rests on book, fill via User WS). If already filled synchronously (rare for post-only), `already_filled: true`. If rejected (ask crossed bid), `OrderFailed` |
| Leg 1 cancel | `CancelLeg1Order { order_id }` → `cancel_order()` → `CancelResult { was_cancelled, is_leg2: false }` feedback |
| Leg 2 hedge (Phase 1 initial) | Post once at profit target. Cancel block is defensive-only (Phase 1 doesn't repost). If rejected/crosses-book → `attempt_favorable_maker_then_fok()` |
| Leg 2 hedge rejected | Post-only rejected or "crosses book" → `attempt_favorable_maker_then_fok()`: try maker at ask-1tick (poll up to `favorable_maker_timeout_ms`), then FOK fallback. `fill_method=FavorableMaker` or `FavorableTaker` |
| Leg 2 emergency | Cancel ALL resting Leg 2 orders (Phase 1 + Phase 2) → confirmed: `emergency_fok_fallback()`. NOT confirmed: `CancelResult` feedback, skip replacement |
| Leg 2 balance error | "balance"/"allowance" error → set `balance_exhausted` flag, send `BalanceExhausted` feedback + `OrderFailed`. All subsequent Leg 2 commands rejected until rotation |
| Market rotation | `cancel_all()` → reset state → clear `balance_exhausted` → pre-warm SDK caches → pre-warm CLOB connection pool (`sdk.order("0x000...")`) → `caches_warm = true` |

On network error during cancel, the executor cannot determine state — it proceeds with the replacement (cancel counted, no `CancelResult` sent). The User WS is the final authority: if the old order filled, the MATCHED event will arrive and be matched by the engine.

**Stale command prevention**: `leg2_command_pending` flag on the engine prevents new Leg 2 signals while the executor is still processing the previous one. Set on dispatch (live mode only), cleared on any Leg 2 feedback. Prevents the scenario where a favorable exit takes ~3.6s (3 HTTP calls) and the phase transition timer fires a stale command that contaminates the next trade. **Stale feedback guard**: `on_order_posted()` and `on_order_failed()` ignore Leg 2 feedback when `leg1_state` is not `Filled`. Defense in depth against stale executor responses arriving after trade reset.

On placement failure or CLOB rejection, sends `OrderFailed` feedback so the engine resets the leg state to `None`. **Exception**: Emergency FOK orders (`emergency_fok_fallback`) use price escalation — on each liquidity failure (Rejected or non-transient Err), the price is bumped +1 tick and retried, sweeping the book up to a hard cap of `$1.00` (~23 ticks max from any starting price, ~2.3s to sweep). No fixed retry count. Non-transient SDK errors (e.g., "decimal places", "Validation", "balance", "allowance") abort immediately. `clob_safe_fok_size()` is recomputed each iteration (price changes affect the size constraint) to ensure `price × size` has ≤2 decimal places; if it returns zero the sweep aborts with `OrderFailed`. After Leg 1 fills, the position must be hedged; the ~1.2s HTTP round-trip per attempt is the natural rate limiter. **Filled FOK tracking**: When a FOK returns `OrderStatus::Filled`, the executor sets `active_leg2_order_id = None` instead of storing the order ID — this prevents subsequent stale signals from cancelling an already-filled order. **Sync FOK fill detection**: All FOK `OrderPosted` feedback includes `already_filled: resp.status == OrderStatus::Filled`. When the engine receives this, it transitions Leg 2 directly to `OrderState::Filled` and triggers immediate trade completion — bypassing the User WS wait that previously caused the double-fill bug (engine kept evaluating and dispatching additional FOK signals for an already-filled position). **$1 minimum notional**: Before attempting favorable exits, the executor checks `price × size >= $1` (CLOB minimum for marketable orders). Below $1, the attempt is skipped with `OrderFailed` and the hedge phase continues at a different price. **"Crosses book" errors**: If a Leg 2 hedge post-only order fails with a "crosses book" error (SDK returns this as `Err`, not `Ok(Rejected)`) or is rejected, the executor routes to `attempt_favorable_maker_then_fok()` — posts maker at ask-1tick, polls for fill up to `favorable_maker_timeout_ms`, then FOK taker fallback if not filled. Cancel-not-confirmed → `OrderPosted` with `already_filled=false` (User WS decides). **Fill method tagging**: `OrderPosted` includes `fill_method: Option<FillMethod>` — `FavorableMaker` for try-maker succeeded, `FavorableTaker` for FOK favorable exits, `EmergencyTaker` for emergency FOK fills. The engine sets `LiveTradeMeta` flags from this so Telegram shows the correct tag. `EmergencyTaker` sets `leg2_was_taker=true` and `emergency_maker=false`, preventing successful maker fills from being mislabeled as emergency exits when a cancel-not-confirmed race resolves.


---

## 3. Signal Detection

### Primary: Buildup Detection (`engine/buildup/`)

**Predictive composite detector**: The `BuildupDetector` runs inline in the engine, fed by spot and futures events as they arrive. It combines 6 real-time metrics into a single composite score to detect pre-spike buildup conditions before the actual price movement materializes.

**Data sources** (all via Binance WebSocket):
- **Futures `@aggTrade`** (JSON WS) → CVD acceleration
- **Futures `@bookTicker`** (JSON WS) → Basis delta (futures-spot spread)
- **Futures `@forceOrder`** (JSON WS) → Liquidation pressure
- **Spot `@trade`** (SBE WS) → Spot trade flow
- **Spot `@depth20`** (SBE WS, 50ms) → OBI velocity, ATR displacement, spot mid

**6 metrics** (`engine/buildup/metrics.rs`):

| Metric | Source | Signal | Direction |
|--------|--------|--------|-----------|
| **CVD acceleration** | Futures `@aggTrade` | Fast-slow EMA of signed trade qty; positive accel = buying accelerating | Bullish if accel > 0 |
| **Spot trade flow** | Spot `@trade` | Buy vs sell EMA; net flow = buy_ema - sell_ema | Bullish if flow > 0 |
| **OBI velocity** | Spot `@depth20` | Rate of change of order book imbalance (not OBI itself) | Bullish if OBI rising |
| **Basis delta** | Futures `@bookTicker` + spot | Rate of change of futures-spot basis in bps | Bullish if basis rising |
| **Liquidation pressure** | Futures `@forceOrder` | Time-decaying sum of forced liquidation volume | Bullish if short squeezes dominating |
| **ATR displacement** | Spot `@depth20` | Current price displacement in EMA-ATR multiples | Bullish if price rising in ATR terms |

**Evaluation pipeline** (`engine/buildup/detector.rs`):
1. **Normalize**: Each metric → [0, 1] (0 if stale beyond freshness gate)
2. **Direction consensus**: Dominant direction from fresh metrics; veto if >1 disagrees
3. **Causal ordering**: At least 1 leading (CVD, basis — futures-derived) AND 1 confirming (spot flow, OBI — spot-derived) must be fresh and non-zero
4. **Weighted sum**: `composite = sum(weight_i x normalized_i)` (weights must sum to 1.0)
5. **Entry**: `composite > entry_threshold` → emit `BuildupInfo` with all metric snapshots

**Engine integration**: `check_buildup_entry()` is called after every spot/futures event feed. It updates the flow monitoring state (`current_composite_score`, `current_composite_direction`) on every call, and triggers `handle_buildup_confirmed()` when the entry threshold is crossed. The flow state is propagated to `HedgeState` for graduated hedge response during Phase 1.

### Signal Delivery

Entry path:
- **`BuildupConfirmed(BuildupInfo)`** — composite crossed `entry_threshold` → engine sets `buildup_detected = true`, stores `last_buildup`, evaluates entry

Additional event variants for flow monitoring:
- **`BuildupUpdate { composite_score, direction, timestamp_ms }`** — below-threshold score updates for hedge flow monitoring
- **`BuildupDiagnostic { ... }`** — periodic detector diagnostics (60s)

Normal `BinanceTick` events only update `binance_price` — they never trigger entry evaluation.

---

## 4. Trade Lifecycle (Live Mode)

The diagram below shows the complete live-mode trade lifecycle. Every state transition, cancel path, and edge case is covered. Simulation mode is simpler (engine is sole fill authority via `advance_simulation()`; no CLOB round-trips, no cancel races).

```
                    ┌──────────────────────────────────────────────────────────────────┐
                    │                    LIVE TRADE STATE MACHINE                       │
                    └──────────────────────────────────────────────────────────────────┘

     IDLE                                      LEG 1                                          LEG 2
    ═════                          ════════════════════════                        ════════════════════════

             BuildupConfirmed
    leg1=None ─────────────────► evaluate() passes guards
                                  │
                                  │  Engine: leg1_state = Posted("sim-leg1-{ts}")
                                  │          pending_leg1_signal = Some(signal)
                                  │          buildup_detected cleared (self-gate)
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
               Flow cancel   OrderPosted    OrderFailed                     │
               (composite     feedback      feedback                       │
                dropped or     │              │                          │
                timeout)       ▼              ▼                          │
                    │      overwrite ID    reset to None                  │
                    │      with real hex   (slot freed)                   │
                    │      CLOB hash                                     │
                    │          │                                          │
                    │         ┌┴──────────┐                               │
                    │         │  Leg 1    │                               │
                    │         │  POSTED   │                               │
                    │         │ (real ID) │                               │
                    │         └─────┬─────┘                               │
                    │               │                                     │
                    │     ┌─────────┼──────────┐                          │
                    │     │         │          │                          │
                    │   User WS   User WS   Whipsaw                      │
                    │   MATCHED   CANCELED   (opposite                    │
                    │     │         │         buildup)                    │
                    │     │         ▼          │                          │
                    │     │      reset to     CancelLeg1Order             │
                    │     │      None         sent to executor            │
                    ▼     │      (CLOB          │                         │
              ┌───────────┤      killed it)     │                         │
              │ CancelLeg1│                     │                         │
              │ Order     │                     │                         │
              │ sent      │                     │                         │
              ▼           │                     │                         │
         ┌─────────────────────────┐            │                         │
         │    CANCEL RESULT        │◄───────────┘                         │
         │  (from executor)        │                                      │
         │                         │                                      │
         │  was_cancelled = true   │                                      │
         │    → clear state        │                                      │
         │    → slot freed         │                                      │
         │                         │                                      │
         │  was_cancelled = false  │                                      │
         │    → RESTORE leg1_state │                                      │
         │      from saved info    │──── User WS MATCHED ──┐             │
         │    → replay pending_fills                       │             │
         └─────────────────────────┘                       │             │
                                                           │             │
                                                           ▼             │
                                                    ┌────────────┐       │
                                                    │   Leg 1    │       │
                                                    │   FILLED   │       │
                                                    │            │       │
                                                    │ init_leg2()│       │
                                                    │ Telegram   │       │
                                                    └──────┬─────┘       │
                                                           │             │
                                                evaluate_leg2()          │
                                                           │             │
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
              │                          OrderPosted              Phase transition    Emergency trigger
              │                          feedback                 (timer elapsed)      (BE/phase1/timeout)
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
              │  Engine: evaluate_leg2() returns Hedge or Emergency       │
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
              │    │      → Rejected / "crosses book" (post-only would cross): │
              │    │          Hedge: attempt_favorable_maker_then_fok()   │
              │    │            → try maker at ask-1tick, poll, FOK fallback │
              │    │          Emergency: emergency_fok_fallback (retries) │
              │    │      → Network error:                               │
              │    │          Emergency FOK: price escalation (+1 tick)   │
              │    │          Other orders: OrderFailed feedback            │
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

**Predictive maker entry**: On `BuildupConfirmed`, `evaluate()` clears `buildup_detected`, sets `leg1_state = Posted` with a provisional `"sim-leg1-{ts}"` ID, and increments `cumulative_used`. One trade at a time. The evaluator sets `signal.price = best_ask` (maker posts at the ask).

**Pre-entry guards** (abort if any fail):

| Guard | Threshold | Notes |
|-------|-----------|-------|
| No market | `active_condition_id` absent | Rare — awaiting rotation |
| No book | book or bid/ask missing | — |
| No Binance price | `binance_price` absent | — |
| Stale book | book age > `stale_book_ms` (500ms) | — |
| Price skew | YES mid > 0.80 or < 0.20 | Near-certain-resolution, Leg 2 fill collapses |
| Active trade | `leg1_state != None` | `rej_busy` counts valid-book buildups lost to busy executor |
| Expiry | < `entry_cutoff_secs` (see config.toml) | Defence-in-depth; normally caught upstream |
| Repricing model | `expected_pct < min_reprice_pct` (2.0%) | Model output too low |
| OBI alignment | Binance OBI contradicts buildup direction | `ObiMismatch` — rejects if book imbalance strongly against buildup |
| Paused/Draining | `paused` or `draining` flag set | `/stop` or `/shutdown` in effect |

**Pricing**: `signal.price = best_ask`. Executor posts a single post-only GTC maker order at this price.

**Sizing**: `(alloc / ask_price).round_dp(2)`.

**Fee model**: Leg 1 is a maker order — `leg1_fee = -compute_maker_rebate(ask_price, entry_size)` (negative fee = rebate). No taker fee on Leg 1.

**Execution flow** (live mode):

```
evaluate() sets:          leg1_state = Posted("sim-leg1-{ts}")
Executor: POST            post_only GTC at best_ask (single HTTP request)
                          → OrderPosted { already_filled: false } (order rests on book)
                          → User WS MATCHED when filled
                          → OrderPosted { already_filled: true } if filled synchronously (rare)
                          → OrderFailed if post-only rejected (ask crossed bid)
```

If the maker order is rejected (post-only would cross the book), `OrderFailed` resets state to `None`. Unfilled makers that are cancelled cost nothing.

**Flow-based Leg 1 sustain**: After posting, the engine monitors the composite flow score on every event. Two cancel triggers:
1. **Cancel window timeout**: If `cancel_window_ms` elapses without a fill, the maker is cancelled regardless of composite score
2. **Flow dropout**: If `current_composite_score < cancel_threshold` before the timeout, the maker is cancelled immediately

Cancel is sent as `CancelLeg1Order { order_id }`. The executor returns `CancelResult { was_cancelled, is_leg2: false }`. If `was_cancelled = false`, the order may have filled before the cancel reached the CLOB — state is restored and User WS MATCHED reconciles.

**Whipsaw guard**: If an opposite buildup arrives while Leg 1 is `Posted` (unfilled), the engine sends `CancelLeg1Order` to explicitly cancel the resting maker (maker orders must be cancelled — unlike FAK, they do not auto-cancel). If Leg 1 is `Filled`, triggers emergency FOK exit.

### Leg 2: Hedge

Triggered when Leg 1 fills. In live mode, fills arrive via User WS MATCHED events — the engine's `on_trade_status_update()` transitions `Posted → Filled`, calls `init_leg2()`, and fires the Telegram opportunity alert. If `OrderPosted` has `already_filled: true` (rare for post-only), the engine transitions directly to `Filled` without waiting for User WS. In sim mode, `advance_simulation()` transitions `Posted → Filled` internally when `ask <= fill_price` and ask depth > 0.

**Two-phase repricing**: Hedge targeting uses a two-phase repricing model:
- **Phase A (entry)**: Uses the composite-based `expected_pct` computed by the evaluator at signal time
- **Phase B (hedge targeting)**: Refines using observed spot displacement since entry — computes the actual movement and blends with the Phase A prediction to produce the best estimate. `refined_strength = max(observed_norm, composite_score)`

**Target price**: `round_to_tick(1.0 - target_profit - leg1_price, tick)`.

**2-phase hedge system with dual-order** (all post-only until emergency):

| Phase | Resting Price | Duration | Purpose |
|-------|---------------|----------|---------|
| **Phase 1** | Profit target (`1.0 - target_profit - leg1_price`) | `phase1_timeout_ms` (2000ms) | Rest at the ideal price. If the book reprices favorably, this fills at full profit |
| **Phase 2** | `best_ask - 1 tick` (aggressive) | Until emergency trigger | Undercut the ask to maximize fill probability while preserving maker status (zero fee) |

**Phase 1 (post-once-and-wait)**: Posts once at the raw profit target price — no don't-cross-ask clamping, no smart outbid, no reposts. If the price crosses the book, the executor's `attempt_favorable_maker_then_fok()` handles it. Evaluator returns `None` if `leg2_state` is `Posted` or `Filled`. `OrderFailed` resets state to `None` for retry. Rests for `phase1_timeout_ms` (default 2000ms). If unfilled after timeout, Phase 2 posts at `best_ask - 1 tick` **without cancelling the Phase 1 order** — two maker orders rest simultaneously (dual-order). Whichever fills first wins; the engine cancels the other and completes the trade. This preserves FIFO queue priority on both orders.

**Phase 2 entry guard**: If `ask - tick > breakeven_hedge_price` ($1.00 - leg1_price), even the best maker fill would give pair cost > $1.00 → skip posting Phase 2, FOK immediately.

**Double-fill rebalance**: Rare race where both orders fill before cancel (~3ms window). Excess Leg 2 shares → FOK taker buy on Leg 1 side via `handle_rebalance_leg1()`. Tracked via `post_trade_orphan` on engine, persists across trade reset.

### Emergency Triggers

All emergency exits are **immediate FOK taker** at `best_ask` (`sim_was_taker=true`). There is no post-only chase or deadline-based escalation — every trigger results in a direct FOK order. **FOK dedup**: Once an emergency FOK is emitted (`fok_emitted=true` on `HedgeState`), subsequent evaluation cycles return `None` — the executor's price-escalating sweep handles persistence. **Signal stacking prevention**: The engine's `emergency_signal_in_flight` flag gates `evaluate_leg2()` while an emergency signal is in the executor channel, preventing multiple signals from queueing up.

| Trigger | Condition | Action |
|---------|-----------|--------|
| **Phase 1 breach** | Pair cost (leg1 + opposing ask) > `phase1_breach_threshold` during Phase 1 | **Immediate FOK taker** at `best_ask` |
| **Flow reversal** | Composite flow direction reversed from hedge direction (score > `cancel_threshold`) during Phase 1 | **Immediate FOK taker** at `best_ask` (`ExitReason::WhipsawReversal`) |
| **Flow collapse** | Composite score dropped below `cancel_threshold` during Phase 1 | **Immediate FOK taker** at `best_ask` (`ExitReason::FlowCollapse`) |
| **Flow weakening** | Composite score dropped below `entry_threshold` during Phase 1 | **Post Phase 2 alongside** at `ask - 1tick` (not an emergency — tightens hedge). If `ask - tick > breakeven` → FOK instead |
| **Phase 2 breach** | Opposing ask > `phase2_posted_price` during Phase 2 | **Immediate FOK taker** at `best_ask` |
| **Phase 2 timeout** | `phase2_timeout_ms` (2000ms) elapsed in Phase 2 without fill | **Immediate FOK taker** at `best_ask` |
| **Market expiry** | `MarketRotation` while Leg 1 Filled, Leg 2 incomplete | **Last-resort FOK** before state reset |
| **Whipsaw reversal** | Opposite buildup detected after Leg 1 fill | **Immediate FOK** at `best_ask`, bypasses hedge phases entirely |

**Flow-based graduated response** (fires faster than time-based backstops): During Phase 1, the evaluator checks the composite flow score from the `BuildupDetector` (propagated to `HedgeSnap` via `HedgeState`). Three tiers of response based on flow deterioration severity: reversal (immediate emergency), collapse (immediate emergency), weakening (Phase 2 alongside). This provides earlier protection than the fixed `phase1_timeout_ms` timer when flow conditions deteriorate.

**Live executor emergency cancel**: Same fire-and-confirm pattern as hedge phase transitions. If the cancel is not confirmed (order filled mid-cancel), the executor skips the replacement and sends `CancelResult`. Engine restores state, User WS MATCHED confirms the fill → trade complete.

### Favorable Exits (Try Maker First)

`ExitReason::FavorableTaker` covers paths where the pair cost is below $1.00 at exit time:

1. **Crosses-book rejection**: The opposing ask drops strictly below the posted Leg 2 bid — a post-only order would be rejected.
   - Sim mode: `advance_simulation()` detects `ask < posted_price` on book update, fills at ask price.
   - Live mode: `handle_leg2_hedge()` receives `Rejected` or "crosses book" error → `attempt_favorable_maker_then_fok()`:
     1. Post maker at `best_ask - 1tick` (rests below current ask)
     2. Poll for fill up to `favorable_maker_timeout_ms` (default 1000ms)
     3. **Breakeven breach guard**: each poll also queries `GET /book` — if `current_ask - tick > breakeven`, cancel maker early and FOK immediately
     4. If filled → `fill_method=FavorableMaker` (no taker fee + rebate = ~$0.37 savings)
     5. If not filled → cancel → FOK taker fallback → `fill_method=FavorableTaker`

2. **Phase 1 breach with favorable cost**: Phase 1 breach now triggers immediate FOK taker at `best_ask`. If the pair cost is favorable (`< $1.00`), the executor's favorable exit path handles it.

- **`FillMethod` metadata on `OrderPosted`**: `FavorableMaker` (try-maker succeeded), `FavorableTaker` (FOK fallback), `EmergencyTaker` (emergency FOK paths). Engine sets `LiveTradeMeta` flags from this.
- **Tracking**: `diag_favorable_exits` (total exits), `diag_favorable_maker_fills` (maker succeeded), `diag_favorable_maker_timeouts` (FOK fallback). Telegram tags: `[FAVORABLE MAKER]`, `[FAVORABLE FOK FALLBACK]`, `[FAVORABLE POST-ONLY]`.

### CLOB Auto-Cancel (Heartbeat Failure)

If the CLOB cancels all orders (heartbeat failure, admin action), User WS sends `"order"` events with status `CANCELED`. The engine handles these:
- **Leg 1 CANCELED**: Reset `leg1_state = None`. Slot freed for next buildup signal.
- **Leg 2 CANCELED**: Reset `leg2_state = None`, clear `prev_leg2_order`. Hedge continues — `evaluate_leg2()` will generate a new signal on the next iteration.

### Unmatched Event Buffer (`pending_fills`)

When a `TradeStatusUpdate` arrives but `order_id` doesn't match either `leg1_state` or `leg2_state`, the event is buffered in `pending_fills` (VecDeque, capacity 8). This catches two race conditions:

1. **Post-only fills before OrderPosted**: User WS MATCHED arrives before the executor's `OrderPosted` feedback updates the provisional ID. The event is buffered, then replayed when `on_order_posted()` sets the real ID. Note: FOK orders that return `Filled` synchronously bypass this — the `already_filled` flag on `OrderPosted` transitions directly to `Filled` without waiting for User WS.

2. **Cancel-then-fill race**: Engine clears state for a cancel. MATCHED event arrives for the now-cleared order ID. Event buffered. `CancelResult { was_cancelled: false }` restores the state. `replay_pending_fills()` finds the match → `Filled` (also sends opportunity alert + increments `live_market_signals` for Leg 1). `leg1_cancel_race` set AFTER replay so it survives `LiveTradeMeta::default()` reset.

Buffer is cleared on `on_trade_complete()` and `MarketRotation`.

### Deferred Partial Fill Alerts (`pending_partial_fills`)

The CLOB can split a large fill across multiple rapid MATCHED events (~3ms apart). To avoid false "PARTIAL FILL" alerts on fully-filled orders, partial fill detection is deferred:

1. **On MATCHED with `size_matched < original_size`**: Instead of alerting immediately, the engine stores a `PendingPartialFill { leg, size_matched, original_size }` keyed by `order_id` in `pending_partial_fills` (HashMap).

2. **On subsequent MATCHED for the same `order_id`**: Updates the cumulative `size_matched`. If now fully filled, the entry is removed silently. If still partial, re-inserted to wait for MINED.

3. **On MINED/CONFIRMED**: Final `size_matched` is authoritative. If still below `original_size`, the Telegram alert fires. Otherwise resolved silently.

4. **FAILED/CANCELED/RETRYING**: Entry discarded — other handlers deal with these statuses.

This applies at all 4 partial-fill-check sites (Leg 1 and Leg 2, both in `on_event()` and `replay_pending_fills()`). The map is cleared on `MarketRotation` but NOT on `on_trade_complete()` — deferred checks must survive trade reset to catch MINED events that arrive after.

### Trade Completion

Both legs `Filled` → `on_trade_complete()` resets all state. Detected in two places: (1) the main engine loop after processing each event (User WS fills), and (2) immediately after `OrderPosted` feedback with `already_filled=true` (synchronous FOK fills — the engine transitions Leg 2 to `Filled` and triggers completion in the same feedback drain iteration, preventing duplicate FOK signals).
- `leg1_state`, `leg2_state` → `None`
- `hedge` → `None`
- `prev_leg2_order` → `None`
- `pending_fills` → cleared
- `pending_leg1_cancel` → `None`
- `cumulative_used` persists (capital cap per market window, reset on rotation)

---

## 5. Repricing Model & Allocation

**Two-phase repricing** (`engine/confidence.rs`):

**Phase A (entry)** — computed by `Leg1Evaluator` at signal time:
```
expected_reprice_pct = norm_strength × (4P(1-P) × alignment) × time_factor × reprice_scale

norm_strength = composite_score (already [0,1]-normalized), min=0, strong=1

alignment = with_consensus ? (1 + |yes_mid - 0.5|) : (1 - |yes_mid - 0.5|)
time_factor = min((300 / max(T, 10)) ^ time_exponent, max_time_factor)
```

**Phase B (hedge targeting)** — computed by `init_leg2()` after Leg 1 fills:
```
observed_displacement = |current_spot - spot_mid_at_entry|
observed_ratio = observed_displacement / ema_atr
observed_norm = clamp((observed_ratio - min) / (strong - min), 0, 1)
refined_strength = max(observed_norm, composite_score)

Phase B repricing uses the same formula with refined_strength.
Final = max(Phase A, Phase B) — take the better of prediction and observation.
```

The raw model output drives entry gate and allocation. Phase 1 profit target is **dampened**: `round_to_tick(expected_pct × phase1_target_dampen, tick)` (default 0.8 = 80% of raw expected_pct). Separates "is this worth trading?" from "what target is achievable?".

**Entry guards** (in order):
1. Hard skew cap: YES mid > `hard_skew_cap` (0.90) or < 0.10 → reject
2. Min repricing: model output < `min_reprice_pct` (2.0%) → reject (`InsufficientRepricing`)
3. OBI alignment: Binance book imbalance contradicts buildup direction → reject (`ObiMismatch`)
4. Dynamic allocation: `clamp(output / reprice_scale, min_alloc_pct, 1.0)`

**Allocation**: `alloc = max(round(max_alloc_per_trade × alloc_fraction, 2dp), $0.01)`. Dynamic — better signals get more capital.

**ProfitTier** (display-only): HIGH ≥ `reprice_scale`, MED ≥ `reprice_scale/2`, LOW below. Labels for Telegram/diagnostics only.

---

## 6. Market Rotation

- **Discovery**: Gamma API `GET /events?tag_id=102892&closed=false&order=endDate&ascending=true&limit=100` every 10 min
  - Tag 102892 = "5M". Filter by slug prefix `btc-updown-5m-`
  - `clobTokenIds` is a JSON-encoded string (index 0 = YES, index 1 = NO)
- **Anticipatory pre-warming** (T-180s = 3 min before current market expires):
  - `discover_market_after(current_end_ms)` queries Gamma for markets ending **after** the current one, skipping the still-active Market A to find Market B
  - Pre-fetches YES and NO order books for Market B via REST
  - Caches `MarketInfo` + books in memory, ready for instant switch
- **Tick size fetch**: After Gamma discovery (both poll and prewarm paths), `fetch_tick_size()` calls `GET /tick-size?token_id={yes_id}` to get the real CLOB tick size. Defaults to `0.01` on failure. The value is included in `IngestorEvent::MarketRotation { tick_size }` and the engine sets `state.tick_size` from it. Mid-market tick_size changes (price >0.96 or <0.04) are handled separately by the `tick_size_change` WS event
- **Precise expiry timer**: A dedicated `tokio::time::sleep_until` fires at the exact market boundary (±10ms), replacing the old 5s poll that added 0–5s random latency. On expiry: emits the pre-warmed `MarketRotation` + book events immediately (zero gap), or transitions to marketless state if no prewarm is available. The 5s interval only handles prewarm discovery and marketless retry — never expiry detection
- **Fallback**: If pre-warming failed (Market B not yet on Gamma, REST error, etc.), the expiry timer transitions to marketless state and the 5s housekeeping timer retries Gamma discovery
- **`MarketRotation`** uses blocking `send()` (not `try_send()`) to guarantee delivery. Book events use `try_send()` (expendable — WS will provide updates)
- **Rotation emergency**: If Leg 1 is Filled but Leg 2 is incomplete when rotation arrives, the engine builds an emergency FOK signal at the opposing ask BEFORE resetting state. The main loop sends this signal to the executor before `ExecutorCommand::MarketRotation`, so the position is hedged (or best-effort attempted) instead of force-closed at full loss. Exit reason: `ExitReason::MarketExpiry`. In live mode, a `fire_critical()` Telegram alert is sent with position details (direction, price, size, whether a FOK was submitted) so abandoned positions are never silent
- **Cutoff window**: After `entry_cutoff_secs` (`entry_cutoff_secs` before expiry), no new Leg 1 entries are allowed (buildups dropped, evaluate() blocked). However, existing open positions continue their Leg 2 hedge phases and emergency exit paths unimpeded until rotation

```
Timeline:
  T-180s  Cutoff window — no new Leg 1 entries
  T-180s  Pre-warm: discover Market B, fetch books (5s housekeeping timer)
  T-0     Precise expiry timer fires → instant switch: emit pre-warmed MarketRotation + books
          Market WS resubscribes to new tokens
```

### Resolution

- **UMA Optimistic Oracle**: Proposer submits outcome → 2-hour challenge period → resolved
- Capital locked between expiry and resolution (~2h minimum)
- Redeem via `redeemPositions()` on CTF contract. EOA pays POL gas (capped at `MAX_GAS_PRICE` 100 gwei). Uses `CachedNonceManager` for sequential transactions (prevents "nonce too low" on rapid redeems). Per-tx receipt timeout (8s) prevents slow RPC confirmations from starving remaining positions. Null RPC receipts (tx broadcast but receipt lost) counted as redeemed. Unresolved markets revert quickly (no pre-check needed — on-chain state is authoritative)
- **Persistent condition ID tracking**: `redeems.txt` (newline-delimited) stores condition IDs from completed trades. Written at rotation (if market had trades) and at shutdown. Merged with Data API positions during redemption — fixes the Data API returning 0 positions for resolved markets. Cleanup via atomic write-temp-rename after successful redemption. `/redeem <condition_id>` for targeted single-ID redemption (30s timeout)

---

## 7. Risk Controls

| Risk | Mitigation |
|------|------------|
| **Legging** | Seven independent exit triggers, all immediate FOK taker at ask: (1) Phase 1 breach — pair cost > `phase1_breach_threshold` during Phase 1; (2) Flow reversal — composite direction reversed during Phase 1; (3) Flow collapse — composite below `cancel_threshold` during Phase 1; (4) Break-even breach — pair cost > $1.00 during Phase 2; (5) Phase 2 timeout — `phase2_timeout_ms` elapsed without fill; (6) Market expiry — rotation while Leg 1 filled; (7) Whipsaw reversal — opposite buildup after Leg 1 fill, bypasses hedge phases. Plus flow weakening (Phase 2 alongside), favorable taker (when opposing ask drops below posted bid). 2-phase hedge system: Phase 1 (profit target, `phase1_timeout_ms`), Phase 2 (ask-1tick, hold position — no reposts, `phase2_timeout_ms` deadline). FIFO queue priority preserved |
| **False positive entries** | Predictive maker entry with flow-based sustain: post Leg 1 as post-only maker when composite crosses `entry_threshold`, cancel if composite drops below `cancel_threshold` or `cancel_window_ms` elapses. Post-only = zero cost on cancel. Six-metric composite with direction consensus, causal ordering, and freshness gates. **Whipsaw guard**: opposite buildup sends `CancelLeg1Order` for unfilled maker, or triggers immediate FOK if filled |
| **Stale book entries** | Post-rotation quiet period (`rotation_quiet_ms`) blocks entries after market rotation, preventing trades on stale/repricing books |
| **Rapid re-entry** | Post-trade cooldown (`trade_cooldown_ms`) blocks new entries after trade completion, preventing rapid-fire losses on the same market |
| **Signal flooding** | Self-gating on both success and failure. One trade at a time |
| **Taker fees** | Both legs post-only (maker — negative fee = rebate). Emergency exits are immediate FOK taker. Fee: `C × 0.25 × (p×(1-p))²`, max 1.56% at p=0.50. Maker fills earn est. rebate: 20% of fee-equivalent |
| **Competing bots** | Leg 1: predictive entry at best ask before price move (queue priority). Leg 2: post-once-and-wait at target (executor handles crosses-book). Post-only = unfilled orders cost nothing |
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
[rotation]             # 1 param
prewarm_lead_secs

[entry_guards]         # 5 params
entry_cutoff_secs, binance_stale_event_ms, stale_book_ms,
rotation_quiet_ms, trade_cooldown_ms

[capital]              # 1 param
max_alloc_per_trade

[repricing]            # 8 params
reprice_scale, min_reprice_pct, min_alloc_pct, hard_skew_cap, time_exponent,
max_time_factor, phase1_target_dampen, min_obi_alignment

[risk]                 # 4 params
phase1_timeout_ms, phase1_breach_threshold,
phase2_timeout_ms, favorable_maker_timeout_ms

[buildup]              # 26 params — composite buildup detector
entry_threshold, cancel_threshold, cancel_window_ms
# Metric weights (must sum to 1.0):
w_cvd, w_spot_flow, w_obi, w_basis, w_liq, w_atr
# Freshness gates (ms) — metric treated as stale after this:
freshness_cvd_ms, freshness_spot_flow_ms, freshness_obi_ms,
freshness_basis_ms, freshness_liq_ms, freshness_atr_ms
# Normalization: [min_threshold, saturation] for each metric:
cvd_min, cvd_saturation, spot_flow_min, spot_flow_saturation,
obi_min, obi_saturation, basis_min, basis_saturation,
liq_min, liq_saturation, atr_min, atr_saturation
# EMA half-lives (ms) — time-based decay, consistent regardless of data cadence:
cvd_fast_halflife_ms, cvd_slow_halflife_ms, spot_flow_halflife_ms,
obi_velocity_halflife_ms, basis_halflife_ms
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
| `trade_signals` | market_id, direction, action, expected_pct, composite_score | SimulationExecutor |
| `executed_trades` | market_id, leg1/2 price+size, pair_cost, profit | LiveExecutor |
| `simulated_trades` | Full trade record: tier, hedge_phase, taker_fee, expected_pct, exit_reason, composite_score, etc. | SimulationExecutor |

`binance_ticks` and `poly_book_snapshots` are recorded continuously in the engine loop for backtesting and parameter tuning. Trade tables are written by the executor on fill events.

### Tuning Analytics

`simulated_trades` carries full context for outcome correlation:
- `exit_reason` (symbol): `NormalHedge`, `BreakEvenBreach`, `Phase2Timeout`, `Phase2PriceBreach`, `MarketExpiry`, `FavorableTaker`, `Phase1Breach`, `WhipsawReversal`, `FlowCollapse` — loss attribution
- `composite_score` (f64): buildup signal quality vs. outcome correlation
- `favorable_taker`, `emergency_maker` (bool): exit type flags
- `phase1_breach` (bool): `true` if Leg 2 triggered by Phase 1 breach (fast book move during Phase 1)
- `whipsaw_reversal` (bool): `true` if Leg 2 triggered by whipsaw reversal (opposite buildup → immediate FOK)
- `leg1_cancel_race` (bool): `true` if Leg 1 filled mid-cancel (cancel-not-confirmed replay path)

See `queries.sql` for 15 analytics queries (7 operational + 8 tuning). Tuning queries map loss causes directly to config parameters:

| Query | Answers | Tune |
|-------|---------|------|
| Loss attribution by exit reason | What's the #1 cause of losses? | `phase1_breach_threshold`, `entry_cutoff_secs` |
| Signal quality vs outcome | Am I trading weak buildups? | `entry_threshold`, buildup metric weights |
| Repricing calibration | Are targets matching actual repricing? | `reprice_scale`, `min_reprice_pct` |
| Phase analysis | Are trades filling in Phase 1 vs Phase 2? | `reprice_scale`, `phase1_timeout_ms` |
| Taker fee impact | How much are emergency fees eating profits? | `phase1_breach_threshold`, `phase2_timeout_ms` |

---

## 10. Telegram Reporting & Control

### Reporting (Outbound)

Four tiers via `hyper` + `tokio-rustls` (fire-and-forget, no teloxide):

1. **Opportunity Alert**: Per signal — buildup info, composite score, expected repricing %, allocation, Leg 1 entry, Leg 2 target
2. **Trade Completed**: Per trade — "Buy YES"/"Buy NO" labels, pair cost, profit (USDC), hedge phase. Leg 1 line shows `[FILLED MID-CANCEL]` when `leg1_cancel_race=true`. Leg 2 line shows `[FAVORABLE MAKER]`, `[FAVORABLE FOK FALLBACK]`, `[FAVORABLE POST-ONLY]`, `[EMERGENCY POST-ONLY]`, or `[FOK FALLBACK]` based on `LiveTradeMeta` flags. Phase tags: `(PHASE-1)`, `(PHASE-2)`, `(PHASE-1-DUAL)` (Phase 1 order filled during dual-order Phase 2)
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
| `/redeem` | Redeem resolved positions to USDC.e (merges Data API + persistent file) |
| `/redeem <condition_id>` | Redeem a specific condition ID (30s timeout) |
| `/help` | List commands with usage |

**Wallet commands (`/balance`, `/polybalance`, `/redeem`)**: Spawned as independent `tokio::spawn` tasks — the listener loop continues polling for new messages immediately. Each spawned task sends its own Telegram reply directly. Prevents slow Polygon RPC or on-chain tx confirmation from blocking other commands. Defense-in-depth timeouts: 10s for `/balance` and `/polybalance`, 60s for `/redeem` (on-chain txs are slow). `/redeem` uses `CachedNonceManager` (local nonce tracking) to prevent "nonce too low" errors when redeeming multiple positions sequentially — the default `SimpleNonceManager` queries the RPC for each send, which returns stale nonces between rapid transactions.

**Security**: Every message verified against `TELEGRAM_ALLOWED_USER_ID` + `TELEGRAM_CHAT_ID`. 2s rate limit between commands. `/set` uses a strict allowlist of 27 params with min/max ranges. No shell execution.

**Notification toggles**: `AtomicBool` flags (`Relaxed` ordering) shared between the command listener and `TelegramReporter`. One CPU instruction per check — zero hot-path impact.

### Pause vs Shutdown

**`/stop` (pause)**: Sets `engine.paused = true`, blocking new Leg 1 entries. Cancels any unfilled Leg 1 maker. Existing Leg 2 continues through hedge phases/emergency. The bot stays alive — `/balance`, `/status`, `/redeem`, `/polybalance` all remain functional. Use `/resume` to unpause.

**`/shutdown` (full exit)**: Triggers drain mode then exits. The bot never abandons an open position:

| Current State | Behavior |
|---|---|
| No open position | Immediate exit |
| Leg 1 posted, unfilled | Cancel Leg 1 → immediate exit |
| Leg 1 filled, Leg 2 in progress | Block new entries, let Leg 2 continue through hedge phases/emergency → exit after resolution |

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
├── listener.rs         # TelegramCommandListener: getUpdates polling, auth, dispatch. Wallet commands spawned as independent tasks (non-blocking)
├── handlers.rs         # Command handlers (pure logic, returns reply strings)
├── config_editor.rs    # TOML read/write, param allowlist with min/max ranges
├── wallet.rs           # /balance, /polybalance, /redeem — Polygon RPC + CTF contract calls. CachedNonceManager for sequential txs. Timeouts: 10s balance, 120s redeem (8s per-tx receipt). Persistent redeems.txt management (append/read/cleanup)
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
Verify: WS connections, buildup detection, simulated trades, Telegram alerts.

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
| Buildup-to-CLOB (internal, eu-west-2) | <50ms |
| Leg 1 fill rate | 30-50% of signals |
| Win rate (hedged trades) | 85-95% |
| Avg net profit per trade | >1.0% |
| Emergency taker fills | <15% of Leg 2 |
| System uptime | >99.5% |
