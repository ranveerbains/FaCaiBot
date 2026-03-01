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
Dedicated OS thread        tokio task                 tokio task
CPU-pinned core 0          Manual runtime (2 workers)  Manual runtime (2 workers)
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

A reverse `ExecutorFeedback` channel (executor → engine) sends real CLOB order IDs back after placement, so the engine can match User WS fill events to the correct leg.

**Speculative posting**: Leg 1 is posted to the CLOB immediately on `SpikeCandidate` (gaining ~300ms queue priority). If `SpikeFailed` arrives and the order hasn't filled yet, a `CancelLeg1` is sent to the executor. If the order already filled before `SpikeFailed`, Leg 2 proceeds normally.

```
drain feedback_rx    → apply ExecutorFeedback::OrderPosted / OrderFailed
                       (updates OrderState with real CLOB order IDs)
on_event()           → update book/price state + match User WS fills
                       SpikeCandidate/SpikeConfirmed/SpikeFailed handling
take_spike_cancel()  → drain CancelLeg1 from SpikeFailed
evaluate()           → Leg 1 signal → executor places post-only GTC
evaluate_leg2()      → ALL signals sent (erosion + emergency post-only/FOK)
trade completion     → both legs Filled → on_trade_complete() → reset
```

**Key differences from sim**:
- `advance_simulation()` never runs — no speculative fill gate needed (CLOB is fill authority)
- Emergency signals are NOT filtered — the executor places FOK orders on the CLOB
- Trade completion detected in the main loop (both `leg1_state` and `leg2_state` are `Filled`)
- Feedback is drained before `on_event()` — CLOB round-trip (~50-100ms) is faster than User WS notification (~200-500ms), so order IDs are set before fill events arrive

### LiveExecutor (`executor/live.rs`)

Handles the full trade lifecycle via the SDK-backed `PolymarketGateway`:

| Signal | Action |
|--------|--------|
| Leg 1 | Post-only GTC → feedback `OrderPosted` to engine |
| Leg 1 rejected | CLOB returns `Rejected` → feedback `OrderFailed` to engine |
| Leg 2 erosion | Cancel previous resting order → repost at eroded price |
| Leg 2 erosion rejected | Post-only rejected (ask < bid) → attempt favorable exit (post-only first, FOK fallback) |
| Leg 2 emergency | Cancel resting → aggressive post-only at `best_ask - 1 tick` → FOK fallback if rejected → Telegram critical alert |
| Market rotation | `cancel_all()` → reset state |

On placement failure or CLOB rejection, sends `OrderFailed` feedback so the engine resets the leg state to `None`.

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

## 4. Trade Lifecycle

### Leg 1: Entry

**Speculative entry**: Leg 1 is posted speculatively on `SpikeCandidate` (before sustain confirmation). `evaluate(&mut self)` clears `spike_detected`, sets `leg1_state = Posted`, increments `cumulative_used`. One trade at a time. If `SpikeFailed` arrives before fill, the order is cancelled and state reset (post-only = zero cost).

**Pre-entry guards** (abort if any fail):

| Guard | Threshold | Notes |
|-------|-----------|-------|
| No market | `active_condition_id` absent | Rare — awaiting rotation |
| No book | book or bid/ask missing | — |
| No Binance price | `binance_price` absent | — |
| Stale book | book age > `stale_book_ms` (500ms) | — |
| Price skew | YES mid > 0.80 or < 0.20 | Near-certain-resolution, Leg 2 fill collapses |
| Spread | > `max_spread_ticks` (2 ticks) | Tick-based, uniform regardless of mid price |
| Active trade | `leg1_state != None` | Checked **after** spread — `rej_busy` counts only valid-book spikes lost to a busy executor |
| Expiry | < `entry_cutoff_secs` (see config.toml) | Defence-in-depth; normally caught upstream |
| Depth | < `depth_min_pct` (20%) of required | — |
| Hedge feasibility | `bid_price + opposing_best_ask > $1.00` | Pair already underwater at current opposing book |

**Bidding**: `round_to_tick(best_bid + tick, tick)` → smart outbid walls by 1 tick (>4x avg depth, capped by hedge feasibility) → hedge feasibility guard. Submit GTC, post_only=true.

**Sizing**: Confidence-weighted allocation (see Section 5).

### Leg 2: Hedge

Triggered when Leg 1 fills. In live mode, fills arrive via User WS `TradeStatusUpdate` (matched by CLOB order ID from `ExecutorFeedback`). In sim mode, `advance_simulation()` transitions `Posted → Filled` internally — but only after `SpikeConfirmed` clears the speculative fill gate.

**Target price**: `round_to_tick(1.0 - target_profit - leg1_price, tick)`

**Erosion cascade** (all post-only until emergency, cancel-replace on each step):

**Step sizes** — front-loaded using triangle weights `[5, 4, 3, 2, 1]` (sum=15). Early steps give up more margin (higher chance of fill at a good price); later steps give up less:

| Step | Weight | % of margin | HIGH (2.5%) | MED (1.5%) | LOW (1.0%) |
|------|--------|-------------|-------------|------------|------------|
| 1 | 5/15 | 33.3% | 0.83% | 0.50% | 0.33% |
| 2 | 4/15 | 26.7% | 0.67% | 0.40% | 0.27% |
| 3 | 3/15 | 20.0% | 0.50% | 0.30% | 0.20% |
| 4 | 2/15 | 13.3% | 0.33% | 0.20% | 0.13% |
| 5 | 1/15 | 6.7% | 0.17% | 0.10% | 0.07% |

**Intervals** — exponential decay: `interval_i = base × decay^i`. Early steps wait longer (market has time to fill); later steps fire faster (urgency):

```
Config: erosion_base_interval_ms = 3000, erosion_interval_decay = 0.5
Step 0: 3000ms → Step 1: 1500ms → Step 2: 750ms → Step 3: 375ms → Step 4: 200ms
Total: ~5.8s to break-even
```

Each erosion step cancels the previous unfilled order before posting the new one (cancel-replace). All prices rounded to tick size. Steps are capped at `MAX_EROSION_STEPS` (5). After step 5, the cascade is exhausted (profit target = 0) and auto-escalates to a `BreakEvenBreach` emergency.

**Skip guard**: Before reposting, the evaluator checks if the current resting Leg 2 order is already at a price equal to or better than the new erosion target. If so, the cancel-and-repost is skipped — preserving a favorable position (e.g. from a favorable exit post-only). This prevents erosion from overwriting a good price with a worse one.

**Leg 1 staleness timeout**: If a posted Leg 1 order is not filled within `leg1_timeout_ms` (default 5000ms), the engine cancels it and frees the slot for the next spike. In sim mode this rarely fires (instant fills); in live mode it prevents indefinite blocking on stale orders.

### Emergency Triggers (price-improvement chase with hard deadline)

Three independent exit paths triggered by different signals. All use a **price-improvement chase with hard deadline** strategy: the engine posts an aggressive post-only limit at `best_ask - 1 tick` (zero fee) and preserves FIFO queue priority. The engine only cancels and reposts when the Polymarket book offers a strictly better price (price-improvement). After `emergency_deadline_ms` (default 2500ms) from the first emergency post without a fill, the engine escalates to a FOK taker at `best_ask` to guarantee execution.

| Trigger | Condition | Timing |
|---------|-----------|--------|
| **Adverse movement** | Binance reversal > `adverse_threshold` (0.1%) from Binance price at Leg 1 fill | **Immediate** — zero grace period. Post-only first, price-chase until deadline |
| **Break-even breach** | Pair cost (leg1 + opposing ask) >= $1.00 | **After first erosion step** (~3s). Gives Polymarket time to react to spike momentum |
| **Erosion exhausted** | All 5 erosion steps applied, profit target = 0. Cascade reached break-even without filling | **After step 5** (~5.8s). Auto-escalates as `BreakEvenBreach` emergency |
| **Market expiry** | `MarketRotation` arrives while Leg 1 is Filled but Leg 2 incomplete | **At rotation** — last-resort FOK before state reset. Best-effort; CLOB may reject if market expired |

**Price-improvement chase flow**: Once an emergency is triggered, the evaluator tracks two fields: `emergency_first_post_ms` (timestamp of initial post, starts the deadline clock) and `emergency_posted_price` (current resting order price). On each Polymarket book update, the evaluator checks: (1) Has `emergency_deadline_ms` elapsed since first post? → FOK at `best_ask` (`sim_was_taker=true`). (2) Is `best_ask - 1 tick > emergency_posted_price`? → cancel and repost at the improved price (price-chase). (3) Neither? → hold current order, preserve FIFO queue priority. Binance tick events are skipped during emergency mode (only Polymarket book changes matter for exit pricing).

**Simulation model**: Emergency fills wait the full deadline window. If `best_ask <= posted_price` at any point → maker fill (zero fee). If deadline expires without fill → taker FOK at `best_ask` (taker fee applies). The `sim_was_taker` flag propagates to the executor for fee treatment.

**Live executor**: The evaluator signals whether to FOK or price-chase via the `sim_was_taker` flag. `sim_was_taker=true` (deadline expired) → cancel existing + direct FOK at `best_ask`. `sim_was_taker=false` (price-chase) → cancel existing + aggressive post-only at the evaluator-computed price. If the CLOB rejects the post-only (price would cross spread), the executor falls back to a FOK.

FOK fallback size capped at `min(remaining_position, ask_depth_within_2_ticks)`.

### Favorable Taker (Sim + Live)

When the opposing ask drops strictly below the posted Leg 2 bid, a post-only order would be rejected by the CLOB. Instead of leaving Leg 1 unhedged, the bot market-takes at the ask price via FOK. The taker fee is acceptable insurance vs the risk of an open position.

- **Sim mode**: `advance_simulation()` detects `ask < posted_price` on each book update (all direction branches: Up → `poly_no_book`, Down → `poly_yes_book`, None → `poly_book`). Fills at the ask price with `ExitReason::FavorableTaker`.
- **Live mode**: When `handle_leg2_erosion()` receives a `Rejected` response from the CLOB, it calls `attempt_favorable_exit()` which first tries an aggressive post-only at `best_ask - 1 tick`, then falls back to FOK if rejected. The CLOB fills at the actual best ask (which is below the limit), giving automatic price improvement.
- **Tracking**: `favorable_taker_fills` counter in `SimulationState`, `MarketSummary`, `SessionSummary`, and `LiveExecutor` diagnostics. `[FAVORABLE TAKER]` / `[FAVORABLE POST-ONLY]` / `[FAVORABLE FOK FALLBACK]` tags in Telegram trade completions.

### Trade Completion

Both legs Filled → reset `leg1_state`, `leg2_state`, `erosion` to None. `cumulative_used` persists (capital cap enforced across multiple trades per market). Reset on market rotation.

---

## 5. Confidence & Allocation

**Scoring** (`engine/confidence.rs`):
```
confidence = 0.4 * min(spike_magnitude / ATR, 1.0)   [spike quality vs. recent volatility]
           + 0.2 * min(poly_book_depth / avg_book_depth, 1.0)
           + 0.2 * (time_remaining / 900.0)

Max score: 0.8 (sustain removed — all confirmed spikes already passed the gate,
               so it contributed a constant offset with no discriminative value)
```

**Allocation**: `alloc = max(round(max_alloc_per_trade × tier_pct), $1)`
| Confidence | Tier | Profit Target | Default tier_pct | Example ($10 max) |
|------------|------|---------------|-------------------|-------------------|
| ≥ 0.6 | HIGH | 2.5% | 100% | $10 |
| ≥ 0.3 | MED | 1.5% | 50% | $5 |
| < 0.3 | LOW | 1.0% | 25% | $3 |

Minimum allocation is $1 regardless of tier. `max_alloc_per_trade` is the sole capital control; the wallet balance is the real constraint in live trading.

---

## 6. Market Rotation

- **Discovery**: Gamma API `GET /events?tag_id=102467&active=true&closed=false&limit=10` every 10 min
  - Tag 102467 = "15M". Filter by slug prefix `btc-updown-15m-` / `eth-updown-15m-`
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
max_spread_ticks, depth_min_pct, entry_cutoff_secs, stale_book_ms, max_price_skew,
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
3. **Market Summary**: Per 15-min expiry — fill rate, trades, PnL
4. **Session Summary**: Hourly + shutdown — aggregate stats, win rate, balance

Rate limited at 5s intervals. Critical messages (trade completions) bypass the limiter.

Opportunity alerts and trade completions are gated by `NotifyFlags::trades_enabled`; market summaries by `NotifyFlags::summary_enabled`. Both default to `true`, toggled via `/trades` and `/summary` commands.

### Bot Control (Inbound)

Bidirectional Telegram control via `getUpdates` long-polling (30s timeout). Enabled when `TELEGRAM_ALLOWED_USER_ID` is set. Runs as a separate tokio task — zero overhead on the hot path.

| Command | Action |
|---------|--------|
| `/trades on\|off` | Toggle opportunity + trade-completed notifications |
| `/summary on\|off` | Toggle market summary notifications |
| `/stop` | Drain mode → graceful shutdown (exit 0, no systemd restart) |
| `/set <param> <value>` | Validate + write config.toml → drain → restart (exit 42) |
| `/config [section]` | Show all params, or just one section (e.g. `/config risk`) |
| `/status` | Uptime, mode, current market, leg states, trade counters, toggle states |
| `/help` | List commands with usage |

**Security**: Every message verified against `TELEGRAM_ALLOWED_USER_ID` + `TELEGRAM_CHAT_ID`. 2s rate limit between commands. `/set` uses a strict allowlist of 27 params with min/max ranges. No shell execution.

**Notification toggles**: `AtomicBool` flags (`Relaxed` ordering) shared between the command listener and `TelegramReporter`. One CPU instruction per check — zero hot-path impact.

### Drain Mode

The bot never abandons an open position. `/stop` and `/set` both trigger drain mode before exiting:

| Current State | Behavior |
|---|---|
| No open position | Immediate exit |
| Leg 1 posted, unfilled | Cancel Leg 1 → immediate exit |
| Leg 1 filled, Leg 2 in progress | Block new entries, let Leg 2 continue through erosion/emergency → exit after resolution |

Drain progress is published via `tokio::sync::watch<DrainStatus>` (Idle → Draining → Complete). The command listener watches the channel and sends real-time Telegram updates:

```
User: /stop
Bot:  "Stopping bot..."
Bot:  "Drain mode activated (stop) — Leg 2 in progress, waiting for position to close"
Bot:  "Bot stopped. Exiting."
```

**Exit codes**: `/stop` exits with code 0 (success — systemd does not restart). `/set` exits with code 42 (on-failure — systemd restarts in 5s with new config).

### Control Architecture (`src/control/`)

```
src/control/
├── mod.rs              # Module declarations
├── listener.rs         # TelegramCommandListener: getUpdates polling, auth, dispatch
├── handlers.rs         # Command handlers (pure logic, returns reply strings)
├── config_editor.rs    # TOML read/write, param allowlist with min/max ranges
└── types.rs            # NotifyFlags, BotStatus, DrainStatus
```

**Data flow**: Commands inject `IngestorEvent::Shutdown` or `IngestorEvent::DrainAndRestart` into the existing ingestor channel. The engine handles these in its main loop — sets `draining = true`, publishes `DrainStatus` updates, and breaks when the position resolves.

---

## 11. Polymarket API Reference

### Endpoints Used

| Endpoint | Purpose |
|----------|---------|
| `wss://ws-subscriptions-clob.polymarket.com/ws/market` | Public book/price/tick events |
| `wss://ws-subscriptions-clob.polymarket.com/ws/user` | Authenticated fill tracking (live) |
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
3. If either step fails → read-only mode (`sdk_client = None`, order placement unavailable)

**Order flow** (`place_order`):
```
OrderRequest
  → sdk.limit_order().token_id().side().price().size().order_type().post_only().build().await
    (SDK fetches tick_size per token internally — DashMap cache, one CLOB call then cached)
  → sdk.sign(signer, signable).await
    (EIP-712 typed data — auto-detects neg_risk for correct exchange contract domain separator)
  → sdk.post_order(signed).await
    (L2 HMAC auth headers constructed internally, POST /order)
  → PostOrderResponse mapped to OrderResponse { order_id, status, timestamp_ms }
```

**Cancel flow**:
- `cancel_order(id)` → `sdk.cancel_order(id).await` → `DELETE /order`
- `cancel_all()` → `sdk.cancel_all_orders().await` → `DELETE /cancel-all`

**SDK cache pre-population**: On market rotation, `LiveExecutor` pre-populates the SDK's per-token `DashMap` caches (`tick_size`, `fee_rate_bps=0`, `neg_risk=true`) using token IDs from the `MarketRotation` command. This eliminates the first-order CLOB round-trip per token that the SDK would otherwise make to fetch tick size.

`signing.rs` contains only `build_signer()` — hex private key parsing to `PrivateKeySigner`.

---

## 12. Deployment

### Runtime Optimizations

- **jemalloc**: Global allocator (`tikv-jemallocator`) eliminates glibc malloc latency spikes. Conditional on `cfg(not(target_env = "msvc"))` — active on both macOS (local dev) and Linux (production)
- **Manual tokio runtime**: 2 worker threads pinned to cores 1-2 via `on_thread_start` + `core_affinity`. Replaces `#[tokio::main]` for explicit core control. Core 0 reserved for ingestor (dedicated OS thread)
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

**systemd restart policy**: `Restart=on-failure` with `RestartSec=5s`. Exit 0 (`/stop`) = success → no restart. Exit 42 (`/set` config change) = failure → restart in 5s with new config. Crashes = failure → restart in 5s.

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
