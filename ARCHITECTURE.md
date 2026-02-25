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
- **`live`**: Signs and submits real orders via CLOB API, monitors fills via User WS
- **`simulation`**: Full pipeline against live data, but engine simulates fills internally. Reports via Telegram + QuestDB

---

## 2. Pipeline

Three-layer lock-free pipeline connected by crossbeam SPSC bounded(8192) channels:

```
Ingestor (gateway/)        Engine (engine/)           Executor (executor/)
─────────────────          ──────────────             ─────────────────────
Dedicated OS thread        tokio task                 tokio task
CPU-pinned core 0          Main runtime               Main runtime

Binance WS ──────┐
  @depth20@100ms │
  @ticker        ├──► IngestorEvent ──► MarketState update
Polymarket WS ───┤                      Spike detection
  Market channel │                      Signal evaluation
  User channel   │                      Erosion cascade
Gamma API ───────┘                      advance_simulation() (sim)
Heartbeat (5s) ──┘                            │
                                        TradeSignal / ExecutorCommand
                                              │
                                              ▼
                                        Live: LiveExecutor ──► CLOB REST
                                              ◄── ExecutorFeedback (order IDs)
                                        Sim: SimulationExecutor ──► Telegram+QuestDB
```

### Simulation Engine Loop

**Single-authority fill model**: the engine is the sole fill authority in sim mode (mirrors live where the CLOB decides fills). `advance_simulation()` acts as the simulated CLOB — the executor only records what the engine tells it via the `sim_confirmed_fill` flag on `TradeSignal`.

```
on_event()           → update book/price state
advance_simulation() → simulate fills: Posted→Filled→Complete→Reset
                       emits confirmed fill signals (sim_confirmed_fill=true)
evaluate()           → Leg 1 signal (self-gates after emitting)
                       emits detection signal (sim_confirmed_fill=false)
                       records rejection reason if spike blocked (rej_busy/no_book/stale/skew/spread/depth/other)
evaluate_leg2()      → Leg 2 erosion/emergency signal (self-gates after emitting)
                       erosion steps sent to executor (sim_confirmed_fill=false)
                       emergency signals handled internally by advance_simulation()
```

### Live Engine Loop

In live mode, the CLOB is the fill authority. Fills arrive via the authenticated User WebSocket as `TradeStatusUpdate` events. The engine matches these against posted order IDs to transition `OrderState::Posted → Filled`.

A reverse `ExecutorFeedback` channel (executor → engine) sends real CLOB order IDs back after placement, so the engine can match User WS fill events to the correct leg.

```
drain feedback_rx    → apply ExecutorFeedback::OrderPosted / OrderFailed
                       (updates OrderState with real CLOB order IDs)
on_event()           → update book/price state + match User WS fills
evaluate()           → Leg 1 signal → executor places post-only GTC
evaluate_leg2()      → ALL signals sent (erosion + emergency post-only/FOK)
trade completion     → both legs Filled → on_trade_complete() → reset
```

**Key differences from sim**:
- `advance_simulation()` never runs
- Emergency signals are NOT filtered — the executor places FOK orders on the CLOB
- Trade completion detected in the main loop (both `leg1_state` and `leg2_state` are `Filled`)
- Feedback is drained before `on_event()` — CLOB round-trip (~50-100ms) is faster than User WS notification (~200-500ms), so order IDs are set before fill events arrive

### LiveExecutor (`executor/live.rs`)

Handles the full trade lifecycle via CLOB REST API:

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

1. **Mid-price**: `(best_bid + best_ask) / 2` from `@depth20` updates
2. **Rolling EMA-ATR**: alpha=0.002 (~500 ticks / ~50s memory at 100ms ticks). No spikes emitted for first 10 ticks while ATR warms up.
3. **Spike trigger**: `|delta| > multiplier * ATR` (2.5×) within `window_ms` (350ms)
4. **Sustain check**: Hold for `sustain_ms` (300ms)
   - **Momentum ratio**: displacement at sustain must be ≥50% of peak (filters fading spikes)
5. **Magnitude gate**: `sustained_displacement / origin_price` must exceed `min_magnitude_pct` (1 bp ≈ $6.50 at BTC $65K)
   - Uses sustain-time displacement, not peak overshoot
6. **One attempt**: Each spike evaluates exactly once. `spike_detected` cleared regardless of outcome

### Spike Signal Delivery

Confirmed spikes are delivered as `IngestorEvent::SpikeConfirmed(SpikeInfo)` — a dedicated event variant carrying direction, magnitude, sustained_ms, and timestamp. The engine stores the spike in `MarketState.last_spike` and sets `spike_detected = true`. Normal `BinanceTick` events only update `binance_price` — they never trigger spike evaluation.

---

## 4. Trade Lifecycle

### Leg 1: Entry

**Self-gating**: `evaluate(&mut self)` clears `spike_detected`, sets `leg1_state = Posted`, increments `cumulative_used`. One trade at a time.

**Pre-entry guards** (abort if any fail):

| Guard | Threshold | Notes |
|-------|-----------|-------|
| No market | `active_condition_id` absent | Rare — awaiting rotation |
| No book | book or bid/ask missing | — |
| No Binance price | `binance_price` absent | — |
| Stale book | book age > `stale_book_ms` (500ms) | — |
| Price skew | YES mid > 0.80 or < 0.20 | Near-certain-resolution, Leg 2 fill collapses |
| Spread | > `max_spread_pct` (2%) | — |
| Active trade | `leg1_state != None` | Checked **after** spread — `rej_busy` counts only valid-book spikes lost to a busy executor |
| Expiry | < `entry_cutoff_secs` (300s) | Defence-in-depth; normally caught upstream |
| Depth | < `depth_min_pct` (20%) of required | — |

**Bidding**: `round_to_tick(best_bid + tick, tick)` → smart outbid walls by 1 tick (>4x avg depth) → cap at break-even. Submit GTC, post_only=true.

**Sizing**: Confidence-weighted allocation (see Section 5).

### Leg 2: Hedge

Triggered when Leg 1 fills. In live mode, fills arrive via User WS `TradeStatusUpdate` (matched by CLOB order ID from `ExecutorFeedback`). In sim mode, `advance_simulation()` transitions `Posted → Filled` internally.

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
Config: erosion_base_interval_ms = 3500, erosion_interval_decay = 0.5
Step 1: 3500ms → Step 2: 1750ms → Step 3: 875ms → Step 4: 437ms → Step 5: 218ms
Total: ~6.8s to break-even
```

Each erosion step cancels the previous unfilled order before posting the new one (cancel-replace). All prices rounded to tick size. Steps are capped at `MAX_EROSION_STEPS` (5). After step 5, the cascade is exhausted (profit target = 0) and auto-escalates to a `BreakEvenBreach` emergency.

### Emergency Triggers (post-only first, FOK fallback)

Three independent exit paths triggered by different signals. All use a **post-only first, FOK fallback** strategy: the engine emits an aggressive post-only limit at `best_ask - 1 tick` (zero fee). Emergency signals are re-evaluated every `emergency_repost_interval_ms` (500ms) at the current top-of-book price. After `emergency_max_maker_attempts` (default 3) post-only attempts without a fill, the engine escalates to a FOK taker at `best_ask` to guarantee execution.

| Trigger | Condition | Timing |
|---------|-----------|--------|
| **Adverse movement** | Binance reversal > `adverse_threshold` (0.1%) from Binance price at Leg 1 fill | **Immediate** — zero grace period. Post-only first, FOK fallback after N attempts |
| **Break-even breach** | Opposing ask worsens > `break_even_tolerance_ticks` (2) beyond `opposing_ask_at_fill` | **After first erosion step** (~3.5s). Gives Polymarket time to react to spike momentum |
| **Erosion exhausted** | All 5 erosion steps applied, profit target = 0. Cascade reached break-even without filling | **After step 5** (~6.8s). Auto-escalates as `BreakEvenBreach` emergency |
| **Market expiry** | `MarketRotation` arrives while Leg 1 is Filled but Leg 2 incomplete | **At rotation** — last-resort FOK before state reset. Best-effort; CLOB may reject if market expired |

**Post-only emergency flow**: Once an emergency is triggered, the evaluator re-emits signals every `emergency_repost_interval_ms` (default 500ms). Each repost uses the current hedge book's `best_ask - 1 tick` as the limit price. After `emergency_max_maker_attempts` post-only attempts, the engine switches to FOK at `best_ask` (crosses the spread, guaranteed fill). In live mode, the executor cancels the previous resting order and posts a new aggressive post-only. If the CLOB rejects (price would cross), the executor falls back to a FOK at the same price. In sim mode, `advance_simulation()` checks whether the posted price would be valid as post-only (best_ask > posted_price): if yes, fills as maker (`sim_was_taker=false`, zero fee); if no, fills as taker (`sim_was_taker=true`, taker fee applies).

**Break-even breach** (Polymarket hedge cost worsened): gated behind `steps_applied >= 1`. FOK fallback price capped at `opposing_ask_at_fill + max_loss_ticks × tick` (default 3 ticks). If ask has gapped beyond the cap, defer to erosion — don't crystallize a catastrophic loss.

**Adverse movement** (Binance reversal): fires at ANY point after Leg 1 fill with zero delay. First signal is post-only at `best_ask - 1 tick`. Subsequent reposts follow the standard post-only → FOK escalation. Compares current Binance price against the Binance price captured at Leg 1 fill time (`ErosionState.binance_at_fill`). If the spike has reversed, the trade thesis is dead regardless of Polymarket book state.

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
- **Cutoff window**: After `entry_cutoff_secs` (300s before expiry), no new Leg 1 entries are allowed (spikes dropped, evaluate() blocked). However, existing open positions continue their Leg 2 erosion cascade and emergency exit paths unimpeded until rotation

```
Timeline:
  T-300s  Cutoff window — no new Leg 1 entries
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
| **Legging** | Three independent exit paths, all using **post-only first, FOK fallback**: (1) Adverse movement — immediate aggressive post-only on Binance reversal >0.3%, FOK if rejected, zero grace; (2) Break-even breach — after first erosion step, aggressive post-only capped at initial+3 ticks, FOK fallback; (3) Favorable taker — when opposing ask drops below posted bid, post-only at `best_ask - 1 tick`, FOK if rejected. Emergency reposts every 500ms at current top-of-book. Triangle-weighted erosion cascade (front-loaded steps, accelerating pace, ~9.2s to break-even) |
| **False positive spikes** | Sustain filter (300ms hold above 2.5×ATR) + momentum ratio (≥50% of peak at sustain time) + magnitude gate (1 bp). One evaluation per spike. Post-fill reversals caught by adverse_threshold FOK |
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
[spike_detection]      # 6 params
multiplier, atr_alpha, window_ms, sustain_ms,
min_magnitude_pct, momentum_ratio_min
# NOTE: no spikes emitted for first ~1s (10 ticks) while ATR warms up

[entry_guards]         # 5 params
max_spread_pct, depth_min_pct, entry_cutoff_secs, stale_book_ms, max_price_skew

[capital]              # 4 params
max_alloc_per_trade, high_alloc_pct, med_alloc_pct, low_alloc_pct

[confidence]           # 5 params
high_threshold, med_threshold,
high_target_pct, med_target_pct, low_target_pct

[risk]                 # 9 params
adverse_threshold,
erosion_base_interval_ms, erosion_interval_decay,
depth_wall_multiplier, quick_reversal_threshold,
break_even_tolerance_ticks, max_loss_ticks,
emergency_repost_interval_ms, emergency_max_maker_attempts
```

All have `#[serde(default)]` with production defaults. `Config::test_defaults()` uses `BotConfig::default()` (same defaults as config.toml).

### `.env` — Secrets & Infrastructure

| Variable | Required | Description |
|----------|----------|-------------|
| `MODE` | Yes | `simulation` or `live` |
| `PRIVATE_KEY` | Live only | Hex private key for EIP-712 signing |
| `POLYMARKET_API_KEY/SECRET/PASSPHRASE` | Live only | L2 HMAC auth credentials |
| `TELEGRAM_BOT_TOKEN/CHAT_ID` | Sim only | Telegram reporting |
| `QUESTDB_URL` | No (default localhost) | Cold storage (analytics only) |
| `BINANCE_WS_URL` | No (default stream.binance.com) | Binance WS |

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
| Loss attribution by exit reason | What's the #1 cause of losses? | `adverse_threshold`, `break_even_tolerance_ticks`, `entry_cutoff_secs` |
| Spike quality vs outcome | Am I trading weak spikes? | `min_magnitude_pct`, `multiplier` |
| Confidence threshold calibration | Where should HIGH/MED/LOW cutoffs be? | `high_threshold`, `med_threshold` |
| Erosion step analysis | Are trades filling too late? | `*_target_pct`, `erosion_base_interval_ms` |
| Taker fee impact | How much are emergency fees eating profits? | `emergency_repost_interval_ms` |

---

## 10. Telegram Reporting

Three tiers via `hyper` + `tokio-rustls` (fire-and-forget, no teloxide):

1. **Opportunity Alert**: Per signal — spike info, confidence, allocation, Leg 1 entry, Leg 2 target
2. **Trade Completed**: Per trade — "Buy YES"/"Buy NO" labels, pair cost, profit (USDC), erosion steps
3. **Market Summary**: Per 15-min expiry — fill rate, trades, PnL
4. **Session Summary**: Hourly + shutdown — aggregate stats, win rate, balance

Rate limited at 5s intervals. Critical messages (trade completions) bypass the limiter.

---

## 11. Polymarket API Reference

### Endpoints Used

| Endpoint | Purpose |
|----------|---------|
| `wss://ws-subscriptions-clob.polymarket.com/ws/market` | Public book/price/tick events |
| `wss://ws-subscriptions-clob.polymarket.com/ws/user` | Authenticated fill tracking (live) |
| `https://clob.polymarket.com/order` | Order submission (EIP-712 signed) |
| `https://clob.polymarket.com/heartbeat` | Keep-alive (5s, live) |
| `https://gamma-api.polymarket.com/events` | Market discovery |

### Contracts (Polygon, Chain ID 137)

| Contract | Address |
|----------|---------|
| CTF Exchange | `0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E` |
| Neg Risk CTF Exchange | `0xC5d563A36AE78145C45a50134d48A1215220f80a` |
| Conditional Tokens (CTF) | `0x4D97DCd97eC945f40cF65F87097ACe5EA0476045` |
| USDC.e | `0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174` |

**Auth**: EOA (signature type 0). Signer = maker = funder. L2 HMAC credentials derived once from private key.

---

## 12. Deployment

### Stage 1: Local Simulation
```bash
docker-compose up -d
cp .env.example .env  # Set MODE=simulation, Telegram creds
cargo build --release && cargo run
```
Verify: WS connections, spike detection, simulated trades, Telegram alerts.

### Stage 2: Server Simulation
Deploy to AWS `us-east-1` or Hetzner Ashburn. Run 48+ hours, validate latency (<350ms P99).

### Stage 3: Live Trading
Set `MODE=live`, fill CLOB credentials, fund EOA wallet (USDC.e + POL). Start with reduced allocation. Approve Exchange contract for spending.

**Go-live gate**: Leg 1 fill rate >25%, win rate >80% over 200+ trades, average net >1.0%, emergency taker <15% of Leg 2 fills.

---

## 13. Performance Targets

| Metric | Target |
|--------|--------|
| Signal-to-MATCHED (P99) | <350ms |
| Leg 1 fill rate | 30-50% of signals |
| Win rate (hedged trades) | 85-95% |
| Avg net profit per trade | >1.0% |
| Emergency taker fills | <15% of Leg 2 |
| System uptime | >99.5% |
