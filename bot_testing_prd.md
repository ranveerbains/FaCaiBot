# FaCaiBot — Testing / Simulation PRD

## 1. Overview

This document defines the simulation mode for FaCaiBot. The simulation mode runs the full Ingestor → Engine pipeline against **live** Binance and Polymarket data feeds but **does not execute real trades**. Instead, it:

- Simulates order fills based on live orderbook state
- Tracks virtual positions and PnL
- Posts trade opportunities, simulated entries, and performance summaries to a **Telegram bot**

**Purpose**: Validate that the trading logic correctly identifies profitable arbitrage opportunities and that the entry/hedge strategy performs as expected — before risking real capital. Both legs are post-only (maker, zero fee) in normal flow. Taker fees only apply during emergency failsafes (break-even breach, time expiry, adverse movement) — simulation must track when these emergency scenarios trigger.

**Activation**: Set `MODE=simulation` in `.env` (default is `MODE=live`).

---

## 2. Architecture Delta

### What Stays The Same

| Component | Notes |
|-----------|-------|
| Ingestor layer | Full live WebSocket feeds from Binance + Polymarket, anticipatory market loading at <180s |
| Engine layer | Full spike detection, ATR computation, confidence scoring, signal generation |
| crossbeam channels | Same bounded(8192) SPSC channels |
| Redis hot storage | Orderbook caching, active market tracking |
| QuestDB cold storage | Tick recording, simulated trade logs |
| Config loading | Same `.env` mechanism with additional simulation vars |

### What Changes

| Component | Production | Simulation |
|-----------|-----------|------------|
| Executor | Signs + submits orders via CLOB API (HTTP POST to `clob.polymarket.com`) | Simulates fills against orderbook snapshot |
| Order signing | EIP-712 via alloy | Skipped entirely |
| Wallet interaction | Polygon USDC.e transfers | None — virtual balance tracking |
| Heartbeat | Dedicated async task, `POST /heartbeat` every 5s | Not needed — no open orders to protect |
| Fill tracking | User WS channel (trade events: MATCHED→CONFIRMED) | Simulated from orderbook state |
| Reporting | Structured JSON logs + User WS trade tracking | Telegram bot messages + JSON logs |

### What's Added

| Component | Description | File |
|-----------|-------------|------|
| SimulationExecutor | Receives `TradeSignal`, simulates post-only fills, tracks virtual PnL (both legs maker in normal flow) | `src/executor/simulation.rs` (new) |
| TelegramReporter | Formats and sends messages to Telegram | `src/reporting/telegram.rs` (new) |
| SimulationState | Virtual portfolio: positions, PnL, trade history, fee tracking, resolution lock | `src/types/simulation.rs` (new) |

### Mode Toggle

In `src/main.rs`, the mode determines which executor runs:

```
MODE=simulation → spawn SimulationExecutor (receives TradeSignal, simulates, reports)
MODE=live       → spawn production Executor (signs, submits via CLOB API, monitors via User WS)
```

Both modes share the identical Ingestor and Engine layers — the only difference is what happens after a `TradeSignal` is emitted.

---

## 3. Simulation Executor

**File**: `src/executor/simulation.rs`

### Responsibilities

1. Receive `TradeSignal` from Engine via crossbeam channel
2. Simulate post-only fill using current orderbook state (from Redis or Engine state)
3. Track Leg 1 fill rate (post-only may not fill — unfilled signals cost nothing)
4. Simulate Leg 2 post-only fill with erosion cascade; calculate taker fees only for emergency fills
5. Track virtual positions, running PnL, and capital locked in pending resolutions
6. Forward events to TelegramReporter
7. Log all simulated trades to QuestDB

### No Real Execution

- No wallet private key usage
- No EIP-712 signing
- No CLOB API order submissions (no `POST /order` or `POST /orders`)
- No heartbeat loop required (no open orders to protect)
- No Polygon transactions
- `PRIVATE_KEY` is not required in simulation mode

---

## 4. Simulated Fill Logic

### Leg 1 (Directional Entry)

When a `TradeSignal` is received:

1. Snapshot the current Polymarket orderbook (from `MarketState.poly_book`)
2. Simulate post-only bid at `best_bid + tick_size` (top of bid side)
3. **Smart outbidding**: if depth wall detected (single price > 4x avg depth), simulate outbid by 1 tick (capped at break-even)
4. **Fill simulation**: Estimate whether counterparty flow would fill the resting bid during repricing:
   - Check if sufficient sell-side depth exists within 1-2 ticks of our bid
   - **Filled**: Simulate fill at our bid price (maker, zero fee)
   - **Partial**: Fill up to available depth, log remainder as missed
   - **Not filled**: Log as "opportunity detected but unfilled" — zero cost
5. Record: fill price, size, timestamp, spread at entry, confidence score, allocated amount, profit target tier
6. Leg 1 is always post-only (maker) → **zero fee**
7. Track fill rate: `signals_detected` vs `leg1_fills` for post-only fill rate analysis
8. **Bot analytics**: Log depth wall detections and outbid events. Flag as `bot_contested` if walls present. No aborts or cooldowns.

### Leg 2 (Hedge)

After simulated Leg 1 fill:

1. Compute hedge target (both legs maker, zero fee in normal flow):
   ```
   target_profit = confidence_tier_target(confidence)  // 2.5%, 1.5%, or 1.0%
   target = 1.0 - target_profit - entry_price
   step_size = target_profit / 5    // proportional erosion: 20% of margin per step
   // HIGH (2.5%): step_size = 0.005 → path: 2.5→2.0→1.5→1.0→0.5→break-even
   // MED  (1.5%): step_size = 0.003 → path: 1.5→1.2→0.9→0.6→0.3→break-even
   // LOW  (1.0%): step_size = 0.002 → path: 1.0→0.8→0.6→0.4→0.2→break-even
   ```
2. Smart outbidding: if depth wall at target price, adjust by 1 tick (capped at break-even: pair cost < 1.0)
3. **Quick reversal check**: If Binance shows >0.05% opposite move within 100ms of Leg 1 fill → cancel Leg 2, log as "reversed_pre_hedge"
4. Start simulated hedge timer (same as production: 10-15s)
5. On each orderbook snapshot during the timer window and extended erosion window:
   - Check if opposite side `best_ask` <= current hedge target
   - **Yes**: Simulate hedge fill as **maker** (post-only resting order → zero fee)
   - **No**: every 2s, raise hedge target by `step_size` (simulate erosion step, still post-only)
   - Continue erosion after timer window until `market_expiry - 90s` deadline
6. **Adverse movement check**: Monitor simulated Binance price after Leg 1 fill
   - **Grace period**: Ignore price fluctuations for first `ADVERSE_GRACE_PERIOD` (3s) after Leg 1 fill
   - After grace period: if price reverses > `ADVERSE_THRESHOLD` (0.3%) → simulate immediate FOK hedge at best_ask (**taker fee applies**)
   - If price continues in Leg 1 direction past break-even → simulate force fill (**taker fee**, may be at a loss)
7. **Emergency deadline**: `market_expiry - 90s` — force FOK fill regardless of price (**taker fee applies**)
8. Record: hedge price, size, erosion steps taken, final paired cost, whether fill was maker or taker, taker fee paid (0 for maker), adverse_movement flag, reversed_pre_hedge flag

### Taker Fee Calculation (Emergency Only)

Both legs are post-only (maker, zero fee) in normal flow. Taker fees only apply during emergency scenarios (adverse movement, break-even breach, timer/market expiry). The simulation must apply fees accurately when emergency fills occur:

```
taker_fee_per_share = 0.25 * (price * (1.0 - price))^2
total_taker_fee = shares * taker_fee_per_share
```

| Price | Effective Taker Rate |
|-------|---------------------|
| $0.10 | 0.20% |
| $0.30 | 1.10% |
| $0.50 | **1.56%** |
| $0.70 | 1.10% |
| $0.90 | 0.20% |

- Leg 1 (post-only) → always maker → **zero fee**
- Leg 2 (post-only within erosion cascade) → maker → **zero fee**
- Leg 2 (emergency FOK: deadline/adverse/break-even) → taker → **fee applies**
- Track `leg2_was_taker: bool` for each simulated trade

### PnL Calculation

```
pair_cost = leg1_fill_price + leg2_fill_price
gross_profit = 1.00 - pair_cost
// Normal flow (both legs maker): net_profit = gross_profit (zero fees)
// Emergency taker: taker_fee = 0.25 * (leg2_price * (1.0 - leg2_price))^2 * shares
taker_fee = if leg2_was_taker { fee_formula } else { 0 }
net_profit = gross_profit - taker_fee
profit_pct = net_profit / pair_cost * 100
```

For unfilled hedges at market expiry:
- Query Gamma API for resolution outcome (note: UMA resolution takes ~2h minimum)
- If winning side held: `profit = 1.00 - leg1_fill_price` (lucky but risky)
- If losing side held: `loss = leg1_fill_price` (worst case)

### Resolution Delay Tracking

Simulated trades should track the UMA Optimistic Oracle resolution timeline:
- Minimum 2-hour challenge period after market end before redemption
- DVM dispute escalation excluded from bot scope — if disputed, capital remains locked
- Track `virtual_locked_capital` for positions awaiting resolution
- Factor locked capital into available allocation for subsequent markets
- Record `resolution_delay_ms` between market end and resolution confirmation

---

## 5. Simulation State

**File**: `src/types/simulation.rs`

### Virtual Portfolio

```rust
struct SimulationState {
    virtual_balance: Decimal,           // Starts at FIXED_ALLOC (e.g., $100)
    open_positions: Vec<SimPosition>,
    closed_trades: Vec<SimTrade>,
    session_start: u64,                 // Timestamp ms
    markets_observed: u32,
    signals_detected: u32,              // Total signals generated by engine
    leg1_fills: u32,                    // Signals where Leg 1 post-only filled
    trades_hedged: u32,
    trades_adverse_hedged: u32,         // Hedged via adverse movement protocol (emergency taker)
    trades_emergency_taker: u32,        // Leg 2 filled as taker (any emergency reason)
    walls_outbid: u32,                  // Smart outbidding events (depth walls detected)
    total_pnl: Decimal,
    total_taker_fees_paid: Decimal,     // Cumulative taker fees (emergency fills only)
    total_maker_rebates_earned: Decimal, // Estimated 20% of taker fees on maker fills
    locked_in_resolution: Decimal,       // Capital awaiting UMA resolution
    cumulative_used: Decimal,            // Used allocation in current market
}

struct SimPosition {
    market_id: String,
    leg1: SimFill,
    leg2: Option<SimFill>,
    status: PositionStatus,            // Open, Hedged, Expired, AwaitingResolution
}

struct SimFill {
    side: Side,                        // YES or NO
    price: Decimal,
    size: Decimal,
    timestamp_ms: u64,
    was_partial: bool,
    was_taker: bool,                   // true if fill crossed the spread (taker fee applies)
    taker_fee: Decimal,                // 0 if maker, computed fee if taker
}

struct SimTrade {
    market_id: String,
    leg1: SimFill,
    leg2: Option<SimFill>,
    confidence: Decimal,               // Signal confidence score (0-1)
    profit_target_tier: String,        // "HIGH" (2.5%), "MED" (1.5%), or "LOW" (1.0%)
    alloc_amount: Decimal,             // USDC allocated to this trade
    pair_cost: Decimal,
    gross_profit: Decimal,
    taker_fee: Decimal,                // 0 in normal flow (both legs maker); non-zero only for emergency taker
    net_profit: Decimal,               // gross_profit - taker_fee (= gross_profit when both maker)
    profit_pct: Decimal,               // net_profit / pair_cost * 100
    resolution: Option<String>,        // "YES" or "NO" or None (if still open)
    resolution_timestamp_ms: Option<u64>, // When UMA resolution confirmed
    erosion_steps: u32,                // How many times price was eroded for Leg 2
    leg2_was_taker: bool,              // Whether Leg 2 executed as emergency taker
    adverse_movement_hedge: bool,      // Whether Leg 2 was triggered by adverse price movement
    bot_contested: bool,               // Whether depth wall was detected during this trade
}
```

---

## 6. Telegram Integration

**File**: `src/reporting/telegram.rs`

### Setup

FaCaiBot is configured to send real-time simulation alerts to your Telegram bot. The bot (@f4c4ibot) is already created and ready to receive messages.

**Setup Instructions:**

1. ✅ Bot already created: `@f4c4ibot`
2. Copy `.env.example` to `.env` and fill in Telegram credentials:
   ```bash
   cp .env.example .env
   ```
3. Edit `.env` and add your Telegram credentials:
   ```env
   MODE=simulation
   TELEGRAM_BOT_TOKEN=<your-bot-token-from-botfather>
   TELEGRAM_CHAT_ID=<your-chat-id-from-userinfobot>
   ```
4. To get your credentials:
   - **Bot Token**: Message `@BotFather` → `/mybots` → select `@f4c4ibot` → view token
   - **Chat ID**: Message `@userinfobot` on Telegram → it will reply with your user ID
5. Ensure the bot can message you:
   - Start a chat with `@f4c4ibot` on Telegram (send any message)
   - The bot will use this connection to send simulation alerts

**Important**: Never commit `.env` to git — it contains sensitive credentials. Keep `.env.example` as a template.

### Crate

Use `teloxide` (the most popular Rust Telegram bot framework) for sending messages.

### Message Types

The bot sends three tiers of messages:

---

#### Tier 1: Real-Time Alert (per opportunity)

Sent immediately when an opportunity is detected and simulated.

```
--- OPPORTUNITY DETECTED ---

Market: BTC Up/Down 15m (#12345)
Expires: 12:45 UTC (8m 32s remaining)

Spike: UP +0.42% (2.1x ATR)
Sustained: 340ms

Signal confidence: 0.82 (HIGH)
Profit target: 2.5% (HIGH tier)
Allocated: $30.00 (30% of $100)

Leg 1 (simulated):
  Buy YES @ $0.481 x 62.4 shares (post-only, maker, $0 fee)
  Bid position: top of book (outbid wall at $0.480)
  Orderbook depth: $847 available

Leg 2 target:
  Buy NO @ $0.494 (break-even: $0.519)
  Both legs maker → est. profit: 2.5%
  Emergency taker fee (if FOK needed): $0.0039/share (1.56% effective)

Status: WATCHING FOR HEDGE...
```

After hedge fills (or fails):

```
--- TRADE COMPLETED ---

Market: BTC Up/Down 15m (#12345)

Leg 1: YES @ $0.481 x 41.6 (maker, $0 fee)
Leg 2: NO  @ $0.494 x 41.6 (maker, $0 fee, eroded 1x)

Pair cost: $0.975
Gross profit: $0.025 (2.5%)
Net profit: $0.025 (2.5%) — both legs maker, zero fee
Erosion: 1 step (target was $0.494)
```

---

#### Tier 2: Market Summary (per 15-min market expiry)

Sent when each 15-minute market resolves.

```
--- MARKET SUMMARY ---

Market: BTC Up/Down 15m (#12345)
Resolution: pending (UMA challenge period: ~2h remaining)
Period: 12:30 - 12:45 UTC

Signals detected: 5
Leg 1 fills: 2 (40% fill rate)
Hedged: 2/2 (100%)
Walls outbid: 1
Emergency taker fills: 0

Trade 1: conf=0.82 target=2.5% alloc=$30 → YES@0.481 + NO@0.494 = $0.975 → maker+maker → net +$0.025 (+2.5%)
Trade 2: conf=0.54 target=1.5% alloc=$20 → YES@0.512 + NO@0.479 = $0.991 → maker+maker → net +$0.009 (+0.9%)

Allocation used: $50 / $100 (50%)
Taker fees paid: $0.00 (both legs maker)
Gross market PnL: +$0.034
Net market PnL: +$0.034 (zero fees in normal flow)
Capital locked in resolution: $20.00
```

---

#### Tier 3: Session Summary (hourly + on shutdown)

Sent every hour and when the bot shuts down.

```
--- SESSION SUMMARY (1h) ---

Uptime: 1h 00m
Markets observed: 4

Fill rate:
  Signals detected: 20
  Leg 1 fills (post-only): 7 (35% fill rate)
  Hedged: 6/7 (85.7%)

Smart outbidding:
  Depth walls detected: 4
  Walls outbid: 3
  Outbids that led to fills: 2

Emergency taker (Leg 2):
  Adverse movement FOK: 1
  Break-even breach FOK: 0
  Timer/expiry deadline FOK: 0

Allocation:
  High confidence (≥0.8, target 2.5%): 3 trades, avg $30
  Medium confidence (≥0.5, target 1.5%): 3 trades, avg $20
  Low confidence (<0.5, target 1.0%): 1 trade, avg $10
  Avg confidence: 0.64

Gross PnL: +$0.350
Emergency taker fees: $0.016 (1 emergency fill)
Est. maker rebates: $0.003
Net PnL: +$0.337

Win rate: 85.7% (6/7)
Average net profit: +1.7% per trade (both legs maker in 6/7 trades)
Best trade: +2.5% (market #12341, conf=0.91, both maker)
Worst trade: -0.3% (market #12343, conf=0.42, emergency taker hedge)

Unfilled signals: 13
  - 8x post-only bid not matched (normal — zero cost)
  - 3x insufficient liquidity
  - 2x spread too wide

Capital locked in resolution: $40.00 (2 markets pending)
Virtual balance: $100.337 (started: $100.00)
```

---

### Message Formatting

- Use Telegram's MarkdownV2 or HTML formatting for readability
- Prefix each message type with a distinct header for quick scanning
- Include timestamps in UTC
- All prices in USD with 3 decimal places
- Percentages with 1 decimal place
- Show gross and net profit. In normal flow (both legs maker) these are equal. Show taker fee impact only when emergency fills occur

### Rate Limiting

- Telegram API limit: ~30 messages/second per bot
- Batch rapid-fire alerts if multiple signals in <1s (unlikely but possible)
- Never block the simulation executor waiting for Telegram — send async, log failures

---

## 7. Environment Variables

### New Variables (Simulation Mode Only)

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `MODE` | Yes | `live` | `simulation` or `live` |
| `TELEGRAM_BOT_TOKEN` | Yes (sim) | — | Bot token from @BotFather |
| `TELEGRAM_CHAT_ID` | Yes (sim) | — | Target chat/channel ID |
| `RTDS_ENABLED` | No | `false` | Enable Polymarket RTDS for Chainlink price reference and divergence logging |

### Variables NOT Required in Simulation Mode

| Variable | Why |
|----------|-----|
| `PRIVATE_KEY` | No order signing |
| `POLYMARKET_API_KEY` | No order submission. Read-only CLOB market data (orderbook, prices) does NOT require authentication. Market WS channel is public. Gamma API is fully public |
| `POLYMARKET_SECRET` | Not needed — only required for authenticated L2 operations (order placement, cancellation) |
| `POLYMARKET_PASSPHRASE` | Not needed — same as above |
| `POLYMARKET_SIG_TYPE` | No order signing (EOA type 0 is used in production) |

### Updated `.env.example`

```env
# Mode: "simulation" (no real trades, Telegram reporting) or "live" (real trading)
MODE=simulation

# Polymarket credentials (required for live only; read-only data is public)
POLYMARKET_API_KEY=
POLYMARKET_SECRET=
POLYMARKET_PASSPHRASE=

# Wallet (required for live only — EOA type 0, signer = funder)
PRIVATE_KEY=

# Infrastructure
REDIS_URL=redis://127.0.0.1:6379
QUESTDB_URL=127.0.0.1:9009
BINANCE_WS_URL=wss://stream.binance.com:9443
RTDS_ENABLED=false
RUST_LOG=facaibot=info

# Telegram (required for simulation mode)
TELEGRAM_BOT_TOKEN=
TELEGRAM_CHAT_ID=
```

---

## 8. QuestDB Logging (Simulation)

In addition to production tick logging, simulation mode writes a `simulated_trades` table:

| Column | Type | Description |
|--------|------|-------------|
| market_id | symbol | Polymarket condition ID |
| direction | symbol | "YES" or "NO" |
| leg1_price | f64 | Simulated entry price |
| leg1_size | f64 | Simulated entry size |
| leg2_price | f64 | Simulated hedge price (null if unhedged) |
| leg2_size | f64 | Simulated hedge size |
| pair_cost | f64 | Total cost of paired position |
| gross_profit | f64 | Gross profit/loss before fees |
| leg2_was_taker | bool | Whether Leg 2 executed as taker |
| taker_fee | f64 | Taker fee paid on Leg 2 (0 if maker) |
| net_profit | f64 | Profit after taker fee |
| profit_pct | f64 | Net profit as percentage |
| erosion_steps | i64 | Number of erosion steps for Leg 2 |
| hedged | bool | Whether hedge completed |
| confidence | f64 | Signal confidence score (0-1) |
| profit_target_tier | symbol | "HIGH" (2.5%), "MED" (1.5%), or "LOW" (1.0%) |
| alloc_amount | f64 | USDC allocated to this trade |
| adverse_hedge | bool | Whether hedge was triggered by adverse price movement (emergency taker) |
| bot_contested | bool | Whether depth wall was detected (smart outbidding) |
| resolution | symbol | Market outcome ("YES"/"NO") |
| resolution_delay_ms | i64 | Time between market end and resolution confirmation |
| timestamp | timestamp | Trade execution time |

This allows post-session analysis via QuestDB's web console (`localhost:9000`) with SQL queries like:
```sql
-- Overall performance including fee impact
SELECT avg(profit_pct) as avg_net_pct, avg(taker_fee) as avg_fee,
       count(*) as trades, sum(net_profit) as total_net
FROM simulated_trades
WHERE timestamp > dateadd('h', -24, now());

-- Fee impact by price range
SELECT
  CASE WHEN leg2_price BETWEEN 0.4 AND 0.6 THEN 'mid-range'
       ELSE 'extreme' END as zone,
  avg(taker_fee) as avg_fee, avg(profit_pct) as avg_net_pct, count(*) as n
FROM simulated_trades
WHERE leg2_was_taker = true
GROUP BY zone;
```

### Polymarket Book Snapshots (24h rolling)

Table: `poly_book_snapshots` — snapshot every 5s for backtesting

| Column | Type | Description |
|--------|------|-------------|
| token_id | symbol | Polymarket token ID |
| best_bid | f64 | Best bid price |
| best_ask | f64 | Best ask price |
| bid_depth | f64 | Total bid depth (USDC) |
| ask_depth | f64 | Total ask depth (USDC) |
| spread | f64 | Spread percentage |
| timestamp | timestamp | Snapshot time |

### Trade Signals Log (24h rolling)

Table: `trade_signals` — every signal the engine generates (fired or not)

| Column | Type | Description |
|--------|------|-------------|
| market_id | symbol | Polymarket condition ID |
| direction | symbol | "YES" or "NO" |
| confidence | f64 | Confidence score (0-1) |
| spike_magnitude | f64 | Spike size relative to ATR |
| atr | f64 | Current ATR value |
| book_depth | f64 | Polymarket book depth at signal time |
| time_remaining | i64 | Seconds to market expiry |
| alloc_amount | f64 | Allocated USDC for this signal |
| action | symbol | "entered", "unfilled_postonly", "aborted_spread", "aborted_liquidity", "skipped_confidence" |
| timestamp | timestamp | Signal time |

### Chainlink Divergence Table (for backtesting)

When `RTDS_ENABLED=true`, log Binance vs Chainlink price divergence for backtesting:

Table: `price_divergence`

| Column | Type | Description |
|--------|------|-------------|
| symbol | symbol | "btcusdt" or "ethusdt" |
| binance_price | f64 | Binance spot mid-price |
| chainlink_price | f64 | Chainlink feed price (via RTDS) |
| divergence_pct | f64 | (binance - chainlink) / chainlink * 100 |
| timestamp | timestamp | Observation time |

```sql
-- Analyze Binance-Chainlink divergence over 24h
SELECT avg(abs(divergence_pct)) as avg_divergence,
       max(abs(divergence_pct)) as max_divergence,
       count(*) as observations
FROM price_divergence
WHERE timestamp > dateadd('h', -24, now())
AND symbol = 'btcusdt';

-- Find periods where divergence exceeds threshold
SELECT timestamp, divergence_pct, binance_price, chainlink_price
FROM price_divergence
WHERE abs(divergence_pct) > 0.1
AND symbol = 'btcusdt'
ORDER BY timestamp DESC
LIMIT 100;
```

---

## 9. Verification Plan

### Step 1: Basic Connectivity (Day 1)

- [ ] Bot connects to Binance WebSocket and receives ticks
- [ ] Bot connects to Polymarket Market WS (`wss://ws-subscriptions-clob.polymarket.com/ws/market`) and receives orderbook updates
- [ ] Market WS subscription with `custom_feature_enabled: true` receives `best_bid_ask` events
- [ ] RTDS connection receives `crypto_prices_binance` and `crypto_prices_chainlink` (if `RTDS_ENABLED=true`)
- [ ] Stale event filter working: events with `timestamp > 500ms` old are discarded (check logs for discard counts)
- [ ] Config validation passes at startup: all env vars parsed and validated before entering main loop
- [ ] Telegram bot sends a startup message: "FaCaiBot simulation started"

### Step 2: Signal Detection (Day 1-2)

- [ ] Run during volatile BTC period (news events, market opens)
- [ ] Verify spike detection fires on real Binance moves
- [ ] Verify phantom filter correctly discards brief reversions
- [ ] Telegram receives real-time alerts for each detected opportunity

### Step 2.5: Post-Only Fill & Fee Validation (Day 2)

- [ ] Verify Leg 1 post-only orders rest on book (never cross spread)
- [ ] Verify Leg 1 fill rate tracking works (signals_detected vs leg1_fills)
- [ ] Verify both legs show zero fee in normal flow (post-only maker)
- [ ] Fetch `fee_rate_bps` for a 15-min crypto market token — confirm non-zero (for emergency calc)
- [ ] Verify taker fee correctly applied only during emergency fills (adverse movement, deadline)
- [ ] Verify PnL: net = gross for normal trades, net < gross only for emergency taker fills

### Step 3: Simulated Trading (Day 2-3)

- [ ] Simulated Leg 1 post-only fills match orderbook state (fill at bid price, not ask)
- [ ] Simulated Leg 2 post-only fills with erosion cascade (all post-only until emergency)
- [ ] Taker fee correctly applied ONLY during emergency FOK fills (not normal erosion)
- [ ] PnL calculations correct: net = gross for maker fills, net < gross for emergency taker
- [ ] Dynamic profit targets: 2.5% / 1.5% / 1.0% based on confidence tier
- [ ] Confidence scores produce sensible allocations (high vol → higher confidence, low vol → lower)
- [ ] Dynamic allocation varies correctly: $10 / $20 / $30 per signal based on confidence tier
- [ ] Cumulative allocation respects $100 hard cap per market
- [ ] Smart outbidding fires correctly when depth walls detected (outbid by 1 tick, capped at break-even)
- [ ] Adverse movement protocol: grace period (3s) prevents false triggers, then FOK on reversal > 0.3%
- [ ] Emergency deadline: force FOK at market_expiry - 90s (aligned with 180s no-entry cutoff)
- [ ] Anticipatory market loading: next market warm before current market expires
- [ ] Leg 1 fill rate tracked and reported in Telegram messages
- [ ] Market summaries post at each 15-min market expiry

### Step 4: Extended Run (Day 3-5)

- [ ] Run for 24+ continuous hours
- [ ] Session summaries post hourly with correct aggregations (including fees)
- [ ] No memory leaks or channel overflow (monitor with `top`/`htop`)
- [ ] Virtual balance tracking is consistent (accounts for fees and resolution locks)
- [ ] QuestDB `simulated_trades` table has complete records with fee columns
- [ ] Chainlink vs Binance divergence logging populates `price_divergence` table (if `RTDS_ENABLED`)
- [ ] Resolution delay tracking shows correct ~2h UMA challenge periods
- [ ] UMA dispute detection: if a market enters dispute state, Telegram alert fires with locked capital amount
- [ ] Capital lock tracking correctly reduces available allocation for subsequent markets
- [ ] Automated QuestDB pruning runs hourly without errors (check logs for pruned partition counts)

### Step 5: Strategy Validation (Week 1-2)

- [ ] Collect 200+ simulated trades
- [ ] Analyze win rate (target: 85-95%)
- [ ] Analyze average **net** profit per trade (target: >1.0% — both legs maker in normal flow)
- [ ] Analyze Leg 1 fill rate (target: 30-50% of signals)
- [ ] Analyze emergency taker rate (target: <15% of Leg 2 fills)
- [ ] Identify false positives (spikes that didn't resolve as predicted)
- [ ] Tune thresholds (ATR multiplier, sustain window, erosion rates) based on data
- [ ] Compare simulated PnL against what actual execution would have yielded
- [ ] Analyze Chainlink-Binance divergence data for correlation validation (target: >95% for moves >1%)

### Decision Gate

Before switching to `MODE=live`:
- [ ] Leg 1 fill rate >25% of signals (post-only fills are working)
- [ ] Win rate >80% over 200+ filled trades
- [ ] Average **net** profit >1.0% per trade (both legs maker in normal flow → net = gross)
- [ ] Emergency taker fills <15% of Leg 2 fills
- [ ] Confidence-weighted allocation and dynamic profit targets produce reasonable risk-adjusted returns
- [ ] Smart outbidding fires correctly when depth walls detected
- [ ] Adverse movement protocol: grace period prevents false triggers; emergency FOK fires appropriately
- [ ] No unhedged positions held past absolute deadline (market_expiry - 90s)
- [ ] No unhandled errors in 48h continuous run
- [ ] Latency metrics meet P99 <350ms (signal to simulated fill) target
- [ ] Anticipatory market loading achieves <100ms cold-start on market rotation
- [ ] Tiered kill switch verified: WARN at 3%, PAUSE at 5%, HALT at 8% daily loss (test by adjusting thresholds temporarily)
- [ ] Resolution delay observed and capital lock properly tracked
- [ ] 24h rolling data tables (poly_book_snapshots, trade_signals) populated and queryable
- [ ] Manual review of 20+ individual trades confirms correct logic (confidence scoring, allocation sizing, profit target tiers, smart outbidding)

---

## 10. Future Extensions (Out of Scope for Now)

| Extension | Description |
|-----------|-------------|
| **Historical replay** | Replay QuestDB tick history against simulated orderbooks for backtesting |
| **Telegram commands** | `/pause`, `/resume`, `/status`, `/pnl`, `/config` — interactive control via Telegram |
| **Web dashboard** | Real-time browser UI for monitoring (replace or supplement Telegram) |
| **Multi-asset** | Add ETH 15-minute markets alongside BTC |
| **Alert thresholds** | Configurable Telegram alert levels (e.g., only notify on >2% opportunities) |
| **Comparison mode** | Run simulation alongside live to measure execution quality vs theoretical |
| **Chainlink direct feed** | Toggle between Binance-only and Binance+Chainlink (via RTDS or sponsored Chainlink Streams API key) for dual-source price reference |
| **Operator health dashboard** | Real-time monitoring of CLOB API latency, heartbeat reliability, matching engine status |
| **Taker Leg 1 option** | If post-only fill rate < 20%, consider hybrid approach: selective taker entry on highest-confidence signals only. Trade fill rate for fee cost |
| **Fee optimization** | Analyze emergency taker fee impact by price range; prefer entries at price extremes where fees are minimal |
| **Capital efficiency** | Track UMA resolution times; optimize allocation to minimize capital locked in challenge periods |
| **Kelly criterion allocation** | Replace confidence tiers with continuous Kelly criterion sizing: `f* = (bp - q) / b` where b=odds, p=estimated win prob, q=1-p. Requires calibrated win probability model from 500+ trades. More mathematically optimal but needs larger dataset to avoid overfitting |
| **Cross-market signal correlation** | Use 24h rolling data to detect correlated signals across consecutive markets and adjust confidence scoring |
| **Adaptive profit targets** | Replace fixed 2.5%/1.5%/1.0% tiers with continuous function: `target = f(confidence, volatility, book_depth)`. Tune from 500+ trades |
| **Contested rate tracking (live-mode)** | In live mode, track % of posted orders that are outbid within 1s of posting. If >70% contested, re-evaluate strategy viability. Not measurable in simulation — no real orders posted |
| **Monte Carlo EV model** | After collecting 500+ live trades, build Monte Carlo simulation using empirical distributions (fill rate, divergence magnitude, emergency taker %). Requires real data — premature before live deployment |
