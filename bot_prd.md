# FaCaiBot — Product Requirements Document

## 1. Overview

FaCaiBot is an automated arbitrage bot targeting Polymarket's 15-minute "Bitcoin Up or Down" prediction markets. It exploits the repricing lag on Polymarket's CLOB — when a directional spike occurs on Binance BTC/USDT, market makers on the CLOB take seconds to adjust their quotes. By detecting the spike in real time and entering before the book reprices, the bot acquires a cheap directional position and hedges with the opposite side, locking in a risk-neutral pair costing less than $1 that pays out $1 on resolution.

Polymarket 15-min crypto markets use Chainlink price feeds as the reference source and resolve via the **UMA Optimistic Oracle** (proposal + 2-hour challenge period). The bot uses Binance as a leading signal because it is the largest liquidity venue and Binance price movements precede CLOB repricing by seconds.

**Modes**: The bot operates in two modes controlled by `MODE` env var:
- **`live`** — Signs and submits real orders via CLOB API, monitors fills via User WS
- **`simulation`** — Runs the full Ingestor → Engine pipeline against live data feeds but does not execute real trades. The engine simulates fill lifecycle internally (`advance_simulation()`), and the SimulationExecutor reports via Telegram + QuestDB

Both modes share the identical Ingestor and Engine layers — the only difference is what happens after a `TradeSignal` is emitted and how fills are tracked.

---

## 2. Core Concept

### The Arbitrage Mechanism

```
Binance spike detected (real-time)
    ↓ CLOB hasn't repriced yet (seconds of lag)
    ↓
Leg 1: Buy directional shares cheap (e.g., YES at $0.48) — post-only (maker, zero fee)
    ↓ CLOB reprices as market makers adjust
    ↓
Leg 2: Buy opposite shares (e.g., NO at $0.495) — post-only (maker, zero fee)
    ↓
Paired position: $0.48 + $0.495 = $0.975 → pays $1.00 → 2.5% profit (both legs maker, zero fee)
```

Emergency only: If Leg 2 cannot fill as maker before deadline, force fill via FOK (taker). Taker fee formula: `fee = 0.25 * (p * (1 - p))^2` per share. Max 1.56% at p=0.50.

### Why It Works

- **CLOB repricing lag**: Human and bot market makers on Polymarket don't instantly update quotes when Binance spikes — this window is the alpha
- **Binance as leading signal**: Largest liquidity venue, >95% correlation with Chainlink for moves >1% (requires validation via backtesting)
- **Both legs post-only (zero fee)**: Both Leg 1 and Leg 2 submit as `post_only=true` (GTC). Net profit = gross profit in normal flow. Taker orders (FOK) reserved for emergency failsafes only (break-even breach, time expiry, adverse movement). Emergency taker fee: `fee = C * 0.25 * (p * (1 - p))^2` — max 1.56% at p=0.50, declining toward extremes. 20% redistributed as daily maker rebates

### Post-Only Fill Mechanics

Post-only orders on Polymarket are **REJECTED** if they would cross the spread — they cannot fill immediately. They always rest on the book first. This is a hard constraint, not a preference.

**How Leg 1 fills during a directional spike:**
1. Binance spikes UP → bot detects signal
2. Post buy YES at `best_bid + tick` (post-only, rests on book as new best bid)
3. If this price would cross the ask → order REJECTED → no trade for this signal
4. During the repricing lag (300ms-2s), counterparty flow fills our resting bid:
   - Market makers rebalancing (selling YES to flatten exposure after detecting the spike)
   - Profit-takers exiting existing YES positions
   - Counter-signal traders entering the opposite direction
   - Automated portfolio rebalancers
5. If filled: we acquired YES below post-spike fair value → proceed to Leg 2
6. If not filled: cancel order, wait for next signal → **zero cost, zero risk**

**Fill rate**: Estimated 30-50% of signals result in Leg 1 fills. Each fill captures the full spread with zero fees. Unfilled signals cost nothing. Even at lower fill rates, expected value can exceed taker approaches because the full 2-3% spread is preserved without fee erosion.

**Why not taker entry?** Taking stale asks guarantees fills but costs ~1.56% in taker fees at p=0.50. With a typical 2.5% gross spread, that leaves only ~0.94% net. Post-only entry preserves the full 2.5% at the cost of lower fill rate.

### Key Terminology

| Term | Meaning |
|------|---------|
| Leg 1 | Directional entry (buy YES if spike up, buy NO if spike down) |
| Leg 2 | Hedge (buy the opposite side to lock in sub-$1 pair) |
| Paired position | Leg 1 + Leg 2 combined — guaranteed $1 payout regardless of outcome |
| ATR | Average True Range — adaptive volatility measure |
| Phantom bid | Brief spike that reverts before CLOB reprices — must be filtered |
| Operator | Polymarket's CLOB matching engine — matches orders off-chain, settles on-chain |
| UMA Oracle | UMA Optimistic Oracle — resolves market outcomes via proposal + challenge |

---

## 3. Architecture

Three-layer lock-free pipeline (see `CLAUDE.md` for full stack details):

```
Ingestor (Ear) ──▶ Engine (Brain) ──▶ Executor (Hand)
   │                    │                    │
   │ crossbeam SPSC     │ crossbeam SPSC     │
   │ bounded(8192)      │ bounded(8192)      │
   ▼                    ▼                    ▼
Binance WS          MarketState          MODE=live: Sign + Submit
Polymarket WS       ATR / Spike eval       CLOB API (HTTP POST)
Gamma API (REST)    TradeSignal gen        WS fill tracking
                    advance_simulation()   Redis / QuestDB
                    (sim mode only)      MODE=simulation:
                                           SimulationExecutor
                                           Telegram + QuestDB
```

| Layer | Thread Model | Key Files |
|-------|-------------|-----------|
| Ingestor | Dedicated OS thread, CPU-pinned core 0, single-threaded tokio | `src/gateway/binance.rs`, `src/gateway/polymarket_ws.rs` |
| Engine | tokio task on main runtime | `src/engine/strategy.rs`, `src/types/market.rs` |
| Executor (live) | tokio task on main runtime | `src/gateway/polymarket.rs`, `src/storage/hot.rs`, `src/storage/cold.rs` |
| Executor (sim) | tokio task on main runtime | `src/executor/simulation.rs`, `src/reporting/telegram.rs`, `src/storage/cold.rs` |

Entry point: `src/main.rs` — wires channels, spawns layers, selects executor based on `MODE`.

### Engine Loop (Simulation Mode)

In simulation mode, the engine runs an additional `advance_simulation()` step each tick to simulate the fill lifecycle without User WS feedback:

```
Engine loop (sim mode):
  on_event()           → update book/price state
  advance_simulation() → simulate fills: Posted→Filled, complete trades → reset state
  evaluate()           → Leg 1 signal (self-gates to Posted after emitting)
  evaluate_leg2()      → Leg 2 erosion signal (self-gates to Posted after emitting)
  → signals sent to executor for Telegram/QuestDB reporting
```

Trade lifecycle in simulation:
```
Spike → evaluate() → Leg1 Posted → advance_sim → Leg1 Filled (init erosion)
  → evaluate_leg2() → Leg2 Posted (erosion cascade, every 2s)
  → advance_sim → Leg2 Filled (ask <= target OR emergency)
  → advance_sim → both Filled → RESET (leg1=None, leg2=None, erosion=None)
  → next spike can trade (if capital remains)
```

**Optional data source**: Polymarket RTDS (`wss://ws-live-data.polymarket.com`) provides `crypto_prices_binance` (btcusdt, ethusdt) and `crypto_prices_chainlink` (btc/usd, eth/usd) topics. Can supplement or replace direct Binance connection and provide Chainlink reference for divergence analysis.

---

## 4. Configuration

All configuration via environment variables (`.env` file). No config files.

### Credentials

| Variable | Required (live) | Required (sim) | Description |
|----------|----------------|----------------|-------------|
| `POLYMARKET_API_KEY` | Yes | No | L2 auth credential (HMAC) |
| `POLYMARKET_SECRET` | Yes | No | L2 auth credential (HMAC) |
| `POLYMARKET_PASSPHRASE` | Yes | No | L2 auth credential (HMAC) |
| `PRIVATE_KEY` | Yes | No | Hex private key for EIP-712 order signing |
| `TELEGRAM_BOT_TOKEN` | No | Yes | Telegram Bot API token from @BotFather |
| `TELEGRAM_CHAT_ID` | No | Yes | Target Telegram chat/channel ID |

L2 credentials are derived once from the private key via `create_or_derive_api_creds()` (L1 EIP-712 signature). These HMAC credentials then authenticate all subsequent trading requests.

**Wallet type**: EOA (signature type 0). The signer address derived from `PRIVATE_KEY` is both the maker address and funder address. This means:
- No relayer dependency — one fewer failure point
- User pays POL gas for infrequent on-chain operations (approvals, split/merge/redeem)
- CLOB order placement speed is identical across all wallet types (HTTP POST)
- Hold a small POL balance (~0.1 POL) for gas on Polygon

**Simulation mode** does not require `PRIVATE_KEY` or CLOB auth credentials. Read-only CLOB market data (orderbook, prices) does NOT require authentication. Market WS channel is public. Gamma API is fully public.

### Infrastructure

| Variable | Default | Description |
|----------|---------|-------------|
| `MODE` | `live` | `simulation` or `live` |
| `REDIS_URL` | `redis://127.0.0.1:6379` | Hot cache |
| `QUESTDB_URL` | `127.0.0.1:9009` | Cold storage (ILP) |
| `BINANCE_WS_URL` | `wss://stream.binance.com:9443` | Binance WebSocket |
| `RTDS_ENABLED` | `false` | Enable Polymarket RTDS for Chainlink price reference |
| `RUST_LOG` | `facaibot=info` | Log level |

### Trading Constants

| Constant | Value | Description |
|----------|-------|-------------|
| `FIXED_ALLOC` | 100 USDC | Total allocation per 15-min market (hard cap) |
| `MAX_ALLOC_PCT` | 0.30 (30%) | Maximum allocation per signal as fraction of FIXED_ALLOC (risk concentration cap) |
| `HIGH_CONFIDENCE` | 0.8 | Confidence threshold for max allocation (30%) |
| `MED_CONFIDENCE` | 0.5 | Confidence threshold for medium allocation (20%) |
| `PROFIT_TARGET_HIGH` | 0.025 (2.5%) | Target pair discount for high-confidence signals (confidence >= 0.8) |
| `PROFIT_TARGET_MED` | 0.015 (1.5%) | Target pair discount for medium-confidence signals (confidence >= 0.5) |
| `PROFIT_TARGET_LOW` | 0.010 (1.0%) | Target pair discount for low-confidence signals (confidence < 0.5) |
| `SPIKE_WINDOW` | 400ms | Time window for spike detection |
| `SUSTAIN_WINDOW` | 200ms | Min sustain (dynamic: +300ms if ATR < daily avg) |
| `PING_THRESHOLD` | 100ms (P99) | Max acceptable Binance latency |
| `SPREAD_ABORT` | 0.03 (3%) | Abort if spread wider than this |
| `ADVERSE_THRESHOLD` | 0.003 (0.3%) | Binance price reversal threshold that triggers emergency Leg 2 fill (FOK taker) |
| `ADVERSE_GRACE_PERIOD` | 3000ms (3s) | Grace period after Leg 1 fill before adverse monitoring activates |
| `HEARTBEAT_INTERVAL` | 5000ms | CLOB heartbeat interval (live mode only) |
| `OPERATOR_LATENCY_MAX` | 1000ms | Kill switch: abort if operator matching P99 > 1s |
| `MAX_GAS_PRICE` | 100 gwei | Cap for on-chain operations (approve, redeem, merge) |
| `STALE_EVENT_THRESHOLD` | 500ms | Discard `IngestorEvent` if `now_ms - event.timestamp_ms` exceeds this |

---

## 5. Layer 1: Ingestor

**Purpose**: Maintain persistent WebSocket connections, parse raw data into `IngestorEvent` structs, push to Engine via crossbeam channel.

**Files**: `src/gateway/binance.rs`, `src/gateway/polymarket_ws.rs`, `src/types/market.rs`

### 5.1 Data Sources

| Source | Stream | Data |
|--------|--------|------|
| Binance | `btcusdt@depth20@100ms` | Depth snapshots (bids/asks) |
| Binance | `btcusdt@ticker` | Best bid/ask + last price |
| Polymarket CLOB | Market WS (`wss://ws-subscriptions-clob.polymarket.com/ws/market`) | `book` (full snapshot), `price_change` (level updates), `best_bid_ask` (requires `custom_feature_enabled: true`), `tick_size_change`, `market_resolved`. Subscribe with asset IDs (token IDs) |
| Polymarket CLOB | User WS (`wss://ws-subscriptions-clob.polymarket.com/ws/user`) | `trade` events (MATCHED→MINED→CONFIRMED), `order` events. Authenticated, subscribes by condition_id. **Skipped in simulation mode** |
| Polymarket RTDS (optional) | `wss://ws-live-data.polymarket.com` | `crypto_prices_binance` (btcusdt, ethusdt), `crypto_prices_chainlink` (btc/usd, eth/usd) |
| Gamma API | REST `GET /events?tag_id=102467&active=true&closed=false&limit=10` | Upcoming 15-min market IDs (public, no auth). Tag 102467 = "15M". Client-side filtering by slug prefix (`btc-updown-15m-` / `eth-updown-15m-`) |

### 5.1.1 Polling Schedule & Rate Limit Alignment

Every endpoint the bot uses, with exact intervals and rate limit headroom:

| Endpoint | Method | Interval | Rate Limit | Usage/10s |
|----------|--------|----------|------------|-----------|
| Binance `@depth20@100ms` | WS push | 100ms (server-pushed) | N/A | N/A |
| Binance `@ticker` | WS push | ~1s (server-pushed) | N/A | N/A |
| Polymarket Market WS | WS push | Real-time (server-pushed) | N/A | N/A |
| Polymarket User WS | WS push | Real-time (server-pushed) | N/A | N/A |
| RTDS WS (optional) | WS push | Real-time (server-pushed) | N/A | N/A |
| Gamma `GET /events` | REST GET | Every 600s (10 min) | 500/10s | ~0.02 |
| CLOB `GET /tick-size` | REST GET | Once per market rotation | 200/10s | ~0.02 |
| CLOB `GET /fee-rate` | REST GET | Once per market rotation | 9,000/10s | ~0.02 |
| CLOB `POST /heartbeat` | REST POST | Every 5,000ms (live only) | 9,000/10s | ~2 |
| CLOB `POST /order` | REST POST | On signal (event-driven) | 3,500 burst/10s | ~1-2 |
| CLOB `DELETE /order` | REST DEL | On cancel (event-driven) | 3,000 burst/10s | ~1-2 |
| Gamma `GET /events/{id}` | REST GET | At expiry + every 60s until resolved | 500/10s | ~0.17 |

All REST endpoints operate well under 1% of their rate limits. WebSocket streams are server-pushed and do not count against rate limits.

### 5.2 Spike Detection

1. **Calculate mid-price**: `(best_bid + best_ask) / 2` from `@depth20` updates
2. **Compute rolling EMA-ATR**: 1-minute window, alpha=0.1
   - Fallback: 5-minute EMA if data is sparse
3. **Detect spike**: `|delta| > 1.5 * ATR` within `SPIKE_WINDOW` (400ms)
4. **Sustain check**: Spike must hold for `SUSTAIN_WINDOW` (200-500ms)
   - Dynamic: extend +300ms when ATR < daily average (slower builds in low vol)
5. **Phantom filter**: If price reverts >0.5x delta within 100ms post-sustain → discard
   - Rationale: Phantoms revert before the CLOB reprices, producing false triggers

### 5.3 Market Rotation (Anticipatory Loading)

**Discovery**:
- Query Gamma API every 10 minutes: `GET https://gamma-api.polymarket.com/events?tag_id=102467&active=true&closed=false&limit=10`
- Response includes nested event→market structure with `clobTokenIds` (JSON-encoded string, index 0 = YES, index 1 = NO) and `conditionId`
- Cache upcoming market IDs (condition_id + token_ids) in Redis via `HotStorage::set_active_market()`

**Anticipatory transition** (triggered when current market hits <180s remaining — the same threshold as the no-entry guard):

1. **Stop entering** new positions on current market (no new Leg 1 signals)
2. **Begin preparing** next market in parallel:
   - Pre-subscribe to next market's Market WS channel (subscribe with next token IDs)
   - Fetch and **cache** `tick_size` for next market tokens via `GET /tick-size?token_id={id}` (queried once, cached for duration of market. Only refresh on rare `tick_size_change` WS event at price extremes >0.96 or <0.04)
   - Fetch and **cache** `fee_rate_bps` for next market tokens via `GET /fee-rate?token_id={id}` (used only for emergency taker fee calculations)
   - Warm orderbook cache in Redis for next market (first `book` snapshot arrives via WS)
3. **Continue monitoring** current market for existing positions (Leg 2 hedges, erosion cascade)
4. **At market expiry**: instant switch — `IngestorEvent::MarketRotation` emitted, next market is already warm
   - Zero cold-start delay: orderbook, tick size, fee rate all pre-cached
   - WS subscriptions already active

**Implementation note**: `MarketRotation` is emitted immediately on Gamma API discovery (not deferred to <180s). This ensures the engine can trade from the full market duration.

### 5.4 Guards & Fallbacks

| Guard | Action |
|-------|--------|
| Stale data | Discard any `IngestorEvent` where `now_ms - event.timestamp_ms > STALE_EVENT_THRESHOLD` (500ms). Stale prices lead to false signals. Log discards at `debug` level, count at `info` |
| Ping P99 >100ms (3 consecutive checks) | Enter dormant mode, retry after 30s |
| Allocation exhausted | Pre-check Redis `cumulative_used` before emitting spike event |
| Binance WS disconnect | Exponential backoff reconnect (1s, 2s, 4s); REST fallback during gap |
| Polymarket WS disconnect | Exponential backoff reconnect |
| Heartbeat maintenance (live only) | Dedicated async task sends `POST /heartbeat` every 5s with latest `heartbeat_id`. On 400 response: update `heartbeat_id` from response and retry immediately. **Consecutive failure handling**: if 2 consecutive heartbeats fail, treat as "all orders cancelled by CLOB" — reset executor order state, log alert to Telegram. Re-establish heartbeat before resuming order placement |
| Tick size change | Handle `tick_size_change` WS event (rare, at price extremes >0.96 or <0.04). Primary source is cached value from market rotation query. Update cache on event |
| Matching engine restart | Monday 20:00 ET, ~90s downtime. **5-minute pre-cancel window**: at 19:55 ET, cancel all open orders via `DELETE /cancel-all` and enter cancel-only mode. Resume after successful 200 response to any CLOB endpoint post-window. HTTP 425 during restart → exponential backoff retry (5s, 10s, 20s) |
| Operator P99 > 100ms | Monitor round-trip CLOB API latency. If P99 > 100ms for 3 consecutive checks, enter dormant mode |
| Operator matching delay > 1s | Kill switch: abort all pending trades if operator MATCHED latency > 1s |
| POL balance low | Warn if EOA wallet POL balance < 0.05 POL (needed for on-chain approvals/redemptions) |
| Config validation | On startup, validate all env vars before entering main loop: parse `PRIVATE_KEY` through `build_signer()`, parse all Decimal config values, validate URL formats. Fail fast with descriptive errors |

---

## 6. Layer 2: Engine

**Purpose**: Pull `IngestorEvent` from channel, maintain `MarketState`, evaluate arbitrage conditions, emit `TradeSignal` to Executor.

**Files**: `src/engine/strategy.rs`, `src/types/market.rs`, `src/types/order.rs`

### 6.1 Market State Management

The `StrategyEngine` maintains a `MarketState` struct (defined in `src/types/market.rs`):
- `poly_book: Option<OrderBook>` — latest Polymarket snapshot
- `binance_price: Option<Decimal>` — latest Binance mid-price
- `active_condition_id` / `active_token_id` — current 15-min market
- `tick_size: Decimal` — current market tick size (dynamic)
- `fee_rate_bps: u16` — current taker fee rate in basis points
- `last_update_ms` — staleness tracking
- `leg1_state` / `leg2_state` — tracks order lifecycle: `None | Posted { order_id, price, size, timestamp_ms } | Filled { order_id, price, size, timestamp_ms }`
- `available_capital` — initialized to `FIXED_ALLOC` (100 USDC) at engine startup
- `cumulative_used` — tracks capital allocated within current market

Updates via `on_event()` (handles all 11 `IngestorEvent` variants).

### 6.2 Entry Signal Generation (Leg 1)

**Self-gating**: `evaluate(&mut self)` mutates engine state after generating a signal to prevent duplicate signals from the same spike. On signal emission:
- `spike_detected` → `false` (clears the trigger)
- `leg1_state` → `Posted { ... }` (blocks further Leg 1 signals while trade is active)
- `cumulative_used` += allocated amount

Only one trade can be in progress at a time (Leg 1 + Leg 2). After the trade completes (both legs Filled), state resets to allow the next trade — if capital remains within the market.

**Pre-entry checks** (abort if any fail):

| Check | Condition | Rationale |
|-------|-----------|-----------|
| Spread | > `SPREAD_ABORT` (3%) | Too wide for profitable arb |
| Expiry | < 180s remaining | Risk of liquidity evaporation (also triggers anticipatory market loading) |
| Balance | < required size | Insufficient funds |
| Liquidity | < 15% of required depth | Can't fill meaningfully |
| Tick size | Price doesn't conform to market tick size | Order will be REJECTED by CLOB |
| Active trade | `leg1_state != None` | A trade is already in progress |

**Sizing formula** (confidence-weighted — see Section 6.4):
```
alloc = confidence_alloc(signal)  // based on confidence score, capped at MAX_ALLOC_PCT * FIXED_ALLOC
entry_size = min(
    alloc / best_bid,
    opposing_depth / (1 - best_bid - target_profit)
)
```

**Leg 1 Bidding Strategy**:
1. Calculate initial bid: `round_to_tick(best_bid + tick_size, tick_size)` — top of bid side
2. **Smart outbidding**: if depth wall detected at or above our bid (single price level > 4x average depth), outbid by 1 tick to reclaim top-of-book queue priority
3. **Cap**: bid must not exceed break-even price `(1.0 - estimated_leg2_target - tick_size)`. If outbidding would push past this cap, keep our original price and accept lower queue priority
4. Submit: `GTC, post_only=true`
5. If post-only **rejected** (would cross spread): no trade for this signal — the spread is too tight for profitable entry

Price must conform to the market's tick size or the order is REJECTED. Tick size is cached at market rotation (see Section 5.3). Handle `tick_size_change` WebSocket events as rare edge case.

**Smart outbidding** (replaces bot detection cooldowns):
- Monitor depth walls: single price level > 4x average depth → competitor wall
- Post-only provides **natural protection**: if a competitor outbids us and our order doesn't fill, we incur zero cost. No cooldowns or aborts needed.
- Log all wall detections and outbid events for post-trade analytics
- Flag trades where walls were detected as `bot_contested: bool`

### 6.3 Hedge Signal Generation (Leg 2)

**Live mode**: Triggered when Leg 1 fill is confirmed (via User WS `trade` event with status MATCHED, forwarded through Ingestor as `TradeStatusUpdate`).

**Simulation mode**: Triggered when `advance_simulation()` transitions Leg 1 from `Posted` → `Filled` (see Section 6.6).

**Self-gating**: `evaluate_leg2()` sets `leg2_state = Posted { ... }` at all signal emission points (normal erosion and all 3 emergency paths) to prevent duplicate Leg 2 signals.

**Break-even calculation** (both legs post-only, zero fee in normal flow):
```
break_even = 1.0 - entry_avg
target_profit = confidence_tier_target(confidence)  // 2.5%, 1.5%, or 1.0%
target = 1.0 - target_profit - entry_avg
```

No taker fee adjustment in normal flow — both legs are maker. Taker fee (`0.25 * (p*(1-p))^2`) is only factored into break-even during emergency fallback (see Section 7.2). Fee rate (`fee_rate_bps`) is cached at market rotation for emergency calculations only.

**Leg 2 Bidding Strategy**:
1. Calculate target price: `round_to_tick(1.0 - target_profit - leg1_fill_price, tick_size)`
2. **Smart outbidding**: if depth wall detected at or near target price (single price level > 4x average depth), outbid by 1 tick to improve queue priority
3. **Cap**: pair cost (`leg1_price + leg2_bid`) must stay below `1.0` (break-even). If outbidding would breach break-even, keep target price.
4. Submit: `GTC, post_only=true`
5. If post-only rejected (would cross spread): erode target by 1 tick, retry

**Partial fills**: Adjust proportionally — hedge only the filled portion.

**Timers**:
- Start 10s hedge timer on Leg 1 fill
- Extend to 15s in low ATR conditions (slower book adjustment)

**Reversal check**: If Binance shows >0.05% opposite move within 100ms → emit cancel signal.

**Adverse Movement Protocol** (emergency failsafe — taker fees apply):

Monitor Binance price continuously after Leg 1 fill. **Adverse monitoring activates after `ADVERSE_GRACE_PERIOD` (3s)** — post-fill price oscillation is normal and should not trigger emergency action.

After grace period expires:

1. **Price moves AGAINST Leg 1 direction** by > `ADVERSE_THRESHOLD` (0.3%):
   - The hedge side is getting CHEAPER (this is good for us)
   - Immediately submit aggressive Leg 2 at current `best_ask` via FOK (taker fee applies)
   - Accept taker fee — this is likely the best hedge price we'll get before further reversal
2. **Price continues IN Leg 1 direction** (making hedge MORE expensive):
   - Erosion cascade handles this (Section 7.2) — all post-only until emergency deadline
   - **HARD STOP**: if `hedge_cost > (1.0 - entry_price)`, position is underwater
     - Force fill via FOK anyway — losing the taker fee + small loss is better than risking up to 50% loss on an unhedged position
3. **Maximum unhedged exposure time**: absolute deadline is `market_expiry - 90s`
   - Since no new Leg 1 entries are allowed at <180s remaining, the latest a Leg 1 can fill is ~180s before expiry
   - Between hedge timer expiry and this absolute deadline: progressive post-only erosion (Section 7.2)
   - At the deadline: emergency FOK force fill regardless of price (taker fee applies)
   - Worst case: 180s - 90s = **90s maximum erosion window**

### 6.4 Allocation Tracking (Confidence-Weighted)

**Signal Confidence Scoring**:
```
confidence = 0.4 * min(spike_magnitude / ATR, 1.0)
           + 0.2 * min(sustained_duration / SUSTAIN_WINDOW, 1.0)
           + 0.2 * min(poly_book_depth / avg_book_depth, 1.0)
           + 0.2 * (time_remaining / 900.0)
```

**Allocation per signal**:
```
if confidence >= HIGH_CONFIDENCE (0.8):  alloc = FIXED_ALLOC * 0.30  (= $30)
elif confidence >= MED_CONFIDENCE (0.5): alloc = FIXED_ALLOC * 0.20  (= $20)
else:                                    alloc = FIXED_ALLOC * 0.10  (= $10)
```

**Constraints**:
- `cumulative_used <= FIXED_ALLOC` (hard cap: $100 per market)
- No fixed trade count limit — number of trades varies by confidence and market conditions
- Per-signal maximum: `MAX_ALLOC_PCT * FIXED_ALLOC` ($30) — risk concentration cap
- No minimum allocation floor — both legs are maker (zero fees), so even small positions are fee-efficient
- Track `locked_in_resolution` — capital awaiting UMA resolution from previous markets
- `available_capital = total_capital - locked_in_resolution`
- Available allocation per market = `min(FIXED_ALLOC, available_capital) - cumulative_used`
- **Skip signal** if `available_capital < alloc` for the confidence tier — do not enter with insufficient capital
- Reset `cumulative_used` on market rotation

### 6.5 Multiple Trades Per Market

The engine supports multiple trades within a single 15-minute market window, subject to:
- **One trade at a time**: Only one Leg 1 + Leg 2 pair can be in progress. The `leg1_state` guard in `evaluate()` blocks new signals while a trade is active
- **Capital cap**: `cumulative_used` tracks total allocated capital. New trades are blocked when remaining allocation is insufficient
- **State reset on completion**: When both legs reach `Filled` state, `advance_simulation()` (sim) or the executor (live) resets `leg1_state`, `leg2_state`, and `erosion` to `None` — allowing the next spike to trigger a new trade
- **`cumulative_used` persists**: Capital allocation is NOT reset on trade completion, only on market rotation. This ensures the per-market cap ($100) is respected across multiple trades

### 6.6 Simulation Fill State Machine (`advance_simulation()`)

In simulation mode, the engine calls `advance_simulation()` on each event to simulate fill progression without User WS feedback.

**Leg 1: Posted → Filled**
- When `leg1_state == Posted { price, .. }` and the current orderbook has `best_ask <= posted_bid_price`
- Transitions to `Filled`, calls `init_erosion()` to start the Leg 2 cascade
- `init_erosion()` computes confidence, profit tier, and creates an `ErosionState` with target price, step size, and timing

**Leg 2: Posted → Filled**
- When `leg2_state == Posted { price, .. }`:
  - Normal fill: `best_ask <= posted_price` (target reached via erosion or initial target)
  - Emergency fill: `emergency_submitted == true` on the erosion state (FOK crosses spread)

**Trade Completion**
- When both `leg1_state == Filled` and `leg2_state == Filled`:
  - Resets `leg1_state` → `None`, `leg2_state` → `None`, `erosion` → `None`
  - `cumulative_used` is NOT reset — capital stays allocated within the market
  - Next spike can trigger a new trade if capital remains

**No-op when idle**: If no trade is in progress (`leg1_state == None`), the method returns immediately.

---

## 7. Layer 3: Executor

### 7.1 Live Mode — Order Submission

**Purpose**: Receive `TradeSignal`, sign and submit orders via CLOB API, monitor fills via User WS, enforce hedges, manage risk, persist data.

**Files**: `src/gateway/polymarket.rs`, `src/utils/signing.rs`, `src/storage/hot.rs`, `src/storage/cold.rs`

1. Receive `TradeSignal` from channel
2. Use cached `tick_size` from market rotation (see Section 5.3). Only refresh on `tick_size_change` WS event
3. Use cached `fee_rate_bps` from market rotation (only needed for emergency taker fee calculations)
4. Two-step order creation:
   - **Sign**: `create_order(token_id, price, side, size, tick_size, neg_risk, fee_rate_bps)`
     - EIP-712 signing via `build_signer()` (`src/utils/signing.rs`)
     - Signature type: **EOA (0)** — signer address = maker address = funder address
   - **Submit**: `post_order(signed_order, order_type=GTC, post_only=true)`
     - HTTP POST to `https://clob.polymarket.com/order`
     - For batch: `post_orders([...])` to `/orders` (max 15 per request)
5. Track fill status via User WS channel:
   - Subscribe by condition_id to `wss://ws-subscriptions-clob.polymarket.com/ws/user`
   - Listen for `trade` events: MATCHED → MINED → CONFIRMED
   - On RETRYING: operator is resubmitting on-chain; wait for resolution
   - On FAILED: log, evaluate re-entry
6. Maintain heartbeat loop (see Section 5.4 for full protocol)
7. Idempotency: Track order nonces to prevent double-fills on resubmits

### 7.2 Erosion Cascade (Leg 2 Fill Strategy)

When Leg 2 doesn't fill at the initial target price, progressively erode the profit target. **All normal erosion steps are post-only (maker, zero fee)**. Emergency taker fills are reserved for deadlines and adverse movement only.

**Proportional erosion formula:**
```
step_size = initial_profit_target / 5   // 20% of initial margin per step
```
Each erosion step raises the Leg 2 bid by `step_size` (always `+delta`, toward the ask). The sign is the same regardless of whether Leg 2 is YES or NO — we are always raising our bid to attract counterparty flow.

| Confidence Tier | Initial Target | step_size | Erosion path (profit remaining) |
|-----------------|---------------|-----------|----------------------------------|
| HIGH (≥0.8) | 2.5% | 0.005 (0.5%) | 2.5% → 2.0% → 1.5% → 1.0% → 0.5% → break-even |
| MED  (≥0.5) | 1.5% | 0.003 (0.3%) | 1.5% → 1.2% → 0.9% → 0.6% → 0.3% → break-even |
| LOW  (<0.5) | 1.0% | 0.002 (0.2%) | 1.0% → 0.8% → 0.6% → 0.4% → 0.2% → break-even |

All tiers reach break-even in exactly 5 steps × 2s = **10 seconds**. After the hedge timer window, erosion continues at the same cadence (every 2s, post-only) until the absolute emergency deadline.

| Time | Action | Price Adjustment | Order Type |
|------|--------|-----------------|------------|
| 0-2s | Post at initial target price | initial target | GTC, post_only=true (maker, $0 fee) |
| 2-4s | Erode: step 1 | +step_size | GTC, post_only=true (maker, $0 fee) |
| 4-6s | Erode: step 2 | +step_size | GTC, post_only=true (maker, $0 fee) |
| 6-8s | Erode: step 3 | +step_size | GTC, post_only=true (maker, $0 fee) |
| 8-10s | Erode: step 4 | +step_size | GTC, post_only=true (maker, $0 fee) |
| 10s → `market_expiry - 90s` | Continue erosion at same cadence | +step_size every 2s | GTC, post_only=true (maker, $0 fee) |
| `market_expiry - 90s` | **Emergency fill** | Market price | **FOK (taker fee applies)** |
| Adverse movement (post-grace) | **Binance reversal > `ADVERSE_THRESHOLD`** | Current best_ask | **FOK (taker fee applies)** |
| Break-even breach | **Hedge cost ≥ (1.0 - entry_price)** | Any available price | **FOK (taker fee applies)** |

Normal erosion is all post-only, zero fee. Break-even during normal erosion is simply `1.0 - leg1_price`. Taker fee calculation (`fee = 0.25 * (p*(1-p))^2` per share) applies only to the emergency rows. The fee curve determines whether an emergency taker fill is still profitable — at p=0.50, the taker fee is 1.56% effective.

**Emergency FOK size cap**: When submitting emergency taker orders (deadline, adverse, break-even breach), cap the order size to available depth within slippage tolerance: `fok_size = min(remaining_position, ask_depth_within_2_ticks)`. This prevents eating through multiple price levels on thin books near expiry. If insufficient depth exists, split into the fillable amount and accept the remainder as unhedged exposure through resolution.

Rationale: All tiers exhaust their margin in the same time window (10s primary, extended post-timer). High-confidence trades get larger step sizes proportional to their wider margin. Low-confidence trades take smaller steps — less margin, but the same 10s budget to find a fill before emergency triggers.

### 7.3 Simulation Mode — SimulationExecutor

**Purpose**: Receive `TradeSignal` from Engine, report via Telegram + QuestDB. The engine handles fill simulation internally via `advance_simulation()`.

**File**: `src/executor/simulation.rs`

**Responsibilities**:
1. Receive `TradeSignal` from Engine via crossbeam channel
2. Forward events to `TelegramReporter` for real-time alerts
3. Log all signals to QuestDB `simulated_trades` table
4. Track virtual positions, running PnL, and capital locked in pending resolutions
5. Handle market rotation events (reset state, emit market summaries)
6. Emit shutdown summary on exit

**No real execution**:
- No wallet private key usage or EIP-712 signing
- No CLOB API order submissions
- No heartbeat loop (no open orders to protect)
- No Polygon transactions

**Taker Fee Calculation (Emergency Only)**:

Both legs are post-only (maker, zero fee) in normal flow. Taker fees only apply during emergency scenarios. The simulation applies fees accurately when emergency fills occur:

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

### 7.4 Risk Oversight (Live Mode)

- Monitor User WS channel for trade status progression (MATCHED→MINED→CONFIRMED)
- **Toxicity**: jitter >3x P99 baseline → smart outbidding paused, `cancel_all()` via `DELETE /cancel-all` if no fills
- **Trade status tracking**:
  - MATCHED: Trade matched off-chain, sent to operator for on-chain settlement
  - MINED: Transaction mined on Polygon, awaiting finality
  - CONFIRMED: Trade finalized successfully (terminal)
  - RETRYING: Operator handling resubmission (revert or reorg) — bot waits
  - FAILED: Trade failed permanently (terminal) — log, evaluate re-entry
- **On-chain fallback**: Only needed for cancellations (call `cancelOrder()` on Exchange contract `0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E`) and redemptions (call `redeemPositions()` on CTF contract). Use if CLOB API is unavailable. EOA wallet pays POL gas for these on-chain transactions.
- **Operator kill switch**: If operator MATCHED→CONFIRMED P99 latency > 1s, abort all pending trades
- **Heartbeat monitoring**: Dedicated async task, logs warnings if heartbeat round-trip > 2s
- **Tiered drawdown kill switch**:
  - **WARN** at 3% daily loss → log warning, send Telegram alert, continue trading
  - **PAUSE** at 5% daily loss → cancel all open orders, stop new entries, send Telegram alert. Resume after 30-min cooldown
  - **HALT** at 8% daily loss → cancel all orders, shut down executor entirely, send Telegram alert. Requires manual restart
- **Adverse movement monitoring**: Continuous Binance price tracking after Leg 1 fill. Triggers emergency hedge per Section 6.3 protocol.
- **UMA dispute monitoring**: After market expiry, poll resolution status via Gamma `GET /events/{id}`. If market enters dispute state, send Telegram alert with locked capital amount. Track `locked_in_resolution` and reduce available capital accordingly. DVM escalation remains excluded from bot scope.

### 7.5 Market Rotation & Redemption

- Anticipatory loading begins at <180s remaining (see Section 5.3)
- On market expiry: query Gamma `GET /events/{market_id}` for resolution status
- Resolution uses **UMA Optimistic Oracle**:
  1. Proposer submits outcome with bond (~$750 USDC.e)
  2. 2-hour challenge period follows
  3. If undisputed: market resolves (~2h after proposal)
  - DVM dispute escalation (4-6 day vote) is excluded from bot scope. If a dispute occurs, capital remains locked until resolution. The bot treats this as extended lock time, tracked via `locked_in_resolution`.
- **Capital is LOCKED** between market end and resolution confirmation
  - For 15-min markets, expect ~2h minimum lock time
  - Factor this into capital allocation: `available_capital = total - locked_in_pending_resolution`
- Redeem winning tokens for $1.00 via `redeemPositions()` on CTF contract (`0x4D97DCd97eC945f40cF65F87097ACe5EA0476045`)
  - Parameters: collateralToken (USDC.e), parentCollectionId (bytes32(0)), conditionId, indexSets ([1, 2])
  - EOA wallet pays POL gas for on-chain redemption (~$0.01 per tx on Polygon)
  - **Gas cap**: Check current gas price before submitting. If gas > `MAX_GAS_PRICE` (100 gwei), defer redemption and retry next cycle
- Switch to next market (already warm from anticipatory loading)
- Reset `cumulative_used` allocation counter

---

## 8. Telegram Integration

**File**: `src/reporting/telegram.rs`

### Setup

FaCaiBot sends real-time simulation alerts to Telegram via the Bot API.

**Setup Instructions:**
1. Create a bot via `@BotFather` or use the existing `@f4c4ibot`
2. Copy `.env.example` to `.env` and fill in Telegram credentials:
   ```env
   MODE=simulation
   TELEGRAM_BOT_TOKEN=<your-bot-token-from-botfather>
   TELEGRAM_CHAT_ID=<your-chat-id-from-userinfobot>
   ```
3. To get credentials:
   - **Bot Token**: Message `@BotFather` → `/mybots` → select your bot → view token
   - **Chat ID**: Message `@userinfobot` on Telegram → it replies with your user ID
4. Start a chat with the bot on Telegram (send any message) so it can send alerts

**Important**: Never commit `.env` to git — it contains sensitive credentials.

### Implementation

Uses `hyper` + `tokio-rustls` for direct HTTPS POST to the Telegram Bot API (no teloxide dependency). Fire-and-forget async — never blocks the executor waiting for Telegram.

**Rate limiting**: `AtomicU64` tracks `last_send_ms`. Messages are dropped silently if sent within 5 seconds of the previous message. This prevents Telegram 429 errors during burst signal generation.

### Message Types

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

#### Tier 2: Market Summary (per 15-min market expiry)

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

Trade 1: conf=0.82 target=2.5% alloc=$30 → YES@0.481 + NO@0.494 = $0.975 → net +$0.025 (+2.5%)
Trade 2: conf=0.54 target=1.5% alloc=$20 → YES@0.512 + NO@0.479 = $0.991 → net +$0.009 (+0.9%)

Allocation used: $50 / $100 (50%)
Taker fees paid: $0.00 (both legs maker)
Net market PnL: +$0.034
Capital locked in resolution: $20.00
```

#### Tier 3: Session Summary (hourly + on shutdown)

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
Average net profit: +1.7% per trade
Best trade: +2.5% (market #12341, conf=0.91, both maker)
Worst trade: -0.3% (market #12343, conf=0.42, emergency taker hedge)

Unfilled signals: 13
  - 8x post-only bid not matched (normal — zero cost)
  - 3x insufficient liquidity
  - 2x spread too wide

Capital locked in resolution: $40.00 (2 markets pending)
Virtual balance: $100.337 (started: $100.00)
```

### Message Formatting

- Use Telegram HTML formatting for readability
- Prefix each message type with a distinct header for quick scanning
- Include timestamps in UTC
- All prices in USD with 3 decimal places
- Percentages with 1 decimal place
- Show gross and net profit. In normal flow (both legs maker) these are equal

---

## 9. Risk Management

| Risk | Trigger | Mitigation |
|------|---------|------------|
| **Legging** (one-sided exposure) | Leg 1 fills, Leg 2 price moves away | Adverse movement protocol (post-grace): force FOK hedge on reversal > 0.3%. Break-even breach: force fill at any price. Absolute deadline: `market_expiry - 90s`. All erosion steps are post-only until emergency |
| **Taker fees (emergency only)** | Emergency hedge via FOK (deadline, adverse movement, break-even breach) | Fee curve: max 1.56% at p=0.50. Both legs are post-only (zero fee) in normal flow. Taker fee only applies to emergency FOK fills. Fee formula for emergency calc: `0.25 * (p*(1-p))^2` |
| **Competing bots** (depth walls) | Depth walls > 4x average at a single price level | Smart outbidding: outbid wall by 1 tick (capped at break-even). Post-only provides natural protection — if outbid, order doesn't fill = zero cost. Log for analytics |
| **Operator trust** | CLOB operator matching delay or failure | Monitor MATCHED→CONFIRMED latency via User WS. Kill switch if P99 > 1s. On-chain cancel fallback via Exchange contract |
| **Resolution capital lock** | UMA 2-hour challenge period | Track capital locked in pending resolutions. Reduce available allocation accordingly. Alert if total locked > 50% of capital. Dispute alert via Telegram |
| **Heartbeat failure** (live only) | System lag prevents heartbeat delivery | Dedicated heartbeat async task with highest priority. If heartbeat fails, all orders auto-cancelled by CLOB — re-establish session before resuming |
| **Tick size rejection** | Order price doesn't conform to tick size | Cache tick size per token, handle `tick_size_change` WS events. Validate all prices before signing |
| **Matching engine restart** | Monday 20:00 ET, ~90s downtime | 5-minute pre-cancel window (19:55 ET): cancel all orders, enter cancel-only mode. Retry with exponential backoff on HTTP 425 |
| **Trade failure (RETRYING/FAILED)** | Operator settlement fails on-chain | Monitor trade status via User WS. RETRYING = operator handles resubmission. FAILED = permanent; log and evaluate re-entry |
| **Thin liquidity** | Reduced depth near expiry | Depth check pre-entry (min 15% required). No entries <180s expiry. Position sizing constrained to available depth |
| **Stale data** | Binance/Polymarket event arrives with timestamp > 500ms old | Discard event silently (see Section 5.4). Count discards for monitoring |
| **Phantom bids** | Brief spike reverts before CLOB reprices | Sustain filter (200-500ms) + reversion check (>0.5x delta in 100ms) |
| **Oracle mismatch** | Binance decouples from Chainlink reference | Cap per-signal exposure via confidence weighting; post-resolution logging; optional RTDS Chainlink feed for divergence monitoring |
| **Double-fill on resubmit** | Resubmitted order fills twice | Use unique nonces per order. Track all order IDs. Verify fill status before resubmitting |
| **Slippage (emergency only)** | Emergency FOK fills at worse-than-expected price | Post-only orders have exact price (zero slippage risk). Slippage only possible on emergency FOK fills. Break-even cap prevents catastrophic overpay |
| **Rate limits** | Excessive API calls | Cloudflare throttling (delayed, not rejected). Bot budget is well within limits. Monitor for latency increase as throttling indicator |
| **Signal flooding** | Engine generates duplicate signals from same spike | Engine self-gating: `evaluate(&mut self)` clears spike flag and sets `leg1_state = Posted` after signal emission. Only one trade at a time |
| **Telegram flooding** | Burst signals overwhelm Telegram API | 5-second rate limiter (`AtomicU64`). Messages dropped silently when rate-limited |

---

## 10. Edge Cases & Error Handling

### Edge Cases

| Scenario | Handling |
|----------|---------|
| Low volatility | ATR scales spike threshold down; extend timers +20s; confidence scores will be lower → smaller allocations |
| High volatility | Thresholds scale up; confidence scores may be higher → larger allocations but still capped at MAX_ALLOC_PCT * FIXED_ALLOC ($30) and FIXED_ALLOC ($100) |
| Multiple spikes in one market | Confidence-weighted allocation ensures high-confidence signals get more capital; cumulative cap ($100) prevents overexposure. Multiple trades allowed (1 at a time) after state reset |
| Bot depth wall | Smart outbidding: outbid by 1 tick if profitable. If outbid and not filled, zero cost. Flag as "bot-contested" for analytics |
| Adverse price movement (Leg 2) | After ADVERSE_GRACE_PERIOD (3s): if Binance reverses > ADVERSE_THRESHOLD, force immediate FOK hedge (taker fee applies). If hedge cost > break-even: force fill anyway to cap losses |
| Trade RETRYING status | Operator is resubmitting on-chain. Wait — do NOT manually resubmit. Monitor via User WS |
| Trade FAILED status | Log permanently failed trade. Do NOT resubmit with same nonce. Create new order if re-entry warranted |
| Over-hedging (excess shares filled) | Sell excess at market post-resolution; monitor frequency to tune sizes |
| Matching engine restart | Detect via HTTP 425. Pre-cancel orders before Monday 20:00 ET window. Retry with exponential backoff during ~90s downtime |
| Heartbeat desync | On 400 response: update heartbeat_id from server response and retry immediately. Never let heartbeat lapse — all orders auto-cancelled |
| Tick size change mid-trade | Handle `tick_size_change` WS event. Re-validate pending order prices. Cancel and resubmit orders at non-conforming prices |
| UMA resolution dispute | Dispute extends lock time. Bot does not actively manage disputes. Capital remains locked, tracked in `locked_in_resolution`, reduces available allocation |
| Market transition | Anticipatory loading at <180s ensures zero cold-start delay for next market |

### Error Handling

| Error | Response |
|-------|----------|
| API throttling (Cloudflare) | Requests are delayed, not rejected. Monitor for latency increase. If latency > 2x baseline, reduce request rate |
| HTTP 425 (matching engine restart) | Exponential backoff: 5s, 10s, 20s. Do not submit orders until 200 response |
| Heartbeat 400 | Update heartbeat_id from response body and retry immediately |
| Order reject (tick size) | Catch `400 order breaks minimum tick size rule`. Re-fetch tick size via `GET /tick-size?token_id={id}`, update cache, round price to new tick, retry once. If second attempt fails, abort signal |
| Order reject (general) | Log error; retry with incremented price if slippage-related |
| Trade FAILED | Log permanently failed trade. Do NOT resubmit — nonce is burned. Create new order if re-entry warranted |
| WS disconnect | Reconnect with exponential backoff; load state from Redis |
| Low balance | Abort trade sequence; log alert |
| Low POL balance | Warn via logs; on-chain operations (redemptions) will fail without POL gas |
| Unexpected fill | Immediately recalculate exposure; adjust hedge signal |
| System crash | On restart (`cargo run`), recover from Redis snapshots |

---

## 11. QuestDB Storage

**File**: `src/storage/cold.rs`

All QuestDB tables use ILP (InfluxDB Line Protocol) for ingestion via port 9009. Tables are partitioned by DAY with 24-hour rolling retention.

### Tables

**Table: `binance_ticks`** — Binance price feed history

| Column | Type | Description |
|--------|------|-------------|
| symbol | symbol | "btcusdt" or "ethusdt" |
| bid | f64 | Best bid |
| ask | f64 | Best ask |
| mid | f64 | Mid-price |
| timestamp | timestamp | Tick time |

**Table: `poly_book_snapshots`** — Polymarket orderbook snapshots (every 5s)

| Column | Type | Description |
|--------|------|-------------|
| token_id | symbol | Polymarket token ID |
| best_bid | f64 | Best bid price |
| best_ask | f64 | Best ask price |
| bid_depth | f64 | Total bid depth (USDC) |
| ask_depth | f64 | Total ask depth (USDC) |
| spread | f64 | Spread percentage |
| timestamp | timestamp | Snapshot time |

**Table: `trade_signals`** — Every signal the engine generates

| Column | Type | Description |
|--------|------|-------------|
| market_id | symbol | Polymarket condition ID |
| direction | symbol | "YES" or "NO" |
| action | symbol | "entered", "unfilled_postonly", "aborted_spread", "aborted_liquidity", "skipped_confidence" |
| confidence | f64 | Confidence score (0-1) |
| spike_magnitude | f64 | Spike size relative to ATR |
| atr | f64 | Current ATR value |
| book_depth | f64 | Polymarket book depth at signal time |
| time_remaining | i64 | Seconds to market expiry |
| alloc_amount | f64 | Allocated USDC for this signal |
| timestamp | timestamp | Signal time |

**Table: `executed_trades`** — Live mode trade logs

| Column | Type | Description |
|--------|------|-------------|
| market_id | symbol | Polymarket condition ID |
| direction | symbol | "YES" or "NO" |
| leg1_price | f64 | Entry price |
| leg1_size | f64 | Entry size |
| leg2_price | f64 | Hedge price |
| leg2_size | f64 | Hedge size |
| pair_cost | f64 | Total cost of paired position |
| gross_profit | f64 | Gross profit/loss |
| net_profit | f64 | Profit after fees |
| timestamp | timestamp | Trade execution time |

**Table: `simulated_trades`** — Simulation mode trade logs

| Column | Type | Description |
|--------|------|-------------|
| market_id | symbol | Polymarket condition ID |
| direction | symbol | "YES" or "NO" |
| profit_tier | symbol | "HIGH" (2.5%), "MED" (1.5%), or "LOW" (1.0%) |
| resolution | symbol | Market outcome ("YES"/"NO") |
| leg1_price | f64 | Simulated entry price |
| leg1_size | f64 | Simulated entry size |
| leg2_price | f64 | Simulated hedge price |
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
| alloc_amount | f64 | USDC allocated to this trade |
| adverse_hedge | bool | Whether hedge was triggered by adverse price movement |
| bot_contested | bool | Whether depth wall was detected |
| resolution_delay_ms | i64 | Time between market end and resolution confirmation |
| timestamp | timestamp | Trade execution time |

**Table: `price_divergence`** — Binance vs Chainlink divergence (when `RTDS_ENABLED=true`)

| Column | Type | Description |
|--------|------|-------------|
| symbol | symbol | "btcusdt" or "ethusdt" |
| binance_price | f64 | Binance spot mid-price |
| chainlink_price | f64 | Chainlink feed price (via RTDS) |
| divergence_pct | f64 | (binance - chainlink) / chainlink * 100 |
| timestamp | timestamp | Observation time |

### Retention & Pruning

- All tables partitioned by DAY
- **Automated pruning**: Background `tokio::interval` task runs every hour, executes `ALTER TABLE {table} DROP PARTITION WHERE timestamp < dateadd('h', -24, now())` via QuestDB Postgres wire protocol (port 8812)
- Estimated storage: ~50MB/day (ticks dominate at ~100ms intervals)
- Batch flush: every 1000 ticks for `binance_ticks`
- See `queries.sql` for common analytics queries

---

## 12. Performance Targets

### Delta-T Pipeline (P99 targets)

| Stage | Target | Component |
|-------|--------|-----------|
| Signal ingest (Binance WS push) | 50ms jitter | `src/gateway/binance.rs` |
| Event parse + emit | 1ms | Ingestor → crossbeam |
| ATR compute + confidence score + signal gen | 1-2ms | `src/engine/strategy.rs` |
| Order sign (EIP-712) | 5ms | `src/utils/signing.rs` |
| API submit (HTTP POST to CLOB) | 50ms | Executor → `clob.polymarket.com` |
| Operator matching + WS fill confirm | 200ms | CLOB operator → User WS MATCHED event |
| On-chain settlement (operator → Polygon) | 300ms | Operator responsibility (MINED → CONFIRMED) |
| **Total (signal to MATCHED confirm)** | **<350ms** | Bot's controllable latency ends at API submit |

### Success Metrics

| Metric | Target |
|--------|--------|
| Leg 1 fill rate | 30-50% of signals (post-only may not fill; unfilled = zero cost) |
| Win rate (hedged trades) | 85-95% |
| Average net profit per trade | >1.0% (both legs maker = net equals gross in normal flow) |
| System uptime | >99.5% |
| P99 signal-to-MATCHED | <350ms |
| Operator MATCHED latency P99 | <200ms |
| Heartbeat success rate | >99.99% |
| Monthly drawdown | <3% |
| Emergency taker fills | <15% of Leg 2 fills (most should complete as maker) |
| Contested rate (live-mode only) | Track % of posted orders outbid within 1s. If >70%, re-evaluate strategy viability |

### API Rate Limits

All Polymarket rate limits are enforced via Cloudflare throttling (requests delayed/queued, not rejected). Limits reset on sliding time windows.

#### Gamma API (`https://gamma-api.polymarket.com`)

| Endpoint | Limit |
|----------|-------|
| General | 4,000 req / 10s |
| `/events` | 500 req / 10s |
| `/markets` | 300 req / 10s |
| `/markets` + `/events` combined | 900 req / 10s |
| `/public-search` | 350 req / 10s |

#### CLOB API (`https://clob.polymarket.com`)

| Endpoint | Limit |
|----------|-------|
| General | 9,000 req / 10s |
| `/book` | 1,500 req / 10s |
| `/price` | 1,500 req / 10s |
| `/midpoint` | 1,500 req / 10s |
| `/prices-history` | 1,000 req / 10s |
| Tick size | 200 req / 10s |
| `GET` balance allowance | 200 req / 10s |
| Trades/orders query | 900 req / 10s |
| API key endpoints | 100 req / 10s |

#### CLOB Trading (burst + sustained)

| Endpoint | Burst Limit | Sustained Limit |
|----------|-------------|-----------------|
| `POST /order` | 3,500 req / 10s | 36,000 req / 10 min |
| `DELETE /order` | 3,000 req / 10s | 30,000 req / 10 min |
| `POST /orders` (batch) | 1,000 req / 10s | 15,000 req / 10 min |
| `DELETE /orders` (batch) | 1,000 req / 10s | 15,000 req / 10 min |
| `DELETE /cancel-all` | 250 req / 10s | 6,000 req / 10 min |
| `DELETE /cancel-market-orders` | 1,000 req / 10s | 1,500 req / 10 min |

#### Data API (`https://data-api.polymarket.com`)

| Endpoint | Limit |
|----------|-------|
| General | 1,000 req / 10s |
| `/trades` | 200 req / 10s |
| `/positions` | 150 req / 10s |

#### Other

| Endpoint | Limit |
|----------|-------|
| General rate limiting | 15,000 req / 10s |
| Health check (`/ok`) | 100 req / 10s |

All endpoints operate under 1% of their rate limits. Primary indicator of approaching limits is latency increase due to Cloudflare throttling.

---

## 13. Deployment Stages & Verification

### Stage 1: Local Machine (Simulation)

1. `docker-compose up -d` — start Redis + QuestDB
2. Copy `.env.example` → `.env`, set `MODE=simulation`, fill Telegram credentials
3. `cargo build --release` — startup validates all config. Fix any errors before proceeding
4. `cargo run` — bot connects to Binance WS, Polymarket Market WS, discovers markets via Gamma API

**Verification (Day 1-2)**:
- [ ] Bot connects to Binance WebSocket and receives ticks
- [ ] Bot connects to Polymarket Market WS and receives orderbook updates
- [ ] Market WS subscription with `custom_feature_enabled: true` receives `best_bid_ask` events
- [ ] Stale event filter working: events with `timestamp > 500ms` old are discarded
- [ ] Config validation passes at startup
- [ ] Telegram bot sends startup message
- [ ] Spike detection fires on real Binance moves
- [ ] Phantom filter correctly discards brief reversions
- [ ] Engine self-gating prevents duplicate signals (1 signal per spike)
- [ ] `advance_simulation()` transitions Leg 1 Posted → Filled when book conditions met
- [ ] Erosion cascade fires at 2s intervals for Leg 2
- [ ] Trade completes and state resets, allowing next trade
- [ ] Multiple trades per market window (when capital available)

**Verification (Day 2-3)**:
- [ ] Simulated Leg 1 post-only fills match orderbook state
- [ ] Simulated Leg 2 fills with erosion cascade (all post-only until emergency)
- [ ] Emergency taker fee correctly applied only during FOK fills
- [ ] Dynamic profit targets: 2.5% / 1.5% / 1.0% based on confidence tier
- [ ] Confidence scores produce sensible allocations
- [ ] Cumulative allocation respects $100 hard cap per market
- [ ] Smart outbidding fires when depth walls detected
- [ ] Adverse movement protocol: grace period (3s), then FOK on reversal > 0.3%
- [ ] Emergency deadline: force FOK at market_expiry - 90s
- [ ] Anticipatory market loading: next market warm before current expires
- [ ] No Telegram 429 errors (rate limiting working)

**Extended Run (Day 3-5)**:
- [ ] Run for 24+ continuous hours
- [ ] Session summaries post hourly with correct aggregations
- [ ] No memory leaks or channel overflow
- [ ] QuestDB `simulated_trades` table has complete records
- [ ] Automated QuestDB pruning runs hourly without errors
- [ ] Resolution delay tracking shows correct ~2h UMA periods

### Stage 2: Server (Simulation)

1. Deploy to VPS — **recommended: AWS EC2 `us-east-1` (N. Virginia) or Hetzner Ashburn** for lowest latency to Polymarket CLOB. Dedicated cores recommended
2. Run in simulation mode with Telegram reporting
3. Validate latency targets (P99 <350ms signal-to-simulated-fill) from server location
4. Monitor for 48+ hours across varying volatility conditions

### Stage 3: Server (Live Trading)

1. Switch `MODE=live` in `.env`
2. Fill in `PRIVATE_KEY`, `POLYMARKET_API_KEY`, `POLYMARKET_SECRET`, `POLYMARKET_PASSPHRASE`
3. Derive L2 API credentials from private key via `create_or_derive_api_creds()`
4. Fund EOA wallet with USDC.e on Polygon + small POL balance (~0.1 POL for gas)
5. Approve Exchange contract for USDC.e spending and token transfers (one-time)
6. Start with reduced allocation ($50 per market via `FIXED_ALLOC=50`)
7. Monitor first 24 hours closely via logs + Telegram
8. Validate adverse movement protocol fires correctly on real reversals
9. Scale to full $100 allocation after validation
10. Set up systemd for auto-restart; health checks via heartbeat success rate

### Decision Gate (Before Going Live)

- [ ] Leg 1 fill rate >25% of signals
- [ ] Win rate >80% over 200+ filled trades
- [ ] Average net profit >1.0% per trade
- [ ] Emergency taker fills <15% of Leg 2 fills
- [ ] Confidence-weighted allocation produces reasonable risk-adjusted returns
- [ ] Smart outbidding fires correctly when depth walls detected
- [ ] No unhedged positions held past absolute deadline
- [ ] No unhandled errors in 48h continuous run
- [ ] Anticipatory market loading achieves <100ms cold-start
- [ ] Tiered kill switch verified: WARN 3%, PAUSE 5%, HALT 8%
- [ ] Resolution delay observed and capital lock properly tracked
- [ ] 24h rolling data tables populated and queryable
- [ ] Manual review of 20+ individual trades confirms correct logic

---

## 14. Operational Reference

### SDK Initialization

Client requires: CLOB host (`https://clob.polymarket.com`), chain_id (137 for Polygon), signer (from `PRIVATE_KEY`), API credentials (key/secret/passphrase derived via `create_or_derive_api_creds()`), signature_type **0 (EOA)**, and funder address (same as signer address for EOA). L2 credentials are derived once from L1 EIP-712 signature.

### Key Polymarket Contracts (Polygon, Chain ID 137)

| Contract | Address |
|----------|---------|
| CTF Exchange | `0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E` |
| Neg Risk CTF Exchange | `0xC5d563A36AE78145C45a50134d48A1215220f80a` |
| Conditional Tokens (CTF) | `0x4D97DCd97eC945f40cF65F87097ACe5EA0476045` |
| USDC.e | `0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174` |
| UMA CTF Adapter | `0x6A9D222616C90FcA5754cd1333cFD9b7fb6a4F74` |

### Simulation State Types

**File**: `src/types/simulation.rs`

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
    trades_adverse_hedged: u32,         // Hedged via adverse movement (emergency taker)
    trades_emergency_taker: u32,        // Leg 2 filled as taker (any emergency reason)
    walls_outbid: u32,                  // Smart outbidding events
    total_pnl: Decimal,
    total_taker_fees_paid: Decimal,     // Cumulative taker fees (emergency fills only)
    total_maker_rebates_earned: Decimal, // Estimated 20% of taker fees on maker fills
    locked_in_resolution: Decimal,       // Capital awaiting UMA resolution
    cumulative_used: Decimal,            // Used allocation in current market
}
```

---

## 15. Future Extensions

| Extension | Description |
|-----------|-------------|
| **Historical replay** | Replay QuestDB tick history against simulated orderbooks for backtesting |
| **Telegram commands** | `/pause`, `/resume`, `/status`, `/pnl`, `/config` — interactive control via Telegram |
| **Web dashboard** | Real-time browser UI for monitoring (replace or supplement Telegram) |
| **Multi-asset** | Add ETH 15-minute markets alongside BTC |
| **Alert thresholds** | Configurable Telegram alert levels (e.g., only notify on >2% opportunities) |
| **Comparison mode** | Run simulation alongside live to measure execution quality vs theoretical |
| **Chainlink direct feed** | Toggle between Binance-only and Binance+Chainlink (via RTDS) for dual-source price reference |
| **Operator health dashboard** | Real-time monitoring of CLOB API latency, heartbeat reliability, matching engine status |
| **Taker Leg 1 option** | If post-only fill rate < 20%, consider hybrid approach: selective taker entry on highest-confidence signals only |
| **Fee optimization** | Analyze emergency taker fee impact by price range; prefer entries at price extremes where fees are minimal |
| **Capital efficiency** | Track UMA resolution times; optimize allocation to minimize capital locked in challenge periods |
| **Kelly criterion allocation** | Replace confidence tiers with continuous Kelly criterion sizing. Requires calibrated win probability model from 500+ trades |
| **Cross-market signal correlation** | Use 24h rolling data to detect correlated signals across consecutive markets |
| **Adaptive profit targets** | Replace fixed 2.5%/1.5%/1.0% tiers with continuous function tuned from 500+ trades |
| **Contested rate tracking (live-mode)** | Track % of posted orders outbid within 1s. If >70%, re-evaluate strategy viability |
| **Monte Carlo EV model** | After 500+ live trades, build Monte Carlo simulation using empirical distributions |
