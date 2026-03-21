# FaCaiBot — Architecture & Infrastructure

## 1. Core Concept

FaCaiBot v2 is a bilateral accumulation market maker for Polymarket BTC 5-minute prediction markets. It continuously posts maker orders on both YES and NO sides, accumulating shares throughout each market. At resolution, paired shares (1 YES + 1 NO) pay $1.00 — if the total acquisition cost is below $1.00, the difference is locked profit.

The fair value model uses Binance spot and futures data to estimate P(BTC > strike at expiry). The quoter posts orders at `fair_value - edge` on each side, where edge scales dynamically with volatility, time remaining, and data quality.

See [V2_SYSTEM.md](V2_SYSTEM.md) for complete trading logic, fair value model, quoting, closing, and config reference.

## 2. Pipeline

Three-layer lock-free pipeline connected by crossbeam SPSC bounded(8192) channels:

```
Ingestor (gateway/)          Engine (engine/)              Executor (executor/)
─────────────────            ──────────────                ─────────────────────
Dedicated OS thread          spawn_blocking thread         tokio task (worker thread)
CPU-pinned core 0            (off worker pool)             Manual runtime (2 workers)
                             Pinned cores 1-2              Pinned cores 1-2

Binance Spot SBE WS ─┐
  @depth20 (50ms)     │
  @bestBidAsk         │
  @trade              │
Binance Futures WS ───┤
  @aggTrade           │
  @bookTicker         ├──► IngestorEvent ──► V2StrategyEngine
  @forceOrder         │                      ├─ FairValueEstimator
Polymarket WS ────────┤                      ├─ Quoter
  Market channel      │                      ├─ BilateralPosition
  User channel        │                      ├─ ClosingManager
Gamma API ────────────┘                      │         │
Heartbeat (5s) ───────┘                      │    V2ExecutorCommand
                                             │         │
CommandListener ◄──► Engine                  │         ▼
(control/)           Status                  │    LiveExecutor
Telegram getUpdates  Publish                 │    ├─ Post maker orders
                                             │    ├─ Cancel orders
                              V2ExecutorFeedback  ├─ Closing FOK
                              ◄──────────────┘    ├─ Telegram reports
                                                  └─ QuestDB analytics
```

**Data flow**:
1. Ingestor sends `IngestorEvent` variants to the engine via a bounded channel
2. Engine processes events, updates fair value, decides quoting actions
3. Engine sends `V2ExecutorCommand` variants to the executor (post, cancel, closing FOK, rotation)
4. Executor sends `V2ExecutorFeedback` back (order confirmations, fill data, cancel results)
5. Feedback is drained BEFORE `on_event()` in the main loop — ensures fill data is processed before new quoting decisions

## 3. Ingestor Layer

The ingestor runs on a dedicated OS thread pinned to core 0, with its own single-threaded tokio runtime. All data sources run as concurrent async tasks within this runtime.

### Binance Spot SBE WebSocket (`gateway/binance/ws.rs`)

Connects to Binance's SBE (Simple Binary Encoding) binary stream for minimal latency:
- **@bestBidAsk**: Tick-by-tick BBO updates → `BinanceTick` (feeds fair value model, vol trackers)
- **@depth20**: 50ms order book snapshots → `BinanceDepth` (feeds OBI velocity tracker)
- **@trade**: Individual spot trades → `SpotTrade` (currently forwarded, reserved for future metrics)

SBE frames are decoded inline — no JSON parsing overhead. Ed25519 API key authentication.

### Binance Futures JSON WebSocket (`gateway/binance/futures_ws.rs`)

Standard JSON WebSocket for futures-specific streams:
- **@aggTrade**: Aggregated futures trades → `FuturesAggTrade` (feeds CVD acceleration tracker)
- **@bookTicker**: Futures BBO → `FuturesBookTicker` (feeds basis delta tracker)
- **@forceOrder**: Forced liquidations → `FuturesForceOrder` (forwarded, reserved for future metrics)

### Polymarket Market WebSocket (`gateway/polymarket/market_ws.rs`)

Public WebSocket subscribed to the active market's YES and NO token IDs:
- Book updates → `PolymarketBook` (feeds spread guard, staleness guard, closing phase)
- Price changes → `PolymarketPriceChange`
- Best bid/ask → `PolymarketBestBidAsk`
- Tick size changes → `PolymarketTickSizeChange`

Token subscriptions are updated dynamically via a `watch` channel when markets rotate.

### Polymarket User WebSocket (`gateway/polymarket/user_ws.rs`)

Authenticated WebSocket (L2 HMAC credentials) for real-time fill detection:
- Order events with status MATCHED/MINED/CONFIRMED/FAILED/CANCELED → `TradeStatusUpdate`
- The `order_id` field matches hex hashes stored from executor `OrderPosted` feedback
- LIVE status events are silently skipped

### Heartbeat (`gateway/polymarket/heartbeat.rs`)

POST `/heartbeat` to Polymarket every 5 seconds. Reports success/failure + latency as `HeartbeatStatus` events. The engine tracks consecutive failures — quoting is blocked when failures exceed `heartbeat_dead_threshold`.

### Market Rotation (`gateway/polymarket/rotation.rs`)

Polls the Gamma API for upcoming BTC 5-minute markets. When a new market is discovered:
1. Emits `MarketRotation` event with condition ID, token IDs, end timestamp, tick size
2. Updates the `watch` channel so Market WS resubscribes to new token IDs
3. Starts searching `prewarm_lead_secs` before the current market ends

## 4. Engine Layer

The engine runs on `spawn_blocking` (off the tokio worker pool) and processes events synchronously in a tight loop. See `V2_SYSTEM.md` for complete behavioral documentation.

### Core Components

| Component | File | Purpose |
|-----------|------|---------|
| `V2StrategyEngine` | `engine/strategy.rs` | Event routing, state machine (IDLE→QUIET→QUOTING→CLOSING), quoting/closing tick dispatch |
| `FairValueEstimator` | `engine/fair_value.rs` | BTC probability model, momentum adjustment, edge sizing |
| `Quoter` | `engine/quoter.rs` | Per-side order management, requoting logic, inventory skewing |
| `BilateralPosition` | `engine/position.rs` | Share tracking, pairing math, PnL computation |
| `ClosingManager` | `engine/closing.rs` | End-of-market cancel + FOK pairing logic |
| Metric trackers | `engine/buildup/metrics.rs` | CVD acceleration, OBI velocity, basis delta, realized volatility |

### Engine Loop (one iteration per `IngestorEvent`)

```
1. DRAIN FEEDBACK (non-blocking)
   while feedback_rx.try_recv():
     OrderPosted  → record order ID, detect sync fills
     CancelResult → compute fill delta, record position
     ClosingFok   → record closing fill

2. CONTROL EVENTS
   Shutdown / DrainAndRestart / Pause / Resume

3. ON_EVENT (state + data update)
   BinanceTick     → update FV model, vol trackers, strike warmup
   BinanceDepth    → update OBI tracker, spot mid for basis
   FuturesAggTrade → update CVD tracker
   FuturesBookTicker → update basis tracker
   PolymarketBook  → update book state, staleness timestamps
   TradeStatusUpdate → match fill to resting order, record position
   MarketRotation  → reset all state, enter QUIET phase
   HeartbeatStatus → update health counter

4. QUOTING DECISIONS (quote_tick)
   Check global guards → per-side evaluation → post/cancel/requote

5. CLOSING PHASE (closing_tick)
   Cancel all → wait → FOK pair

6. DIAGNOSTICS
   60s diagnostic messages, status publishing
```

## 5. Executor Layer

The executor runs as a tokio task on the worker thread pool. It consumes `V2ExecutorCommand` from the engine and interacts with the Polymarket CLOB via the SDK.

### Commands

| Command | Action |
|---------|--------|
| `PostOrder` | Post a maker (post-only GTC) order on YES or NO side. Returns `OrderPosted` feedback with CLOB order ID |
| `CancelOrder` | Cancel a specific resting order by ID. Returns `CancelResult` with fill information |
| `CancelAll` | Cancel all resting orders (used during closing phase) |
| `ClosingFok` | Fill-or-Kill taker order to pair remaining shares. Uses `clob_safe_fok_size()` for CLOB constraints |
| `MarketRotation` | Reset SDK caches, pre-warm for new market's token IDs |
| `TickSizeChange` | Update the executor's tick size for the current market |

### SDK Integration (`gateway/polymarket/rest.rs`)

The `PolymarketGateway` wraps the `polymarket-client-sdk`:
- `place_order()`: `build()` → `sign()` (EIP-712) → `post_order()` (HTTP POST). ~1.2-1.4s round-trip
- `cancel_order()`: HTTP DELETE by order ID
- `cancel_all()`: HTTP DELETE all orders for given asset IDs
- Order status detection: `OrderStatus::Matched` (filled), `Rejected`, `Delayed`

### Fill Engine (`executor/fill_engine.rs`)

Utility functions:
- `compute_taker_fee(price, size)`: CLOB taker fee formula `C × 0.25 × (p(1-p))²`
- `compute_maker_rebate(price, size)`: Estimated 20% of taker fee equivalent
- `round_to_tick(price, tick_size)`: Floor price to tick grid

## 6. Control Layer

### Telegram Command Listener (`control/listener.rs`)

Long-polls Telegram `getUpdates` API. Authorized by `TELEGRAM_ALLOWED_USER_ID`. Dispatches commands to handlers.

### Command Handlers (`control/handlers.rs`)

| Command | Action |
|---------|--------|
| `/status` | Build and send current bot status (phase, position, counters) |
| `/config [section]` | Display current config values |
| `/set <param> <value>` | Update config.toml param, trigger drain-and-restart |
| `/trades on\|off` | Toggle trade fill notifications |
| `/summary on\|off` | Toggle market summary notifications |
| `/diag on\|off` | Toggle 60s diagnostic forwarding |
| `/stop` | Pause trading (connections stay alive) |
| `/resume` | Resume trading |
| `/shutdown` | Graceful shutdown (drains open position first) |
| `/balance` | Wallet USDC.e + POL balance via Polygon RPC |
| `/polybalance` | Polymarket positions via Data API |
| `/redeem` | Redeem resolved CTF positions to USDC.e |

### Config Editor (`control/config_editor.rs`)

Reads/writes `config.toml` with an allowlist of settable parameters and valid ranges. `/set` triggers a `DrainAndRestart` event — the engine waits for the current market's closing phase before restarting.

### Wallet (`control/wallet.rs`)

- `/balance`: Queries USDC.e (ERC-20) and POL balances via Polygon RPC
- `/polybalance`: Queries Polymarket Data API for open positions
- `/redeem`: Calls CTF `redeemPositions()` on-chain. Merges Data API positions with persistent `redeems.txt` file for markets that have resolved but aren't in the API anymore

## 7. Storage Layer

### QuestDB (`storage/cold.rs`)

Analytics-only — not on the execution critical path. Uses QuestDB ILP (InfluxDB Line Protocol) over TCP.

Tables:
- `binance_ticks`: Downsampled BTC price data (1 row/sec)
- `v2_fills`: Every fill event (side, price, size, taker/maker, fee)
- `v2_market_reports`: End-of-market summaries (paired shares, profit, rebates)

Flush strategy: ticks are batched (flushed on next write or shutdown), fills and reports flush immediately.

## 8. Deployment

### Infrastructure

- **Instance**: AWS `c7i.xlarge` (4 vCPUs, 8 GiB RAM) in `eu-west-2` (London)
- **OS**: Amazon Linux 2023
- **Core allocation**: Core 0 = ingestor, Cores 1-2 = tokio workers (engine + executor), Core 3 = QuestDB + system
- **Clock sync**: `chrony` with AWS Time Sync Service (169.254.169.123) — sub-1ms offset required

### systemd Service

- Binary at `/opt/facaibot/facaibot`, config at `/opt/facaibot/{.env,config.toml}`
- Exit 0 = clean shutdown (no restart)
- Exit 42 = config change (systemd restarts automatically)
- Crash = automatic restart with 5s delay

### Deploy Scripts (`deploy/`)

- `setup.sh`: One-time server provisioning (Rust, Docker, QuestDB, systemd unit)
- `deploy.sh`: Pull latest code, rebuild with `RUSTFLAGS="-C target-cpu=native"`, restart service
