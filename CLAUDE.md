# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

FaCaiBot is a high-frequency Polymarket arbitrage bot targeting BTC/ETH 15-minute prediction markets. It uses Binance spot price feeds as a reference signal and exploits price discrepancies on the Polymarket CLOB.

## Key Documents

- `bot_prd.md` — Full production PRD (strategy, execution, risk, API reference)
- `bot_testing_prd.md` — Simulation mode spec (Telegram reporting, fill rate tracking)
- `agent_team.md` — Multi-agent build team spec (9 agents, spawn prompts, file ownership)
- `queries.sql` — Common QuestDB analytics queries (fill rate, PnL by tier, pruning)

## Build Commands

```bash
cargo build              # Build (debug)
cargo build --release    # Build (release — LTO, single codegen unit)
cargo run                # Run the bot
cargo test               # Run all tests
cargo test <name>        # Run a single test by name
cargo clippy             # Lint
cargo fmt                # Format code
cargo fmt -- --check     # Check formatting without modifying
```

## Local Infrastructure

```bash
docker-compose up -d     # Start Redis + QuestDB
docker-compose down      # Stop
```

- **Redis**: localhost:6379 — hot cache for orderbook state and active market IDs
- **QuestDB**: localhost:9000 (web console), :9009 (ILP ingestion), :8812 (Postgres wire)

Copy `.env.example` to `.env` and fill in credentials before running.

## Technology Stack

- **Language**: Rust (Edition 2024)
- **Async Runtime**: tokio
- **Polymarket**: polymarket-client-sdk (official CLOB client — market reads, orders, WebSocket)
- **EVM/Signing**: alloy (EIP-712 signing, Polygon interactions)
- **Binance**: binance crate (WebSocket streams: @depth@100ms, @ticker)
- **WebSocket Client**: fastwebsockets (SIMD-accelerated, for raw low-latency connections)
- **Inter-layer Channels**: crossbeam-channel (lock-free SPSC)
- **Serialization**: rkyv (zero-copy for internal messaging), serde/serde_json (external APIs)
- **Hot Storage**: Redis (orderbook snapshots, active 15m market IDs)
- **Cold Storage**: QuestDB (millisecond tick history, trade logs)
- **Arithmetic**: rust_decimal (fixed-point, no floats in pricing)
- **Logging**: tracing + tracing-subscriber

## Architecture

Three-layer lock-free pipeline connected by crossbeam SPSC channels:

```
Ingestor (Ear) ──▶ Engine (Brain) ──▶ Executor (Hand)
```

### Layer 1 — Ingestor (`src/gateway/`)
- Runs on a dedicated OS thread, CPU-pinned to core 0 via core_affinity
- Maintains WebSocket connections to Polymarket CLOB and Binance
- Parses events into `IngestorEvent` and pushes to channel

### Layer 2 — Strategy Engine (`src/engine/`)
- Pulls `IngestorEvent` from channel, updates `MarketState`
- Evaluates arbitrage conditions using fixed-point arithmetic
- Emits `TradeSignal` to executor channel

### Layer 3 — Executor (`src/storage/`, `src/gateway/polymarket.rs`)
- Receives `TradeSignal`, signs and submits orders via polymarket-client-sdk
- Caches state in Redis, batches tick data to QuestDB (flushes every 1000 ticks)

## Key Source Files

```
src/
├── main.rs              # Entry point — tokio runtime, channel wiring, task spawning
├── config.rs            # Config struct from env vars (dotenvy)
├── engine/strategy.rs   # MarketState, TradeSignal generation, arbitrage evaluation
├── gateway/polymarket.rs # CLOB client wrapper (reads, orders, WS streaming)
├── gateway/binance.rs   # Binance WS streams (btcusdt/ethusdt @depth @ticker)
├── storage/hot.rs       # Redis async ops
├── storage/cold.rs      # QuestDB batch writer
├── types/market.rs      # MarketState, OrderBook, BinanceTick, IngestorEvent
├── types/order.rs       # TradeSignal, OrderRequest, OrderResponse, Side
└── utils/signing.rs     # EIP-712 signing helpers (alloy)
```

## Implementation Status

### Implemented (production-ready skeleton)
- **`src/main.rs`** — Full pipeline wiring: tokio runtime, crossbeam channel creation (`CHANNEL_CAP = 8192`), CPU-pinned ingestor OS thread (core 0), strategy engine task loop, executor task loop. All three layers running concurrently.
- **`src/config.rs`** — Env var loading via `dotenvy`. Currently wired: `POLYMARKET_API_KEY`, `POLYMARKET_SECRET`, `POLYMARKET_PASSPHRASE`, `PRIVATE_KEY`, `REDIS_URL`, `QUESTDB_URL`, `BINANCE_WS_URL`. **Missing** (not yet added to `Config`): `MODE`, `TELEGRAM_BOT_TOKEN`, `TELEGRAM_CHAT_ID`, `HIGH_CONFIDENCE`, `MED_CONFIDENCE`, `MAX_ALLOC_PCT`, `ADVERSE_THRESHOLD`, `ADVERSE_GRACE_PERIOD`, `STALE_EVENT_THRESHOLD`, `MAX_GAS_PRICE`.
- **`src/types/market.rs`** — `PriceLevel`, `OrderBook` (with `best_bid`/`best_ask`/`spread`), `BinanceTick`, `MarketState`, `IngestorEvent` (PolymarketBook, BinanceTick, MarketRotation).
- **`src/types/order.rs`** — `Side`, `TradeSignal`, `OrderRequest`, `OrderResponse`, `OrderStatus`.
- **`src/storage/hot.rs`** — Redis ops fully implemented: `cache_orderbook`, `get_orderbook`, `set_active_market`, `get_active_market`.
- **`src/storage/cold.rs`** — QuestDB ILP batch writer fully implemented: `record_tick` (buffers into `binance_ticks` table), `flush` at 1000-tick threshold, `Drop` flushes remainder. Uses correct questdb-rs v4 API.
- **`src/utils/signing.rs`** — `build_signer(hex_key)` → `PrivateKeySigner` via alloy.

### Stubs (TODO — not yet implemented)
- **`src/gateway/polymarket.rs`** — All methods are stubs returning dummy data: `get_orderbook`, `get_midpoint`, `get_price`, `place_order`, `cancel_order`, `stream_orderbook`. SDK integration pending.
- **`src/gateway/binance.rs`** — `run()` is a stub (warns + exits). `parse_ticker_json()` is implemented (parses `@ticker` JSON). WS subscription loop pending.
- **`src/engine/strategy.rs`** — `on_event()` correctly updates `MarketState`. `evaluate()` returns `None` always — arbitrage logic not yet implemented (placeholder spread check commented).

## Conventions

- All pricing arithmetic uses `rust_decimal::Decimal` — never use f32/f64 for prices or sizes.
- Inter-layer communication is via crossbeam bounded channels — no Arc<Mutex> for data flow.
- The ingestor layer runs on its own OS thread with a single-threaded tokio runtime (not the main multi-threaded runtime).
- Environment variables are the only config mechanism — no config files.
- Every update to this project should be recorded in this CLAUDE.md file.

## Changelog

- **2026-02-20**: Initial project scaffold — three-layer architecture, all dependencies, skeleton code for all modules, docker-compose for Redis/QuestDB.
- **2026-02-20**: Created `bot_prd.md` (restructured production PRD from bot_prd.txt) and `bot_testing_prd.md` (simulation mode with Telegram reporting). Simulation mode toggled via `MODE=simulation` env var — same pipeline, executor swaps real orders for simulated fills + Telegram posts.
- **2026-02-21**: Major PRD corrections based on Polymarket API documentation review:
  - Removed all references to private RPC bundles / MEV protection (orders are HTTP POST to CLOB API at `clob.polymarket.com`, not blockchain transactions)
  - Removed PGA gas bidding risk (irrelevant — operator pays gas for on-chain settlement)
  - Fixed oracle: UMA Optimistic Oracle resolves markets (not Chainlink directly); Chainlink is the price reference source for 15-min crypto markets
  - Added taker fee accounting: 15-min crypto markets charge taker fees (max 1.56% effective at p=0.50, formula: `C * 0.25 * (p*(1-p))^2`); 20% redistributed as daily maker rebates
  - Fixed WebSocket endpoints: Market channel (public, `wss://ws-subscriptions-clob.polymarket.com/ws/market`) and User channel (authenticated, `/ws/user`)
  - Fixed order flow: two-step create (sign) → post (submit with `post_only` flag); `post_only` is on `postOrder()`, not `createOrder()`
  - Added heartbeat requirement: `POST /heartbeat` every 5s; 10s+5s buffer timeout auto-cancels all orders
  - Added tick size handling: dynamic (0.1-0.0001), `tick_size_change` WS events at price extremes
  - Added matching engine restart handling: Monday 20:00 ET, ~90s, HTTP 425
  - Added RTDS (Real-Time Data Socket) for optional Binance + Chainlink price feeds
  - Fixed trade status tracking: MATCHED→MINED→CONFIRMED (RETRYING/FAILED); reorgs are operator's responsibility
  - Added batch orders (`POST /orders`, max 15 per request)
  - Fixed delta-T pipeline: API submit + operator matching (not PGA); total <350ms target
  - Added UMA resolution capital lock (2-hour challenge period minimum)
  - Added SDK init details: signature_type (0/1/2), funder address, `create_or_derive_api_creds()`
  - Added operator monitoring, kill switches, idempotency guards, ATR-scaled slippage
  - Added key Polymarket contract addresses (Polygon chain ID 137)
  - Updated profit targets: >0.5% net after taker fees (was >1.2% gross)
  - Updated simulation PRD: taker fee simulation, resolution delay tracking, `price_divergence` table for Chainlink backtesting
- **2026-02-21**: PRD strategic refinements (9 changes across both PRDs):
  - Anticipatory market loading: prep next market at <180s remaining (pre-subscribe WS, warm orderbook cache) instead of waiting for expiry
  - Added comprehensive rate limit reference (Section 10.6) with all Polymarket API limits and bot budget analysis showing ample headroom
  - Bot detection: state-aware retract-or-proceed logic (not posted → abort; posted+unfilled → cancel; posted+filled → proceed to Leg 2; enter cooldown after abort)
  - Leg 2 adverse movement protocol: force FOK hedge when Binance reverses > ADVERSE_THRESHOLD (0.3%); hard stop at break-even breach; never hold naked past timer or 90s to expiry
  - Confidence-weighted dynamic allocation: replaced fixed 5×20% with tiered sizing (HIGH ≥0.8 → 30%, MED ≥0.5 → 20%, LOW → 10%) using weighted signal scoring (spike/ATR, sustain, depth, time)
  - EOA (Type 0) auth: selected over POLY_PROXY/GNOSIS_SAFE — CLOB speed identical, no relayer dependency, user pays POL gas for infrequent on-chain ops
  - 24h rolling data storage: added `poly_book_snapshots` (5s intervals) and `trade_signals` tables in QuestDB for cross-market backtesting (~50MB/day)
  - Updated config: removed `MAX_TRADES_PER_MARKET`, `PER_TRADE_PCT`, `POLYMARKET_FUNDER`; added `HIGH_CONFIDENCE`, `MED_CONFIDENCE`, `MIN_ALLOC`, `MAX_ALLOC`, `ADVERSE_THRESHOLD`, `BOT_COOLDOWN`; changed `POLYMARKET_SIG_TYPE` default to 0
  - Updated testing PRD: confidence/allocation in Telegram messages, bot detection + adverse movement simulation, Kelly criterion as future extension
- **2026-02-21**: Major PRD refinements — post-only both legs + operational precision (14 changes):
  - Both legs post-only by default: Leg 1 AND Leg 2 are now `post_only=true` (GTC). Zero fees in normal flow (net = gross). Taker orders (FOK) reserved exclusively for emergency failsafes (adverse movement, break-even breach, timer/market expiry deadline)
  - Added "Post-Only Fill Mechanics" section (Section 2): explains resting order fills via counterparty flow during repricing lag. Fill rate ~30-50% of signals, each fill captures full spread with zero fees
  - Smart outbidding replaces bot detection: outbid depth walls by 1 tick (capped at break-even). No cooldowns or aborts — post-only provides natural protection (if outbid, order doesn't fill = zero cost). Analytics-only logging
  - Leg 1 bidding: post at top of bid side (best_bid + tick), smart outbid walls for queue priority
  - Leg 2 bidding: post at confidence-tier target price, smart outbid walls if present
  - Dynamic profit targets: replaced fixed PROFIT_TARGET with three tiers — HIGH (2.5%), MED (1.5%), LOW (1.0%) — based on signal confidence score
  - Simplified break-even: `1.0 - entry_price` (no taker fee in normal flow)
  - Faster erosion cascade: 2s intervals (was 3s), ALL steps post-only until emergency. Taker fee formula moved to emergency-only context
  - Removed slippage from pre-entry checks (post-only = exact price, no slippage)
  - Removed MIN_ALLOC (zero fees = no minimum threshold). MAX_ALLOC changed to percentage (MAX_ALLOC_PCT = 0.30)
  - Removed BOT_COOLDOWN, MAX_BID_INC (replaced by smart outbidding)
  - Added ADVERSE_GRACE_PERIOD (3s) before adverse monitoring activates post-fill
  - Aligned max unhedged time: absolute deadline = market_expiry - 90s (worst case: 180s - 90s = 90s erosion window)
  - Removed "zero liquidity" / "unhedgeable" concepts (liquidity always exists, depth varies, position sizing constrained)
  - Added precise polling schedule (Section 5.1.1): exact intervals for every endpoint with rate limit headroom
  - Tick size cached once at market rotation (not re-queried). Handle tick_size_change WS as rare edge case
  - Simplified UMA dispute: DVM escalation excluded from bot scope — if disputed, capital stays locked
  - Updated testing PRD: fill rate tracking, smart outbidding simulation, emergency-only taker fees, dynamic profit tiers in Telegram messages, taker Leg 1 as future extension
- **2026-02-21**: Refined Leg 2 erosion cascade to proportional step-size model (Option C):
  - Step size = `initial_profit_target / 5` (20% of margin per 2s step) — all tiers reach break-even in exactly 5 steps × 2s = 10s
  - HIGH (2.5%): step = 0.005 → 2.5→2.0→1.5→1.0→0.5→break-even
  - MED (1.5%): step = 0.003 → 1.5→1.2→0.9→0.6→0.3→break-even
  - LOW (1.0%): step = 0.002 → 1.0→0.8→0.6→0.4→0.2→break-even
  - Erosion continues at same cadence (every 2s, post-only) after timer window until `market_expiry - 90s` absolute deadline
  - Direction clarified: Leg 2 erosion is always +delta (bid raised toward ask) regardless of YES/NO token
  - Updated `bot_testing_prd.md` simulation to compute step_size from tier and apply per 2s step
- **2026-02-21**: PRD redundancy cleanup — final pass:
  - Merged duplicate "Both legs post-only" + "Taker fees (emergency only)" bullets in Section 2 into single bullet
  - Trimmed heartbeat duplication in Section 7.1 to cross-reference Section 5.4
  - Consolidated Section 10.5: removed 5 subsections (Heartbeat, Tick Size, Matching Engine, Batch Orders, RTDS) that were verbatim repeats of Sections 5.3, 5.4, 7.1; kept SDK Init, Data Retention schemas, Contracts
  - Replaced Section 10.6 Bot Budget table (duplicate of Section 5.1.1 polling schedule) with cross-reference
  - Added missing 100ms quick reversal check to `bot_testing_prd.md` Leg 2 simulation (aligns with main PRD Section 6.3)
- **2026-02-21**: Created `agent_team.md` — multi-agent team specification for building FaCaiBot via Claude Code Task tool. 9 agents across 4 tiers (2 Opus, 6 Sonnet, 1 Haiku): PM orchestrator, Architect, 5 domain developers (Ingestor/Engine/Executor/Storage/Simulation), Integration agent, QA agent. Includes cross-review chain, build order with parallelism, file ownership map, and spawn prompt patterns.
- **2026-02-21**: Configured Telegram bot for simulation testing — Bot: `@f4c4ibot`, Token: `8416091125:AAHk7iWsZbbVlQQ8LTGeIBaxR8c3VbeIHjg`, Chat ID: `927062548`. Updated `bot_testing_prd.md` Section 6 with bot credentials and setup instructions. Updated `.env.example` with Telegram config and MODE selection.
- **2026-02-21**: Audited current code state — added "Implementation Status" section to CLAUDE.md. Storage layer (Redis, QuestDB) and type definitions are fully implemented. Pipeline wiring and thread architecture in `main.rs` is complete. All three gateway/engine modules are stubs awaiting implementation: `gateway/polymarket.rs` (SDK integration), `gateway/binance.rs` (WS loop), `engine/strategy.rs` (arbitrage logic). `Config` struct is missing new env vars from PRD updates (MODE, Telegram, confidence tiers, allocation params).
- **2026-02-21**: PRD hardening pass — 14 changes across both PRDs based on external review:
  - Added stale data guard: discard `IngestorEvent` where `now_ms - timestamp > 500ms` (Section 5.4)
  - Added `STALE_EVENT_THRESHOLD` (500ms) and `MAX_GAS_PRICE` (100 gwei) to config constants
  - Strengthened available capital tracking: `available_capital = total_capital - locked_in_resolution`; skip signal if insufficient
  - Matching engine restart: 5-minute pre-cancel window at 19:55 ET (was just "pre-cancel")
  - Tiered drawdown kill switch: WARN 3% → PAUSE 5% → HALT 8% (was single >5% shutdown)
  - Heartbeat consecutive failure handling: 2 consecutive failures → reset executor state + Telegram alert
  - UMA dispute monitoring: poll resolution status, Telegram alert on dispute, track locked capital
  - Automated QuestDB pruning: hourly `tokio::interval` task via Postgres wire protocol
  - MAX_GAS_PRICE cap on all on-chain operations (approve, redeem, merge)
  - Emergency FOK size cap: `min(remaining_position, ask_depth_within_2_ticks)` — prevents eating through thin books
  - Tick size rejection retry: catch 400, re-fetch tick size, retry once, then abort
  - Contested rate metric (live-mode only): track % of orders outbid within 1s, re-evaluate if >70%
  - VPS recommendation: AWS `us-east-1` or Hetzner Ashburn for lowest CLOB latency
  - Config validation at startup: parse private key, decimal values, URLs — fail fast
  - Created `queries.sql` — common QuestDB analytics queries (fill rate, PnL by tier, spread over time, pruning)
  - Updated `bot_testing_prd.md`: stale data + config validation in Step 1, UMA dispute + pruning in Step 4, tiered kill switch in Decision Gate, contested rate + Monte Carlo as future extensions
