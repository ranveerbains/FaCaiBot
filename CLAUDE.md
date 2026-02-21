# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

FaCaiBot is a high-frequency Polymarket arbitrage bot targeting BTC/ETH 15-minute prediction markets. It uses Binance spot price feeds as a reference signal and exploits price discrepancies on the Polymarket CLOB.

## Key Documents

- `bot_prd.md` — Unified PRD covering both Live and Simulation modes (strategy, execution, risk, Telegram, QuestDB, API reference, deployment)
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
- **Polymarket CLOB**: reqwest (REST gateway with EIP-712 signed orders, HMAC L2 auth)
- **Polymarket WS**: fastwebsockets + tokio-rustls (Market WS, User WS, heartbeat)
- **EVM/Signing**: alloy (PrivateKeySigner for EIP-712, address derivation)
- **Binance**: fastwebsockets + tokio-rustls (combined @depth20@100ms + @ticker streams)
- **Inter-layer Channels**: crossbeam-channel (lock-free SPSC bounded(8192))
- **HTTP Auth**: hmac + sha2 + base64 (HMAC-SHA256 for CLOB API headers)
- **Serialization**: serde/serde_json (API payloads), rkyv (internal messaging)
- **Hot Storage**: Redis (orderbook snapshots, active market IDs, tick size, fee rate, PnL)
- **Cold Storage**: QuestDB (5 tables: binance_ticks, poly_book_snapshots, trade_signals, executed_trades, simulated_trades)
- **Arithmetic**: rust_decimal (fixed-point, no floats in pricing)
- **Configuration**: toml (TOML config file parsing for tuning parameters)
- **Logging**: tracing + tracing-subscriber
- **Telegram**: hyper + tokio-rustls (direct HTTPS POST to Bot API)

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
├── main.rs                           # Entry point — MODE-based executor selection, channel wiring
├── config.rs                         # Hybrid config: TOML tuning params + env secrets (6 sub-structs)
│
├── engine/
│   ├── mod.rs                        # Re-exports + module-level docs
│   ├── strategy.rs                   # StrategyEngine orchestrator: event routing, state, simulation
│   ├── evaluator.rs                  # Leg1Evaluator + Leg2Evaluator: pure guard-check + signal-build
│   ├── confidence.rs                 # compute_confidence() 4-factor scoring, round_to_tick()
│   └── erosion.rs                    # ErosionState + ErosionSnap: Leg 2 profit-target erosion FSM
│
├── executor/
│   ├── mod.rs                        # Re-exports + module-level docs
│   ├── simulation.rs                 # SimulationExecutor: simulated fills, Telegram reporting
│   └── fill_engine.rs                # FillSimulator: depth-based fill model (Leg1Result, Leg2Result)
│
├── gateway/
│   ├── mod.rs                        # Re-exports + module-level docs
│   ├── binance/
│   │   ├── mod.rs                    # Re-exports BinanceGateway
│   │   ├── ws.rs                     # BinanceGateway: fastwebsockets TLS, JSON parsing, event dispatch
│   │   └── spike.rs                  # SpikeDetector: EMA-ATR spike detection with sustain + phantom filter
│   └── polymarket/
│       ├── mod.rs                    # PolymarketWsGateway facade, shared constants, re-exports
│       ├── rest.rs                   # CLOB REST gateway: EIP-712 signing, order placement, book queries
│       ├── market_ws.rs              # Public Market WS: book, price, tick events
│       ├── user_ws.rs                # Authenticated User WS: trade fills, order events
│       ├── heartbeat.rs              # POST /heartbeat loop (every 5s)
│       ├── rotation.rs               # Gamma API market discovery and rotation manager
│       └── tls_helpers.rs            # Shared TLS WebSocket + HTTP helpers
│
├── reporting/
│   ├── mod.rs                        # Re-exports
│   └── telegram.rs                   # TelegramReporter: fire-and-forget Telegram Bot API via hyper
│
├── storage/
│   ├── mod.rs                        # Re-exports
│   ├── hot.rs                        # Redis: orderbook cache, active market, allocation, PnL tracking
│   └── cold.rs                       # QuestDB: 5 tables (ticks, book snapshots, signals, trades, sim trades)
│
├── types/
│   ├── mod.rs                        # Re-exports
│   ├── market.rs                     # 11 IngestorEvent variants, MarketState (20 fields), OrderBook
│   ├── order.rs                      # TradeSignal (18 fields), ProfitTier, OrderRequest, Side, OrderType
│   └── simulation.rs                 # SimulationState, SimPosition, SimFill, SimTrade, summaries
│
└── utils/
    ├── mod.rs                        # Re-exports
    └── signing.rs                    # build_signer, HMAC-SHA256 auth headers, contract addresses
```

## Implementation Status

**All modules fully implemented. 120 tests pass. Ready for Stage 1 deployment testing.**

### Fully Implemented
- **`src/main.rs`** — MODE-based executor selection (simulation vs live), full 3-layer pipeline wiring, CPU-pinned ingestor thread, Binance + Polymarket WS + market rotation in ingestor, Leg 1 + Leg 2 signal evaluation in engine, simulation/live executor dispatch.
- **`src/config.rs`** — Hybrid config: `config.toml` (tuning params via `toml` crate + serde) + `.env` (secrets/infrastructure via `dotenvy`). 6 TOML sub-structs (`SpikeDetectionConfig`, `EntryGuardsConfig`, `CapitalConfig`, `ConfidenceConfig`, `RiskConfig`, `SimulationConfig`) with `BotConfig` top-level. `Config::load()` reads `CONFIG_FILE` env var (default `"config.toml"`), parses TOML if exists else uses defaults, loads secrets from env. Config validation at startup.
- **`src/engine/strategy.rs`** — Full StrategyEngine: handles all 11 IngestorEvent variants, spike-encoded tick detection, Leg 1 signal generation with self-gating (`evaluate(&mut self)` clears spike + sets Posted + tracks allocation), Leg 2 erosion cascade with self-gating (sets Posted at all 4 emission points), `advance_simulation()` state machine (Posted→Filled→Complete→Reset), `init_erosion()` helper, 4-factor confidence scoring, smart outbidding, emergency deadline detection, adverse movement monitoring, quick reversal check. Multiple trades per market (1 at a time, state resets after completion). 33 unit tests.
- **`src/gateway/binance.rs`** — fastwebsockets TLS to `stream.binance.com`, combined btcusdt @depth20@100ms + @ticker streams, SpikeDetector with rolling EMA-ATR (fast α=0.1, slow α=0.002, daily α=0.0001), sustain check (200ms + low-vol extension), phantom filter (100ms window, >50% reversion rejection), stale event guard with 60s telemetry. 12 unit tests.
- **`src/gateway/polymarket.rs`** — Full CLOB REST gateway: reqwest HTTP client, EIP-712 signed order payloads, HMAC-SHA256 L2 auth headers, place_order/place_orders (batch max 15)/cancel_order/cancel_all/get_orderbook/get_midpoint/get_price/get_tick_size/get_fee_rate. Graceful degradation (no signer = read-only mode). 10 unit tests.
- **`src/gateway/polymarket_ws.rs`** — Market WS + User WS + heartbeat + Gamma API market discovery + market rotation manager. 23 unit tests.
- **`src/executor/simulation.rs`** — SimulationExecutor: receives TradeSignal via crossbeam, simulates post-only Leg 1 fills (spread check, depth check), simulates Leg 2 with erosion cascade and emergency taker, market rotation handling, shutdown summary. 13 unit tests.
- **`src/reporting/telegram.rs`** — TelegramReporter: fire-and-forget via hyper+tokio-rustls, 6 message types (startup, alert, opportunity, trade completed, market summary, session summary), HTML formatting, 5s rate limiter (`AtomicU64`).
- **`src/storage/hot.rs`** — Full Redis ops: orderbook cache, active market (yes/no/condition IDs), resolution tracking, cumulative allocation, tick size cache, fee rate cache, daily PnL.
- **`src/storage/cold.rs`** — Full QuestDB ILP: 5 tables (binance_ticks, poly_book_snapshots, trade_signals, executed_trades, simulated_trades), batch flush at 1000 for ticks, prune_old_partitions stub.
- **`src/types/`** — Full type system: 11 IngestorEvent variants, MarketState (20 fields), TradeSignal (18 fields), ProfitTier with methods, OrderBook with depth helpers, SimulationState with full business logic.
- **`src/utils/signing.rs`** — build_signer, HMAC-SHA256 auth headers (generate_api_headers, build_hmac_signature), Polymarket contract addresses (CTF_EXCHANGE, NEG_RISK_CTF_EXCHANGE, CHAIN_ID=137). 10 unit tests.

## Conventions

- All pricing arithmetic uses `rust_decimal::Decimal` — never use f32/f64 for prices or sizes.
- Inter-layer communication is via crossbeam bounded channels — no Arc<Mutex> for data flow.
- The ingestor layer runs on its own OS thread with a single-threaded tokio runtime (not the main multi-threaded runtime).
- Hybrid config: `config.toml` for tuning parameters (spike detection, entry guards, capital, confidence, risk, simulation), `.env` for secrets and infrastructure only. Override config path with `CONFIG_FILE` env var.
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
- **2026-02-21**: Created `src/gateway/polymarket_ws.rs` — Polymarket WebSocket + REST gateway for the Ingestor layer:
  - Market WS (public): `wss://ws-subscriptions-clob.polymarket.com/ws/market` — full TLS+fastwebsockets connection with subscription (`custom_feature_enabled: true`), parses `book`→`PolymarketBook`, `price_change`→`PolymarketPriceChange`, `best_bid_ask`→`PolymarketBestBidAsk`, `tick_size_change`→`PolymarketTickSizeChange`, `market_resolved`→`PolymarketMarketResolved`, `last_trade_price` logged at debug. Exponential backoff reconnection (1s→30s). Emits `WsStatus` on connect/disconnect.
  - User WS (authenticated): `wss://ws-subscriptions-clob.polymarket.com/ws/user` — parses `trade` events → `TradeStatusUpdate` (MATCHED/MINED/CONFIRMED/RETRYING/FAILED), `order` events logged at info. Skipped in simulation mode (no auth credentials). Exponential backoff reconnection.
  - Gamma API market discovery: `GET /events?tag_id=102467&active=true&closed=false&limit=10` — tag 102467 = "15M"; filters client-side by slug prefix (`btc-updown-15m-` / `eth-updown-15m-`), parses nested event→market structure, selects soonest non-expired market with `acceptingOrders: true`. Custom RFC 3339 parser avoids pulling in chrono dependency.
  - Market rotation manager (`run_market_rotation`): polls Gamma every 10 minutes; at <180s remaining emits `IngestorEvent::MarketRotation` once per market (idempotent); 5s check cadence for anticipatory loading.
  - Heartbeat loop (`run_heartbeat`): `POST /heartbeat` every 5s; tracks `heartbeat_id`; on 400 updates id from response; logs `error!` alert on 2+ consecutive failures; skipped in simulation mode. Includes matching engine restart guard (Monday 19:55–20:00 ET warning).
  - HTTP helpers: `http_get` + `http_post` over TLS using hyper/tokio-rustls (same build stack as binance.rs). Handles HTTP 425 (matching engine restart) with warning log.
  - `PolymarketWsGateway::shutdown()` method for graceful stop via `AtomicBool` flag.
  - 23 unit tests covering: Gamma response parsing (epoch secs, ISO 8601, expired markets, mixed), ISO 8601 epoch calculation, all event parsers (book, best_bid_ask, tick_size_change, market_resolved, price_change), User WS trade status parsing (all 5 statuses, case-insensitive, unknown error), array frame dispatch, price level parsing (object and array forms), subscription message builder.
  - Updated `src/gateway/mod.rs` to add `pub mod polymarket_ws;`.
  - Fixed pre-existing syntax error in `src/engine/strategy.rs` (stray `,;` in test struct) and borrow-checker violations in `evaluate_leg2` (restructured to use boolean flags to avoid conflicting `&mut self` borrows; converted `emit_emergency_leg2` from `&self` method to static method with explicit parameters).
- **2026-02-21**: Full implementation build — all modules implemented via multi-agent pipeline (16 tasks, 11 phases):
  - **Phase 0 (Architect)**: Expanded types — IngestorEvent to 11 variants, MarketState to 20 fields, TradeSignal to 18 fields, ProfitTier enum with methods, SimulationState with full business logic.
  - **Phase 2a (Ingestor Dev)**: `gateway/binance.rs` — 1123 lines, fastwebsockets TLS, SpikeDetector (EMA-ATR with sustain+phantom filter), stale guard, 12 tests.
  - **Phase 2b (Storage Dev)**: `storage/hot.rs` — 7 Redis op groups (orderbook, active market, resolution, allocation, tick size, fee rate, PnL). `storage/cold.rs` — 5 QuestDB tables with batch flushing.
  - **Phase 2c (Sim Dev)**: `types/simulation.rs` — SimulationState with 20+ business methods. `reporting/telegram.rs` — hyper+tokio-rustls Telegram reporter with 6 message types.
  - **Phase 4a (Engine Dev)**: `engine/strategy.rs` — Full Leg 1 + Leg 2 evaluation, confidence scoring, erosion cascade, adverse movement detection, 25 tests.
  - **Phase 4b (Ingestor Dev)**: `gateway/polymarket_ws.rs` — Market/User WS, heartbeat, Gamma API market rotation, 23 tests.
  - **Phase 6a (Executor Dev)**: `gateway/polymarket.rs` — Full CLOB REST gateway with EIP-712 signing, reqwest HTTP, HMAC-SHA256 L2 auth, 10 tests. `utils/signing.rs` — HMAC auth headers, contract addresses, 10 tests.
  - **Phase 6b (Sim Dev)**: `executor/simulation.rs` — SimulationExecutor with Leg 1/Leg 2 simulation, market rotation, Telegram + QuestDB integration, 13 tests.
  - **Phase 9 (Integration)**: Rewrote `main.rs` — MODE-based executor dispatch, Binance + Polymarket WS + market rotation in ingestor, Leg 1 + Leg 2 evaluation in engine. Rewrote `config.rs` — Mode enum, all PRD env vars, config validation.
  - **Bug fix**: SpikeDetector was evaluating per-tick delta against ATR-inflated threshold during sustain — changed to total displacement from origin for active candidates.
  - **Final QA**: 103 tests pass, 0 build errors, 0 clippy errors, format clean.
- **2026-02-21**: Runtime bug fixes — first successful bot startup:
  - Fixed rustls CryptoProvider panic: added `rustls::crypto::ring::default_provider().install_default()` as first line in `main()` — required by rustls 0.23+ before any TLS connection (Binance WS, Polymarket WS, Telegram reporter all use tokio-rustls).
  - Fixed Gamma API market discovery (422 Unprocessable Entity): old query `/markets?question=...&order=endTimestamp` used invalid params. Replaced with `/events?tag_id=102467&active=true&closed=false&limit=10` — tag 102467 is "15M" which filters 15-minute crypto markets. Updated response parser from flat market list to event→market nested structure. `clobTokenIds` is a JSON-encoded string (not array) on the Gamma API. Client-side filtering by slug prefix (`btc-updown-15m-` / `eth-updown-15m-`), selects soonest non-expired event. Series slug: `btc-up-or-down-15m`. Updated 4 unit tests.
  - Bot successfully connects to Binance WS, Polymarket Market WS, discovers active BTC 15-min market via Gamma API, and emits MarketRotation events to the engine.
- **2026-02-21**: Pipeline wiring fixes — end-to-end data flow verified:
  - Fixed Binance depth parsing: partial book depth stream (`@depth20@100ms`) has no `"T"` (transaction_time) field — made it optional with wall-clock fallback. Depth ticks now flow correctly, spike detection fires in real-time.
  - Wired dynamic Market WS subscription: added `tokio::sync::watch` channel between `run_market_rotation` (sender) and `run_market_ws` (receiver). Market WS waits for first rotation, then subscribes to discovered YES/NO token IDs. On subsequent rotations, WS drops current session and reconnects with new tokens (backoff reset).
  - Verified full pipeline: Binance WS → depth ticks → SpikeDetector → confirmed spikes → Engine. Gamma API → MarketRotation → Market WS subscribes → receives `last_trade_price` events. SimulationExecutor listening for trade signals.
- **2026-02-21**: Engine signal generation fixes — 4 blockers resolved:
  - **Rotation timing deadlock**: `ANTICIPATORY_THRESHOLD_MS` (180s) == `ENTRY_CUTOFF_SECS` (180s) meant signals were always blocked after rotation. Fixed by emitting MarketRotation immediately on Gamma discovery (not waiting for <180s remaining). Markets now trade from full duration.
  - **`available_capital` = 0**: `MarketState::new()` initialized `available_capital` to `Decimal::ZERO`, causing `remaining_alloc()` to always return 0, blocking all allocation checks. Fixed by initializing to `FIXED_ALLOC` (100 USDC) in `StrategyEngine::new()`.
  - **`poly_book` always None**: After MarketRotation, `poly_book` set to None; `BestBidAsk` and `PriceChange` handlers only update existing books, never create them. Fixed two ways: (1) REST book fetch via `GET /book?token_id=` after rotation emits `PolymarketBook` event, (2) `BestBidAsk` handler bootstraps minimal book with synthetic depth (500 per side) when `poly_book` is None.
  - **Diagnostic logging**: Added info-level logging to `evaluate()` showing all guard states on spike detection (has_book, has_binance, time_remaining, available_capital, remaining_alloc) and which specific guard blocks signal generation.
  - 103 tests pass.
- **2026-02-21**: SimulationExecutor runtime bug fixes — book data + QuestDB ILP protocol:
  - **QuestDB symbol ordering**: Fixed 3 methods in `storage/cold.rs` where `.symbol()` calls appeared after `.column_*()` calls, violating ILP protocol (`table → symbols → columns → at`). Moved `action` in `record_signal`, `profit_tier`/`leg1_order_id`/`leg2_order_id` in `record_trade`, and `profit_tier`/`resolution` in `record_simulated_trade` before all column calls.
  - **Book data wiring to executor**: Added `book_snapshot: Option<OrderBook>` field to `TradeSignal`. Engine attaches `self.state.poly_book.clone()` at all signal construction sites (Leg 1 in `evaluate()`, Leg 2 via `make_leg2_signal()`). SimulationExecutor extracts book + reference_price + market params from each signal in the `run()` loop, so `current_book` is always populated when handling fills.
  - 103 tests pass.
- **2026-02-21**: Fix signal flooding + simulate full Leg 1 → Leg 2 trade lifecycle:
  - **Engine self-gating**: Changed `evaluate(&self)` → `evaluate(&mut self)`. After signal emission: clears `spike_detected`, sets `leg1_state = Posted`, increments `cumulative_used`. Prevents duplicate signals from same spike. Only 1 trade in progress at a time.
  - **Leg 2 self-gating**: `evaluate_leg2()` sets `leg2_state = Posted` at all 4 signal emission points (3 emergency FOK paths + 1 normal erosion return). Prevents duplicate Leg 2 signals.
  - **`advance_simulation()` state machine**: New engine method called each tick in sim mode. Simulates fill progression: Leg 1 Posted→Filled when `best_ask <= posted_bid` (calls `init_erosion()`), Leg 2 Posted→Filled when target/eroded price reached or emergency submitted. Trade completion: both Filled → reset `leg1_state`, `leg2_state`, `erosion` to None. `cumulative_used` persists (capital cap respected across multiple trades per market).
  - **`init_erosion()` helper**: Extracted from `TradeStatusUpdate` handler for reuse across live and sim paths. Computes confidence, profit tier, creates `ErosionState` with target price, step size, and timing.
  - **Multiple trades per market**: After trade completion, state resets allow next spike to generate a new signal — if capital remains within `FIXED_ALLOC` cap.
  - **Telegram rate limiting**: Added `AtomicU64` rate limiter to `TelegramReporter` with 5s minimum interval. Messages dropped silently when rate-limited. Prevents 429 errors during burst signal generation.
  - **Log noise reduction**: Demoted 8 diagnostic `info!` → `debug!` in `evaluate()` (guard state checks). Kept `"Leg 1 signal generated"` at `info!`.
  - **main.rs wiring**: Added `engine.advance_simulation()` call in sim mode before signal evaluation. Uses `config.mode` (Copy) captured before spawn.
  - 8 new unit tests: `test_evaluate_self_gates_after_signal`, `test_evaluate_allows_new_signal_after_reset`, `test_advance_simulation_leg1_fill`, `test_advance_simulation_leg1_no_fill_when_ask_above_bid`, `test_advance_simulation_full_trade_cycle`, `test_advance_simulation_noop_when_idle`, `test_multiple_trades_per_market`, `test_init_erosion_helper`.
  - 111 tests pass, 0 build errors, format clean.
- **2026-02-21**: Merged `bot_prd.md` + `bot_testing_prd.md` into single unified `bot_prd.md`:
  - 15 sections covering both Live and Simulation modes in one document
  - Updated to reflect current implementation: engine self-gating, `advance_simulation()`, multiple trades per market, Leg 2 fill simulation, Telegram rate limiting
  - Added Section 6.5 (Multiple Trades Per Market) and Section 6.6 (Simulation Fill State Machine)
  - Added signal flooding + Telegram flooding to Risk Management table
  - Merged verification plans into unified Deployment Stages (Stage 1 sim → Stage 2 server sim → Stage 3 live)
  - Deleted `bot_testing_prd.md`. Updated CLAUDE.md Key Documents reference.
- **2026-02-21**: Gamma poll fix — immediate market discovery on expiry:
  - Root cause: after a market expired, `run_market_rotation()` cleared `current_market = None` then waited up to 10 minutes for the next scheduled Gamma poll, silently blocking all signals during the gap.
  - Fix: added `needs_immediate_poll: bool` flag. When expiry is detected, flag is set; next rotation_check tick (≤5s) calls `poll_gamma_and_emit()` immediately.
  - Extracted `poll_gamma_and_emit()` as a shared helper used by both the scheduled 10-minute poll and the immediate expiry-triggered poll.
  - Verified: new market discovered within 5s of expiry. Signals begin immediately after the new market's entry window opens.
- **2026-02-21**: Fixed 4 bugs causing simulated trades to always lose money (pair_cost=1.01, net_profit=-0.01):
  - **Bug 1 (pre-entry profitability guard)**: Added opposing-token ask check in `evaluate()` before emitting Leg 1 signal. For Direction::Down: blocks entry if `YES_ask ≥ (1 - bid_price)` (no arbitrage gap). For Direction::Up: blocks if `NO_ask ≥ (1 - bid_price)` when `poly_no_book` is available; allows otherwise. Prevents entering trades where the CLOB has already fully repriced.
  - **Bug 2 (dual-book tracking)**: Added `poly_yes_book: Option<OrderBook>` and `poly_no_book: Option<OrderBook>` to `MarketState`. All `PolymarketBook` and `PolymarketBestBidAsk` events are routed by `asset_id` to the correct directional book. Added `leg1_direction: Option<Direction>` to `StrategyEngine`. `advance_simulation()` now uses `poly_no_book` for Direction::Down Leg 1 fills and `poly_yes_book` for Direction::Up Leg 1 fills, preventing instant false fills from the opposing token's book.
  - **Bug 3 (Leg 2 executor position matching)**: Changed `SimulationExecutor::handle_leg2()` to match by first Open position (not by token_id). Leg 2 signal carries the opposing token_id which never matched the Leg 1 position's market_id.
  - **Bug 4 (info logging)**: Added `info!` log when spike detected but no active market (awaiting rotation). Promoted "< 180s remaining" from `debug!` to `info!`.
  - `MarketRotation` handler clears `poly_yes_book`, `poly_no_book`, and `leg1_direction`.
  - Trade completion in `advance_simulation()` clears `leg1_direction`.
- **2026-02-21**: Strategy correction — momentum-based entry (not lag-based):
  - **Removed pre-entry profitability guard (Bug 1 fix was incorrect)**: The guard blocked entries where `YES_ask ≥ (1 - bid_price)`, which is the normal condition for the momentum strategy. At entry time YES + NO = 1.00 (efficient market); profit comes from post-entry repricing driven by Binance spike momentum (e.g. buy YES at 0.70 → YES rises to 0.71, NO drops to 0.29 → Leg 2 fills at 0.29 → pair cost 0.99 < 1.00). Guard removed from `evaluate()` in `src/engine/strategy.rs`.
  - **Fixed Direction::Down bid price (core bug)**: `evaluate()` was using `poly_book` (YES book) for bid price computation for ALL directions. For Direction::Down (buying NO): YES bid = 0.70 → engine wrongly computed NO bid as 0.71, but NO is actually at ~0.30. Now direction-aware: Direction::Up → `poly_yes_book` / `poly_book` fallback; Direction::Down → `poly_no_book` if available, else derives from YES complement (`NO_bid ≈ 1 - YES_ask`, `NO_ask ≈ 1 - YES_bid`). All book-dependent calculations (spread, depth, wall detection, bid price) now use the correct directional book.
  - **Fixed `evaluate_leg2()` hedge book**: Was always using `poly_book` (YES book) for Leg 2 best_ask, depth, and wall detection. Now uses the hedge book: Direction::Up (bought YES) → `poly_no_book`; Direction::Down (bought NO) → `poly_yes_book`. Fallback to `poly_book` if directional book unavailable.
  - 111 tests pass, 0 build errors, format clean.
- **2026-02-22**: Fixed 5 bugs preventing simulation Leg 2 from firing and causing all trades to lose money:
  - **Bug 1 (Leg 1 fill never transitions → Leg 2 never fires)**: `advance_simulation()` used `best_ask <= fill_price` which never triggers in momentum trades (ask moves UP after spike). Replaced with depth-based fill model: (1) post-only still valid (`fill_price < best_ask`), (2) sell-side depth exists within 2 ticks of bid, (3) 500ms elapsed since posting (`SIM_FILL_DELAY_MS`). Uses directional book (`poly_yes_book` for Up, `poly_no_book` for Down).
  - **Bug 2 (Market rotation never reaches executor)**: Engine loop processed `MarketRotation` via `on_event()` but never notified executor. Added `ExecutorCommand` enum (`Signal(TradeSignal)` | `MarketRotation { condition_id }`) to `src/types/order.rs`. Engine detects rotation events before `on_event()` and forwards to executor channel. `SimulationExecutor::run()` now accepts `Receiver<ExecutorCommand>` and dispatches rotation to `on_market_rotation()` for trade reviews and Telegram summaries.
  - **Bug 3 (Break-even breach fires immediately)**: `evaluate_leg2()` checked `leg1_price + ask_price >= 1.0` which is ALWAYS true in efficient markets (YES + NO ≈ 1.0). Every trade immediately triggered emergency FOK at a loss before any repricing. Fixed by gating break-even check behind `now_ms >= snap.adverse_grace_expiry_ms` (3s grace period).
  - **Bug 4 (Wrong book_snapshot on signals)**: `evaluate()` always attached `poly_book` (typically YES book) regardless of direction. For Direction::Down entries, executor got wrong book, depth checks failed silently. Fixed to attach directional book (`poly_yes_book` for Up, `poly_no_book` for Down). Similarly fixed `evaluate_leg2()` to use hedge book for all Leg 2 signal snapshots.
  - **Bug 5 (Logging)**: Promoted 4 key guard failure logs from `debug!` to `info!` (stale book, spread too wide, insufficient allocation, insufficient depth) for runtime visibility.
  - Test updates: added `backdate_leg1()` helper, updated 4 existing tests for new fill model, added 2 new tests (`test_advance_simulation_leg1_no_fill_before_delay`, `test_advance_simulation_leg1_no_fill_no_depth`). 112 tests pass, 0 build errors, clippy clean.
- **2026-02-22**: Made all trading constants configurable — hybrid TOML + env approach:
  - **Problem**: 36+ hardcoded `const` values across `strategy.rs` (14), `binance.rs` (7), and `order.rs` (2). Config parsed 5 strategy env vars (`FIXED_ALLOC`, `MAX_ALLOC_PCT`, etc.) but they were never passed to `StrategyEngine` — changing `.env` had zero effect. Constants could not be tuned without recompiling.
  - **Solution**: `config.toml` for all tuning parameters (loaded via `toml` crate + serde with `#[serde(default)]`), `.env` for secrets/infrastructure only (loaded via `dotenvy`). `CONFIG_FILE` env var overrides config path.
  - **New config structs**: 6 TOML sub-structs (`SpikeDetectionConfig`, `EntryGuardsConfig`, `CapitalConfig`, `ConfidenceConfig`, `RiskConfig`, `SimulationConfig`) under `BotConfig` top-level. All have `Default` impls with production values.
  - **"Meet in the middle" defaults**: Widened entry conditions (allow weekend liquidity) but require stronger Binance signals: `spike_multiplier` 1.5→2.0, `sustain_ms` 200→300, `max_spread_pct` 0.03→0.10, `stale_book_ms` 500→1000.
  - **Parameterized modules**: `SpikeDetector` (7 config fields from `SpikeDetectionConfig`), `StrategyEngine` (16 config fields from all sub-structs), `ProfitTier::from_confidence()` (accepts `high_threshold`, `med_threshold` params).
  - **Config flow**: `Config::load()` replaces `Config::from_env()`. `main.rs` passes config to `BinanceGateway::new(url, spike_config)` and `StrategyEngine::new(&config)`. Startup log shows loaded config values.
  - **Test compatibility**: `Config::test_defaults()` (`#[cfg(test)]`) returns old hardcoded constant values. All test sites updated.
  - **Files changed**: `Cargo.toml` (+toml dep), `src/config.rs` (rewrite), `src/types/order.rs`, `src/gateway/binance.rs`, `src/engine/strategy.rs`, `src/main.rs`, `config.toml` (new), `.env.example` (trimmed), `.env` (trimmed).
  - 112 tests pass, 0 build errors, clippy clean, format clean.
- **2026-02-22**: Modular refactoring — split 5 monolithic files (8,772 lines) into 15 focused modules:
  - **Engine split**: `strategy.rs` (2,356→1,433 lines) decomposed into 4 modules. Extracted `evaluator.rs` (876 lines) with `Leg1Evaluator` + `Leg2Evaluator` + `Leg2Decision` enum (pure guard-checking, no state mutation). Extracted `confidence.rs` (compute_confidence, round_to_tick + 7 tests). Extracted `erosion.rs` (ErosionState, ErosionSnap, ConnectivityState).
  - **Polymarket WS split**: `polymarket_ws.rs` (2,008 lines) → `gateway/polymarket/` directory with 7 files: `mod.rs` (facade + constants), `market_ws.rs` (Market WS), `user_ws.rs` (User WS), `heartbeat.rs` (heartbeat loop), `rotation.rs` (Gamma API + rotation manager), `rest.rs` (CLOB REST gateway from old `polymarket.rs`), `tls_helpers.rs` (shared TLS/HTTP helpers).
  - **Binance split**: `binance.rs` (1,127 lines) → `gateway/binance/` directory with 3 files: `ws.rs` (BinanceGateway + JSON parsing), `spike.rs` (SpikeDetector EMA-ATR), `mod.rs` (re-exports).
  - **Executor split**: Created `fill_engine.rs` (382 lines) with `FillSimulator`, typed `Leg1Result`/`Leg2Result` enums, 7 new tests. `simulation.rs` delegates fill logic to FillSimulator.
  - **Reporting cleanup**: Extracted inner `mod formatter` in `telegram.rs` (5 format functions + escape_html).
  - **Types cleanup**: Added section separators to `types/simulation.rs`.
  - **Doc comments**: Added `//!` module-level docs to `engine/mod.rs`, `executor/mod.rs`, `gateway/mod.rs`.
  - **Zero behavioral changes** — pure structural reorganization. All import paths updated.
  - 120 tests pass (8 new), 0 build errors, 0 unused import warnings from refactoring.
- **2026-02-22**: Fixed 3 bugs preventing Leg 2 simulation fills from reaching the executor:
  - **Bug 1 (Critical)**: `advance_simulation()` filled Leg 2 internally (set `leg2_state = Filled`, reset to `None`) without sending any signal to the executor. The executor's `SimulationState` never received a `handle_leg2()` call — no `SimTrade` created, no Telegram sent, no QuestDB record. Fixed by changing `advance_simulation()` to return `Option<TradeSignal>`. New `build_sim_leg2_fill_signal()` helper constructs the signal from erosion state and hedge book. `main.rs` now forwards this signal to `executor_tx`.
  - **Bug 2**: `on_market_rotation()` called `lock_for_resolution()` for Open positions (Leg 1 filled, no Leg 2), setting `AwaitingResolution` but never calling `close_trade()` or `send_trade_completed()`. Positions silently accumulated with no Telegram notification. Fixed to call `close_trade()` for Open positions at rotation, generating a `SimTrade` with `leg2 = None` (full loss formula: `gross_profit = -leg1.price`), with Telegram + QuestDB recording.
  - **Bug 3**: Telegram 5s global rate limiter in `fire_and_forget()` silently dropped `send_trade_completed()` if an opportunity alert fired within 5 seconds. Added `fire_critical()` method that bypasses the rate limiter; used by `send_trade_completed()`.
  - Files changed: `src/engine/strategy.rs`, `src/main.rs`, `src/executor/simulation.rs`, `src/reporting/telegram.rs`.
  - 120 tests pass, 0 build errors.
