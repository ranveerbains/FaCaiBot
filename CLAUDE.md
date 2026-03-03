# CLAUDE.md

## Project Overview

FaCaiBot is a Polymarket arbitrage bot targeting BTC 5-minute prediction markets. It detects Binance price spikes in real-time, buys cheap directional shares on the Polymarket CLOB before it reprices, then hedges with the opposite side — locking in a sub-$1.00 pair that pays $1.00 on resolution. See `ARCHITECTURE.md` for full system design.

## Build Commands

```bash
cargo build              # Build (debug)
cargo build --release    # Build (release — LTO, single codegen unit)
cargo run                # Run the bot
cargo test               # Run all tests (141 tests)
cargo clippy             # Lint
cargo fmt                # Format code
```

## Local Infrastructure

```bash
docker-compose up -d     # Start QuestDB
docker-compose down      # Stop
```

- **QuestDB**: localhost:9000 (console), :9009 (ILP ingestion), :8812 (Postgres wire) — analytics only, not on execution path

Copy `.env.example` → `.env` and fill credentials before running. Tuning params live in `config.toml`.

## Production Deployment (AWS)

```bash
sudo bash deploy/setup.sh                                    # One-time server setup
RUSTFLAGS="-C target-cpu=native" cargo build --release       # Build with native CPU opts
bash deploy/deploy.sh                                        # Deploy/update
```

- **Instance**: `c7i.xlarge` in `eu-west-2` (London) — co-located with Polymarket CLOB
- **OS**: Amazon Linux 2023
- See `README.md` for full deployment guide and `deploy/` for scripts

## Architecture

Three-layer lock-free pipeline connected by crossbeam SPSC channels:

```
Ingestor (gateway/) ──▶ Engine (engine/) ──▶ Executor (executor/)
  Binance SBE WS          MarketState          MODE=live: CLOB API
  Polymarket WS            Spike→Signal         MODE=sim: Telegram+QuestDB
  Gamma API (REST)        Erosion cascade
```

- **Ingestor**: Dedicated OS thread, CPU-pinned core 0, single-threaded tokio runtime
- **Engine**: Runs on `spawn_blocking` thread (off the tokio worker pool). Evaluates arbitrage, emits TradeSignal. In sim mode also runs `advance_simulation()` fill state machine
- **Executor**: Live mode submits orders via polymarket-client-sdk; sim mode reports via Telegram+QuestDB

## Source Files

```
deploy/
├── facaibot.service               # systemd unit (auto-restart, CPU affinity, security)
├── sysctl.conf                    # Kernel network tuning for low-latency trading
├── setup.sh                       # One-time EC2 server provisioning
├── deploy.sh                      # Update deployment (git pull, build, restart)
└── healthcheck.sh                 # Cron health check with Telegram alerts

src/
├── main.rs                        # Entry point: jemalloc, manual tokio runtime (2 workers), engine on spawn_blocking, channel wiring
├── config.rs                      # Hybrid config: config.toml (tuning) + .env (secrets)
├── engine/
│   ├── strategy.rs                # StrategyEngine: event routing, state, simulation FSM
│   ├── evaluator.rs               # Leg1Evaluator + Leg2Evaluator (pure, no state mutation)
│   ├── confidence.rs              # Confidence scoring (spike/ATR + depth + time), round_to_tick()
│   └── erosion.rs                 # ErosionState/ErosionSnap: Leg 2 profit erosion FSM
├── executor/
│   ├── live.rs                    # LiveExecutor: CLOB order placement, cancel/repost, FOK emergency
│   ├── simulation.rs              # SimulationExecutor: simulated fills, Telegram reporting
│   └── fill_engine.rs             # Utility helpers: compute_fill_size, opposite_side
├── gateway/
│   ├── binance/
│   │   ├── ws.rs                  # BinanceGateway: fastwebsockets TLS, SBE binary decoding (50ms depth + real-time bestBidAsk)
│   │   └── spike.rs               # SpikeDetector: EMA-ATR with sustain + momentum filter
│   └── polymarket/
│       ├── mod.rs                 # Facade, shared constants
│       ├── rest.rs                # CLOB REST: SDK-based order placement and cancellation
│       ├── market_ws.rs           # Public Market WS: book, price, tick events
│       ├── user_ws.rs             # Authenticated User WS: trade fills
│       ├── heartbeat.rs           # POST /heartbeat every 5s (live only)
│       ├── rotation.rs            # Gamma API market discovery + rotation
│       └── tls_helpers.rs         # Shared TLS/HTTP helpers
├── reporting/
│   └── telegram.rs                # Fire-and-forget Telegram Bot API via hyper
├── storage/
│   └── cold.rs                    # QuestDB: 5 ILP tables, batch flush (analytics only)
├── types/
│   ├── market.rs                  # IngestorEvent (13 variants), MarketState, OrderBook
│   ├── order.rs                   # TradeSignal, ProfitTier, ExecutorCommand, ExecutorFeedback, Side
│   └── simulation.rs              # SimulationState, SimPosition, SimTrade
└── utils/
    ├── signing.rs                 # build_signer() helper (hex private key → PrivateKeySigner)
    └── time.rs                    # epoch_ms() — single source of truth for millisecond timestamps
```

## Key Conventions

- **jemalloc allocator**: Global allocator via `tikv-jemallocator` — eliminates glibc malloc latency spikes from arena contention. Conditional on `cfg(not(target_env = "msvc"))`
- **Tokio runtime**: Manual `Builder::new_multi_thread()` with 2 worker threads pinned to cores 1-2 via `on_thread_start`. Core 0 reserved for ingestor. Engine loop runs on `tokio::task::spawn_blocking` (off the worker pool) so that async tasks (command listener, auto-redeem, Telegram sends) always have a free worker thread — prevents thread starvation in live mode where the executor also blocks a worker
- **Decimal arithmetic**: All pricing uses `rust_decimal::Decimal` — never f32/f64 for prices or sizes
- **Channel-based data flow**: crossbeam bounded(8192) SPSC channels between layers — no Arc<Mutex>. Reverse `ExecutorFeedback` channel sends CLOB order IDs from live executor back to engine.
- **Hybrid config**: `config.toml` for tuning params (serde + `#[serde(default)]`), `.env` for secrets only. Override path with `CONFIG_FILE` env var
- **Evaluator pattern**: `Leg1Evaluator`/`Leg2Evaluator` are pure (no state mutation) — caller applies mutations after receiving results
- **Self-gating**: `evaluate(&mut self)` clears `spike_detected` on both success AND failure — each spike gets exactly 1 attempt
- **Binance SBE**: Binary market data via `stream-sbe.binance.com` (Ed25519 API key required). `@depth20` at 50ms cadence + `@bestBidAsk` real-time. Zero-copy decode: `i64::from_le_bytes()` at known offsets → `Decimal::new(mantissa, -exponent)`. Schema `stream_1_0.xml`, templates 10001 (BestBidAsk) and 10002 (DepthSnapshot)
- **Spike delivery**: Confirmed spikes delivered as `IngestorEvent::SpikeConfirmed(SpikeInfo)` — dedicated event variant, not encoded in BinanceTick fields
- **Erosion model**: Triangle-weighted steps `[5,4,3,2,1]` (front-loaded) with exponential decay intervals (3s→1.5s→0.75s→0.375s→0.2s). ~5.8s to break-even. Capped at `MAX_EROSION_STEPS` (5) — exhaustion auto-triggers `BreakEvenBreach` emergency. **Skip guard**: if posted Leg 2 price is already at or better than the next erosion target, the repost is skipped (preserves favorable exits)
- **Emergency exits**: Price-improvement chase with hard deadline. Post-only at `best_ask - 1 tick`, only repost when book offers strictly better price (preserves FIFO queue priority). After `emergency_deadline_ms` (2500ms) → FOK taker at `best_ask`. Three triggers: (1) Adverse movement — Binance reversal >0.1%, zero grace; (2) Break-even breach — pair cost strictly > $1.00, after first erosion step; (3) Erosion exhausted — all 5 steps applied without fill
- **Leg 1 staleness**: Unfilled Leg 1 post-only orders are cancelled after `leg1_timeout_ms` (default 5000ms) of actual book resting time. In live mode, `check_leg1_staleness()` skips provisional order IDs (`"sim-..."`) — the timer starts when `on_order_posted()` resets `timestamp_ms` with the real CLOB ID, so the ~1.2s CLOB round-trip doesn't count against the timeout. Sim mode uses `advance_simulation()`'s own staleness check (no CLOB round-trip, so provisional timing is correct)
- **Provisional order ID race safety**: Speculative posting creates a provisional `"sim-leg1-{ts}"` ID. If `SpikeFailed` or staleness fires before the real CLOB ID arrives, a `cancel_leg1_on_feedback` flag defers the cancel until `on_order_posted()` receives the real ID — preventing ghost orders and invalid CLOB cancel requests
- **SDK cache pre-warm (hard gate)**: On every `MarketRotation`, `LiveExecutor` calls `sdk.tick_size()`, `sdk.neg_risk()`, and `sdk.fee_rate_bps()` for both tokens — populating the SDK's `DashMap` caches with real CLOB values. `caches_warm: bool` gates all order placement: if any fetch fails, ALL signals are rejected with `OrderFailed` feedback until the next rotation. Eliminates the ~150ms first-order latency penalty from auto-fetch while guaranteeing correctness (no hardcoded values)
- **Centralized timestamps**: All `epoch_ms()` calls use `crate::utils::time::epoch_ms` — single implementation, no duplicates
- **Telegram rate limit**: 5s `AtomicU64` rate limiter; `fire_critical()` bypasses for trade completions

## Key Documents

- `ARCHITECTURE.md` — System design, trade lifecycle, risk controls, configuration, deployment
- `queries.sql` — QuestDB analytics queries (fill rate, PnL by tier, pruning)
- `config.toml` — All tunable parameters with comments
