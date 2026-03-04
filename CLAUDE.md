# CLAUDE.md

## Session Workflow (MANDATORY)

**Every session, without exception, must follow this workflow:**

1. **Read & Orient**: Before doing ANY work, read this `CLAUDE.md` file and then read whichever referenced documents (`ARCHITECTURE.md`, `TRADING_LOGIC.md`, `config.toml`, relevant source files) pertain to the question or task at hand. Build a full picture of the current state before touching anything.

2. **Plan First**: Write a plan (use plan mode) before executing any code changes. The plan must reference specific files, functions, and conventions from the documents read in step 1. No code changes without a plan.

3. **Execute**: Implement the plan.

4. **Update Documentation**: After completing any work, update ALL relevant documentation to reflect the changes:
   - `CLAUDE.md` — Update conventions, source file descriptions, key patterns, or any section affected by the change
   - `ARCHITECTURE.md` — Update system design, trade lifecycle, risk controls, or configuration docs if affected
   - `TRADING_LOGIC.md` — Update if trading logic, evaluation, erosion, or execution flow changed
   - Any other referenced `.md` files that are affected

   **This is critical.** Future sessions rely on these docs to understand the system. Missing or outdated details cause bugs. Every behavioral change, new flag, new state transition, new guard, or new edge case MUST be documented in the appropriate file so the next session has full context.

## Project Overview

FaCaiBot is a Polymarket arbitrage bot targeting BTC 5-minute prediction markets. It detects Binance price spikes in real-time, buys cheap directional shares on the Polymarket CLOB before it reprices, then hedges with the opposite side — locking in a sub-$1.00 pair that pays $1.00 on resolution. See `ARCHITECTURE.md` for full system design.

## Build Commands

```bash
cargo build              # Build (debug)
cargo build --release    # Build (release — LTO, single codegen unit)
cargo run                # Run the bot
cargo test               # Run all tests (147 tests)
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
│       ├── user_ws.rs             # Authenticated User WS: "order" events → fill detection, "trade" events → logged only
│       ├── heartbeat.rs           # POST /heartbeat every 5s (live only)
│       ├── rotation.rs            # Gamma API market discovery + rotation
│       └── tls_helpers.rs         # Shared TLS/HTTP helpers
├── reporting/
│   └── telegram.rs                # Fire-and-forget Telegram Bot API via hyper
├── storage/
│   └── cold.rs                    # QuestDB: 5 ILP tables, batch flush (analytics only)
├── control/
│   ├── mod.rs                     # Module declarations
│   ├── types.rs                   # NotifyFlags, BotStatus, DrainStatus
│   ├── listener.rs                # TelegramCommandListener: getUpdates polling, auth, dispatch. Wallet commands spawned as independent tasks
│   ├── handlers.rs                # Command handlers (pure logic, returns reply strings)
│   ├── config_editor.rs           # TOML read/write, param allowlist with min/max ranges
│   └── wallet.rs                  # /balance, /polybalance, /redeem — Polygon RPC + CTF contract. CachedNonceManager for sequential txs. Resolution gate via CLOB API (skips unresolved markets)
├── types/
│   ├── market.rs                  # IngestorEvent (13 variants), MarketState, OrderBook
│   ├── order.rs                   # TradeSignal, ProfitTier, ExecutorCommand, ExecutorFeedback, FillMethod, Side
│   └── simulation.rs              # SimulationState, SimPosition, SimTrade
└── utils/
    ├── signing.rs                 # build_signer() helper (hex private key → PrivateKeySigner)
    ├── time.rs                    # epoch_ms() — single source of truth for millisecond timestamps
    └── tls.rs                     # Shared TLS config (build_tls_config) + SpawnExecutor for fastwebsockets
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
- **Erosion model**: Triangle-weighted steps `[5,4,3,2,1]` (front-loaded) with exponential decay intervals (3s→1.5s→0.75s→0.375s→0.2s). ~5.8s to break-even. Capped at `MAX_EROSION_STEPS` (5) — exhaustion auto-triggers emergency (distinct from `BreakEvenBreach`). **Exhaustion exit reason**: if `leg1_price + post_only_price < $1.00` at exhaustion → `FavorableTaker`; else `ErosionExhausted`. **Skip guard**: if posted Leg 2 price is already at or better than the next erosion target, the repost is skipped (preserves favorable exits). **Silent step advance guard**: steps only advance silently when `leg2_state == Posted` — prevents burning steps while no order is resting on the book
- **Emergency exits**: Price-improvement chase with hard deadline. Post-only at `best_ask - 1 tick`, only repost when book offers strictly better price (preserves FIFO queue priority). After `emergency_deadline_ms` (2500ms) → FOK taker at `round_to_tick(best_ask, tick)` (rounded to prevent SDK validation errors from raw book prices). **FOK dedup**: Once a deadline FOK is emitted (`fok_emitted=true` on `ErosionState`), subsequent evaluations return `None` — the executor's retry loop handles persistence. **Signal stacking prevention**: `emergency_signal_in_flight` flag on engine gates `evaluate_leg2()` while an emergency signal is in the executor channel — set on dispatch, cleared on feedback (OrderPosted, OrderFailed, CancelResult for leg2, trade complete, rotation). **Leg 2 command pending gate**: `leg2_command_pending` flag on engine gates `evaluate_leg2()` while ANY Leg 2 command (erosion or emergency) is being processed by the executor — prevents stale erosion commands from queuing while the executor is processing a multi-step favorable exit (~3.6s for 3 HTTP calls). Set on dispatch (live mode only, gated by `reporter.is_some()`), cleared on ANY Leg 2 feedback. **Stale feedback guard**: Defense in depth — `on_order_posted()` and `on_order_failed()` ignore Leg 2 feedback when `leg1_state` is not `Filled` (trade has already been reset). Prevents stale commands from contaminating the next trade. Executor sets `active_leg2_order_id = None` when FOK returns `Filled` (prevents stale cancel). Three triggers: (1) Adverse movement — Binance reversal >0.1%, zero grace; (2) Break-even breach — pair cost strictly > $1.00, after first erosion step; (3) Erosion exhausted — all 5 steps applied without fill. **FOK retry**: Emergency FOK orders retry up to `MAX_FOK_RETRIES` (10) before sending `OrderFailed`. Non-transient SDK errors ("decimal places", "Validation", "balance", "allowance") abort retries immediately; only liquidity errors keep retrying. FOK sizes sanitized via `clob_safe_fok_size()` — truncates size to 2dp then decrements by 0.01 until `price × size` has ≤2dp (verify-and-decrement; worst case ~2-3 iterations). **Zero-size guard**: If `clob_safe_fok_size()` returns zero, the order is aborted with `OrderFailed` instead of sending invalid orders. **Sync FOK fill detection**: All FOK `OrderPosted` feedback includes `already_filled=true` when `resp.status == OrderStatus::Filled` — the engine transitions Leg 2 directly to `Filled` and triggers immediate trade completion, bypassing the User WS wait that previously caused double-fill bugs. **$1 notional check**: Favorable exits check `price × size >= $1` before attempting (CLOB minimum for marketable orders). **"Crosses book" routing**: Leg 2 erosion `Err` containing "crosses book" routes to `attempt_favorable_exit()` instead of `OrderFailed` — executor sends `fill_method=FavorableMaker` (post-only) or `fill_method=FavorableTaker` (FOK fallback) on `OrderPosted` so the engine's `LiveTradeMeta` gets correct flags for Telegram tags. **Balance exhaustion**: `balance_exhausted` flag on `LiveExecutor` — set when "balance"/"allowance" error detected during Leg 2 placement. All subsequent Leg 2 commands immediately return `OrderFailed` without calling CLOB. Cleared on rotation. Executor sends `BalanceExhausted` feedback → engine fires critical Telegram alert with position details. **Rotation Telegram**: When `leg1_filled && !leg2_filled` at rotation, `fire_critical()` sends position details (direction, price, size, FOK status) so abandoned positions are never silent
- **Leg 1 staleness**: Unfilled Leg 1 post-only orders are cancelled after `leg1_timeout_ms` (default 5000ms) of actual book resting time. In live mode, `check_leg1_staleness()` skips provisional order IDs (`"sim-..."`) — the timer starts when `on_order_posted()` resets `timestamp_ms` with the real CLOB ID, so the ~1.2s CLOB round-trip doesn't count against the timeout. Sim mode uses `advance_simulation()`'s own staleness check (no CLOB round-trip, so provisional timing is correct)
- **User WS fill detection (live)**: The Polymarket User WS sends two event types for fills: `"order"` events (hex order hash, e.g. `0x13828d75...`) and `"trade"` events (UUID trade ID, e.g. `89f124e7-...`). Only `"order"` events are forwarded to the engine as `IngestorEvent::TradeStatusUpdate` — their hex hash matches the format stored by the engine from `ExecutorFeedback::OrderPosted`. `"trade"` UUIDs never match any stored order ID and are harmlessly ignored. Actionable statuses (MATCHED, MINED, CONFIRMED, FAILED, RETRYING, CANCELED) are forwarded via `parse_trade_status()`; non-actionable statuses (LIVE, etc.) are silently skipped. This covers all order types: Leg 1 entry, Leg 2 erosion, Leg 2 emergency, and Leg 2 favorable exits. **Exception**: FOK orders that return `Filled` synchronously from the REST API bypass User WS entirely — the `already_filled` flag on `OrderPosted` feedback triggers immediate `Filled` state transition + trade completion in the engine
- **Fire-and-confirm cancels**: All cancel operations return `Result<bool>` from the CLOB. The executor sends `CancelResult { was_cancelled, is_leg2 }` feedback to the engine. If the cancel was NOT confirmed (order may have filled before the cancel reached the CLOB), the engine restores the order's Posted state from saved info (`cancelled_leg1_info` / `prev_leg2_order`) so User WS MATCHED events can still match. After restore, `replay_pending_fills()` is called — for Leg 1 fills this also sends the opportunity alert and increments `live_market_signals`. `leg1_cancel_race = true` is set AFTER replay so it survives the `LiveTradeMeta::default()` reset inside replay — propagated to `SimTrade` and shown as `[FILLED MID-CANCEL]` in Telegram. `cancelled_leg1_info` saves `signal`, `direction`, AND `last_spike` — all three are restored on unconfirmed cancel so `init_erosion()` has spike info available for Leg 2 hedging. For Leg 2 erosion/emergency, the executor also skips posting the replacement order when the cancel is not confirmed. Unmatched `TradeStatusUpdate` events are buffered (up to 8) and replayed when state changes (OrderPosted, CancelResult) make them matchable
- **Provisional order ID race safety**: Speculative posting creates a provisional `"sim-leg1-{ts}"` ID. If `SpikeFailed` or staleness fires before the real CLOB ID arrives, a `cancel_leg1_on_feedback` flag defers the cancel until `on_order_posted()` receives the real ID — preventing ghost orders and invalid CLOB cancel requests
- **Tick size sync**: At rotation, `rotation.rs` fetches `GET /tick-size?token_id={yes_id}` from the CLOB and includes it in `IngestorEvent::MarketRotation { tick_size }`. The engine sets `state.tick_size` from this value. Mid-market changes are handled by `tick_size_change` WS events. Default `0.01` on fetch failure
- **SDK cache pre-warm (hard gate)**: On every `MarketRotation`, `LiveExecutor` calls `sdk.tick_size()`, `sdk.neg_risk()`, and `sdk.fee_rate_bps()` for both tokens — populating the SDK's `DashMap` caches with real CLOB values. `caches_warm: bool` gates all order placement: if any fetch fails, ALL signals are rejected with `OrderFailed` feedback until the next rotation. Eliminates the ~150ms first-order latency penalty from auto-fetch while guaranteeing correctness (no hardcoded values)
- **Deferred partial fill alerts**: The CLOB splits large fills across multiple rapid MATCHED events (~3ms apart). Partial fill checks (`size_matched < original_size`) are NOT alerted on MATCHED — instead stored in `pending_partial_fills` (HashMap keyed by order_id). Resolved when subsequent MATCHED shows fully filled (silent) or MINED/CONFIRMED arrives with final size (alert if still partial). Cleared on `MarketRotation` but NOT `on_trade_complete()` — MINED events may arrive after trade reset
- **Centralized timestamps**: All `epoch_ms()` calls use `crate::utils::time::epoch_ms` — single implementation, no duplicates
- **Telegram rate limit**: 5s `AtomicU64` rate limiter; `fire_critical()` bypasses for trade completions

## Key Documents

- `ARCHITECTURE.md` — System design, trade lifecycle, risk controls, configuration, deployment
- `queries.sql` — QuestDB analytics queries (fill rate, PnL by tier, pruning)
- `config.toml` — All tunable parameters with comments
