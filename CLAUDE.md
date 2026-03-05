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
│   ├── confidence.rs              # Confidence scoring (ATR-ratio spike + depth + time), round_to_tick()
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
│   └── wallet.rs                  # /balance, /polybalance, /redeem — Polygon RPC + CTF contract. CachedNonceManager for sequential txs. Per-tx receipt timeout (8s)
├── types/
│   ├── market.rs                  # IngestorEvent (13 variants), MarketState, OrderBook
│   ├── order.rs                   # TradeSignal, ProfitTier, ExecutorCommand, ExecutorFeedback, FillMethod (3 variants: FavorableMaker, FavorableTaker, EmergencyTaker), Side, ExitReason (7 variants: AdverseMovement, BreakEvenBreach, MarketExpiry, FavorableTaker, ErosionExhausted, PreErosionBreach, WhipsawReversal)
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
- **Zero-alloc tier guard**: If `tier_pct == 0` for any tier, the signal is rejected (`Leg1RejectReason::Other`) before allocation. Currently all tiers are enabled: `high_alloc_pct = 1.0`, `med_alloc_pct = 0.8`, `low_alloc_pct = 0.5`. Disable a tier by setting its `*_alloc_pct = 0`
- **Binance SBE**: Binary market data via `stream-sbe.binance.com` (Ed25519 API key required). `@depth20` at 50ms cadence + `@bestBidAsk` real-time. Zero-copy decode: `i64::from_le_bytes()` at known offsets → `Decimal::new(mantissa, -exponent)`. Schema `stream_1_0.xml`, templates 10001 (BestBidAsk) and 10002 (DepthSnapshot)
- **Spike delivery**: Confirmed spikes delivered as `IngestorEvent::SpikeConfirmed(SpikeInfo)` — dedicated event variant, not encoded in BinanceTick fields. `SpikeInfo` carries `atr_ratio: Decimal` (abs_displacement / ema_atr, computed in spike detector) — used by confidence scoring f1 factor instead of raw magnitude
- **Erosion model**: Triangle-weighted steps `[5,4,3,2,1]` (front-loaded) with exponential decay intervals (2s→1s→0.5s→0.25s→0.2s). ~4.0s to break-even. Capped at `MAX_EROSION_STEPS` (5) — exhaustion auto-triggers emergency (distinct from `BreakEvenBreach`). **Exhaustion exit reason**: if `leg1_price + post_only_price < $1.00` at exhaustion → `FavorableTaker`; else `ErosionExhausted`. **Skip guard**: if posted Leg 2 price is already at or better than the next erosion target, the repost is skipped (preserves favorable exits). **Silent step advance guard**: steps only advance silently when `leg2_state == Posted` — prevents burning steps while no order is resting on the book
- **Emergency exits**: Price-improvement chase with hard deadline. Post-only at `best_ask - 1 tick`, only repost when book offers strictly better price (preserves FIFO queue priority). After `emergency_deadline_ms` (2000ms) → FOK taker at `round_to_tick(best_ask, tick)` (rounded to prevent SDK validation errors from raw book prices). **Deadline FOK exit_reason re-evaluation**: At deadline, if `pair_cost >= $1.00`, stale `FavorableTaker` is overridden to `ErosionExhausted`. Similarly, `already_filled` FOK feedback checks `leg1_price + price >= $1.00` and clears `favorable_taker` if the pair is unprofitable. **FOK dedup**: Once a deadline FOK is emitted (`fok_emitted=true` on `ErosionState`), subsequent evaluations return `None` — the executor's retry loop handles persistence. **Signal stacking prevention**: `emergency_signal_in_flight` flag on engine gates `evaluate_leg2()` while an emergency signal is in the executor channel — set on dispatch, cleared on feedback (OrderPosted, OrderFailed, CancelResult for leg2, trade complete, rotation). **Leg 2 command pending gate**: `leg2_command_pending` flag on engine gates `evaluate_leg2()` while ANY Leg 2 command (erosion or emergency) is being processed by the executor — prevents stale erosion commands from queuing while the executor is processing a multi-step favorable exit (~3.6s for 3 HTTP calls). Set on dispatch (live mode only, gated by `reporter.is_some()`), cleared on ANY Leg 2 feedback. **Stale feedback guard**: Defense in depth — `on_order_posted()` and `on_order_failed()` ignore Leg 2 feedback when `leg1_state` is not `Filled` (trade has already been reset). Prevents stale commands from contaminating the next trade. Executor sets `active_leg2_order_id = None` when FOK returns `Filled` (prevents stale cancel). Five triggers: (1) Adverse movement — Binance reversal >0.1%, zero grace; (2) Pre-erosion breach — pair cost > `pre_erosion_breach_threshold` ($1.02) before first erosion step (`steps_applied == 0`), catches fast book repricing within ~2s; (3) Break-even breach — pair cost strictly > $1.00, after first erosion step (`steps_applied >= 1`); (4) Erosion exhausted — all 5 steps applied without fill; (5) Whipsaw reversal — opposite spike detected after Leg 1 fill, immediate FOK at best ask bypassing erosion entirely. **Pre-erosion vs break-even**: Mutually exclusive — pre-erosion fires only at `steps_applied == 0`, break-even only at `steps_applied >= 1`. **FOK price escalation**: Emergency FOK orders (`emergency_fok_fallback` and `emergency_fok_at_price`) escalate price +1 tick per attempt on liquidity failure (Rejected or non-transient Err), sweeping the book up to a hard cap of `$1.00`. No fixed retry count — the natural ceiling is ~23 ticks from any starting price (~2.3s to sweep). Non-transient SDK errors ("decimal places", "Validation", "balance", "allowance") abort immediately. `clob_safe_fok_size()` recomputed each iteration (price changes → size constraint changes) — truncates size to 2dp then decrements by 0.01 until `price × size` has ≤2dp. **Zero-size guard**: If `clob_safe_fok_size()` returns zero at any price, the sweep aborts with `OrderFailed`. **Price cap guard**: If escalated price exceeds `$1.00`, aborts with `OrderFailed`. **Sync FOK fill detection**: All FOK `OrderPosted` feedback includes `already_filled=true` when `resp.status == OrderStatus::Filled` — the engine transitions Leg 2 directly to `Filled` and triggers immediate trade completion, bypassing the User WS wait that previously caused double-fill bugs. **$1 notional check**: Favorable exits check `price × size >= $1` before attempting (CLOB minimum for marketable orders). **"Crosses book" routing**: Leg 2 erosion `Err` containing "crosses book" (or `Ok(Rejected)`) routes to `attempt_favorable_exit()` instead of `OrderFailed`. **Favorable walk-down**: `attempt_favorable_exit()` tries up to 4 post-only attempts at exponential tick offsets `[1, 2, 4, 8]` from `signal.price` (i.e., `price - 1*tick`, `price - 2*tick`, `price - 4*tick`, `price - 8*tick`). Each attempt is ~100ms (CLOB HTTP round-trip). First accepted placement rests as maker for the remaining erosion window (~4.0s). If all 4 cross or fail → FOK fallback at `signal.price`. Guards: `price > 0` and `price × size >= $1`. Non-crossing errors (balance, etc.) skip remaining attempts and go straight to FOK. Executor sends `fill_method=FavorableMaker` (post-only), `fill_method=FavorableTaker` (FOK fallback), or `fill_method=EmergencyTaker` (emergency FOK paths) on `OrderPosted` so the engine's `LiveTradeMeta` gets correct flags for Telegram tags. **EmergencyTaker handling**: Sets `leg2_was_taker=true` and `emergency_maker=false` on `LiveTradeMeta`. **Leg 2 cancel-not-confirmed meta reset**: When a Leg 2 cancel returns `was_cancelled=false` and Posted state is restored, `LiveTradeMeta` is reset (preserving `leg1_cancel_race`) and erosion emergency state (`emergency_submitted`, `exit_reason`) is cleared — prevents successful maker fills from being mislabeled as emergency exits. **Balance exhaustion**: `balance_exhausted` flag on `LiveExecutor` — set when "balance"/"allowance" error detected during Leg 2 placement. All subsequent Leg 2 commands immediately return `OrderFailed` without calling CLOB. Cleared on rotation. Executor sends `BalanceExhausted` feedback → engine fires critical Telegram alert with position details. **Rotation Telegram**: When `leg1_filled && !leg2_filled` at rotation, `fire_critical()` sends position details (direction, price, size, FOK status) so abandoned positions are never silent
- **Rotation quiet period**: After `MarketRotation`, spike candidates are dropped for `rotation_quiet_ms` (default 30000ms). `in_quiet_period` flag set on rotation, cleared when elapsed time exceeds config. Prevents entries on stale-book repricing during the first ~30s of a new market. Modeled on `in_cutoff_window` pattern. Diagnostic counter: `diag_spikes_dropped_quiet`
- **Whipsaw spike guard**: When `SpikeConfirmed` arrives with opposite direction to `leg1_direction`: (a) Leg 1 Posted (unfilled) → cancel immediately (reuses SpikeFailed cancel pattern including provisional ID deferral); (b) Leg 1 Filled → **log only**, rely on existing emergency exits (adverse movement, pre-erosion breach, break-even breach) to handle based on actual book conditions — avoids cancelling favorable resting Leg 2 orders unnecessarily. `emit_whipsaw_fok()`, `WhipsawReversal` exit reason, and `diag_whipsaw_foks` counter remain in codebase but the Filled path no longer sets `whipsaw_fok_pending`. Diagnostic counters: `diag_whipsaw_cancels` (active for Posted cancels), `diag_whipsaw_foks` (no longer incremented for Filled Leg 1)
- **init_erosion direction safety**: Uses `self.leg1_direction.unwrap_or(spike.direction)` instead of `spike.direction` — survives spike overwrites between Leg 1 fill and erosion init, preventing YES/NO label swap in trade completion messages
- **Leg 1 staleness**: Unfilled Leg 1 post-only orders are cancelled after `leg1_timeout_ms` (default 5000ms) of actual book resting time. In live mode, `check_leg1_staleness()` skips provisional order IDs (`"sim-..."`) — the timer starts when `on_order_posted()` resets `timestamp_ms` with the real CLOB ID, so the ~1.2s CLOB round-trip doesn't count against the timeout. Sim mode uses `advance_simulation()`'s own staleness check (no CLOB round-trip, so provisional timing is correct)
- **User WS fill detection (live)**: The Polymarket User WS sends two event types for fills: `"order"` events (hex order hash, e.g. `0x13828d75...`) and `"trade"` events (UUID trade ID, e.g. `89f124e7-...`). Only `"order"` events are forwarded to the engine as `IngestorEvent::TradeStatusUpdate` — their hex hash matches the format stored by the engine from `ExecutorFeedback::OrderPosted`. `"trade"` UUIDs never match any stored order ID and are harmlessly ignored. Actionable statuses (MATCHED, MINED, CONFIRMED, FAILED, RETRYING, CANCELED) are forwarded via `parse_trade_status()`; non-actionable statuses (LIVE, etc.) are silently skipped. This covers all order types: Leg 1 entry, Leg 2 erosion, Leg 2 emergency, and Leg 2 favorable exits. **Exception**: FOK orders that return `Filled` synchronously from the REST API bypass User WS entirely — the `already_filled` flag on `OrderPosted` feedback triggers immediate `Filled` state transition + trade completion in the engine
- **Fire-and-confirm cancels**: All cancel operations return `Result<bool>` from the CLOB. The executor sends `CancelResult { was_cancelled, is_leg2 }` feedback to the engine. If the cancel was NOT confirmed (order may have filled before the cancel reached the CLOB), the engine restores the order's Posted state from saved info (`cancelled_leg1_info` / `prev_leg2_order`) so User WS MATCHED events can still match. After restore, `replay_pending_fills()` is called — for Leg 1 fills this also sends the opportunity alert and increments `live_market_signals`. `leg1_cancel_race = true` is set AFTER replay so it survives the `LiveTradeMeta::default()` reset inside replay — propagated to `SimTrade` and shown as `[FILLED MID-CANCEL]` in Telegram. `cancelled_leg1_info` saves `signal`, `direction`, AND `last_spike` — all three are restored on unconfirmed cancel so `init_erosion()` has spike info available for Leg 2 hedging. For Leg 2 erosion/emergency, the executor also skips posting the replacement order when the cancel is not confirmed. **Leg 2 cancel-not-confirmed**: when Leg 2 Posted state is restored, `LiveTradeMeta` is reset (preserving `leg1_cancel_race`) and erosion `emergency_submitted`/`exit_reason` are cleared — prevents maker fills from being mislabeled as emergency exits. Unmatched `TradeStatusUpdate` events are buffered (up to 8) and replayed when state changes (OrderPosted, CancelResult) make them matchable
- **Provisional order ID race safety**: Speculative posting creates a provisional `"sim-leg1-{ts}"` ID. If `SpikeFailed` or staleness fires before the real CLOB ID arrives, a `cancel_leg1_on_feedback` flag defers the cancel until `on_order_posted()` receives the real ID — preventing ghost orders and invalid CLOB cancel requests
- **Tick size sync**: At rotation, `rotation.rs` fetches `GET /tick-size?token_id={yes_id}` from the CLOB and includes it in `IngestorEvent::MarketRotation { tick_size }`. The engine sets `state.tick_size` from this value. Mid-market changes are handled by `tick_size_change` WS events. Default `0.01` on fetch failure
- **SDK cache pre-warm (hard gate)**: On every `MarketRotation`, `LiveExecutor` calls `sdk.tick_size()`, `sdk.neg_risk()`, and `sdk.fee_rate_bps()` for both tokens — populating the SDK's `DashMap` caches with real CLOB values. `caches_warm: bool` gates all order placement: if any fetch fails, ALL signals are rejected with `OrderFailed` feedback until the next rotation. Eliminates the ~150ms first-order latency penalty from auto-fetch while guaranteeing correctness (no hardcoded values)
- **Deferred partial fill alerts**: The CLOB splits large fills across multiple rapid MATCHED events (~3ms apart). Partial fill checks (`size_matched < original_size`) are NOT alerted on MATCHED — instead stored in `pending_partial_fills` (HashMap keyed by order_id). Resolved when subsequent MATCHED shows fully filled (silent) or MINED/CONFIRMED arrives with final size (alert if still partial). Cleared on `MarketRotation` but NOT `on_trade_complete()` — MINED events may arrive after trade reset
- **Centralized timestamps**: All `epoch_ms()` calls use `crate::utils::time::epoch_ms` — single implementation, no duplicates
- **Maker rebate estimates**: `SimFill::compute_maker_rebate(price, size)` = `compute_taker_fee(price, size) × 0.20` — upper-bound estimate of Polymarket daily maker rebate. `SimFill.maker_rebate` non-zero for maker fills, zero for taker. `SimTrade.maker_rebate` = sum of both legs. Net profit = `gross_profit - taker_fee + maker_rebate`. Accumulated on `SimulationState.total_maker_rebates_earned` and `MarketSummary.maker_rebates_earned`. Shown in Telegram trade completion and market/session summaries
- **Telegram rate limit**: 5s `AtomicU64` rate limiter; `fire_critical()` bypasses for trade completions

## Key Documents

- `ARCHITECTURE.md` — System design, trade lifecycle, risk controls, configuration, deployment
- `queries.sql` — QuestDB analytics queries (fill rate, PnL by tier, pruning)
- `config.toml` — All tunable parameters with comments
