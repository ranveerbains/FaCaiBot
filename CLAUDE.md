# CLAUDE.md

## Session Workflow (MANDATORY)

**Every session, without exception, must follow this workflow:**

1. **Read & Orient**: Before doing ANY work, read this `CLAUDE.md` file and then read whichever referenced documents (`ARCHITECTURE.md`, `TRADING_LOGIC.md`, `config.toml`, relevant source files) pertain to the question or task at hand. Build a full picture of the current state before touching anything.

2. **Plan First**: Write a plan (use plan mode) before executing any code changes. The plan must reference specific files, functions, and conventions from the documents read in step 1. No code changes without a plan.

3. **Execute**: Implement the plan.

4. **Update Documentation**: After completing any work, update ALL relevant documentation to reflect the changes:
   - `CLAUDE.md` — Update conventions, source file descriptions, key patterns, or any section affected by the change
   - `ARCHITECTURE.md` — Update system design, trade lifecycle, risk controls, or configuration docs if affected
   - `TRADING_LOGIC.md` — Update if trading logic, evaluation, hedge phases, or execution flow changed
   - Any other referenced `.md` files that are affected

   **This is critical.** Future sessions rely on these docs to understand the system. Missing or outdated details cause bugs. Every behavioral change, new flag, new state transition, new guard, or new edge case MUST be documented in the appropriate file so the next session has full context.

## Project Overview

FaCaiBot is a Polymarket arbitrage bot targeting BTC 5-minute prediction markets. It detects Binance price spikes in real-time, buys cheap directional shares on the Polymarket CLOB before it reprices, then hedges with the opposite side — locking in a sub-$1.00 pair that pays $1.00 on resolution. See `ARCHITECTURE.md` for full system design.

## Build Commands

```bash
cargo build              # Build (debug)
cargo build --release    # Build (release — LTO, single codegen unit)
cargo run                # Run the bot
cargo test               # Run all tests (149 tests)
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
  Gamma API (REST)        Hedge phases
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
│   ├── confidence.rs              # Repricing model (compute_expected_repricing), round_to_tick()
│   └── erosion.rs                 # HedgeState/HedgeSnap/HedgePhase: Leg 2 two-phase hedge FSM
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
│   └── wallet.rs                  # /balance, /polybalance, /redeem — Polygon RPC + CTF contract. CachedNonceManager for sequential txs. Per-tx receipt timeout (8s). Persistent redeems.txt: append_condition_id_sync (engine thread), read/cleanup (async redeem)
├── types/
│   ├── market.rs                  # IngestorEvent (13 variants), MarketState, OrderBook
│   ├── order.rs                   # TradeSignal, ProfitTier, ExecutorCommand (incl. PostLeg2Phase2, CancelLeg2Order, RebalanceLeg1), ExecutorFeedback (incl. Leg2OrderCancelResult, RebalanceResult), FillMethod (3 variants: FavorableMaker, FavorableTaker, EmergencyTaker), OrderTag (Leg2Phase1, Leg2Phase2, Rebalance), Side, ExitReason (5 variants: BreakEvenBreach, MarketExpiry, FavorableTaker, Phase1Breach, WhipsawReversal)
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
- **Repricing model**: `compute_expected_repricing()` in `confidence.rs` replaces fixed confidence scoring + tiers. `expected_pct = norm_spike × 4P(1-P) × alignment × time_factor × reprice_scale`. Output IS the Phase 1 profit target (after `round_to_tick()`). Three-layer entry guard: hard skew cap (`hard_skew_cap`, default 0.90), min repricing (`min_reprice_pct`, default 0.5%), dynamic allocation (`clamp(output/reprice_scale, min_alloc_pct, 1.0)`). `ProfitTier::from_expected_reprice()` is display-only (HIGH/MED/LOW labels). Config: `[repricing]` section with `reprice_scale`, `min_reprice_pct`, `min_alloc_pct`, `hard_skew_cap`, `time_exponent`
- **Binance SBE**: Binary market data via `stream-sbe.binance.com` (Ed25519 API key required). `@depth20` at 50ms cadence + `@bestBidAsk` real-time. Zero-copy decode: `i64::from_le_bytes()` at known offsets → `Decimal::new(mantissa, -exponent)`. Schema `stream_1_0.xml`, templates 10001 (BestBidAsk) and 10002 (DepthSnapshot)
- **Spike delivery**: Confirmed spikes delivered as `IngestorEvent::SpikeConfirmed(SpikeInfo)` — dedicated event variant, not encoded in BinanceTick fields. `SpikeInfo` carries `atr_ratio: Decimal` (abs_displacement / ema_atr, computed in spike detector) — used by repricing model's `norm_spike` factor
- **Hedge model (2-phase dual-order)**: `HedgeState`/`HedgeSnap`/`HedgePhase` in `erosion.rs`. **Phase 1 (Profit, post-once-and-wait)**: post once at confidence-scaled profit target (raw price, no clamping — executor handles crosses-book via `attempt_favorable_maker_then_fok()`), wait `phase1_timeout_ms` (default 2000ms). Evaluator returns `None` if `leg2_state` is `Posted` or `Filled` — no reposts, no outbidding. `OrderFailed` resets `leg2_state` to `None`, allowing retry. Phase 1 breach (pair cost > `phase1_breach_threshold`, default $1.05) → **immediate FOK taker** at ask. Phase 1 timeout → transition to Phase 2. **Phase 2 (Dual-order)**: post at `best_ask - 1 tick` **without cancelling Phase 1** — two maker orders rest simultaneously. Whichever fills first → cancel the other → trade complete. `phase2_timeout_ms` (default 2000ms) deadline → FOK taker. Phase 2 breach (`ask > phase2_posted_price`) → **immediate FOK taker** (requires `phase2_posted_price` to be `Some`, which is always set at Phase 2 entry). Phase 2 entry guard: if `ask - tick > breakeven`, skip posting → immediate FOK. **Double-fill rebalance**: Rare race where both orders fill (~3ms window) → FOK taker buy on Leg 1 side. `post_trade_orphan` persists across `on_trade_complete()`, `rebalance_in_progress` gates evaluation. **Dual-order tracking**: `leg2_phase1_order_id` / `leg2_phase2_order_id` on engine, `active_leg2_phase1_id` / `active_leg2_phase2_id` on executor. `OrderTag` enum (`Leg2Phase1`, `Leg2Phase2`, `Rebalance`) on `OrderPosted` feedback routes IDs correctly
- **Emergency exits**: All emergency exits are immediate FOK taker at ask. **FOK dedup**: Once a FOK is emitted (`fok_emitted=true` on `HedgeState`), subsequent evaluations return `None` — the executor's retry loop handles persistence. **Signal stacking prevention**: `emergency_signal_in_flight` flag on engine gates `evaluate_leg2()` while an emergency signal is in the executor channel — set on dispatch, cleared on feedback (OrderPosted, OrderFailed, CancelResult for leg2, trade complete, rotation). **Leg 2 command pending gate**: `leg2_command_pending` flag on engine gates `evaluate_leg2()` while ANY Leg 2 command (hedge or emergency) is being processed by the executor — prevents stale hedge commands from queuing while the executor is processing a multi-step favorable exit. Set on dispatch (live mode only, gated by `reporter.is_some()`), cleared on ANY Leg 2 feedback. **Stale feedback guard**: Defense in depth — `on_order_posted()` and `on_order_failed()` ignore Leg 2 feedback when `leg1_state` is not `Filled` (trade has already been reset). Four triggers: (1) Phase 1 breach — pair cost > `phase1_breach_threshold` ($1.05) during Phase 1, **immediate FOK taker** at ask; (2) Phase 2 breach — `ask > phase2_posted_price` during Phase 2, **immediate FOK taker**; (3) Market expiry — market approaching resolution cutoff; (4) Whipsaw reversal — opposite spike detected after Leg 1 fill, immediate FOK at best ask bypassing hedge phases entirely. **FOK price escalation**: Emergency FOK orders (`emergency_fok_fallback`) escalate price +1 tick per attempt on liquidity failure (Rejected or non-transient Err), sweeping the book up to a hard cap of `$1.00`. No fixed retry count — the natural ceiling is ~23 ticks from any starting price (~2.3s to sweep). Non-transient SDK errors ("decimal places", "Validation", "balance", "allowance") abort immediately. `clob_safe_fok_size()` recomputed each iteration (price changes → size constraint changes) — truncates size to 2dp then decrements by 0.01 until `price × size` has ≤2dp. **Zero-size guard**: If `clob_safe_fok_size()` returns zero at any price, the sweep aborts with `OrderFailed`. **Price cap guard**: If escalated price exceeds `$1.00`, aborts with `OrderFailed`. **Sync FOK fill detection**: All FOK `OrderPosted` feedback includes `already_filled=true` when `resp.status == OrderStatus::Filled` — the engine transitions Leg 2 directly to `Filled` and triggers immediate trade completion, bypassing the User WS wait that previously caused double-fill bugs. **$1 notional check**: Favorable exits check `price × size >= $1` before attempting (CLOB minimum for marketable orders). **"Crosses book" routing**: Leg 2 hedge `Err` containing "crosses book" (or `Ok(Rejected)`) routes to `attempt_favorable_maker_then_fok()` instead of `OrderFailed`. **Favorable try-maker-first**: `attempt_favorable_maker_then_fok()` posts a maker at `best_ask - 1tick`, polls `GET /data/order/{id}` every 200ms for up to `favorable_maker_timeout_ms` (1000ms). If filled → `fill_method=FavorableMaker` (no taker fee + rebate). If not filled → cancel: if cancel confirmed → FOK taker fallback (`fill_method=FavorableTaker`); if cancel NOT confirmed → send `OrderPosted` with `already_filled=false` (let User WS determine outcome). Emergency FOK paths send `fill_method=EmergencyTaker`. **EmergencyTaker handling**: Sets `leg2_was_taker=true` and `emergency_maker=false` on `LiveTradeMeta`. **Leg 2 cancel-not-confirmed meta reset**: When a Leg 2 cancel returns `was_cancelled=false` and Posted state is restored, `LiveTradeMeta` is reset (preserving `leg1_cancel_race`) and hedge emergency state (`emergency_submitted`, `exit_reason`) is cleared — prevents successful maker fills from being mislabeled as emergency exits. **Balance exhaustion**: `balance_exhausted` flag on `LiveExecutor` — set when "balance"/"allowance" error detected during Leg 2 placement. All subsequent Leg 2 commands immediately return `OrderFailed` without calling CLOB. Cleared on rotation. Executor sends `BalanceExhausted` feedback → engine fires critical Telegram alert with position details. **Rotation Telegram**: When `leg1_filled && !leg2_filled` at rotation, `fire_critical()` sends position details (direction, price, size, FOK status) so abandoned positions are never silent
- **Rotation quiet period**: After `MarketRotation`, spike candidates are dropped for `rotation_quiet_ms` (default 30000ms). `in_quiet_period` flag set on rotation, cleared when elapsed time exceeds config. Prevents entries on stale-book repricing during the first ~30s of a new market. Modeled on `in_cutoff_window` pattern. Diagnostic counter: `diag_spikes_dropped_quiet`
- **Trade cooldown**: After trade completion, new Leg 1 entries are blocked for `trade_cooldown_ms` (default 5000ms). `in_trade_cooldown` flag set in `on_trade_complete()`, cleared when elapsed time exceeds config (checked in `update_phase()`). Cleared on `MarketRotation` (new market shouldn't inherit stale cooldown). Guard lives in `evaluate()` — spikes flow through normally but entry is rejected. Prevents rapid-fire re-entry after completing a trade. Diagnostic counter: `diag_spikes_dropped_cooldown`
- **Whipsaw spike guard**: When `SpikeConfirmed` arrives with opposite direction to `leg1_direction`: (a) Leg 1 Posted (unfilled) → cancel immediately (reuses SpikeFailed cancel pattern including provisional ID deferral); (b) Leg 1 Filled → set `whipsaw_fok_pending = true`, triggering immediate emergency exit via `emit_whipsaw_fok()` with `ExitReason::WhipsawReversal` and `sim_was_taker = true` (direct FOK, no post-only attempt — speed is critical during active reversal). The existing price-escalating FOK loop in the executor handles the exit. Diagnostic counters: `diag_whipsaw_cancels` (Posted cancels), `diag_whipsaw_foks` (Filled Leg 1 emergency exits)
- **init_leg2 direction safety**: Uses `self.leg1_direction.unwrap_or(spike.direction)` instead of `spike.direction` — survives spike overwrites between Leg 1 fill and hedge init, preventing YES/NO label swap in trade completion messages
- **Leg 1 staleness**: Unfilled Leg 1 post-only orders are cancelled after `leg1_timeout_ms` (default 2500ms) of actual book resting time. In live mode, `check_leg1_staleness()` skips provisional order IDs (`"sim-..."`) — the timer starts when `on_order_posted()` resets `timestamp_ms` with the real CLOB ID, so the ~1.2s CLOB round-trip doesn't count against the timeout. Sim mode uses `advance_simulation()`'s own staleness check (no CLOB round-trip, so provisional timing is correct)
- **User WS fill detection (live)**: The Polymarket User WS sends two event types for fills: `"order"` events (hex order hash, e.g. `0x13828d75...`) and `"trade"` events (UUID trade ID, e.g. `89f124e7-...`). Only `"order"` events are forwarded to the engine as `IngestorEvent::TradeStatusUpdate` — their hex hash matches the format stored by the engine from `ExecutorFeedback::OrderPosted`. `"trade"` UUIDs never match any stored order ID and are harmlessly ignored. Actionable statuses (MATCHED, MINED, CONFIRMED, FAILED, RETRYING, CANCELED) are forwarded via `parse_trade_status()`; non-actionable statuses (LIVE, etc.) are silently skipped. This covers all order types: Leg 1 entry, Leg 2 hedge, Leg 2 emergency, and Leg 2 favorable exits. **Exception**: FOK orders that return `Filled` synchronously from the REST API bypass User WS entirely — the `already_filled` flag on `OrderPosted` feedback triggers immediate `Filled` state transition + trade completion in the engine
- **Fire-and-confirm cancels**: All cancel operations return `Result<bool>` from the CLOB. The executor sends `CancelResult { was_cancelled, is_leg2 }` feedback to the engine. If the cancel was NOT confirmed (order may have filled before the cancel reached the CLOB), the engine restores the order's Posted state from saved info (`cancelled_leg1_info` / `prev_leg2_order`) so User WS MATCHED events can still match. After restore, `replay_pending_fills()` is called — for Leg 1 fills this also sends the opportunity alert and increments `live_market_signals`. `leg1_cancel_race = true` is set AFTER replay so it survives the `LiveTradeMeta::default()` reset inside replay — propagated to `SimTrade` and shown as `[FILLED MID-CANCEL]` in Telegram. `cancelled_leg1_info` saves `signal`, `direction`, AND `last_spike` — all three are restored on unconfirmed cancel so `init_leg2()` has spike info available for Leg 2 hedging. For Leg 2 hedge/emergency, the executor also skips posting the replacement order when the cancel is not confirmed. **Leg 2 cancel-not-confirmed**: when Leg 2 Posted state is restored, `LiveTradeMeta` is reset (preserving `leg1_cancel_race`) and hedge `emergency_submitted`/`exit_reason` are cleared — prevents maker fills from being mislabeled as emergency exits. Unmatched `TradeStatusUpdate` events are buffered (up to 8) and replayed when state changes (OrderPosted, CancelResult) make them matchable
- **Provisional order ID race safety**: Speculative posting creates a provisional `"sim-leg1-{ts}"` ID. If `SpikeFailed` or staleness fires before the real CLOB ID arrives, a `cancel_leg1_on_feedback` flag defers the cancel until `on_order_posted()` receives the real ID — preventing ghost orders and invalid CLOB cancel requests
- **Tick size sync**: At rotation, `rotation.rs` fetches `GET /tick-size?token_id={yes_id}` from the CLOB and includes it in `IngestorEvent::MarketRotation { tick_size }`. The engine sets `state.tick_size` from this value. Mid-market changes are handled by `tick_size_change` WS events. Default `0.01` on fetch failure
- **SDK cache pre-warm (hard gate)**: On every `MarketRotation`, `LiveExecutor` calls `sdk.tick_size()`, `sdk.neg_risk()`, and `sdk.fee_rate_bps()` for both tokens — populating the SDK's `DashMap` caches with real CLOB values. `caches_warm: bool` gates all order placement: if any fetch fails, ALL signals are rejected with `OrderFailed` feedback until the next rotation. Eliminates the ~150ms first-order latency penalty from auto-fetch while guaranteeing correctness (no hardcoded values)
- **Connection pre-warming**: After SDK cache pre-warm, `LiveExecutor` calls `sdk.order("0x0000...0000")` to establish a TLS+TCP connection in the reqwest pool. The 404 is ignored — the pool is warm for subsequent `post_order()` and `get_order_status()` calls
- **REST fill polling (primary Leg 1 fill detection)**: After placing Leg 1, `LiveExecutor::poll_leg1_fill()` polls `GET /data/order/{id}` every 200ms (up to 13 polls = 2600ms) for deterministic fill detection (~200ms latency). On detecting `Filled` status, sends `RestFillDetected` feedback. User WS remains as backup. During polling, `rx.try_recv()` checks for `CancelLeg1` (executed inline) or other commands (buffered in `deferred_cmd`). `deferred_cmd: Option<ExecutorCommand>` is consumed at the top of the next `run()` loop iteration before blocking on `rx.recv()`. Engine's `on_rest_fill_detected()` dedup-guards on `leg1_state == Posted` with matching `order_id` — User WS events that arrive after REST detection are harmlessly ignored
- **Deferred partial fill alerts**: The CLOB splits large fills across multiple rapid MATCHED events (~3ms apart). Partial fill checks (`size_matched < original_size`) are NOT alerted on MATCHED — instead stored in `pending_partial_fills` (HashMap keyed by order_id). Resolved when subsequent MATCHED shows fully filled (silent) or MINED/CONFIRMED arrives with final size (alert if still partial). Cleared on `MarketRotation` but NOT `on_trade_complete()` — MINED events may arrive after trade reset
- **Centralized timestamps**: All `epoch_ms()` calls use `crate::utils::time::epoch_ms` — single implementation, no duplicates
- **Maker rebate estimates**: `SimFill::compute_maker_rebate(price, size)` = `compute_taker_fee(price, size) × 0.20` — upper-bound estimate of Polymarket daily maker rebate. `SimFill.maker_rebate` non-zero for maker fills, zero for taker. `SimTrade.maker_rebate` = sum of both legs. Net profit = `gross_profit - taker_fee + maker_rebate`. Accumulated on `SimulationState.total_maker_rebates_earned` and `MarketSummary.maker_rebates_earned`. Shown in Telegram trade completion and market/session summaries
- **Persistent redemption file (`redeems.txt`)**: Newline-delimited condition IDs persisted for redemption. Written once per market at rotation (if traded) and at shutdown (via `send_live_session_summary`). Read + merged with Data API positions in `redeem_inner()`. Cleanup via atomic write-temp-rename after successful redemption. `/redeem <condition_id>` for targeted single redemption. `append_condition_id_sync()` is sync I/O on engine thread (deduplicates before append)
- **Telegram rate limit**: 5s `AtomicU64` rate limiter; `fire_critical()` bypasses for trade completions

## Key Documents

- `ARCHITECTURE.md` — System design, trade lifecycle, risk controls, configuration, deployment
- `queries.sql` — QuestDB analytics queries (fill rate, PnL by tier, pruning)
- `config.toml` — All tunable parameters with comments
