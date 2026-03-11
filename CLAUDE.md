# CLAUDE.md

## Session Workflow (MANDATORY)

**Every session, without exception, must follow this workflow:**

1. **Read & Orient**: Read this file first. Then use the Task Routing table below to identify which docs and source files to read for the task at hand. Don't read everything — read only what's relevant.

2. **Plan First**: Write a plan (use plan mode) before executing any code changes. The plan must reference specific files, functions, and conventions from the documents read in step 1.

3. **Execute**: Implement the plan.

4. **Update Documentation (Cascade)**: After completing any work, update ALL affected documentation. See the Documentation Cascade section below for the exact rules. **This is critical.** Future sessions rely on these docs. Every behavioral change, new flag, new state transition, new guard, or new edge case MUST be documented.

## Project Overview

FaCaiBot is a Polymarket arbitrage bot targeting BTC 5-minute prediction markets. It uses composite buildup detection (6 Binance spot + futures metrics) to predict imminent price spikes, posts maker orders on the Polymarket CLOB before the move materializes, then hedges with the opposite side — locking in a sub-$1.00 pair that pays $1.00 on resolution. See `ARCHITECTURE.md` for full system design.

## Build & Run

```bash
cargo build                  # Debug build
cargo build --release        # Release (LTO, single codegen unit)
cargo run                    # Run the bot
cargo test                   # All tests (153)
cargo clippy                 # Lint
cargo fmt                    # Format
docker-compose up -d         # Start QuestDB (analytics only)
```

Copy `.env.example` → `.env` and fill credentials. Tuning params live in `config.toml`.

## Production Deployment (AWS)

```bash
sudo bash deploy/setup.sh                                    # One-time server setup
RUSTFLAGS="-C target-cpu=native" cargo build --release       # Build with native CPU opts
bash deploy/deploy.sh                                        # Deploy/update
```

- **Instance**: `c7i.xlarge` in `eu-west-2` (London) — co-located with Polymarket CLOB
- See `README.md` for full deployment guide and `deploy/` for scripts

## Architecture (Brief)

Three-layer lock-free pipeline connected by crossbeam SPSC channels:

```
Ingestor (gateway/) ──▶ Engine (engine/) ──▶ Executor (executor/)
  Binance Spot SBE WS       MarketState          MODE=live: CLOB API
  Binance Futures JSON WS   Buildup→Signal       MODE=sim: Telegram+QuestDB
  Polymarket WS             Hedge phases
  Gamma API (REST)
```

Full details in `ARCHITECTURE.md`.

## Source Files

```
src/
├── main.rs                        # Entry point: jemalloc, tokio runtime (2 workers), channel wiring
├── config.rs                      # Hybrid config: config.toml (tuning) + .env (secrets)
├── engine/
│   ├── strategy.rs                # StrategyEngine: event routing, state, simulation FSM (largest file)
│   ├── evaluator.rs               # Leg1Evaluator + Leg2Evaluator (pure, no state mutation)
│   ├── confidence.rs              # Repricing model (compute_expected_repricing), round_to_tick()
│   ├── erosion.rs                 # HedgeState/HedgeSnap/HedgePhase: Leg 2 hedge FSM
│   └── buildup/
│       ├── detector.rs            # BuildupDetector: composite 6-metric score, direction consensus
│       └── metrics.rs             # 6 metric trackers (CVD, spot flow, OBI, basis, liq, ATR)
├── executor/
│   ├── live.rs                    # LiveExecutor: CLOB order placement, cancel, FOK emergency
│   └── fill_engine.rs             # Utility: compute_taker_fee, compute_maker_rebate, round_to_tick
├── gateway/
│   ├── binance/
│   │   ├── ws.rs                  # Spot SBE WS: depth20 + bestBidAsk + @trade
│   │   └── futures_ws.rs          # Futures JSON WS: @aggTrade, @bookTicker, @forceOrder
│   └── polymarket/
│       ├── rest.rs                # CLOB REST: order placement, cancellation, book query
│       ├── market_ws.rs           # Public Market WS: book, price, tick events
│       ├── user_ws.rs             # Authenticated User WS: fill detection (order events)
│       ├── heartbeat.rs           # POST /heartbeat every 5s (live only)
│       └── rotation.rs            # Gamma API market discovery + rotation timer
├── reporting/
│   └── telegram.rs                # Telegram Bot API (fire-and-forget, rate-limited)
├── storage/
│   └── cold.rs                    # QuestDB ILP ingestion (analytics only, not on execution path)
├── control/
│   ├── listener.rs                # Telegram command listener (getUpdates polling)
│   ├── handlers.rs                # Command handlers (pure logic)
│   ├── config_editor.rs           # TOML read/write, param allowlist with ranges
│   └── wallet.rs                  # /balance, /polybalance, /redeem (Polygon RPC + CTF)
├── types/
│   ├── market.rs                  # IngestorEvent, MarketState, OrderBook, BuildupInfo
│   ├── order.rs                   # TradeSignal, ExecutorCommand, ExecutorFeedback, OrderRequest
│   └── simulation.rs              # SimulationState, SimPosition, SimTrade
└── utils/
    ├── signing.rs                 # build_signer() (hex key → PrivateKeySigner)
    ├── time.rs                    # epoch_ms() — single timestamp source
    └── tls.rs                     # Shared TLS config + SpawnExecutor
```

## How to Work

### Change Hierarchy (follow this order)

When making any code change, work through these layers top-down. Each layer can reveal dependencies the next layer needs.

1. **Types first** (`types/market.rs`, `types/order.rs`): If the change needs new data, new enum variants, new fields on structs — add them here first. This is the foundation; everything else depends on these types.

2. **Config second** (`config.rs`, `config.toml`): If the change needs new tunable params — add the config struct fields, TOML section, and defaults. Add to `config_editor.rs` allowlist if it should be `/set`-able.

3. **Core logic third** (`engine/evaluator.rs`, `engine/erosion.rs`, `engine/confidence.rs`, `engine/buildup/`): Pure computation. These files should never touch IO or external state. Implement the logic using types from step 1 and config from step 2.

4. **State management fourth** (`engine/strategy.rs`): Wire the new logic into the event loop. This is where state transitions happen. `strategy.rs` is the largest file (3800+ lines) — use grep to find the exact function/section before editing. Key entry points: `on_event()`, `evaluate()`, `evaluate_leg2()`, `on_order_posted()`, `on_trade_complete()`, `handle_buildup_confirmed()`.

5. **Executor fifth** (`executor/live.rs`): If the change affects CLOB interaction — order placement, cancellation, or feedback. Must match the `ExecutorCommand`/`ExecutorFeedback` types from step 1.

6. **Gateway/reporting last** (`gateway/`, `reporting/`, `storage/`): Ingestor changes, Telegram formatting, QuestDB columns. These are leaf nodes — they emit events or consume results, rarely affect upstream logic.

### Dependency Awareness

Before modifying any function or struct, **trace its callers and consumers**:

- **Grep for the function/field name** across the codebase before changing its signature or semantics. A field on `TradeSignal` is used in `evaluator.rs`, `strategy.rs`, `live.rs`, `cold.rs`, and `telegram.rs` — miss one and it silently breaks.
- **Check both directions**: who produces this data (upstream) and who consumes it (downstream). The channel boundary (`engine → executor`) is a common blind spot — a type change in `order.rs` affects both sides.
- **strategy.rs is the hub**: Almost every behavioral change touches this file. If you think your change doesn't need strategy.rs edits, double-check — it probably does.
- **Flags and guards cascade**: Adding a new boolean flag (e.g., `some_new_guard`) requires: (a) initialization, (b) set logic, (c) clear logic on trade complete, (d) clear logic on rotation, (e) documentation. Missing any of (c)/(d) causes state leaks between trades/markets.

### Iterative Verification

After implementing, verify in this order:

1. **`cargo build`** — Catch type errors, missing imports, signature mismatches. Fix all errors before proceeding.
2. **`cargo test`** — Run the full test suite (153 tests). If tests fail, fix them before touching docs. Tests cover evaluator guards, repricing math, fill engine utilities, buildup normalization, and more.
3. **`cargo clippy`** — Fix warnings. Common ones: collapsible if-statements, derivable impls, too many function arguments.
4. **Manual trace** — For behavioral changes, mentally walk through a complete trade lifecycle (buildup → Leg 1 → hedge → completion) to verify no state leaks or missed transitions. Use `trading_logic/state_machines.md` as reference.

### When to Use Subagents

- **Broad codebase search** (e.g., "find all places that reference `leg1_state`"): Use an Explore agent. Don't grep manually across 20+ files.
- **Parallel independent research** (e.g., reading 3 unrelated docs simultaneously): Spin up multiple agents in parallel.
- **Testing after code changes**: Run build/test via bash directly — don't delegate to an agent unless the task is complex (e.g., diagnosing a test failure that requires reading multiple files).
- **Don't use agents for**: Reading a single known file, making a targeted edit, or running a simple command. Use the direct tools.

### Common Pitfalls

- **strategy.rs size**: At 3800+ lines, it's easy to miss existing logic. Always grep for related function names before adding new code.
- **Sim vs live divergence**: Changes to engine logic often need parallel changes for simulation mode (`advance_simulation()`) and live mode (executor feedback handlers). See `trading_logic/edge_cases.md` §16.
- **State reset completeness**: `on_trade_complete()` and the `MarketRotation` handler both reset state. New fields must be cleared in BOTH places.
- **Channel ordering**: Feedback is drained BEFORE `on_event()` in the main loop. Emergency signals must be sent BEFORE rotation commands. Violating these orderings causes subtle race conditions.
- **CLOB constraints**: `price × size` must have ≤2 decimal places for FOK orders. Maker rebate is an estimate (20% of taker fee). Post-only orders can be rejected if price crosses book.

## Code Conventions (Cross-Cutting Rules)

These are codebase-wide rules that apply regardless of which module you're working in:

- **Decimal arithmetic**: All pricing uses `rust_decimal::Decimal` — never f32/f64 for prices or sizes
- **Centralized timestamps**: All `epoch_ms()` calls use `crate::utils::time::epoch_ms` — no duplicates
- **Channel-based data flow**: crossbeam bounded(8192) SPSC between layers — no `Arc<Mutex>`. Reverse `ExecutorFeedback` channel for CLOB order IDs
- **Evaluator pattern**: `Leg1Evaluator`/`Leg2Evaluator` are pure (no state mutation) — caller applies mutations
- **Self-gating**: `buildup_detected` is cleared on both signal emission AND rejection — each buildup gets exactly 1 evaluation attempt
- **Hybrid config**: `config.toml` for tuning params (serde + `#[serde(default)]`), `.env` for secrets only. Override path with `CONFIG_FILE` env var
- **jemalloc**: Global allocator via `tikv-jemallocator`, conditional on `cfg(not(target_env = "msvc"))`
- **Tokio runtime**: Manual 2-worker multi-thread, pinned cores 1-2. Core 0 reserved for ingestor. Engine on `spawn_blocking` (off worker pool)

## Task Routing

**Use this table to decide what to read before making changes.** Read only what's relevant to your task.

| If working on... | Read these docs | Key source files |
|-------------------|----------------|-----------------|
| **BuildupDetector, metrics, weights** | `trading_logic/signal_and_entry.md` §1-2, `config.toml` `[buildup]` | `engine/buildup/detector.rs`, `engine/buildup/metrics.rs` |
| **Entry guards, repricing model** | `trading_logic/signal_and_entry.md` §3-4 | `engine/evaluator.rs`, `engine/confidence.rs` |
| **Leg 1 execution, sustain, maker orders** | `trading_logic/signal_and_entry.md` §5 | `engine/strategy.rs`, `executor/live.rs` |
| **Leg 2 hedge phases, flow monitoring** | `trading_logic/leg2_hedge.md` §6 | `engine/erosion.rs`, `engine/evaluator.rs`, `engine/strategy.rs` |
| **Emergency exits, FOK, favorable exits** | `trading_logic/leg2_hedge.md` §7-8 | `executor/live.rs`, `engine/evaluator.rs` |
| **Trade completion, PnL, state reset** | `trading_logic/lifecycle.md` §9 | `engine/strategy.rs` |
| **Market rotation, cutoff, quiet period** | `trading_logic/lifecycle.md` §10-11c | `gateway/polymarket/rotation.rs`, `engine/strategy.rs` |
| **Capital management** | `trading_logic/lifecycle.md` §12 | `engine/evaluator.rs`, `engine/strategy.rs` |
| **State machines, transitions** | `trading_logic/state_machines.md` | `types/order.rs`, `engine/erosion.rs` |
| **Race conditions, edge cases** | `trading_logic/edge_cases.md` §14 | `engine/strategy.rs`, `executor/live.rs` |
| **Sim vs live differences** | `trading_logic/edge_cases.md` §16 | `engine/strategy.rs` |
| **Binance data feeds (spot SBE)** | `ARCHITECTURE.md` (Ingestor section) | `gateway/binance/ws.rs` |
| **Binance futures feeds** | `ARCHITECTURE.md` (Ingestor section) | `gateway/binance/futures_ws.rs` |
| **Polymarket WS / REST / SDK** | `ARCHITECTURE.md` (Executor section) | `gateway/polymarket/rest.rs`, `user_ws.rs`, `market_ws.rs` |
| **Telegram commands, bot control** | `ARCHITECTURE.md` (Control section) | `control/listener.rs`, `control/handlers.rs` |
| **Wallet, redemption, /redeem** | `ARCHITECTURE.md` (Control section) | `control/wallet.rs` |
| **QuestDB, analytics** | `ARCHITECTURE.md` (Storage section) | `storage/cold.rs` |
| **Config params, /set command** | `config.toml` (comments), `ARCHITECTURE.md` (Config section) | `config.rs`, `control/config_editor.rs` |
| **Deployment, systemd, AWS** | `README.md`, `ARCHITECTURE.md` (Deployment section) | `deploy/` scripts |

## Documentation Cascade

After **every** change, update docs in this order. Skip files that aren't affected.

1. **`CLAUDE.md`** — Only if: source file tree changed, new cross-cutting convention added, or task routing table needs updating. Do NOT add domain-specific definitions here.

2. **`trading_logic/<relevant_file>.md`** — Update the specific file that covers the changed behavior:
   - Signal detection or entry logic changed → `signal_and_entry.md`
   - Hedge phases, emergency exits, or favorable exits changed → `leg2_hedge.md`
   - Completion, rotation, cutoff, cooldown, or capital changed → `lifecycle.md`
   - State transitions changed → `state_machines.md`
   - New race condition or edge case discovered → `edge_cases.md`

3. **`ARCHITECTURE.md`** — Only if: system design, layer boundaries, data flow, infrastructure, or deployment changed.

4. **`config.toml`** — If new params added or defaults changed, update the inline comments.

**Rule**: Definitions live in `trading_logic/` and `ARCHITECTURE.md`. `CLAUDE.md` routes you to them. Don't duplicate definitions here.
