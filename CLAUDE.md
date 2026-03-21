# CLAUDE.md

## Session Workflow (MANDATORY)

**Every session, without exception, must follow this workflow:**

1. **Read & Orient**: Read this file first. Then use the Task Routing table below to identify which docs and source files to read for the task at hand. Don't read everything — read only what's relevant.

2. **Plan First**: Write a plan (use plan mode) before executing any code changes. The plan must reference specific files, functions, and conventions from the documents read in step 1.

3. **Execute**: Implement the plan.

4. **Update Documentation (Cascade)**: After completing any work, update ALL affected documentation. See the Documentation Cascade section below for the exact rules. **This is critical.** Future sessions rely on these docs. Every behavioral change, new flag, new state transition, new guard, or new edge case MUST be documented.

## Project Overview

FaCaiBot is a Polymarket market-making bot targeting BTC 5-minute prediction markets. It uses bilateral accumulation — continuously quoting both YES and NO sides using a Binance-derived fair value model, accumulating matched pairs that pay $1.00 on resolution for less than $1.00 total cost. See `V2_SYSTEM.md` for the complete system design and `ARCHITECTURE.md` for infrastructure details.

## Build & Run

```bash
cargo build                  # Debug build
cargo build --release        # Release (LTO, single codegen unit)
cargo run                    # Run the bot
cargo test                   # All tests (125)
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
  Binance Spot SBE WS       FairValueEstimator       CLOB API (post/cancel/FOK)
  Binance Futures JSON WS   Quoter (both sides)      Telegram reports
  Polymarket WS             BilateralPosition         QuestDB analytics
  Gamma API (REST)          ClosingManager
```

Full details in `V2_SYSTEM.md` and `ARCHITECTURE.md`.

## Source Files

```
src/
├── main.rs                        # Entry point: jemalloc, tokio runtime (2 workers), channel wiring
├── config.rs                      # Hybrid config: config.toml (tuning) + .env (secrets)
├── engine/
│   ├── strategy.rs                # V2StrategyEngine: event routing, state machine (IDLE→QUIET→QUOTING→CLOSING)
│   ├── fair_value.rs              # FairValueEstimator: BTC probability model, momentum, edge sizing
│   ├── quoter.rs                  # Quoter: per-side order management, requoting, inventory skewing
│   ├── position.rs                # BilateralPosition: share tracking, pairing math, PnL
│   ├── closing.rs                 # ClosingManager: end-of-market cancel + FOK pairing
│   └── buildup/
│       ├── metrics.rs             # 4 metric trackers (CVD, OBI velocity, basis delta, realized volatility)
│       └── tests/                 # Extracted test modules (metrics)
├── executor/
│   ├── live.rs                    # LiveExecutor: bilateral maker orders, cancel, closing FOK
│   ├── fill_engine.rs             # Utility: compute_taker_fee, round_to_tick
│   └── tests/                     # Extracted test modules (fill_engine)
├── gateway/
│   ├── binance/
│   │   ├── ws.rs                  # Spot SBE WS: depth20 + bestBidAsk + @trade
│   │   ├── futures_ws.rs          # Futures JSON WS: @aggTrade, @bookTicker, @forceOrder
│   │   └── tests/                 # Extracted test modules (ws, futures_ws)
│   └── polymarket/
│       ├── rest.rs                # CLOB REST: order placement, cancellation, book query
│       ├── market_ws.rs           # Public Market WS: book, price, tick events
│       ├── user_ws.rs             # Authenticated User WS: fill detection (order events)
│       ├── heartbeat.rs           # POST /heartbeat every 5s (live only)
│       ├── rotation.rs            # Gamma API market discovery + rotation timer
│       └── tests/                 # Extracted test modules (rest, market_ws, user_ws, rotation)
├── reporting/
│   └── telegram.rs                # Telegram Bot API (fire-and-forget, rate-limited)
├── storage/
│   └── cold.rs                    # QuestDB ILP ingestion (analytics only, not on execution path)
├── control/
│   ├── listener.rs                # Telegram command listener (getUpdates polling)
│   ├── handlers.rs                # Command handlers (pure logic)
│   ├── config_editor.rs           # TOML read/write, param allowlist with ranges
│   ├── wallet.rs                  # /balance, /polybalance, /redeem (Polygon RPC + CTF)
│   └── tests/                     # Extracted test modules (config_editor, wallet)
├── types/
│   ├── market.rs                  # IngestorEvent, MarketState, OrderBook, Binance structs
│   └── order.rs                   # V2ExecutorCommand, V2ExecutorFeedback, OrderRequest
└── utils/
    ├── signing.rs                 # build_signer() (hex key → PrivateKeySigner)
    ├── time.rs                    # epoch_ms() — single timestamp source
    ├── tls.rs                     # Shared TLS config + SpawnExecutor
    └── tests/                     # Extracted test modules (signing)
```

## How to Work

### Change Hierarchy (follow this order)

When making any code change, work through these layers top-down. Each layer can reveal dependencies the next layer needs.

1. **Types first** (`types/market.rs`, `types/order.rs`): If the change needs new data, new enum variants, new fields on structs — add them here first. This is the foundation; everything else depends on these types.

2. **Config second** (`config.rs`, `config.toml`): If the change needs new tunable params — add the config struct fields, TOML section, and defaults. Add to `config_editor.rs` allowlist if it should be `/set`-able.

3. **Core logic third** (`engine/fair_value.rs`, `engine/quoter.rs`, `engine/position.rs`, `engine/closing.rs`, `engine/buildup/metrics.rs`): Pure computation. These files should never touch IO or external state. Implement the logic using types from step 1 and config from step 2.

4. **State management fourth** (`engine/strategy.rs`): Wire the new logic into the event loop. This is where state transitions happen. Key entry points: `on_event()`, `on_feedback()`, `quote_tick()`, `closing_tick()`, `on_market_rotation()`, `on_trade_status_update()`.

5. **Executor fifth** (`executor/live.rs`): If the change affects CLOB interaction — order placement, cancellation, or feedback. Must match the `V2ExecutorCommand`/`V2ExecutorFeedback` types from step 1.

6. **Gateway/reporting last** (`gateway/`, `reporting/`, `storage/`): Ingestor changes, Telegram formatting, QuestDB columns. These are leaf nodes — they emit events or consume results, rarely affect upstream logic.

### Dependency Awareness

Before modifying any function or struct, **trace its callers and consumers**:

- **Grep for the function/field name** across the codebase before changing its signature or semantics. A field on `V2ExecutorCommand` is used in `strategy.rs`, `live.rs`, `cold.rs`, and `telegram.rs` — miss one and it silently breaks.
- **Check both directions**: who produces this data (upstream) and who consumes it (downstream). The channel boundary (`engine → executor`) is a common blind spot — a type change in `order.rs` affects both sides.
- **strategy.rs is the hub**: Almost every behavioral change touches this file. If you think your change doesn't need strategy.rs edits, double-check — it probably does.
- **State reset completeness**: New fields must be cleared in `on_market_rotation()` (which resets position, quoter, closing, and fair value). Missing a reset causes state leaks between markets.

### Iterative Verification

After implementing, verify in this order:

1. **`cargo build`** — Catch type errors, missing imports, signature mismatches. Fix all errors before proceeding.
2. **`cargo test`** — Run the full test suite (125 tests). If tests fail, fix them before touching docs. Tests cover fair value model, quoting guards, position pairing, closing logic, fill engine utilities, and metric trackers.
3. **`cargo clippy`** — Fix warnings. Common ones: collapsible if-statements, derivable impls, too many function arguments.
4. **Manual trace** — For behavioral changes, mentally walk through a complete market lifecycle (rotation → quiet → quoting → closing) to verify no state leaks or missed transitions. Use `V2_SYSTEM.md` §9 as reference.

### When to Use Subagents

- **Broad codebase search** (e.g., "find all places that reference `leg1_state`"): Use an Explore agent. Don't grep manually across 20+ files.
- **Parallel independent research** (e.g., reading 3 unrelated docs simultaneously): Spin up multiple agents in parallel.
- **Testing after code changes**: Run build/test via bash directly — don't delegate to an agent unless the task is complex (e.g., diagnosing a test failure that requires reading multiple files).
- **Don't use agents for**: Reading a single known file, making a targeted edit, or running a simple command. Use the direct tools.

### Common Pitfalls

- **Pending operation guards**: The quoter tracks pending posts/cancels per side. Issuing a new command while one is in-flight causes race conditions. Always check `is_pending()` before posting or cancelling.
- **State reset on rotation**: `on_market_rotation()` resets position, quoter, closing, and fair value. New fields must be cleared here. Missing a reset causes stale state to leak into the next market.
- **Channel ordering**: Feedback is drained BEFORE `on_event()` in the main loop. This ensures fill data is processed before new quoting decisions.
- **CLOB constraints**: `price × size` must have ≤2 decimal places for FOK orders. Use `clob_safe_fok_size()` for closing FOKs. Maker rebate is an estimate (20% of taker fee). Post-only orders can be rejected if price crosses book.
- **Closing phase timing**: The closing FOK is a one-shot attempt. If the first attempt fails or is too expensive (`pair_cost >= max_closing_pair_cost`), unpaired shares ride to resolution.

## Code Conventions (Cross-Cutting Rules)

These are codebase-wide rules that apply regardless of which module you're working in:

- **Decimal arithmetic**: All pricing uses `rust_decimal::Decimal` — never f32/f64 for prices or sizes
- **Centralized timestamps**: All `epoch_ms()` calls use `crate::utils::time::epoch_ms` — no duplicates
- **Channel-based data flow**: crossbeam bounded(8192) SPSC between layers — no `Arc<Mutex>`. Reverse `V2ExecutorFeedback` channel for CLOB order IDs
- **Module purity**: `fair_value.rs`, `quoter.rs`, `position.rs`, `closing.rs` are pure computation — `strategy.rs` is the only file that mutates state and coordinates
- **Hybrid config**: `config.toml` for tuning params (serde + `#[serde(default)]`), `.env` for secrets only. Override path with `CONFIG_FILE` env var
- **jemalloc**: Global allocator via `tikv-jemallocator`, conditional on `cfg(not(target_env = "msvc"))`
- **Tokio runtime**: Manual 2-worker multi-thread, pinned cores 1-2. Core 0 reserved for ingestor. Engine on `spawn_blocking` (off worker pool)

## Task Routing

**Use this table to decide what to read before making changes.** Read only what's relevant to your task.

| If working on... | Read these docs | Key source files |
|-------------------|----------------|-----------------|
| **Fair value model, momentum, edge** | `V2_SYSTEM.md` §4 | `engine/fair_value.rs`, `engine/buildup/metrics.rs` |
| **Quoting logic, inventory skewing** | `V2_SYSTEM.md` §5 | `engine/quoter.rs`, `engine/strategy.rs` |
| **Position tracking, pairing, PnL** | `V2_SYSTEM.md` §2-3 | `engine/position.rs` |
| **Closing phase, FOK pairing** | `V2_SYSTEM.md` §6 | `engine/closing.rs`, `engine/strategy.rs` |
| **Fill detection, order matching** | `V2_SYSTEM.md` §7 | `engine/strategy.rs`, `gateway/polymarket/user_ws.rs` |
| **Market rotation, state reset** | `V2_SYSTEM.md` §8 | `gateway/polymarket/rotation.rs`, `engine/strategy.rs` |
| **State machine, phase transitions** | `V2_SYSTEM.md` §9 | `engine/strategy.rs` |
| **Metric trackers (CVD, OBI, etc.)** | `V2_SYSTEM.md` §4 | `engine/buildup/metrics.rs` |
| **Binance data feeds (spot SBE)** | `ARCHITECTURE.md` (Ingestor section) | `gateway/binance/ws.rs` |
| **Binance futures feeds** | `ARCHITECTURE.md` (Ingestor section) | `gateway/binance/futures_ws.rs` |
| **Polymarket WS / REST / SDK** | `ARCHITECTURE.md` (Executor section) | `gateway/polymarket/rest.rs`, `user_ws.rs`, `market_ws.rs` |
| **Telegram commands, bot control** | `ARCHITECTURE.md` (Control section) | `control/listener.rs`, `control/handlers.rs` |
| **Telegram notifications, /status** | `V2_SYSTEM.md` §11 | `engine/strategy.rs`, `control/types.rs`, `control/handlers.rs` |
| **Wallet, redemption, /redeem** | `ARCHITECTURE.md` (Control section) | `control/wallet.rs` |
| **QuestDB, analytics, retention** | `V2_SYSTEM.md` §11, `ARCHITECTURE.md` (Storage) | `storage/cold.rs` |
| **Config params, /set command** | `V2_SYSTEM.md` §10, `config.toml` | `config.rs`, `control/config_editor.rs` |
| **Deployment, systemd, AWS** | `README.md`, `ARCHITECTURE.md` (Deployment section) | `deploy/` scripts |
| **Refactoring plan** | `REFACTORING.md` | (varies by item) |

## Documentation Cascade

After **every** change, update docs in this order. Skip files that aren't affected.

1. **`CLAUDE.md`** — Only if: source file tree changed, new cross-cutting convention added, or task routing table needs updating. Do NOT add domain-specific definitions here.

2. **`V2_SYSTEM.md`** — Update the relevant section for any behavioral change: fair value model (§4), quoting logic (§5), closing phase (§6), fill detection (§7), market rotation (§8), state machine (§9), config (§10), operational layer (§11).

3. **`ARCHITECTURE.md`** — Only if: system design, layer boundaries, data flow, infrastructure, or deployment changed.

4. **`config.toml`** — If new params added or defaults changed, update the inline comments.

**Rule**: Trading logic definitions live in `V2_SYSTEM.md`. Infrastructure definitions live in `ARCHITECTURE.md`. `CLAUDE.md` routes you to them. Don't duplicate definitions here.
