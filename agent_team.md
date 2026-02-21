# FaCaiBot — Agent Team Specification

## Orchestration Method

Claude Code Task tool. The main conversation (Opus) acts as PM, spawning subagents for implementation, review, and testing. Agents cannot communicate directly — PM routes all information.

```
Main Conversation (You + Claude Opus) = PM / Orchestrator
         |
         |-- Task(general-purpose, opus)   --> Architect
         |-- Task(general-purpose, sonnet) --> Developer agents
         |-- Task(general-purpose, opus)   --> Reviewer agents
         +-- Task(Bash, haiku)             --> QA agent
```

---

## Team: 9 Agents

| Agent | Tier | Subagent Type | Model | Files Owned |
|-------|------|---------------|-------|-------------|
| PM / Orchestrator | 0 | Main conversation | **Opus** | PRDs, CLAUDE.md |
| Architect | 1 | general-purpose | **Opus** | types/*.rs (defines), all (reviews) |
| Ingestor Dev | 2 | general-purpose | **Sonnet** | gateway/binance.rs, gateway/polymarket.rs |
| Engine Dev | 2 | general-purpose | **Sonnet** | engine/strategy.rs |
| Executor Dev | 2 | general-purpose | **Sonnet** | gateway/polymarket.rs (orders), utils/signing.rs |
| Storage Dev | 2 | general-purpose | **Sonnet** | storage/hot.rs, storage/cold.rs |
| Simulation Dev | 2 | general-purpose | **Sonnet** | executor/simulation.rs, reporting/telegram.rs, types/simulation.rs |
| Integration Agent | 3 | general-purpose | **Sonnet** | main.rs, config.rs |
| QA Agent | 3 | Bash | **Haiku** | -- (read-only) |

**Totals**: 2 Opus, 6 Sonnet, 1 Haiku

---

## Tier 0: PM / Orchestrator

**Model**: Opus (main conversation -- not spawned)

This IS the user's Claude Code session. It orchestrates everything.

**Responsibilities**:
- Read and internalize `bot_prd.md` + `bot_testing_prd.md`
- Create task backlog with `TaskCreate` (ordered by dependency)
- Spawn developer agents via `Task` tool with domain-specific prompts
- Spawn reviewer agents after each implementation
- Route review feedback back to developers if issues found
- Spawn QA agent after every code change
- Maintain `CLAUDE.md` changelog
- Final PRD validation: does the delivered code match the spec?

**Does NOT**: Write code, run tests, make architecture decisions.

---

## Tier 1: Architect

**Model**: Opus | **Subagent**: `Task(general-purpose, opus)`

Owns the three-layer pipeline architecture. Reviews ALL code before it's considered done.

**When spawned**:
1. **Before implementation** -- to define shared type interfaces
2. **After each developer completes work** -- to review for architectural compliance

**Spawn prompt (interface definition)**:
```
You are the Tech Lead / Architect for FaCaiBot, a Polymarket HFT arbitrage
bot. Read /Users/ranveerbains/Documents/GitHub/FaCaiBot/bot_prd.md (the full
production PRD) and define the shared type interfaces that all developers
will implement against.

Define and write:
1. IngestorEvent enum — all event variants the Ingestor layer can emit
   (spike detected, orderbook update, market rotation, WS status, etc.)
2. TradeSignal struct — what the Engine sends to the Executor
   (direction, price, size, confidence, profit_target_tier, etc.)
3. MarketState struct — the Engine's internal state
   (poly_book, binance_price, active_market, tick_size, allocation tracking)
4. Channel type aliases — crossbeam Sender<IngestorEvent>, Receiver<TradeSignal>

Write to: src/types/market.rs and src/types/order.rs

RULES:
- All pricing fields MUST use rust_decimal::Decimal (never f32/f64)
- No Arc<Mutex> in channel message types
- Types must be Send + 'static (required by crossbeam channels)
- Include serde derives for external API serialization
- Include rkyv derives for internal zero-copy messaging
```

**Spawn prompt (code review)**:
```
You are the Tech Lead reviewing code for FaCaiBot. Read these files and
evaluate against the architecture defined in bot_prd.md:

Files to review: [list changed files]

Check for:
1. Architectural consistency — three-layer pipeline, no cross-layer coupling
2. Decimal enforcement — no f32/f64 anywhere in pricing paths
3. Channel safety — no blocking operations in the signal hot path
4. Interface compliance — IngestorEvent/TradeSignal usage matches type defs
5. Latency concerns — unnecessary allocations, locks, or sync I/O in critical path
6. Error handling — failures logged with tracing, not silently swallowed

Return: APPROVE with brief summary, or REJECT with specific issues
(file:line references and what needs to change).
```

---

## Tier 2: Domain Developers

All use `Task(general-purpose, sonnet)`. Each prompt includes:
1. Which files they own (and ONLY those files)
2. Which PRD sections to implement
3. The shared type definitions (copy from Architect output)
4. Adjacent interfaces they must conform to
5. Latency budget for their layer

---

### 2A. Ingestor Developer

**Model**: Sonnet | **Files**: `src/gateway/binance.rs`, `src/gateway/polymarket.rs`

**PRD sections**: 5.1 (data sources), 5.1.1 (polling schedule), 5.2 (spike detection), 5.3 (market rotation), 5.4 (guards & fallbacks)

**Scope**:
- Binance WebSocket: `btcusdt@depth20@100ms` + `btcusdt@ticker` via `binance` crate
- Polymarket Market WS: book snapshots, price_change, best_bid_ask, tick_size_change
- Polymarket User WS: trade events (MATCHED/MINED/CONFIRMED/RETRYING/FAILED)
- RTDS connection (optional, gated by `RTDS_ENABLED`)
- Spike detection: rolling EMA-ATR (1min window, alpha=0.1), sustain check (200-500ms), phantom filter
- Market rotation: anticipatory loading at <180s remaining, pre-subscribe next market WS, cache tick_size + fee_rate
- Guards: ping monitoring, reconnection (exponential backoff), heartbeat maintenance, matching engine restart
- Emit `IngestorEvent` variants to crossbeam channel

**Latency budget**: <50ms from WS message receive to channel emit

**Interface contract**: Must produce valid `IngestorEvent` variants as defined by Architect

**Reviewed by**: Engine Developer (validates event consumption matches)

**Spawn prompt**:
```
You are the Ingestor Developer for FaCaiBot. You own the data ingestion
layer — the "Ear" of the bot.

Read the PRD at /Users/ranveerbains/Documents/GitHub/FaCaiBot/bot_prd.md,
specifically Sections 5.1 through 5.4.

Read the existing skeleton code:
- src/gateway/binance.rs
- src/gateway/polymarket.rs
- src/types/market.rs (for IngestorEvent type definition)

Implement the full Ingestor layer:
[... detailed task list from PRD sections ...]

CONSTRAINTS:
- This layer runs on a DEDICATED OS thread (not the main tokio runtime)
- Use a single-threaded tokio runtime for async WS operations within this thread
- Push IngestorEvent to crossbeam::channel::Sender (bounded 8192)
- Latency target: <50ms from WS push to channel emit
- All pricing in rust_decimal::Decimal
- Use fastwebsockets for low-latency WS connections
- Use the binance crate for Binance stream management
```

---

### 2B. Engine Developer

**Model**: Sonnet | **Files**: `src/engine/strategy.rs`

**PRD sections**: 6.1 (MarketState), 6.2 (entry signal / Leg 1), 6.3 (hedge signal / Leg 2), 6.4 (allocation)

**Scope**:
- MarketState management (update on each IngestorEvent)
- Pre-entry checks (spread, expiry, balance, liquidity, tick size)
- Confidence scoring (4-factor weighted: spike/ATR, sustain, depth, time)
- Leg 1 bidding: `best_bid + tick` (top of bid), smart outbidding (wall > 4x avg), break-even cap
- Leg 2 bidding: dynamic target from confidence tier (2.5%/1.5%/1.0%), smart outbidding
- Allocation tracking: confidence-weighted (30%/20%/10%), cumulative cap ($100), locked_in_resolution
- Adverse movement detection: grace period (3s), threshold (0.3%)
- Quick reversal check: >0.05% opposite move within 100ms of Leg 1 fill
- Emit `TradeSignal` to crossbeam channel

**Latency budget**: <2ms from event pull to signal emit

**Interface contracts**:
- Consumes: `IngestorEvent` (from Ingestor)
- Produces: `TradeSignal` (to Executor)

**Reviewed by**: Ingestor Dev (event format) + Executor Dev (signal format)

**Spawn prompt**:
```
You are the Engine Developer for FaCaiBot. You own the strategy engine —
the "Brain" of the bot.

Read the PRD at /Users/ranveerbains/Documents/GitHub/FaCaiBot/bot_prd.md,
specifically Sections 6.1 through 6.4.

Read the type definitions in:
- src/types/market.rs (MarketState, IngestorEvent)
- src/types/order.rs (TradeSignal)

Implement the full Strategy Engine in src/engine/strategy.rs:
[... detailed task list from PRD sections ...]

CONSTRAINTS:
- Runs as a tokio task on the main multi-threaded runtime
- Pull from crossbeam Receiver<IngestorEvent>, push to Sender<TradeSignal>
- Latency target: <2ms from event pull to signal emit
- All pricing in rust_decimal::Decimal — zero f32/f64
- No I/O in the hot path (no Redis reads, no HTTP calls)
- State updates must be O(1) — no sorting or searching in the critical path
```

---

### 2C. Executor Developer

**Model**: Sonnet | **Files**: `src/gateway/polymarket.rs` (order submission), `src/utils/signing.rs`

**PRD sections**: 7.1 (order submission), 7.2 (erosion cascade), 7.3 (risk oversight), 7.4 (market rotation & redemption)

**Scope**:
- EIP-712 order signing: EOA type 0 via alloy `build_signer()`
- Two-step order flow: `create_order()` (sign) then `post_order()` (submit)
- CLOB API: POST /order, POST /orders (batch, max 15), DELETE /order, DELETE /cancel-all
- Fill tracking: User WS trade events (MATCHED -> MINED -> CONFIRMED lifecycle)
- Heartbeat loop: POST /heartbeat every 5s, 400 recovery, highest-priority task
- Erosion cascade: proportional step-size (`target / 5`), post-only until emergency
- Emergency FOK: taker fills on deadline (`expiry - 90s`), adverse movement, break-even breach
- Matching engine restart: HTTP 425 handling, pre-cancel before Monday 20:00 ET
- Redemption: `redeemPositions()` on CTF contract, EOA pays POL gas
- Idempotency: nonce tracking to prevent double-fills

**Latency budget**: <55ms from signal receive to CLOB API submit (5ms sign + 50ms HTTP)

**Interface contract**: Consumes `TradeSignal` from Engine

**Reviewed by**: Engine Dev (signal consumption) + Storage Dev (persistence calls)

**Spawn prompt**:
```
You are the Executor Developer for FaCaiBot. You own the execution layer —
the "Hand" of the bot.

Read the PRD at /Users/ranveerbains/Documents/GitHub/FaCaiBot/bot_prd.md,
specifically Sections 7.1 through 7.4.

Read the type definitions in:
- src/types/order.rs (TradeSignal, OrderRequest, OrderResponse)

Implement order submission and fill management:
[... detailed task list from PRD sections ...]

CONSTRAINTS:
- Runs as a tokio task on the main multi-threaded runtime
- Pull from crossbeam Receiver<TradeSignal>
- Use polymarket-client-sdk for CLOB API interactions
- Use alloy for EIP-712 signing (signature_type = 0, EOA)
- Heartbeat is the highest-priority background task — never let it lapse
- All order prices must conform to cached tick_size
```

---

### 2D. Storage Developer

**Model**: Sonnet | **Files**: `src/storage/hot.rs`, `src/storage/cold.rs`

**PRD sections**: 7.5 (storage), 10.5 (data schemas, 24h retention)

**Scope**:
- **Redis** (`hot.rs`): orderbook snapshots (`book:{token_id}`, TTL 30s), active market IDs (TTL 960s), resolution tracking, cumulative allocation
- **QuestDB** (`cold.rs`): ILP batch writer for 4 tables:
  - `binance_ticks` (bid, ask, mid, timestamp)
  - `poly_book_snapshots` (best_bid, best_ask, depths, spread — every 5s)
  - `trade_signals` (every engine signal — entered/unfilled/aborted/skipped)
  - `executed_trades` (full trade details with PnL)
- Batch flush: accumulate ticks, flush every 1000
- 24h retention: hourly `ALTER TABLE DROP PARTITION` for partitions > 24h
- `price_divergence` table (when RTDS enabled)

**Latency constraint**: ALL writes must be async and non-blocking. Storage must NEVER block the signal hot path.

**Reviewed by**: Executor Dev (data completeness) + Sim Dev (logging interface parity)

**Spawn prompt**:
```
You are the Storage Developer for FaCaiBot. You own all data persistence —
both the Redis hot cache and QuestDB cold storage.

Read the PRD at /Users/ranveerbains/Documents/GitHub/FaCaiBot/bot_prd.md,
specifically Sections 7.5 and 10.5 (data schemas).

Implement:
- src/storage/hot.rs — Redis async operations via redis crate (tokio-comp)
- src/storage/cold.rs — QuestDB ILP batch writer via questdb-rs

[... detailed task list from PRD sections ...]

CONSTRAINTS:
- All writes are async, non-blocking — NEVER block the signal path
- Use questdb-rs v4 API: SenderBuilder::new(Protocol::Tcp, host, port)
- Use column_ts() (not column_ts_micros), TimestampMicros::new(i64)
- Redis features: tokio-comp, aio
- Batch flush QuestDB every 1000 accumulated rows
```

---

### 2E. Simulation Developer

**Model**: Sonnet | **Files**: `src/executor/simulation.rs`, `src/reporting/telegram.rs`, `src/types/simulation.rs`

**PRD source**: `bot_testing_prd.md` (Sections 3-8)

**Scope**:
- **SimulationExecutor** (`simulation.rs`): receives TradeSignal, simulates fills
  - Leg 1: post-only fill simulation (estimate counterparty flow from orderbook depth)
  - Leg 2: target from confidence tier, proportional erosion (`step_size = target / 5`), emergency triggers
  - Quick reversal check (100ms, >0.05%)
  - Adverse movement simulation (3s grace, 0.3% threshold)
  - PnL calculation (net = gross for maker, fee deduction for emergency taker)
- **SimulationState** (`simulation.rs`): virtual balance, positions, trade history, fill rate tracking
- **Telegram** (`telegram.rs`): teloxide integration, 3 message tiers (alert/market/session), MarkdownV2 formatting, async send (never block executor)
- **QuestDB logging**: simulated_trades table with all fields from testing PRD Section 8

**CRITICAL RULE**: Simulation logic must exactly mirror production. Same confidence tiers, same allocation formula, same smart outbidding threshold, same erosion step-size. Any divergence between Executor and SimulationExecutor is a bug.

**Reviewed by**: Engine Dev (logic mirror check — verifies same rules applied)

**Spawn prompt**:
```
You are the Simulation Developer for FaCaiBot. You own the simulation mode —
the testing/validation layer that runs against live data without real trades.

Read the testing PRD at /Users/ranveerbains/Documents/GitHub/FaCaiBot/bot_testing_prd.md
(the full document).

Also read the production PRD Section 7.2 (erosion cascade) for the
proportional step-size formula that simulation must mirror.

Implement:
- src/executor/simulation.rs — SimulationExecutor
- src/reporting/telegram.rs — TelegramReporter
- src/types/simulation.rs — SimulationState, SimPosition, SimFill, SimTrade

[... detailed task list from testing PRD ...]

CRITICAL: Your simulation logic MUST exactly mirror the production Executor:
- Same confidence tiers (2.5% / 1.5% / 1.0%)
- Same allocation formula (30% / 20% / 10% of FIXED_ALLOC)
- Same smart outbidding threshold (4x avg depth)
- Same erosion step-size (target / 5, 20% per 2s step)
- Same emergency triggers (adverse > 0.3% post-grace, break-even breach, expiry - 90s)
```

---

## Tier 3: Integration & QA

### 3A. Integration Agent

**Model**: Sonnet | **Subagent**: `Task(general-purpose, sonnet)`

**Files**: `src/main.rs`, `src/config.rs`

**When spawned**: After ALL Tier 2 modules pass review + QA.

**Scope**:
- Channel wiring: `crossbeam::channel::bounded::<IngestorEvent>(8192)` and `bounded::<TradeSignal>(8192)`
- Thread model: Ingestor on dedicated OS thread (core_affinity pin to core 0, single-threaded tokio), Engine + Executor as tokio tasks on main multi-threaded runtime
- Mode toggle: `MODE=simulation` -> SimulationExecutor, `MODE=live` -> production Executor
- Config loading: dotenvy, all env vars from PRD Section 4
- Graceful shutdown: cancel all orders, flush QuestDB buffer, close WS connections
- Docker-compose integration: Redis + QuestDB connectivity

**Spawn prompt**:
```
You are the Integration Agent for FaCaiBot. You wire everything together.

Read:
- bot_prd.md Section 3 (architecture) and Section 4 (configuration)
- All src/ files to understand module entry points and public interfaces

Implement:
- src/main.rs — entry point, channel creation, thread/task spawning, mode toggle
- src/config.rs — Config struct, dotenvy loading, env var parsing

CONSTRAINTS:
- Ingestor MUST run on a dedicated OS thread (not the tokio runtime)
- Use core_affinity to pin Ingestor thread to core 0
- Crossbeam channels bounded at 8192
- Mode toggle based on MODE env var (simulation vs live)
- Graceful shutdown on SIGINT/SIGTERM
```

---

### 3B. QA Agent

**Model**: Haiku | **Subagent**: `Task(Bash, haiku)`

**When spawned**: After EVERY code change from any developer agent.

**Spawn prompt**:
```
Run the following commands in order and report results for each:
1. cd /Users/ranveerbains/Documents/GitHub/FaCaiBot && cargo build 2>&1
2. cargo clippy -- -D warnings 2>&1
3. cargo fmt -- --check 2>&1
4. cargo test 2>&1

Report format:
- BUILD: PASS or FAIL (with first error)
- CLIPPY: PASS or FAIL (with warnings)
- FORMAT: PASS or FAIL (with files needing format)
- TESTS: PASS or FAIL (with failing test names)
```

---

## Cross-Review Chain

Every developer reviews one peer and is reviewed by another. PM spawns the reviewer agent after each implementation.

```
Ingestor Dev completes
  -> PM spawns Engine Dev (sonnet) as reviewer    [checks IngestorEvent contract]
  -> PM spawns Architect (opus) as reviewer       [architectural compliance]
  -> PM spawns QA (haiku)                         [cargo build/test/clippy]

Engine Dev completes
  -> PM spawns Executor Dev (sonnet) as reviewer  [checks TradeSignal contract]
  -> PM spawns Architect (opus) as reviewer       [architectural compliance]
  -> PM spawns QA (haiku)                         [cargo build/test/clippy]

Executor Dev completes
  -> PM spawns Storage Dev (sonnet) as reviewer   [checks persistence calls]
  -> PM spawns Architect (opus) as reviewer       [architectural compliance]
  -> PM spawns QA (haiku)                         [cargo build/test/clippy]

Storage Dev completes
  -> PM spawns Sim Dev (sonnet) as reviewer       [checks logging interface parity]
  -> PM spawns Architect (opus) as reviewer       [architectural compliance]
  -> PM spawns QA (haiku)                         [cargo build/test/clippy]

Sim Dev completes
  -> PM spawns Engine Dev (sonnet) as reviewer    [logic mirror check]
  -> PM spawns Architect (opus) as reviewer       [architectural compliance]
  -> PM spawns QA (haiku)                         [cargo build/test/clippy]
```

**Review prompt pattern** (peer review):
```
You are the [Reviewer Role] reviewing code written by the [Author Role]
for FaCaiBot.

Read these files: [list of files changed]
Also read the interface contract in: src/types/market.rs, src/types/order.rs

Check:
1. Does the code correctly consume/produce the shared types?
2. Are all pricing fields using Decimal (no f32/f64)?
3. Are there any blocking operations in the signal hot path?
4. Does error handling use tracing macros (not println or silent swallow)?
5. [Domain-specific check for this review pair]

Return: APPROVE or REJECT with specific file:line issues.
```

**If rejected**: PM spawns the original developer agent (resumed if possible) with the review feedback. Developer fixes. Re-review cycle until approved.

---

## Build Order

PM executes this sequence. Parallel phases use multiple Task calls in a single message.

| Phase | Agent(s) | Task | Blocked By | Parallel |
|-------|----------|------|------------|----------|
| 0 | Architect (opus) | Define shared types in types/*.rs | -- | -- |
| 1 | QA (haiku) | `cargo build` -- verify types compile | Phase 0 | -- |
| 2a | Ingestor Dev (sonnet) | Binance WS + spike detection | Phase 1 | Yes |
| 2b | Storage Dev (sonnet) | Redis + QuestDB interfaces | Phase 1 | Yes |
| 2c | Sim Dev (sonnet) | SimulationState + Telegram reporter | Phase 1 | Yes |
| 3 | QA (haiku) | `cargo build` | 2a+2b+2c | -- |
| 4a | Engine Dev (sonnet) | Strategy engine + signal generation | Phase 1 | Yes |
| 4b | Ingestor Dev (sonnet) | Polymarket WS + market rotation | Phase 2a | Yes |
| 5 | QA (haiku) | `cargo build` | 4a+4b | -- |
| 6a | Executor Dev (sonnet) | Order signing + CLOB submission + erosion | Phase 4a | Yes |
| 6b | Sim Dev (sonnet) | SimulationExecutor (mirrors Executor) | Phase 4a+2c | Yes |
| 7 | QA (haiku) | `cargo build` | 6a+6b | -- |
| 8a | Peer reviews (3x sonnet) | Cross-domain code review | Phase 7 | Yes |
| 8b | Architect review (opus) | Full architectural review | Phase 7 | Yes |
| 9 | Integration Agent (sonnet) | Wire main.rs + config.rs | Phase 8 | -- |
| 10 | QA (haiku) | Full `cargo build + test + clippy + fmt` | Phase 9 | -- |
| 11 | PM (main conversation) | Final PRD validation | Phase 10 | -- |

**Parallelism**: Phases 2a/2b/2c are 3 Task calls in one message. Phases 4a/4b are 2. Phases 6a/6b are 2. Phase 8a/8b are 4.

---

## File Ownership Map

Every source file has exactly one owning agent. No orphan files.

```
src/
+-- main.rs                      -> Integration Agent
+-- config.rs                    -> Integration Agent
+-- engine/
|   +-- strategy.rs              -> Engine Developer
+-- gateway/
|   +-- binance.rs               -> Ingestor Developer
|   +-- polymarket.rs            -> Ingestor Dev (WS) + Executor Dev (orders)
+-- storage/
|   +-- hot.rs                   -> Storage Developer
|   +-- cold.rs                  -> Storage Developer
+-- types/
|   +-- market.rs                -> Architect (defines) / Engine Dev (extends)
|   +-- order.rs                 -> Architect (defines) / Executor Dev (extends)
|   +-- simulation.rs            -> Simulation Developer
+-- executor/
|   +-- simulation.rs            -> Simulation Developer
+-- reporting/
|   +-- telegram.rs              -> Simulation Developer
+-- utils/
    +-- signing.rs               -> Executor Developer
```

**Shared file**: `gateway/polymarket.rs` has two owners:
- Ingestor Dev owns WS connection management (subscribe, parse, reconnect)
- Executor Dev owns order submission (sign, post, cancel, heartbeat)

Split recommendation: if conflicts arise, split into `gateway/polymarket_ws.rs` (Ingestor) and `gateway/polymarket_api.rs` (Executor).
