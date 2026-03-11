# Pre-Spike Detection Overhaul — Implementation Plan

This document is the single source of truth for the architectural overhaul from reactive spike detection to predictive pre-spike entry. Every phase, file, struct, field, and edge case is specified here.

---

## Table of Contents

1. [Executive Summary](#1-executive-summary)
2. [What Changes vs What Stays](#2-what-changes-vs-what-stays)
3. [Phase Breakdown](#3-phase-breakdown)
4. [Agent Orchestration Model](#4-agent-orchestration-model)
5. [Phase Details](#5-phase-details)
6. [Edge Case Rework Matrix](#6-edge-case-rework-matrix)
7. [Dead Code Removal Manifest](#7-dead-code-removal-manifest)
8. [Risk & Rollback](#8-risk--rollback)

---

## 1. Executive Summary

### The Problem

The current system is **reactive**: wait for a confirmed ATR spike on Binance spot, then race market makers to take liquidity on Polymarket. MMs cancel resting orders faster than the bot can take them (<1ms internal cancel vs ~1.2s CLOB round-trip). The structural speed disadvantage means Leg 1 fills are either adverse (maker fill = move stalled) or missed (taker fill = liquidity already gone).

### The Solution

Shift to **predictive entry**: detect institutional flow buildup on Binance futures 50–200ms before the spot price moves, post a maker Leg 1 order before MMs reprice, and immediately post Leg 2 at the refined target once Leg 1 fills.

### Key Architectural Changes

| Layer | Current | New |
|-------|---------|-----|
| **Data Sources** | Spot SBE only (`@depth20`, `@bestBidAsk`) | Spot SBE (`@depth20`, `@bestBidAsk`, `@trade`) + Futures JSON WS (`@aggTrade`, `@bookTicker`, `@forceOrder`) |
| **Signal Detection** | `SpikeDetector` — single EMA-ATR threshold on spot mid-price | `BuildupDetector` — 6-metric composite score with temporal alignment and direction consensus |
| **Leg 1 Entry** | Batch FAK taker at 3 price levels (instant fill) | Maker post-only at best ask (resting order, fill on reprice) |
| **Leg 1 Sustain** | None (immediate confirmation) | Flow-based: cancel unfilled maker if composite drops below cancel threshold |
| **Repricing Model** | Single-phase: `norm_spike` from observed ATR displacement | Two-phase: Phase A (composite score at entry), Phase B (observed displacement after fill, `max(observed, predicted)`) |
| **Leg 2 Entry** | Post after fill → 2-phase time-based hedge (profit target → break-even pursuit) | Pre-emptive: post immediately on Leg 1 fill at refined target + flow-based graduated response |
| **Whipsaw Detection** | Opposite ATR spike (600-700ms lag) | Flow signal reversal (400-450ms, ~200ms faster) + ATR backstop |
| **Fee Model (Leg 1)** | Taker fee (FAK) | Maker rebate (post-only) |
| **Simulation Mode** | Full sim executor + ~2,800 lines of sim-only code | **Removed entirely** — live testing on EC2 instance |

---

## 2. What Changes vs What Stays

### Stays Unchanged

These systems are architecturally sound and unaffected:

- **Three-layer pipeline** (Ingestor → Engine → Executor) with crossbeam SPSC channels
- **Polymarket WS gateway** (market_ws.rs, user_ws.rs) — book updates, fill detection, heartbeat
- **Polymarket REST gateway** (rest.rs) — `place_order()`, `cancel_order()`, `get_order_status()`, `place_orders_batch()`, `get_best_ask()`
- **SDK cache pre-warm** on rotation (`tick_size`, `neg_risk`, `fee_rate_bps`, connection pool)
- **Market rotation system** (rotation.rs) — Gamma API discovery, prewarm, dual-timer
- **Telegram reporting** — fire-and-forget, rate-limited, critical bypass (sim-specific formatters removed — see Phase 0)
- **QuestDB analytics** — ILP tables, batch flush, tick/book recording (`record_simulated_trade()` removed — see Phase 0)
- **Control plane** — `/pause`, `/resume`, `/shutdown`, `/set`, `/status`, `/redeem`
- **Config editor** — TOML read/write, param allowlist
- **Auto-redeem** — rotation-triggered, condition ID persistence
- **Persistent redeems.txt** — append on rotation, cleanup on redeem
- **jemalloc allocator**, **manual tokio runtime** (2 workers, cores 1-2), **engine on spawn_blocking**
- **Decimal arithmetic** everywhere — no f32/f64 for prices/sizes
- **`OrderState` enum** (`None`, `Posted`, `Filled`) — lifecycle tracking unchanged
- **`ExecutorFeedback` enum** — all 6 variants remain (feedback semantics unchanged)
- **User WS fill detection** — hex order hash matching, MATCHED/MINED/CONFIRMED/FAILED handling
- **Deferred partial fill alerts** — MATCHED buffering, MINED/CONFIRMED resolution
- **Fire-and-confirm cancels** (Leg 2) — `Result<bool>` semantics, cancel-not-confirmed restore
- **`prev_leg2_order` restore** — cancel-not-confirmed → restore Posted state
- **`pending_fills` buffer** — up to 8 buffered TradeStatusUpdate events, replayed on state change
- **Double-fill rebalance** — orphan detection, `post_trade_orphan`, rebalance FOK
- **Balance exhaustion** — flag set on "balance"/"allowance" error, blocks Leg 2 until rotation
- **Rotation emergency** — filled Leg 1 + incomplete Leg 2 → emergency FOK buffer before state reset
- **Entry cutoff window** — `entry_cutoff_secs` before market expiry
- **Rotation quiet period** — `rotation_quiet_ms` after market switch
- **Trade cooldown** — `trade_cooldown_ms` after trade completion
- **Drain/pause mode** — `draining`, `paused` flags
- **Session/market summaries** — live trade recording, Telegram formatted reports
- **`LiveTradeMeta`** — Leg 2 exit metadata for Telegram reporting
- **FOK price escalation** — +1 tick per attempt, $1.00 cap, non-transient error abort, zero-size guard
- **`clob_safe_fok_size()`** — price × size ≤2dp constraint

### Removed Entirely

- **Simulation mode** — `Mode::Simulation` variant, `SimulationExecutor`, `advance_simulation()`, all sim-specific types and code paths (~2,800 lines). Live testing on EC2 replaces simulation. See [Phase 0](#phase-0-simulation-mode-removal) for full manifest

### Changes

Everything in the signal detection → entry → hedge pipeline changes as detailed in the phases below.

---

## 3. Phase Breakdown

```
Phase 0: Simulation Mode Removal                     ── Cleanup First (simplifies everything downstream)
Phase 1: Data Types & Event Infrastructure          ──┐
Phase 2: Futures WebSocket Connection                  ├── Data Layer (parallel: 2+3)
Phase 3: Spot Trade Stream (SBE @trade)              ──┘
Phase 4: Buildup Detector — Individual Metrics       ──┐
Phase 5: Buildup Detector — Composite Score            ├── Detection Layer (sequential)
Phase 6: Engine — Buildup Integration                ──┘
Phase 7: Repricing Model Overhaul                    ──┐
Phase 8: Leg 1 Execution Change (FAK → Maker)          ├── Execution Layer (sequential)
Phase 9: Leg 2 Pre-emptive + Flow-Based Hedge        ──┘
Phase 10: Edge Case Rework                           ── Safety Layer
Phase 11: Dead Code Removal                          ── Cleanup Layer
Phase 12: Config & Tests                             ── Validation Layer
Phase 13: Documentation Update                       ── Docs Layer
Phase 14: Architecture Diagram (draw.io)             ── Runs continuously across all phases
```

**Dependency graph:**
```
0 → 1 → 2,3 (parallel) → 4 → 5 → 6 → 7 → 8 → 9 → 10 → 11 → 12 → 13
                                                                        ↑
14 ─── runs across all phases, checks back after each ──────────────────┘
```

---

## 4. Agent Orchestration Model

### Agent Roles

| Agent | Role | When Active |
|-------|------|-------------|
| **PM Agent** | Orchestrates all phases. Reads this PLAN.md. Assigns work. Reviews outputs from all agents. Gives final clear or requests iteration. Tracks phase completion status. | Always (top-level) |
| **Implementation Agent** | Writes code for a single phase. Receives specific file list, struct definitions, and acceptance criteria from PM. Returns diff + explanation. | Per phase |
| **Verification Agent** | Reviews Implementation Agent output. Questions: "Why was this done this way?", "Is there a simpler approach?", "Does this handle edge case X?", "Does this compile?". Returns pass/fail + improvement suggestions. | After each implementation |
| **Architecture Agent** | Maintains the draw.io diagram. After each phase completes, updates the diagram to reflect new components, data flows, state transitions, and edge cases. Cross-references diagram against actual code. | After each phase (and final pass) |

### Communication Flow

```
PM Agent
  │
  ├─→ Implementation Agent (Phase N)
  │     │
  │     └─→ returns code changes
  │
  ├─→ Verification Agent
  │     │
  │     ├─→ PASS: PM approves, proceeds to next phase
  │     └─→ FAIL: returns to Implementation Agent with feedback
  │           │
  │           └─→ Implementation Agent iterates (max 2 rounds)
  │                 │
  │                 └─→ Verification Agent re-reviews
  │
  └─→ Architecture Agent (after phase PASS)
        │
        └─→ updates diagram, confirms consistency with code
```

### Context Management Rules

Each agent invocation must fit within **1 compact context window**. To enforce this:

1. **Max 5–6 files** modified per phase
2. **Clear input**: PM provides exact file paths, struct definitions, method signatures, and acceptance criteria
3. **Clear output**: Implementation Agent returns only the diff, not the full file
4. **No cross-phase state**: Each phase is self-contained. Agent reads current files, applies changes, returns
5. **Compile gate**: Every phase must end with `cargo build` passing. Tests updated in Phase 12

---

## 5. Phase Details

---

### Phase 0: Simulation Mode Removal

**Goal**: Delete all simulation-only code, types, and code paths. This is done first because it eliminates ~2,800 lines of dead code and simplifies every subsequent phase (no need to update sim paths alongside live paths).

**Files Deleted** (2 entire files):
- `src/types/simulation.rs` (945 lines) — `SimFill`, `SimPosition`, `SimTrade`, `MarketSummary`, `SessionSummary`, `SimulationState`, `PositionStatus`
- `src/executor/simulation.rs` (1,168 lines) — `SimulationExecutor`, simulated fills, sim Telegram reporting

**Files Modified** (13 files):

| File | Changes |
|------|---------|
| `src/main.rs` | Remove `Mode::Simulation` branch in executor spawn (lines ~637-666). Remove `advance_simulation()` call in engine loop (lines ~504-511). Remove sim-mode channel wiring. Always spawn `LiveExecutor`. Remove `use` of `SimulationExecutor` |
| `src/config.rs` | Remove `Mode::Simulation` variant from `Mode` enum. Remove sim-specific config defaults/fallbacks. Require live credentials always (`.env` must have CLOB keys) |
| `src/engine/strategy.rs` | Delete `advance_simulation()` method (~235 lines). Remove `pending_leg1_signal` field (used only for sim fill state machine — **verify**: also used for live opportunity alerts → if so, keep). Remove all `if self.is_sim()` / `cfg.mode == Mode::Simulation` conditionals. Remove `SimulationState` field and its init. Remove sim-only fields: `sim_confirmed_fill`, `sim_was_taker` usage on `TradeSignal` |
| `src/types/order.rs` | Remove `sim_confirmed_fill: bool` and `sim_was_taker: bool` fields from `TradeSignal` (sim-only). Keep `leg1_taker_fee` (used by live mode) |
| `src/types/market.rs` | Remove `mod simulation;` declaration. Remove any sim-specific `IngestorEvent` variants (if any) |
| `src/types/mod.rs` | Remove `pub mod simulation;` re-export |
| `src/executor/mod.rs` | Remove `pub mod simulation;` re-export |
| `src/gateway/polymarket/user_ws.rs` | Remove sim-mode parking (`std::future::pending()` when `Mode::Simulation`) — always run User WS |
| `src/gateway/polymarket/heartbeat.rs` | Remove sim-mode parking — always run heartbeat |
| `src/reporting/telegram.rs` | Remove sim-specific formatters that use `SimTrade`, `MarketSummary`, `SessionSummary` types. Keep `format_trade_completed()` if it's shared with live mode (it uses `SimTrade` internally → refactor to use live types or inline) |
| `src/storage/cold.rs` | Remove `record_simulated_trade()` (sim-only ILP recording). Keep `record_executed_trades()` (live) |
| `src/control/config_editor.rs` | Remove any `mode` param from allowlist (if present) |
| `src/executor/fill_engine.rs` | Keep — `compute_fill_size()`, `opposite_side()` are used by live mode |

**Utility Functions to Relocate (NOT delete)**:

These functions live on sim types but are used by live mode's `build_live_sim_trade()`:

| Function | Current Location | Action |
|----------|-----------------|--------|
| `SimFill::compute_taker_fee(price, size)` | `types/simulation.rs` | Move to `src/executor/fill_engine.rs` as free function `compute_taker_fee(price: Decimal, size: Decimal) -> Decimal` |
| `SimFill::compute_taker_fee_per_share(price)` | `types/simulation.rs` | Move to `src/executor/fill_engine.rs` as free function `compute_taker_fee_per_share(price: Decimal) -> Decimal` |
| `SimFill::compute_maker_rebate(price, size)` | `types/simulation.rs` | Move to `src/executor/fill_engine.rs` as free function `compute_maker_rebate(price: Decimal, size: Decimal) -> Decimal` |

**`build_live_sim_trade()` Refactor**:

This method on `StrategyEngine` currently builds a `SimTrade` for live trade reporting. After sim removal:
- Rename to `build_live_trade_report()` or similar
- Replace `SimTrade` return type with a new lightweight `LiveTradeReport` struct (define in `types/order.rs`)
- `LiveTradeReport` contains only the fields needed for Telegram reporting and QuestDB recording: direction, leg1/leg2 prices+sizes, fees, gross profit, net profit, maker rebate, was_partial, exit_reason, fill_method, expected_pct, market info
- This replaces the 7-type `SimFill`/`SimPosition`/`SimTrade`/`MarketSummary`/`SessionSummary`/`SimulationState`/`PositionStatus` hierarchy

**`pending_leg1_signal` Verification**:

Before removing, verify usage:
- **Sim mode**: Used in `advance_simulation()` to track the pending Leg 1 order for simulated fills → **remove**
- **Live mode**: Used in `on_order_posted()` for opportunity alert Telegram message → **keep if used by live**, move to a simpler field name like `leg1_signal` if so

**Diagnostic Counters to Remove**:
- Any counters prefixed with `sim_` or used only in `advance_simulation()`

**Config Changes**:
- Remove `Mode` enum entirely (always live)
- Remove `mode = "simulation"` / `mode = "live"` from config parsing — the field simply doesn't exist
- `.env` requires `CLOB_API_KEY`, `CLOB_API_SECRET`, `CLOB_API_PASSPHRASE`, `CLOB_FUNDER`, `PRIVATE_KEY` always (no sim fallback)

**Test Impact**:
- Tests that create `SimulationState`, `SimTrade`, `SimFill`, etc. must be deleted or rewritten to use live types
- Tests for `advance_simulation()` are deleted
- Tests for `compute_taker_fee()`, `compute_maker_rebate()` are moved to `fill_engine.rs` tests

**Acceptance Criteria**:
- `cargo build` passes with no references to deleted types
- `cargo test` — all remaining tests pass (sim-specific tests removed, count drops by ~20-30)
- No `Mode::Simulation` references anywhere in the codebase
- No `simulation.rs` files exist
- Utility fee functions relocated and working
- Live trade reporting works end-to-end with new `LiveTradeReport` type

---

### Phase 1: Data Types & Event Infrastructure

**Goal**: Define all new types and event variants needed for futures data and buildup detection, without changing any runtime behavior.

**Files Modified**:
- `src/types/market.rs` — new event variants, new data structs
- `src/types/order.rs` — minor: new `ExecutorCommand` variant for Leg 1 cancel

**New Types in `types/market.rs`**:

```rust
/// Aggregated trade event from Binance Futures @aggTrade stream.
pub struct FuturesAggTrade {
    pub price: Decimal,
    pub quantity: Decimal,
    pub is_buyer_maker: bool,  // false = taker buy (aggressor bought)
    pub timestamp_ms: u64,
}

/// Best bid/ask from Binance Futures @bookTicker stream.
pub struct FuturesBookTicker {
    pub bid_price: Decimal,
    pub bid_qty: Decimal,
    pub ask_price: Decimal,
    pub ask_qty: Decimal,
    pub timestamp_ms: u64,
}

/// Forced liquidation event from Binance Futures @forceOrder stream.
pub struct FuturesForceOrder {
    pub side: String,          // "SELL" (long liquidated) or "BUY" (short liquidated)
    pub price: Decimal,
    pub quantity: Decimal,
    pub timestamp_ms: u64,
}

/// Individual trade from Binance Spot SBE @trade stream.
pub struct SpotTrade {
    pub price: Decimal,
    pub quantity: Decimal,
    pub is_buyer_maker: bool,
    pub timestamp_ms: u64,
}

/// Buildup signal emitted by the BuildupDetector.
/// Replaces SpikeInfo as the trigger for Leg 1 entry.
pub struct BuildupInfo {
    /// Composite buildup score [0.0, 1.0].
    pub composite_score: Decimal,
    /// Predicted direction of imminent spike.
    pub direction: Direction,
    /// Individual metric values (for diagnostics/logging).
    pub cvd_accel: Decimal,
    pub spot_flow: Decimal,
    pub obi_velocity: Decimal,
    pub basis_delta: Decimal,
    pub liq_pressure: Decimal,
    pub atr_displacement: Decimal,
    /// Spot mid-price at time of buildup detection (for Phase B refinement).
    pub spot_mid_at_entry: Decimal,
    /// Current EMA ATR (for Phase B observed displacement computation).
    pub ema_atr: Decimal,
    /// Epoch ms when buildup threshold was crossed.
    pub timestamp_ms: u64,
}
```

**New `IngestorEvent` Variants**:

```rust
// Add to existing IngestorEvent enum:

/// Futures aggregated trade (for CVD acceleration).
FuturesAggTrade(FuturesAggTrade),

/// Futures best bid/ask (for basis delta computation).
FuturesBookTicker(FuturesBookTicker),

/// Futures forced liquidation (for liquidation pressure).
FuturesForceOrder(FuturesForceOrder),

/// Spot individual trade from SBE @trade (for spot trade flow).
SpotTrade(SpotTrade),

/// Buildup signal: composite score crossed entry threshold.
/// Replaces SpikeConfirmed for Leg 1 entry decisions.
BuildupConfirmed(BuildupInfo),

/// Buildup score update (below entry threshold but non-zero).
/// Used by engine for flow monitoring after Leg 1 fill.
BuildupUpdate {
    composite_score: Decimal,
    direction: Direction,
    timestamp_ms: u64,
},

/// Buildup detector diagnostics (every 60s, replaces SpikeDiagnostic).
BuildupDiagnostic {
    composite_score: Decimal,
    cvd_accel: f64,
    spot_flow: f64,
    obi_velocity: f64,
    basis_delta: f64,
    liq_pressure: f64,
    atr_displacement: f64,
    fresh_count: u32,
    stale_count: u32,
    signals_emitted: u64,
},
```

**New `ExecutorCommand` Variant** (in `types/order.rs`):

```rust
// Add to ExecutorCommand enum:

/// Cancel unfilled Leg 1 maker order (flow-based sustain failure).
CancelLeg1Order { order_id: String },
```

**`SpikeInfo` retained temporarily**: Keep `SpikeInfo` during transition — it's still used by the ATR backstop fallback. Will be deprecated in Phase 11.

**Acceptance Criteria**:
- `cargo build` passes
- No runtime behavior changes (new types are defined but unused)
- All existing 148 tests pass

---

### Phase 2: Futures WebSocket Connection

**Goal**: Establish a new Binance Futures JSON WebSocket connection that streams `@aggTrade`, `@bookTicker`, and `@forceOrder` into the ingestor channel.

**Files Modified**:
- `src/gateway/binance/futures_ws.rs` — **NEW FILE**: Futures WS client
- `src/gateway/binance/mod.rs` — re-export new module
- `src/main.rs` — wire futures WS into ingestor thread
- `src/config.rs` — new config field for futures WS URL

**New File: `gateway/binance/futures_ws.rs`**:

```rust
/// Binance USDT-M Futures WebSocket client.
///
/// Connects to wss://fstream.binance.com/stream and subscribes to:
/// - btcusdt@aggTrade    (per-trade events, CVD computation)
/// - btcusdt@bookTicker  (BBO updates, basis delta computation)
/// - btcusdt@forceOrder  (liquidation events, cascade detection)
///
/// Uses standard JSON WebSocket (not SBE). Parses each frame and
/// emits typed IngestorEvent variants to the engine channel.
pub struct FuturesGateway {
    ws_url: String,
}
```

**Key Design Decisions**:
- **Separate connection** from spot SBE (different protocol, different server)
- **JSON parsing** via `serde_json` (futures WS doesn't support SBE)
- **Same backoff pattern** as existing `BinanceGateway` (1–30s exponential)
- **Same stale event filtering** pattern (configurable threshold)
- **Emits `WsStatus { DataSource::BinanceFutures, connected }`** — new `DataSource` variant
- **Runs in the same ingestor thread** (single-threaded tokio runtime, select! alongside spot)

**Stream URL**: `wss://fstream.binance.com/stream?streams=btcusdt@aggTrade/btcusdt@bookTicker/btcusdt@forceOrder`

**Config Addition** (`config.rs`):
```rust
// In Config struct:
pub binance_futures_ws_url: String,
// Default: "wss://fstream.binance.com"
// Env: BINANCE_FUTURES_WS_URL
```

**Main.rs Wiring**:
- Add `FuturesGateway::run(tx.clone(), stale_threshold_ms)` to ingestor thread's `select!`
- No auth needed (public streams)

**Acceptance Criteria**:
- Futures WS connects and receives events (verify with logging)
- Events emitted as `FuturesAggTrade`, `FuturesBookTicker`, `FuturesForceOrder` variants
- Connection resilient to drops (backoff + reconnect)
- `WsStatus` events emitted for connectivity tracking
- Existing spot SBE unaffected
- All existing tests pass

---

### Phase 3: Spot Trade Stream (SBE `@trade`)

**Goal**: Add `@trade` event parsing to the existing Binance SBE gateway and emit `SpotTrade` events.

**Files Modified**:
- `src/gateway/binance/ws.rs` — add `@trade` subscription, parse SBE trade template, emit events

**Changes**:

1. **Update subscription URL**:
   ```
   // Old:
   "/stream?streams=btcusdt@depth20/btcusdt@bestBidAsk"
   // New:
   "/stream?streams=btcusdt@depth20/btcusdt@bestBidAsk/btcusdt@trade"
   ```

2. **Add SBE template for Trade events**:
   - Template ID TBD (check Binance SBE schema `stream_1_0.xml` for Trade template)
   - Parse: `price`, `quantity`, `isBuyerMaker`, `tradeTime`
   - Emit: `IngestorEvent::SpotTrade(SpotTrade { ... })`

3. **Stale event filtering**: Same pattern as depth — check `event_ts_ms` against threshold

**Note**: SBE `@trade` events arrive per individual trade. During active markets, this is 100–200 events/second. The ingestor channel (bounded 8192) can handle this. The engine will aggregate via the buildup detector's EMA (not process individually).

**Acceptance Criteria**:
- Spot trade events parsed and emitted correctly
- High-frequency trades don't overflow channel (bounded 8192 is sufficient)
- Existing depth/bestBidAsk parsing unaffected
- All existing tests pass

---

### Phase 4: Buildup Detector — Individual Metrics

**Goal**: Implement the 6 individual metric trackers that feed the composite buildup score.

**Files Created**:
- `src/engine/buildup/mod.rs` — module declaration + re-exports
- `src/engine/buildup/metrics.rs` — 6 individual metric tracker structs

**Each Metric Tracker** follows the same pattern:

```rust
pub struct MetricTracker {
    value: f64,               // Current raw value
    last_update_ms: u64,      // Timestamp of last update
    freshness_max_ms: u64,    // Configurable freshness gate
    min_threshold: f64,       // Normalization: below this → 0
    saturation: f64,          // Normalization: above this → 1
    // ... metric-specific EMA/state fields
}

impl MetricTracker {
    pub fn update(&mut self, ..., now_ms: u64) { ... }
    pub fn normalized(&self, now_ms: u64) -> f64 { ... }  // [0,1] or 0 if stale
    pub fn direction(&self) -> Option<Direction> { ... }   // sign of raw value
    pub fn is_fresh(&self, now_ms: u64) -> bool { ... }
    pub fn raw(&self) -> f64 { ... }
}
```

**Metric Details**:

| # | Metric | Struct Name | Key State Fields | Update Source |
|---|--------|-------------|------------------|---------------|
| 1 | **CVD Acceleration** | `CvdAccelTracker` | `cvd_fast_ema: f64`, `cvd_slow_ema: f64`, `accel: f64` | `FuturesAggTrade` events |
| 2 | **Spot Trade Flow** | `SpotFlowTracker` | `buy_vol_ema: f64`, `sell_vol_ema: f64`, `flow: f64` | `SpotTrade` events |
| 3 | **OBI Velocity** | `ObiVelocityTracker` | `prev_obi: f64`, `obi_delta_ema: f64` | `BinanceDepth` events (existing) |
| 4 | **Basis Delta** | `BasisDeltaTracker` | `futures_mid: f64`, `spot_mid: f64`, `prev_basis_bps: f64`, `basis_ema: f64` | `FuturesBookTicker` + spot mid |
| 5 | **Liquidation Pressure** | `LiqPressureTracker` | `decaying_sum: f64`, `events: VecDeque<(f64, u64)>` (time-decaying sum, 2s half-life) | `FuturesForceOrder` events |
| 6 | **ATR Displacement** | `AtrDisplacementTracker` | `ema_atr: f64`, `prev_mid: f64`, `displacement_ratio: f64`, `delta_sign: f64` | `BinanceDepth` events (existing, adapted from `SpikeDetector`) |

**CVD Acceleration Detail** (most complex metric):
```
On each FuturesAggTrade:
  signed_qty = quantity * (is_buyer_maker ? -1 : +1)
  cvd_fast_ema = alpha_fast * signed_qty + (1 - alpha_fast) * cvd_fast_ema
  cvd_slow_ema = alpha_slow * signed_qty + (1 - alpha_slow) * cvd_slow_ema
  accel = cvd_fast_ema - cvd_slow_ema

  // alpha_fast ~ 100ms window, alpha_slow ~ 500ms window
  // accel > 0 → buying accelerating → bullish
  // accel < 0 → selling accelerating → bearish
```

**Liquidation Pressure Detail** (time-decaying sum):
```
On each FuturesForceOrder:
  signed_qty = quantity * (side == "SELL" ? -1 : +1)
  events.push_back((signed_qty, now_ms))

On query:
  liq_pressure = sum(qty * 2^(-(now_ms - event_ms) / half_life_ms)) for all events
  // Prune events older than 5 * half_life_ms (contribution < 3%)
```

**Acceptance Criteria**:
- Each metric computes correct values from input data (unit tests)
- Freshness gates expire correctly (stale → normalized returns 0)
- Direction returns correct sign
- All 6 metrics are independent (no shared mutable state)
- Unit tests for each metric: warmup behavior, direction, normalization, staleness

---

### Phase 5: Buildup Detector — Composite Score

**Goal**: Combine the 6 metrics into a single composite buildup score with temporal alignment, direction consensus, and causal ordering.

**Files Created/Modified**:
- `src/engine/buildup/detector.rs` — **NEW**: `BuildupDetector` struct
- `src/engine/buildup/mod.rs` — re-export

**`BuildupDetector` Struct**:

```rust
pub struct BuildupDetector {
    // Individual metrics
    cvd: CvdAccelTracker,
    spot_flow: SpotFlowTracker,
    obi_velocity: ObiVelocityTracker,
    basis_delta: BasisDeltaTracker,
    liq_pressure: LiqPressureTracker,
    atr_displacement: AtrDisplacementTracker,

    // Weights (from config)
    w_cvd: f64,          // 0.30
    w_spot_flow: f64,    // 0.15
    w_obi: f64,          // 0.20
    w_basis: f64,        // 0.20
    w_liq: f64,          // 0.05
    w_atr: f64,          // 0.10

    // Thresholds
    entry_threshold: f64,   // 0.40 — composite must exceed to trigger
    cancel_threshold: f64,  // 0.25 — below this, cancel unfilled maker

    // Diagnostic counters
    diag_signals_emitted: u64,
    diag_direction_vetoes: u64,
    diag_causal_vetoes: u64,
    last_diag_ms: u64,
    pending_diag: Option<BuildupDiagSnapshot>,

    // Spot reference for Phase B
    current_spot_mid: f64,
    current_ema_atr: f64,
}
```

**Key Methods**:

```rust
impl BuildupDetector {
    /// Feed a futures aggTrade event.
    pub fn on_futures_agg_trade(&mut self, trade: &FuturesAggTrade, now_ms: u64);

    /// Feed a futures bookTicker event.
    pub fn on_futures_book_ticker(&mut self, ticker: &FuturesBookTicker, now_ms: u64);

    /// Feed a futures forceOrder event.
    pub fn on_futures_force_order(&mut self, order: &FuturesForceOrder, now_ms: u64);

    /// Feed a spot trade event.
    pub fn on_spot_trade(&mut self, trade: &SpotTrade, now_ms: u64);

    /// Feed a spot depth snapshot (updates OBI velocity + ATR displacement).
    /// Called on every BinanceDepth event (50ms cadence).
    pub fn on_spot_depth(&mut self, mid: f64, obi: f64, now_ms: u64);

    /// Feed spot best bid/ask (updates spot_mid for basis computation).
    pub fn on_spot_bba(&mut self, mid: f64, now_ms: u64);

    /// Compute composite score. Called on EVERY incoming event.
    /// Returns (score, direction) or (0, None) if vetoed.
    pub fn evaluate(&self, now_ms: u64) -> (f64, Option<Direction>);

    /// Check if composite exceeds entry threshold.
    /// Returns BuildupInfo if threshold crossed.
    pub fn check_entry(&self, now_ms: u64) -> Option<BuildupInfo>;

    /// Check if composite has dropped below cancel threshold.
    pub fn below_cancel_threshold(&self, now_ms: u64) -> bool;

    /// Get current composite score (for flow monitoring after Leg 1 fill).
    pub fn current_score(&self, now_ms: u64) -> (f64, Option<Direction>);

    /// Take pending diagnostic snapshot.
    pub fn take_diagnostic(&mut self) -> Option<BuildupDiagSnapshot>;
}
```

**Composite Evaluation Logic** (inside `evaluate()`):

```
1. Compute normalized value for each metric (0 if stale)
2. Direction consensus:
   - Collect non-zero directions from fresh metrics
   - Find dominant direction (mode)
   - If >1 metric disagrees with dominant → return (0, None)
3. Causal ordering:
   - Leading metrics: cvd_accel, basis_delta (futures-derived)
   - Confirming metrics: spot_flow, obi_velocity (spot-derived)
   - At least 1 leading AND 1 confirming must be fresh + non-zero
   - Otherwise → return (0, None)
4. Weighted sum:
   composite = w_cvd*norm(cvd) + w_flow*norm(flow) + w_obi*norm(obi)
             + w_basis*norm(basis) + w_liq*norm(liq) + w_atr*norm(atr)
5. Return (composite, dominant_direction)
```

**Acceptance Criteria**:
- Composite score correctly combines 6 metrics
- Direction consensus vetoes contradictory signals
- Causal ordering vetoes spot-only signals
- Freshness gates work (stale metric → excluded)
- Entry/cancel thresholds respected
- Unit tests: composite computation, veto scenarios, threshold crossings

---

### Phase 6: Engine — Buildup Integration

**Goal**: Replace `SpikeConfirmed` handling in the engine with `BuildupConfirmed` + `BuildupUpdate`. Wire the `BuildupDetector` into the ingestor pipeline.

**Files Modified**:
- `src/gateway/binance/ws.rs` — feed spot events to buildup detector, emit `BuildupConfirmed`/`BuildupUpdate`
- `src/engine/strategy.rs` — handle new event variants, replace spike-based evaluate() trigger
- `src/engine/evaluator.rs` — modify `Leg1Evaluator` to use `BuildupInfo` instead of `SpikeInfo`

**Ingestor-Side Changes** (`gateway/binance/ws.rs`):

The `BuildupDetector` runs in the **ingestor thread** (same as `SpikeDetector` currently). On each incoming event:

1. `BinanceDepth` → feed to `BuildupDetector::on_spot_depth()` AND existing `SpikeDetector::update()` (ATR backstop)
2. `SpotTrade` → feed to `BuildupDetector::on_spot_trade()`
3. `FuturesAggTrade` → feed to `BuildupDetector::on_futures_agg_trade()`
4. `FuturesBookTicker` → feed to `BuildupDetector::on_futures_book_ticker()` AND update `spot_mid` via latest BBA
5. `FuturesForceOrder` → feed to `BuildupDetector::on_futures_force_order()`

After each feed, call `check_entry()`:
- If returns `Some(BuildupInfo)` → emit `IngestorEvent::BuildupConfirmed(info)`
- If composite > 0 but below threshold → emit `IngestorEvent::BuildupUpdate { score, direction, ts }`

Also retain `SpikeDetector` as ATR backstop → if `SpikeEvent::Confirmed` AND no recent BuildupConfirmed → emit `SpikeConfirmed` (fallback for spot-only spikes).

**Engine-Side Changes** (`strategy.rs`):

```rust
// In on_event() match:

IngestorEvent::BuildupConfirmed(buildup) => {
    self.diag_buildups_received += 1;
    // Same guard logic as current SpikeConfirmed:
    // - quiet period check
    // - cutoff window check
    // - whipsaw guard (opposite direction while Leg 1 active)

    self.state.buildup_detected = true;
    self.state.last_buildup = Some(buildup);
    // ... (same pattern as current spike handling)
}

IngestorEvent::BuildupUpdate { composite_score, direction, timestamp_ms } => {
    // Store for flow monitoring (used by evaluate_leg2 graduated response)
    self.state.current_composite_score = composite_score;
    self.state.current_composite_direction = Some(direction);
    self.state.composite_update_ms = timestamp_ms;
}
```

**`MarketState` additions**:
```rust
pub buildup_detected: bool,
pub last_buildup: Option<BuildupInfo>,
pub current_composite_score: Decimal,
pub current_composite_direction: Option<Direction>,
pub composite_update_ms: u64,
```

**`Leg1Evaluator::evaluate()` Changes**:
- Replace `spike_detected` check → `buildup_detected` check
- Replace `last_spike: SpikeInfo` usage → `last_buildup: BuildupInfo` usage
- `atr_ratio` in repricing model → `composite_score` (Phase A)
- OBI alignment gate: still applies (OBI velocity is a metric in the composite, but the directional OBI gate from `SpikeInfo.obi` remains as a separate check using the buildup's direction)
- All other guards (stale book, skew cap, active trade, cutoff, min repricing) remain unchanged

**Self-Gating**: `buildup_detected` cleared on `evaluate()` success OR failure (same pattern as `spike_detected`).

**Acceptance Criteria**:
- `BuildupConfirmed` triggers Leg 1 evaluation
- `BuildupUpdate` updates flow monitoring state
- ATR backstop still works as fallback
- All existing guards still fire correctly
- Whipsaw guard works with buildup direction
- `cargo build` passes

---

### Phase 7: Repricing Model Overhaul

**Goal**: Implement the two-phase repricing model. Phase A uses composite score at entry; Phase B refines using observed displacement after fill.

**Files Modified**:
- `src/engine/confidence.rs` — modify `compute_expected_repricing()` to accept composite score
- `src/engine/strategy.rs` — `init_leg2()` computes Phase B refinement
- `src/engine/erosion.rs` — `HedgeState` stores both Phase A and Phase B estimates

**Changes to `compute_expected_repricing()`**:

The function signature stays the same, but the first parameter changes semantics:

```rust
// Before: atr_ratio (observed spot displacement in ATR multiples)
// After:  signal_strength (composite buildup score [0,1] at Phase A,
//         or max(observed_norm, composite) at Phase B)
pub fn compute_expected_repricing(
    signal_strength: Decimal,  // renamed from atr_ratio
    min_strength: Decimal,     // renamed from min_atr_ratio
    strong_strength: Decimal,  // renamed from strong_atr_ratio
    yes_mid: Decimal,
    spike_direction: Direction,
    time_remaining_secs: u64,
    scale: Decimal,
    time_exponent: f64,
    max_time_factor: f64,
) -> Decimal { /* unchanged formula */ }
```

**Phase A (entry decision)** — in `Leg1Evaluator::evaluate()`:
```rust
let predicted_norm = buildup.composite_score;
let estimated_pct = compute_expected_repricing(
    predicted_norm,           // composite score as signal strength
    min_spike_atr_ratio,      // now reinterpreted as min signal strength
    strong_spike_atr_ratio,   // now reinterpreted as strong signal strength
    yes_mid,
    buildup.direction,
    time_remaining_secs,
    reprice_scale,
    time_exponent,
    max_time_factor,
);
```

**Phase B (Leg 2 targeting)** — in `init_leg2()`:
```rust
// After Leg 1 fills, compute observed displacement
let observed_displacement = (current_spot_mid - buildup.spot_mid_at_entry).abs();
let observed_atr_ratio = observed_displacement / buildup.ema_atr;
let observed_norm = clamp((observed_atr_ratio - min) / (strong - min), 0, 1);

// Blend: take the max (if spike exceeded prediction, use the larger)
let refined_norm = max(observed_norm, buildup.composite_score);

let refined_pct = compute_expected_repricing(
    refined_norm,
    min_spike_atr_ratio,
    strong_spike_atr_ratio,
    yes_mid,
    direction,
    time_remaining_secs,
    reprice_scale,
    time_exponent,
    max_time_factor,
);

// Phase 1 profit target from refined estimate
let phase1_target = round_to_tick(refined_pct * phase1_target_dampen, tick);
```

**`HedgeState` additions** (`erosion.rs`):
```rust
pub phase_a_pct: Decimal,     // predicted repricing at entry (composite-based)
pub phase_b_pct: Decimal,     // refined repricing after fill (observed or composite, whichever higher)
pub spot_mid_at_entry: Decimal, // spot mid when buildup triggered (for Phase B displacement calc)
pub entry_ema_atr: Decimal,   // EMA ATR at entry (for Phase B displacement normalization)
```

**`BuildupInfo` fields used**:
- `composite_score` → Phase A `norm_spike` replacement
- `spot_mid_at_entry` → stored for Phase B computation
- `ema_atr` → stored for Phase B normalization

**Acceptance Criteria**:
- Phase A produces reasonable repricing estimates from composite scores
- Phase B refines upward when observed spike exceeds prediction
- Phase B floor = composite score (never targets below buildup-indicated level)
- `init_leg2()` correctly computes and stores both Phase A and Phase B
- Existing repricing tests adapted (signal_strength instead of atr_ratio)

---

### Phase 8: Leg 1 Execution Change (FAK → Maker)

**Goal**: Change Leg 1 from batch FAK taker to maker post-only. Add flow-based sustain (cancel if composite drops). Update fee model.

**Files Modified**:
- `src/executor/live.rs` — `handle_leg1()` → single maker post-only (replace batch FAK)
- `src/engine/strategy.rs` — flow-based sustain logic, Leg 1 cancel handling, fee model
- `src/engine/evaluator.rs` — `Leg1Evaluator` signal building: price = best_ask (maker), fee = rebate
- `src/types/order.rs` — `TradeSignal.leg1_taker_fee` renamed/resemanticized

**Executor Changes** (`live.rs` — `handle_leg1()`):

```rust
// OLD: Build 3 FAK orders at [ask-tick, ask, ask+tick], batch POST, poll fills
// NEW: Single post-only GTC order at best_ask

async fn handle_leg1(&mut self, signal: &TradeSignal) {
    self.active_leg2_phase1_id = None;
    self.active_leg2_phase2_id = None;

    let order = OrderRequest::post_only_gtc(
        &signal.token_id,
        signal.side,
        signal.price,  // evaluator sets price = best_ask
        signal.size,
    );

    match self.poly.place_order(&order).await {
        Ok(resp) => {
            if resp.status == OrderStatus::Rejected {
                // Post-only rejected — ask crossed our bid. Abort.
                warn!("Leg 1 maker REJECTED — aborting");
                self.send_feedback(OrderFailed { is_leg2: false });
                return;
            }
            // Order resting on book — waiting for fill via User WS
            self.send_feedback(OrderPosted {
                is_leg2: false,
                order_id: resp.order_id,
                price: signal.price,
                size: signal.size,
                fill_method: None,
                already_filled: resp.status == OrderStatus::Filled,
                order_tag: None,
            });
        }
        Err(e) => {
            error!("Leg 1 maker placement FAILED: {e}");
            self.send_feedback(OrderFailed { is_leg2: false });
        }
    }
}
```

**Executor: New `CancelLeg1Order` Handler**:
```rust
ExecutorCommand::CancelLeg1Order { order_id } => {
    match self.poly.cancel_order(&order_id).await {
        Ok(was_cancelled) => {
            self.send_feedback(CancelResult {
                order_id,
                was_cancelled,
                is_leg2: false,
            });
        }
        Err(e) => {
            warn!("Leg 1 cancel failed: {e}");
            self.send_feedback(CancelResult {
                order_id,
                was_cancelled: false,
                is_leg2: false,
            });
        }
    }
}
```

**Engine: Flow-Based Sustain** (`strategy.rs`):

New logic in the main loop, runs when `leg1_state == Posted`:

```rust
// After processing events, if Leg 1 is Posted (maker resting):
if matches!(self.state.leg1_state, OrderState::Posted { .. }) && self.leg1_direction.is_some() {
    // Check if composite has dropped below cancel threshold
    if self.state.current_composite_score < cancel_threshold_decimal {
        let elapsed = now_ms - leg1_post_ms;
        if elapsed < cancel_window_ms {
            // Flow faded — cancel unfilled maker
            warn!("flow-based sustain: composite below cancel threshold — cancelling Leg 1");
            let order_id = match &self.state.leg1_state {
                OrderState::Posted { order_id, .. } => order_id.clone(),
                _ => unreachable!(),
            };
            // Send cancel command to executor
            // ... emit CancelLeg1Order
            self.state.leg1_state = OrderState::None;
            self.leg1_direction = None;
            self.pending_leg1_signal = None;
            self.diag_sustain_cancels += 1;
        }
    }
    // Also cancel if cancel_window_ms exceeded without fill
    if elapsed >= cancel_window_ms {
        // Timeout — cancel regardless
        // ... same cancel logic
        self.diag_sustain_timeouts += 1;
    }
}
```

**Fee Model Change**:

```rust
// In TradeSignal:
pub leg1_fee: Decimal,  // Renamed from leg1_taker_fee
// For maker Leg 1: leg1_fee = -compute_maker_rebate(price, size) (negative = rebate)
// For FAK backstop: leg1_fee = compute_taker_fee_per_share(price) (positive = fee)

// In HedgeState::break_even():
pub fn break_even(&self) -> Decimal {
    Decimal::ONE - self.leg1_fill_price - self.leg1_fee
    // If leg1_fee is negative (maker rebate), breakeven improves:
    // e.g., 1.0 - 0.48 - (-0.001) = 0.521
}
```

**Evaluator Changes** (`evaluator.rs`):
```rust
// In Leg1Evaluator::evaluate():
// Price: best_ask (maker posts at ask, fills when someone takes)
signal.price = ask_price;
signal.size = (alloc / ask_price).round_dp(2);
signal.leg1_fee = -SimFill::compute_maker_rebate(ask_price, signal.size);
// Negative fee = maker rebate
```

**New Config Params**:
```toml
[buildup]
cancel_threshold = 0.25      # Cancel unfilled maker below this composite score
cancel_window_ms = 500        # Max time to wait for maker fill before cancelling
```

**Acceptance Criteria**:
- Leg 1 posts as maker at best ask
- Flow-based sustain cancels unfilled maker when composite drops
- Cancel timeout works (max wait)
- Fee model correct: maker = rebate (negative fee), taker = fee (positive)
- Breakeven computation correct with new fee semantics
- `CancelLeg1Order` executor command works end-to-end
- User WS fill detection still works (order hash matching unchanged)

---

### Phase 9: Leg 2 Pre-emptive + Flow-Based Hedge

**Goal**: Post Leg 2 immediately on Leg 1 fill at the refined target. Replace time-based phase transitions with flow-based graduated response.

**Files Modified**:
- `src/engine/strategy.rs` — `init_leg2()` triggers immediate Leg 2 signal, flow monitoring
- `src/engine/evaluator.rs` — `Leg2Evaluator` graduated response based on composite score
- `src/engine/erosion.rs` — `HedgeState` new fields for flow monitoring
- `src/executor/live.rs` — no major changes (existing `handle_leg2_hedge` handles maker post)

**Immediate Leg 2 Placement** (`strategy.rs` — `init_leg2()`):

```rust
fn init_leg2(&mut self, leg1_price: Decimal, leg1_size: Decimal, now_ms: u64) {
    // ... existing Phase B repricing computation ...

    // Build HedgeState with flow monitoring fields
    self.hedge = Some(HedgeState::new(
        now_ms,
        leg1_price,
        leg1_fee,
        tier,
        phase1_target,
        direction,
        buildup_info,
        refined_pct,
        phase1_target_price,
    ));

    // IMMEDIATELY emit Leg 2 signal at Phase 1 target
    // (pre-emptive: book hasn't fully repriced yet)
    let signal = make_leg2_signal(/* ... Phase 1 target price ... */);
    self.state.leg2_state = OrderState::Posted {
        order_id: format!("leg2-hedge-{}", now_ms),
        price: phase1_target_price,
        size: leg1_size,
        timestamp_ms: now_ms,
    };
    // Signal returned to caller for dispatch to executor
}
```

**Flow-Based Graduated Response** (`evaluator.rs` — `evaluate_leg2()`):

Replace the time-based Phase 1 timeout → Phase 2 transition with:

```
Flow State (composite score)     Action
─────────────────────────────    ──────────────────────────────────
Strong (>= entry_threshold)      Hold — wait for fill at profit target
Weakening (cancel..entry)        Tighten — transition to Phase 2 (ask-1tick) alongside Phase 1
Below cancel (<cancel_threshold) Immediate — FOK taker at ask (emergency)
Direction flipped                Emergency — FOK taker at ask (whipsaw, same as current)
```

The time-based Phase 1/Phase 2 timeouts **remain as backstops** in case flow data is stale or unavailable. But the flow-based triggers fire **faster** (50-200ms vs 2000ms timeout).

```rust
// In Leg2Evaluator::evaluate_leg2():

// 1. Check flow-based triggers first (faster)
let score = state.current_composite_score;
let score_direction = state.current_composite_direction;

// Flow reversal → immediate emergency
if score_direction.is_some() && score_direction != Some(snap.direction) && score > cancel_threshold {
    return Some(Leg2Decision::Emergency { /* WhipsawReversal */ });
}

// Flow collapsed → emergency
if score < cancel_threshold && snap.phase == HedgePhase::Phase1 {
    return Some(Leg2Decision::Emergency { /* FlowCollapse */ });
}

// Flow weakening → tighten (Phase 2 alongside)
if score < entry_threshold && snap.phase == HedgePhase::Phase1 {
    // Transition to Phase 2 (post at ask-1tick alongside Phase 1)
    return Some(Leg2Decision::Phase2Alongside { /* ask-1tick */ });
}

// 2. Time-based backstops (existing logic, unchanged)
// Phase 1 timeout → Phase 2
// Phase 2 timeout → FOK
// Phase 1 breach → FOK
// Phase 2 breach → FOK
```

**New `ExitReason` Variant**:
```rust
pub enum ExitReason {
    // ... existing variants ...
    FlowCollapse,  // NEW: composite score dropped below cancel threshold
}
```

**`HedgeState` Additions** (`erosion.rs`):
```rust
pub flow_monitoring_active: bool,  // true while composite data is fresh
pub last_flow_score: Decimal,      // latest composite score
pub last_flow_direction: Option<Direction>,
pub last_flow_update_ms: u64,
```

**Acceptance Criteria**:
- Leg 2 posted immediately on Leg 1 fill (no delay)
- Graduated response: strong → hold, weakening → tighten, collapse → emergency
- Time-based backstops still work when flow data is stale
- Whipsaw detection faster via flow signals
- Existing Phase 1 breach / Phase 2 breach / expiry exits still work
- Dual-order (Phase 1 + Phase 2 alongside) still works

---

### Phase 10: Edge Case Rework

**Goal**: Verify and update every edge case for the new entry system.

**Files Modified**:
- `src/engine/strategy.rs` — edge case adjustments
- `src/executor/live.rs` — Leg 1 cancel handling

See [Section 6: Edge Case Rework Matrix](#6-edge-case-rework-matrix) for the complete matrix.

**Key Edge Case Changes**:

| Edge Case | Old Behavior | New Behavior |
|-----------|-------------|--------------|
| **Whipsaw (Leg 1 Posted)** | Reset `leg1_state = None` (FAK orders auto-cancelled) | Send `CancelLeg1Order` to executor (maker order must be explicitly cancelled) |
| **Whipsaw (Leg 1 Filled)** | ATR-based opposite spike → FOK | Flow reversal detection → FOK (faster, 200ms earlier) + ATR backstop |
| **Rotation emergency** | Build FOK from opposing ask | Same, but signal uses `BuildupInfo` instead of `SpikeInfo` |
| **Leg 1 cancel-not-confirmed** | N/A (FAK is instant) | **NEW**: Executor returns `CancelResult { was_cancelled: false }` → engine keeps `leg1_state = Posted`, waits for User WS |
| **Sustain cancel** | N/A | **NEW**: Flow fades → cancel unfilled maker → `leg1_state = None` |
| **Double-fill rebalance** | Phase 1 + Phase 2 both fill → FOK rebalance | **Same** — dual-order race window still exists |
| **Stale flow data** | N/A | Fall back to time-based Phase 1/2 transitions (flow monitoring deactivated) |
| **Partial fill on Leg 1 maker** | N/A (FAK fills atomically) | **NEW**: User WS `size_matched < original_size` → partial fill tracking. Leg 2 sized to actual fill. |

**Acceptance Criteria**:
- Every edge case in the matrix is verified
- No new edge cases introduced without handling
- All emergency exits reachable
- `cargo build` passes

---

### Phase 11: Dead Code Removal

**Goal**: Remove all code made dead by the architectural change.

See [Section 7: Dead Code Removal Manifest](#7-dead-code-removal-manifest) for the complete list.

**Files Modified**: All affected source files

**Acceptance Criteria**:
- All items in the manifest removed
- `cargo build` passes with no warnings about dead code (except intentional `#[allow(dead_code)]`)
- No unused imports

---

### Phase 12: Config & Tests

**Goal**: Add new config parameters, update the config editor allowlist, and update all tests.

**Files Modified**:
- `src/config.rs` — new `BuildupConfig` section
- `config.toml` — new `[buildup]` section with defaults
- `src/control/config_editor.rs` — update `ALLOWED_PARAMS` with new buildup params
- `src/engine/buildup/metrics.rs` — unit tests for all 6 metrics
- `src/engine/buildup/detector.rs` — unit tests for composite, consensus, thresholds
- All existing test files — update to use `BuildupInfo` instead of `SpikeInfo`

**New Config Section**:
```toml
[buildup]
# Entry threshold — composite must exceed to trigger Leg 1 entry
entry_threshold = 0.40
# Cancel threshold — cancel unfilled Leg 1 if composite drops below
cancel_threshold = 0.25
# Max wait (ms) for Leg 1 maker fill before cancelling
cancel_window_ms = 500

# Metric weights (must sum to 1.0)
w_cvd = 0.30
w_spot_flow = 0.15
w_obi = 0.20
w_basis = 0.20
w_liq = 0.05
w_atr = 0.10

# Freshness gates (ms) — metric treated as stale after this
freshness_cvd_ms = 300
freshness_spot_flow_ms = 200
freshness_obi_ms = 100
freshness_basis_ms = 300
freshness_liq_ms = 3000
freshness_atr_ms = 100

# Normalization: [min_threshold, saturation] for each metric
# Below min → 0, above saturation → 1
cvd_min = 0.0
cvd_saturation = 1.0
spot_flow_min = 0.0
spot_flow_saturation = 1.0
obi_min = 0.0
obi_saturation = 0.5
basis_min = 0.0
basis_saturation = 2.0
liq_min = 0.0
liq_saturation = 10.0
atr_min = 0.0
atr_saturation = 15.0

# EMA alphas for metrics
cvd_fast_alpha = 0.2
cvd_slow_alpha = 0.05
spot_flow_alpha = 0.1
obi_velocity_alpha = 0.1
basis_alpha = 0.1
```

**Acceptance Criteria**:
- All new config params load correctly with defaults
- Config editor validates new params (min/max ranges)
- All existing tests updated and passing (sim-specific tests already removed in Phase 0)
- New unit tests for all 6 metrics + composite detector
- `cargo test` — all tests pass (target: ~130+ tests, down from 148 due to sim test removal)

---

### Phase 13: Documentation Update

**Goal**: Update all documentation to reflect the architectural changes.

**Files Modified**:
- `CLAUDE.md` — update source file descriptions, conventions, key patterns
- `ARCHITECTURE.md` — update system design, trade lifecycle, data sources, state machines
- `TRADING_LOGIC.md` — update signal detection, entry guards, hedge system, edge cases
- `MEMORY.md` — update project memory with new architecture

**Key Documentation Changes**:
- Signal detection pipeline: SpikeDetector → BuildupDetector
- Entry model: reactive FAK → predictive maker
- Repricing model: single-phase → two-phase (A/B)
- Hedge system: time-based → flow-based graduated response + time backstops
- Fee model: Leg 1 taker → Leg 1 maker
- New data sources: futures WS connection
- New edge cases: sustain cancel, Leg 1 cancel-not-confirmed, partial maker fill
- Simulation mode removed: all references to `Mode::Simulation`, `SimulationExecutor`, `advance_simulation()`, sim types, sim code paths purged from docs
- Source file tree updated: `types/simulation.rs` and `executor/simulation.rs` deleted, `fill_engine.rs` gains relocated fee functions

**Acceptance Criteria**:
- All docs accurately reflect the new system
- No stale references to removed features
- Cross-references between docs are consistent

---

### Phase 14: Architecture Diagram (draw.io)

**Goal**: Create a comprehensive draw.io diagram illustrating the complete bot operation flow.

**File Created**: `architecture.drawio`

**Diagram Sections**:

1. **Data Sources Layer**
   - Spot SBE: `@depth20` (50ms), `@bestBidAsk` (real-time), `@trade` (per-trade)
   - Futures JSON WS: `@aggTrade`, `@bookTicker`, `@forceOrder`
   - Polymarket Market WS: book, price_change, best_bid_ask, tick_size_change
   - Polymarket User WS: order events (fill detection)
   - Gamma API: market discovery (REST)

2. **Buildup Detection Pipeline**
   - 6 metrics: CVD Accel → Spot Flow → OBI Velocity → Basis Delta → Liq Pressure → ATR Displacement
   - Normalization → Direction Consensus → Causal Ordering → Weighted Composite
   - Entry threshold gate → BuildupConfirmed event

3. **Leg 1 Entry Flow**
   - BuildupConfirmed → evaluate() guards → Maker post-only at ask
   - Flow-based sustain: composite monitoring → cancel if below threshold
   - Fill detection: User WS MATCHED → init_leg2()
   - ATR backstop: SpikeConfirmed fallback for spot-only spikes

4. **Two-Phase Repricing**
   - Phase A: composite score → estimated_pct → entry gate + allocation
   - Phase B: observed displacement → refined_pct → Leg 2 target

5. **Leg 2 Hedge Flow**
   - Immediate post at Phase 1 target (pre-emptive)
   - Flow monitoring: strong (hold) → weakening (Phase 2 alongside) → collapse (FOK) → reversal (FOK)
   - Time backstops: Phase 1 timeout → Phase 2 timeout → FOK
   - Breach checks: Phase 1 pair cost → Phase 2 ask exceeded

6. **Emergency Exit Paths**
   - Phase 1 breach → FOK
   - Phase 2 breach → FOK
   - Phase 2 timeout → FOK
   - Flow collapse → FOK
   - Whipsaw (flow reversal) → FOK
   - Market expiry → FOK
   - All FOK: price escalation (+1 tick per attempt, $1 cap)

7. **Edge Cases**
   - Cancel-not-confirmed (Leg 1 + Leg 2)
   - Double-fill rebalance
   - Partial fills
   - Balance exhaustion
   - Rotation emergency

8. **State Machine Diagrams**
   - Leg 1: `None → Posted → Filled → None` (with cancel branch)
   - Leg 2: `None → Posted → Filled → None` (with emergency branch)
   - Hedge: `Phase1 → Phase2 → Emergency → Complete`
   - Market: `Marketless → Active → Cutoff → Rotation`

**Agent Behavior**:
- Architecture Agent creates initial diagram after Phase 1
- Updates after each phase completes
- Cross-references diagram against actual code
- Final verification pass after Phase 13

---

## 6. Edge Case Rework Matrix

| # | Edge Case | Current Trigger | Current Response | New Trigger | New Response | Change Type |
|---|-----------|----------------|-----------------|-------------|-------------|-------------|
| 1 | **Whipsaw — Leg 1 Posted** | Opposite `SpikeConfirmed` | Reset `leg1_state = None` (FAK auto-cancelled) | Opposite `BuildupConfirmed` OR flow reversal | Send `CancelLeg1Order` to executor, await `CancelResult`, then reset. If cancel-not-confirmed → keep Posted, wait for User WS | **Modified** |
| 2 | **Whipsaw — Leg 1 Filled** | Opposite `SpikeConfirmed` | `whipsaw_fok_pending` → FOK | Flow reversal (composite flips direction, score > cancel_threshold) OR opposite `BuildupConfirmed` | Same FOK response, but triggered 200ms earlier via flow signals. ATR-based backstop retained | **Modified** |
| 3 | **Phase 1 Breach** | `pair_cost > phase1_breach_threshold` | Immediate FOK at ask | Same trigger | Same response. Flow monitoring may trigger earlier (FlowCollapse) | **Unchanged** |
| 4 | **Phase 2 Timeout** | `elapsed > phase2_timeout_ms` | FOK at ask | Same trigger (time backstop) | Same response. Flow monitoring may trigger earlier | **Unchanged** |
| 5 | **Phase 2 Price Breach** | `ask > phase2_posted_price` | FOK at ask | Same trigger | Same response | **Unchanged** |
| 6 | **Phase 2 Entry Guard** | `ask - tick > breakeven` at Phase 2 entry | FOK immediately (skip Phase 2 post) | Same trigger | Same response | **Unchanged** |
| 7 | **Market Expiry** | `time_remaining < cutoff` with open position | Emergency FOK buffer before rotation | Same trigger | Same response | **Unchanged** |
| 8 | **Rotation Emergency** | Leg 1 Filled + Leg 2 incomplete at rotation | Build FOK from opposing ask | Same trigger | Same response, but uses `BuildupInfo` fields instead of `SpikeInfo` | **Minor** |
| 9 | **Double-Fill Race** | Both Phase 1 + Phase 2 orders fill (~3ms window) | Detect orphan → cancel → if fill → rebalance FOK | Same trigger | Same response — dual-order race window unchanged | **Unchanged** |
| 10 | **Cancel-Not-Confirmed (Leg 2)** | `cancel_order()` returns `Ok(false)` | Restore `leg2_state = Posted` from `prev_leg2_order` | Same trigger | Same response | **Unchanged** |
| 11 | **Cancel-Not-Confirmed (Leg 1)** | N/A (FAK is instant fill-or-cancel) | N/A | `CancelLeg1Order` returns `CancelResult { was_cancelled: false }` | Keep `leg1_state = Posted`, wait for User WS MATCHED or FAILED/CANCELED | **New** |
| 12 | **Sustain Cancel** | N/A | N/A | Composite drops below `cancel_threshold` while Leg 1 is Posted | Send `CancelLeg1Order` → on confirm: `leg1_state = None` → on not-confirmed: keep Posted | **New** |
| 13 | **Sustain Timeout** | N/A | N/A | Leg 1 Posted for > `cancel_window_ms` without fill | Same as sustain cancel — send `CancelLeg1Order` | **New** |
| 14 | **Partial Fill (Leg 1 Maker)** | N/A (FAK fills atomically per order) | N/A | User WS MATCHED with `size_matched < original_size` | Track partial fill size. Leg 2 sized to actual `size_matched`. Deferred alert to MINED. | **New** |
| 15 | **Maker Rejected (Leg 1)** | N/A | N/A | `place_order()` returns `Rejected` for post-only | `OrderFailed` → `leg1_state = None`. Spike consumed, no retry on same buildup signal. | **New** |
| 16 | **Flow Data Stale** | N/A | N/A | Composite score freshness expired (all metrics stale) | Fall back to time-based Phase 1/Phase 2 transitions. `flow_monitoring_active = false` on HedgeState. | **New** |
| 17 | **Balance Exhaustion** | "balance"/"allowance" on Leg 2 | Set `balance_exhausted = true`, block Leg 2 until rotation | Same trigger | Same response | **Unchanged** |
| 18 | **Stale Book** | `book.timestamp_ms > stale_book_ms` | Reject Leg 1 entry | Same trigger | Same response | **Unchanged** |
| 19 | **Hard Skew Cap** | YES mid beyond `hard_skew_cap` | Reject Leg 1 entry | Same trigger | Same response | **Unchanged** |
| 20 | **ATR Backstop** | N/A (primary detection) | N/A | `SpikeConfirmed` fires without recent `BuildupConfirmed` | Treat as current reactive entry (FAK batch). Guard: only if no BuildupConfirmed in last 500ms. Provides fallback for spot-only spikes. | **New** |
| 21 | **FlowCollapse** | N/A | N/A | Composite drops below cancel_threshold while Leg 1 is Filled (Leg 2 in Phase 1) | Immediate FOK taker at ask. New `ExitReason::FlowCollapse`. | **New** |

---

## 7. Dead Code Removal Manifest

Items to remove. **Phase 0 items** are removed first (simulation mode). **Phase 11 items** are removed after the buildup system is in place.

### Phase 0 Removals (Simulation Mode) — Done First

#### Files to Delete
| File | Lines | Reason |
|------|-------|--------|
| `src/types/simulation.rs` | 945 | All 7 sim types: `SimFill`, `SimPosition`, `SimTrade`, `MarketSummary`, `SessionSummary`, `SimulationState`, `PositionStatus` |
| `src/executor/simulation.rs` | 1,168 | `SimulationExecutor`, simulated fills, sim Telegram reporting |

#### Code Blocks to Remove
| Location | Lines (approx) | What |
|----------|----------------|------|
| `src/engine/strategy.rs` | ~235 | `advance_simulation()` method |
| `src/engine/strategy.rs` | scattered | `SimulationState` field, `is_sim()` checks, sim-specific conditionals |
| `src/main.rs` | ~30 | `Mode::Simulation` executor spawn branch, `advance_simulation()` call in engine loop |
| `src/gateway/polymarket/user_ws.rs` | ~5 | Sim-mode `std::future::pending()` parking |
| `src/gateway/polymarket/heartbeat.rs` | ~5 | Sim-mode parking |
| `src/reporting/telegram.rs` | ~50-100 | Sim-specific formatters using `SimTrade`/`MarketSummary`/`SessionSummary` |
| `src/storage/cold.rs` | ~30-50 | `record_simulated_trade()` |

#### Types/Fields to Remove
| Item | Location | Reason |
|------|----------|--------|
| `Mode::Simulation` variant | `src/config.rs` | Mode enum removed entirely (always live) |
| `sim_confirmed_fill: bool` | `TradeSignal` in `types/order.rs` | Sim-only field |
| `sim_was_taker: bool` | `TradeSignal` in `types/order.rs` | Sim-only field |
| `pub mod simulation` | `types/mod.rs`, `executor/mod.rs` | Module declarations |

#### Functions to Relocate (NOT delete)
| Function | From | To | Reason |
|----------|------|----|--------|
| `SimFill::compute_taker_fee()` | `types/simulation.rs` | `executor/fill_engine.rs` | Used by live `build_live_sim_trade()` |
| `SimFill::compute_taker_fee_per_share()` | `types/simulation.rs` | `executor/fill_engine.rs` | Used by live evaluator (Leg 1 fee) |
| `SimFill::compute_maker_rebate()` | `types/simulation.rs` | `executor/fill_engine.rs` | Used by live PnL computation |

### Phase 11 Removals (Dead Spike Detection Code)

#### Files to Delete
| File | Reason |
|------|--------|
| `src/gateway/binance/spike.rs` | Replaced by `engine/buildup/` — ATR backstop logic moves into `BuildupDetector.atr_displacement` tracker |

#### Structs/Enums to Remove
| Item | File | Reason |
|------|------|--------|
| `SpikeEvent` enum | `spike.rs` | Replaced by `BuildupDetector::check_entry()` return |
| `SpikeDiagSnapshot` struct | `spike.rs` | Replaced by `BuildupDiagSnapshot` |
| `SpikeDetector` struct | `spike.rs` | Replaced by `BuildupDetector` |

#### Fields to Remove
| Field | Struct | File | Reason |
|-------|--------|------|--------|
| `spike_detected: bool` | `MarketState` | `types/market.rs` | Replaced by `buildup_detected` |
| `last_spike: Option<SpikeInfo>` | `MarketState` | `types/market.rs` | Replaced by `last_buildup: Option<BuildupInfo>` |
| `atr: Option<Decimal>` | `MarketState` | `types/market.rs` | ATR now internal to `AtrDisplacementTracker` |
| `sustained_ms: u64` | `SpikeInfo` | `types/market.rs` | Always 0 (sustain removed previously), struct may be deprecated entirely or kept minimal for ATR backstop |

#### Config Params to Remove/Rename
| Param | Reason |
|-------|--------|
| `[spike_detection].multiplier` | Replaced by buildup entry_threshold. ATR backstop uses internal threshold |
| `[spike_detection].atr_alpha` | Moved to `[buildup].atr_alpha` (still needed for ATR displacement metric) |
| `[spike_detection].min_magnitude_pct` | Replaced by `[buildup].atr_min` normalization threshold |
| `fak_price_offset_ticks` | Leg 1 no longer uses FAK batch |

#### Executor Code to Remove
| Code | File | Reason |
|------|------|--------|
| Batch FAK logic in `handle_leg1()` | `live.rs` | Replaced by single maker post |
| `fak_price_offset_ticks` field | `LiveExecutor` | FAK batch removed |
| Sequential `get_order_status()` polling after batch | `live.rs` | No batch, no polling needed |
| VWAP computation | `live.rs` | Single order, no VWAP |

#### IngestorEvent Variants to Remove
| Variant | Reason |
|---------|--------|
| `SpikeConfirmed(SpikeInfo)` | Kept temporarily as ATR backstop; can be removed once `BuildupConfirmed` is proven reliable |
| `SpikeDiagnostic { ... }` | Replaced by `BuildupDiagnostic` |

#### Diagnostic Counters to Remove/Rename
| Counter | Reason |
|---------|--------|
| `diag_spike_failures` | No more spike failures concept |
| Rename `diag_spikes_received` → `diag_buildups_received` | Terminology update |

#### Add New Diagnostic Counters
| Counter | Purpose |
|---------|---------|
| `diag_buildups_received` | BuildupConfirmed events received |
| `diag_sustain_cancels` | Leg 1 makers cancelled due to flow fade |
| `diag_sustain_timeouts` | Leg 1 makers cancelled due to cancel_window_ms timeout |
| `diag_flow_collapses` | Leg 2 emergency exits triggered by flow collapse |
| `diag_direction_vetoes` | Composite vetoed by direction consensus |
| `diag_causal_vetoes` | Composite vetoed by causal ordering (no leading + confirming) |
| `diag_atr_backstop_entries` | Entries via ATR backstop (no buildup signal) |

---

## 8. Risk & Rollback

### Risk Assessment

| Risk | Likelihood | Impact | Mitigation |
|------|-----------|--------|------------|
| Futures WS latency adds noise | Medium | Low | Freshness gates + causal ordering. Stale futures data → excluded from composite |
| Maker Leg 1 never fills (adverse selection) | Medium | Medium | Flow-based sustain cancels quickly. Size scaling by confidence limits exposure |
| False positive entries (composite triggers on noise) | Medium | Medium | 5-layer defense: direction consensus, causal ordering, entry threshold, flow-based cancel, existing guards |
| Futures feed disconnects | Low | Medium | `WsStatus` tracking. Fall back to time-based hedging. ATR backstop for entries |
| Composite score tuning wrong | High | Medium | Configurable weights + thresholds. Can be adjusted via `/set` without restart |
| Breaking change to existing working features | Low | High | Each phase has compile + test gate. Unchanged features listed explicitly |

### Rollback Strategy

Each phase is committed separately. To rollback:
1. `git revert` the phase's commit(s)
2. All phases after the reverted one must also be reverted (dependency chain)
3. The ATR backstop (retained throughout) provides a degraded-but-functional fallback
4. **Phase 0 (sim removal) is irreversible by design** — there is no going back to simulation mode. If needed, `git revert` restores the sim files, but this is not expected

### Feature Flag (Optional)

If desired, the entire pre-spike system can be gated behind a config flag:
```toml
[buildup]
enabled = true  # false = use old reactive spike detection
```
When `enabled = false`:
- `BuildupDetector` still runs (collects data for monitoring) but doesn't emit `BuildupConfirmed`
- `SpikeConfirmed` from ATR backstop is the sole entry trigger (current behavior)
- Leg 1 still uses maker post-only (not FAK — that code is removed)

This allows A/B testing in production.
