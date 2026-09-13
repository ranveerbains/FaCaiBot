# Refactoring Roadmap

Planned refactoring items for the v2 codebase. Each item is independent and can be done in any order. None change external behavior — they improve maintainability, reduce duplication, and enforce invariants at the type level.

## 1. strategy.rs Decomposition (926 → ~400 lines)

**Motivation**: `strategy.rs` mixes event routing, quoting orchestration, and diagnostics/reporting. The diagnostics methods are self-contained and accessed only via `check_diagnostic()` and `build_status()`.

**Current state**: Lines 784-926 contain `check_diagnostic()`, `build_status()`, `send_market_report()`, `send_session_summary()`, `take_pending_telegram_diag()`, plus `diag_*` counters, `session_start_ms`, and `pending_telegram_diag` fields.

**Proposed design**: Extract to `src/engine/diagnostics.rs`:
- New `Diagnostics` struct owns all `diag_*` counters, `session_start_ms`, `pending_telegram_diag`
- Methods: `check()`, `build_status()`, `send_market_report()`, `send_session_summary()`, `take_pending_telegram_diag()`
- `V2StrategyEngine` holds a `Diagnostics` field; delegates calls
- ~143 lines extracted

**Affected files**: `engine/strategy.rs`, new `engine/diagnostics.rs`, `engine/mod.rs`

**Dependencies**: None

## 2. Config Consolidation

**Motivation**: Three runtime config structs (`FairValueConfig`, `QuotingConfig`, `RiskV2Config`) duplicate every field from the TOML-parsed `FairValueToml`, `QuotingToml`, `RiskV2Toml` structs. The constructors are 59 lines of field-by-field copying with `as f64` / `try_into()` conversions.

**Current state**: ~280 lines of boilerplate across `config.rs`, `fair_value.rs`, `quoter.rs`, `strategy.rs`.

**Proposed design**: Eliminate the runtime config structs. Use the TOML structs directly. Convert `Decimal` → `f64` at the usage site (one-liner: `config.field.to_f64().unwrap_or(default)`). The TOML structs already have `#[serde(default)]` and correct defaults.

**Affected files**: `config.rs`, `engine/fair_value.rs`, `engine/quoter.rs`, `engine/strategy.rs`

**Dependencies**: None, but touches 4 files — do in a dedicated session.

## 3. Quoter Side-Field Accessor Pattern

**Motivation**: `quoter.rs` has 10+ `match side { Yes => &self.yes_X, No => &self.no_X }` arms for per-side state (resting order, pending flags, last post time, etc.).

**Current state**: Each per-side field is a pair: `yes_resting`/`no_resting`, `yes_pending_post`/`no_pending_post`, etc. Every access requires a match arm.

**Proposed design**: Extract per-side state into a `SideState` struct:
```rust
struct SideState {
    resting: Option<ManagedOrder>,
    pending_post: bool,
    pending_cancel: bool,
    last_post_ms: u64,
    size_filled: Decimal,
}
```
Store `[SideState; 2]` indexed by `side as usize`. Collapses all match arms to `self.sides[side as usize]`.

**Affected files**: `engine/quoter.rs`, `engine/quoter/tests/`

**Dependencies**: None

## 4. ClosingManager State Encapsulation

**Motivation**: `ClosingManager` uses 4 `pub bool` fields (`cancelled_all`, `cancels_confirmed`, `fok_sent`, `fok_done`) to track closing phase progress. Invalid combinations (e.g., `fok_sent && !cancelled_all`) are possible but never checked.

**Current state**: Strategy.rs reads these booleans directly in `closing_tick()` with chained if-else.

**Proposed design**: Replace with a `ClosingPhase` enum:
```rust
enum ClosingPhase {
    CancellingAll,
    WaitingCancels,
    SendingFok,
    Complete,
}
```
Transition methods enforce valid state ordering. `closing_tick()` becomes a simple match on the enum.

**Affected files**: `engine/closing.rs`, `engine/strategy.rs`, `engine/closing/tests/`

**Dependencies**: None

## 5. Rename v1 Display Names

**Motivation**: `BotStatus` still uses `leg1_state` and `leg2_state` field names from v1. These display in Telegram `/status` as "Leg 1" / "Leg 2" which is confusing in the bilateral context.

**Current state**: `control/types.rs` defines `BotStatus { leg1_state, leg2_state }`. Set in `strategy.rs` `build_status()`. Formatted in `control/handlers.rs`.

**Proposed design**: Rename to `position_summary` and `pairing_summary`. Update Telegram formatting to show "Position" / "Pairing" instead of "Leg 1" / "Leg 2".

**Affected files**: `control/types.rs`, `engine/strategy.rs`, `control/handlers.rs`

**Dependencies**: None
