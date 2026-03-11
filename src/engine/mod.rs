//! Strategy Engine — Layer 2 ("The Brain").
//!
//! - [`strategy`] — `StrategyEngine` orchestrator: event routing, state management.
//! - [`evaluator`] — `Leg1Evaluator` + `Leg2Evaluator`: pure guard-checking and signal-building logic.
//! - [`confidence`] — `compute_expected_repricing()` repricing model and `round_to_tick()` helper.
//! - [`erosion`] — `HedgeState` + `HedgeSnap` + `HedgePhase`: Leg 2 two-phase hedge state machine.

pub mod buildup;
pub mod confidence;
pub mod erosion;
pub mod evaluator;
pub mod strategy;
