//! Strategy Engine — Layer 2 ("The Brain").
//!
//! - [`strategy`] — `StrategyEngine` orchestrator: event routing, state management, simulation advances.
//! - [`evaluator`] — `Leg1Evaluator` + `Leg2Evaluator`: pure guard-checking and signal-building logic.
//! - [`confidence`] — `compute_confidence()` 4-factor scoring and `round_to_tick()` helper.
//! - [`erosion`] — `ErosionState` + `ErosionSnap`: Leg 2 profit-target erosion state machine.

pub mod confidence;
pub mod erosion;
pub mod evaluator;
pub mod strategy;
