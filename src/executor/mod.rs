//! Executor — Layer 3 ("The Hand").
//!
//! - [`simulation`] — `SimulationExecutor`: receives trade signals, simulates fills, reports via Telegram.
//! - [`fill_engine`] — `FillSimulator`: depth-based fill probability model for Leg 1 and Leg 2.

pub mod fill_engine;
pub mod simulation;
