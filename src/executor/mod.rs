//! Executor — Layer 3 ("The Hand").
//!
//! - [`simulation`] — `SimulationExecutor`: receives trade signals, simulates fills, reports via Telegram.
//! - [`live`] — `LiveExecutor`: places real orders on Polymarket CLOB, tracks fills via User WS.
//! - [`fill_engine`] — Utility helpers: `compute_fill_size`, `opposite_side`, `epoch_ms`.

pub mod fill_engine;
pub mod live;
pub mod simulation;
