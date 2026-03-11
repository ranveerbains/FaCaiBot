//! Executor — Layer 3 ("The Hand").
//!
//! - [`live`] — `LiveExecutor`: places real orders on Polymarket CLOB, tracks fills via User WS.
//! - [`fill_engine`] — Utility helpers: fee computation, tick rounding.

pub mod fill_engine;
pub mod live;
