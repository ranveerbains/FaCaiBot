//! Buildup detection — predictive pre-spike entry system.
//!
//! - [`metrics`] — 6 individual metric trackers (CVD, spot flow, OBI velocity, basis delta, liq pressure, ATR displacement)
//! - [`detector`] — `BuildupDetector`: composite score, direction consensus, causal ordering

pub mod detector;
pub mod metrics;
