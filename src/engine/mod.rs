//! Strategy Engine — Layer 2 ("The Brain").
//!
//! v2: Bilateral accumulation strategy.
//! - [`strategy`] — `V2StrategyEngine` orchestrator: event routing, state management.
//! - [`fair_value`] — `FairValueEstimator`: Binance-derived YES/NO probability model.
//! - [`quoter`] — `Quoter`: per-side order management and quoting decisions.
//! - [`position`] — `BilateralPosition`: share tracking, pairing, PnL computation.
//! - [`closing`] — `ClosingManager`: end-of-market pairing logic.
//! - [`buildup`] — Metric trackers (reused by fair value model).

pub mod buildup;
pub mod closing;
pub mod fair_value;
pub mod position;
pub mod quoter;
pub mod strategy;
