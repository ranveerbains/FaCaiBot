//! Binance gateway sub-modules.
//!
//! - [`spike`] — EMA-ATR spike detector (`SpikeDetector`)
//! - [`ws`]    — WebSocket gateway (`BinanceGateway`) and JSON parsing helpers

pub mod spike;
pub mod ws;

pub use ws::BinanceGateway;
