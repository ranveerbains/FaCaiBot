//! Binance gateway sub-modules.
//!
//! - [`ws`]         — Spot SBE WebSocket gateway (`BinanceGateway`)
//! - [`futures_ws`] — Futures JSON WebSocket gateway (`FuturesGateway`)

pub mod futures_ws;
pub mod ws;

pub use futures_ws::FuturesGateway;
pub use ws::BinanceGateway;
