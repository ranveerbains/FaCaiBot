//! Ingestor Gateways — Layer 1 ("The Ear").
//!
//! - [`binance`] — Binance WebSocket: depth + ticker streams, EMA-ATR spike detection.
//! - [`polymarket`] — Polymarket CLOB: Market WS, User WS, heartbeat, Gamma API rotation, REST gateway.

pub mod binance;
pub mod polymarket;
