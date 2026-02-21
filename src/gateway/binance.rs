use anyhow::Result;
use crossbeam_channel::Sender;
use rust_decimal::Decimal;
use tracing::{info, warn};

use crate::types::{BinanceTick, IngestorEvent};

/// Streams Binance spot WebSocket feeds for BTC/ETH.
pub struct BinanceGateway {
    ws_url: String,
}

/// Symbols we track on Binance for reference pricing.
const SYMBOLS: &[&str] = &["btcusdt", "ethusdt"];

impl BinanceGateway {
    pub fn new(ws_url: String) -> Self {
        Self { ws_url }
    }

    /// Subscribe to @depth@100ms and @ticker streams for all tracked symbols.
    /// Parsed ticks are pushed into the crossbeam channel toward the engine.
    pub async fn run(&self, tx: Sender<IngestorEvent>) -> Result<()> {
        // TODO: Use the `binance` crate WebSocket manager:
        //
        // let mut ws = BinanceWebsocket::new();
        // for symbol in SYMBOLS {
        //     ws.subscribe_depth(symbol, "100ms");
        //     ws.subscribe_ticker(symbol);
        // }
        // ws.event_loop(|event| {
        //     let tick = parse_tick(event);
        //     tx.send(IngestorEvent::BinanceTick(tick)).ok();
        // }).await?;
        //
        // Alternatively, for ultra-low latency, use fastwebsockets directly:
        //   Connect to: {ws_url}/stream?streams=btcusdt@depth@100ms/ethusdt@depth@100ms/...
        //   Parse JSON frames manually with simd_json or serde_json.

        info!(
            url = %self.ws_url,
            symbols = ?SYMBOLS,
            "starting Binance WebSocket streams"
        );
        let _ = tx;
        warn!("binance WS stream not yet implemented");
        Ok(())
    }
}

/// Parse a raw Binance ticker JSON payload into a BinanceTick.
/// Used when processing @ticker stream events.
pub fn parse_ticker_json(json: &str) -> Result<BinanceTick> {
    #[derive(serde::Deserialize)]
    struct RawTicker {
        #[serde(rename = "s")]
        symbol: String,
        #[serde(rename = "b")]
        bid_price: String,
        #[serde(rename = "B")]
        bid_qty: String,
        #[serde(rename = "a")]
        ask_price: String,
        #[serde(rename = "A")]
        ask_qty: String,
        #[serde(rename = "E")]
        event_time: u64,
    }

    let raw: RawTicker = serde_json::from_str(json)?;
    Ok(BinanceTick {
        symbol: raw.symbol,
        bid_price: raw.bid_price.parse::<Decimal>()?,
        bid_qty: raw.bid_qty.parse::<Decimal>()?,
        ask_price: raw.ask_price.parse::<Decimal>()?,
        ask_qty: raw.ask_qty.parse::<Decimal>()?,
        timestamp_ms: raw.event_time,
    })
}
