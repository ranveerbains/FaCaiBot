//! Binance USDT-M Futures JSON WebSocket gateway.
//!
//! Connects to `wss://fstream.binance.com/stream` and subscribes to:
//! - `btcusdt@aggTrade`    — per-trade events for CVD computation
//! - `btcusdt@bookTicker`  — BBO updates for basis delta computation
//! - `btcusdt@forceOrder`  — liquidation events for cascade detection
//!
//! Uses standard JSON WebSocket (not SBE). Parses each frame and
//! emits typed `IngestorEvent` variants to the engine channel.
//!
//! # Thread model
//! Runs in the ingestor thread (single-threaded tokio runtime), alongside
//! the spot SBE gateway and Polymarket WS gateways.

use std::sync::Arc;
use std::time::Duration;

use anyhow::{Result, anyhow};
use crossbeam_channel::Sender;
use fastwebsockets::{OpCode, handshake};
use http_body_util::Empty;
use hyper::body::Bytes;
use hyper::header::{CONNECTION, UPGRADE};
use hyper::{Request, Uri};
use rust_decimal::Decimal;
use rustls::pki_types::ServerName;
use serde::Deserialize;
use tokio::net::TcpStream;
use tokio_rustls::TlsConnector;
use tracing::{debug, info, warn};

use crate::utils::tls::{SpawnExecutor, build_tls_config};

use crate::types::IngestorEvent;
use crate::types::market::{
    DataSource, FuturesAggTrade, FuturesBookTicker, FuturesForceOrder,
};

// ─── Constants ────────────────────────────────────────────────────────────────

/// Combined stream path for futures data.
const FUTURES_STREAM: &str =
    "/stream?streams=btcusdt@aggTrade/btcusdt@bookTicker/btcusdt@forceOrder";

/// Reconnection backoff: initial delay (ms).
const BACKOFF_INITIAL_MS: u64 = 1_000;

/// Reconnection backoff: maximum delay (ms).
const BACKOFF_MAX_MS: u64 = 30_000;

// ─── JSON message shapes ──────────────────────────────────────────────────────

/// Combined stream wrapper: `{ "stream": "btcusdt@aggTrade", "data": { ... } }`
#[derive(Debug, Deserialize)]
struct StreamWrapper {
    stream: String,
    data: serde_json::Value,
}

/// Binance Futures @aggTrade payload.
#[derive(Debug, Deserialize)]
struct AggTradePayload {
    /// Price
    p: String,
    /// Quantity
    q: String,
    /// Is the buyer the market maker?
    m: bool,
    /// Trade time (epoch ms)
    #[serde(rename = "T")]
    trade_time: u64,
}

/// Binance Futures @bookTicker payload.
#[derive(Debug, Deserialize)]
struct BookTickerPayload {
    /// Best bid price
    b: String,
    /// Best bid qty
    #[serde(rename = "B")]
    bid_qty: String,
    /// Best ask price
    a: String,
    /// Best ask qty
    #[serde(rename = "A")]
    ask_qty: String,
    /// Event time (epoch ms)
    #[serde(rename = "E")]
    event_time: u64,
}

/// Binance Futures @forceOrder wrapper.
#[derive(Debug, Deserialize)]
struct ForceOrderPayload {
    o: ForceOrderInner,
}

#[derive(Debug, Deserialize)]
struct ForceOrderInner {
    /// Side: "SELL" or "BUY"
    #[serde(rename = "S")]
    side: String,
    /// Price
    p: String,
    /// Original quantity
    q: String,
    /// Trade time
    #[serde(rename = "T")]
    trade_time: u64,
}

// ─── FuturesGateway ───────────────────────────────────────────────────────────

/// Binance USDT-M Futures WebSocket client.
pub struct FuturesGateway {
    ws_url: String,
}

impl FuturesGateway {
    pub fn new(ws_url: String) -> Self {
        Self { ws_url }
    }

    /// Run the gateway forever, reconnecting on failure with exponential backoff.
    pub async fn run(
        &self,
        tx: Sender<IngestorEvent>,
        stale_threshold_ms: u64,
    ) -> Result<()> {
        let mut backoff_ms = BACKOFF_INITIAL_MS;

        loop {
            match self.connect_and_stream(&tx, stale_threshold_ms).await {
                Ok(()) => {
                    info!("futures WS stream ended cleanly — reconnecting");
                    backoff_ms = BACKOFF_INITIAL_MS;
                }
                Err(e) => {
                    warn!(error = %e, backoff_ms, "futures WS error — reconnecting");
                }
            }

            // Emit disconnection status.
            let _ = tx.send(IngestorEvent::WsStatus {
                source: DataSource::BinanceFutures,
                connected: false,
            });

            tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
            backoff_ms = (backoff_ms * 2).min(BACKOFF_MAX_MS);
        }
    }

    /// Connect to the futures WS and stream messages until an error occurs.
    async fn connect_and_stream(
        &self,
        tx: &Sender<IngestorEvent>,
        stale_threshold_ms: u64,
    ) -> Result<()> {
        let uri: Uri = format!("{}{}", self.ws_url, FUTURES_STREAM).parse()?;
        let host = uri
            .host()
            .ok_or_else(|| anyhow!("no host in futures WS URL"))?
            .to_string();
        let port = uri.port_u16().unwrap_or(443);

        // TCP + TLS (same pattern as spot SBE gateway).
        let tcp = TcpStream::connect((&*host, port)).await?;
        tcp.set_nodelay(true)?;
        let tls_config = build_tls_config()?;
        let connector = TlsConnector::from(Arc::new(tls_config));
        let server_name = ServerName::try_from(host.as_str())
            .map_err(|e| anyhow!("invalid TLS server name: {e}"))?
            .to_owned();
        let tls_stream = connector.connect(server_name, tcp).await?;

        // WebSocket handshake (no auth needed — public streams).
        let path_and_query = uri.path_and_query().map(|pq| pq.as_str()).unwrap_or("/");
        let req = Request::builder()
            .method("GET")
            .uri(path_and_query)
            .header("Host", &host)
            .header(UPGRADE, "websocket")
            .header(CONNECTION, "upgrade")
            .header("Sec-WebSocket-Key", handshake::generate_key())
            .header("Sec-WebSocket-Version", "13")
            .body(Empty::<Bytes>::new())?;

        let (mut ws, _resp) = handshake::client(&SpawnExecutor, req, tls_stream).await?;

        ws.set_auto_pong(true);
        ws.set_auto_close(true);

        info!("Binance Futures WS connected");
        let _ = tx.send(IngestorEvent::WsStatus {
            source: DataSource::BinanceFutures,
            connected: true,
        });

        loop {
            let frame = ws.read_frame().await?;
            match frame.opcode {
                OpCode::Text => {
                    let payload = std::str::from_utf8(&frame.payload)
                        .map_err(|e| anyhow!("invalid UTF-8: {e}"))?;
                    self.handle_json_message(payload, tx, stale_threshold_ms);
                }
                OpCode::Binary => {
                    debug!("futures WS: unexpected binary frame (ignoring)");
                }
                OpCode::Close => {
                    info!("futures WS: server sent Close");
                    break;
                }
                _ => {}
            }
        }

        Ok(())
    }

    /// Parse a JSON message from the combined stream and emit events.
    fn handle_json_message(
        &self,
        payload: &str,
        tx: &Sender<IngestorEvent>,
        stale_threshold_ms: u64,
    ) {
        let wrapper: StreamWrapper = match serde_json::from_str(payload) {
            Ok(w) => w,
            Err(e) => {
                debug!(error = %e, "futures WS: failed to parse wrapper");
                return;
            }
        };

        let now_ms = crate::utils::time::epoch_ms();

        if wrapper.stream.ends_with("@aggTrade") {
            if let Ok(t) = serde_json::from_value::<AggTradePayload>(wrapper.data) {
                if now_ms.saturating_sub(t.trade_time) > stale_threshold_ms {
                    return; // stale
                }
                if let (Ok(price), Ok(qty)) = (t.p.parse::<Decimal>(), t.q.parse::<Decimal>()) {
                    let _ = tx.send(IngestorEvent::FuturesAggTrade(FuturesAggTrade {
                        price,
                        quantity: qty,
                        is_buyer_maker: t.m,
                        timestamp_ms: t.trade_time,
                    }));
                }
            }
        } else if wrapper.stream.ends_with("@bookTicker") {
            if let Ok(t) = serde_json::from_value::<BookTickerPayload>(wrapper.data) {
                if now_ms.saturating_sub(t.event_time) > stale_threshold_ms {
                    return; // stale
                }
                if let (Ok(bp), Ok(bq), Ok(ap), Ok(aq)) = (
                    t.b.parse::<Decimal>(),
                    t.bid_qty.parse::<Decimal>(),
                    t.a.parse::<Decimal>(),
                    t.ask_qty.parse::<Decimal>(),
                ) {
                    let _ = tx.send(IngestorEvent::FuturesBookTicker(FuturesBookTicker {
                        bid_price: bp,
                        bid_qty: bq,
                        ask_price: ap,
                        ask_qty: aq,
                        timestamp_ms: t.event_time,
                    }));
                }
            }
        } else if wrapper.stream.ends_with("@forceOrder") {
            if let Ok(t) = serde_json::from_value::<ForceOrderPayload>(wrapper.data) {
                if now_ms.saturating_sub(t.o.trade_time) > stale_threshold_ms {
                    return; // stale
                }
                if let (Ok(price), Ok(qty)) =
                    (t.o.p.parse::<Decimal>(), t.o.q.parse::<Decimal>())
                {
                    let _ = tx.send(IngestorEvent::FuturesForceOrder(FuturesForceOrder {
                        side: t.o.side,
                        price,
                        quantity: qty,
                        timestamp_ms: t.o.trade_time,
                    }));
                }
            }
        }
    }
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
#[path = "tests/futures_ws_tests.rs"]
mod tests;
