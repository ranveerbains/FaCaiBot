//! Binance WebSocket gateway — Layer 1 Ingestor ("The Ear").
//!
//! Maintains a persistent combined stream connection to Binance, parses
//! `@depth20@100ms` and `@ticker` events, runs inline spike detection with
//! EMA-ATR, and pushes [`IngestorEvent`]s to the Engine via a crossbeam channel.
//!
//! # Spike detection output
//! When a spike is confirmed (sustained + momentum-filtered), the gateway emits a
//! synthetic [`IngestorEvent::BinanceTick`] whose `timestamp_ms` equals the
//! spike origin timestamp. The tick carries the best bid/ask from the depth
//! snapshot at confirmation time. The Engine's `on_event` handler correlates
//! this spike-tagged tick with `MarketState.last_spike` via the ATR / spike
//! info computed in the gateway. A separate internal state struct (`PendingSpike`)
//! is pushed to the channel as a `BinanceTick` using the spike's origin
//! timestamp so the Engine can distinguish it from normal ticks.
//!
//! # Thread model
//! This module runs on a **dedicated OS thread** with its own single-threaded
//! tokio runtime (see `main.rs`). It must never touch the main multi-threaded
//! runtime.
//!
//! # Latency target
//! < 50 ms from Binance WS push to crossbeam channel emit (P99).

use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, anyhow};
use crossbeam_channel::Sender;
use fastwebsockets::{Frame, OpCode, WebSocket, handshake};
use http_body_util::Empty;
use hyper::body::Bytes;
use hyper::header::{CONNECTION, UPGRADE};
use hyper::upgrade::Upgraded;
use hyper::{Request, Uri};
use hyper_util::rt::TokioIo;
use rust_decimal::Decimal;
use rust_decimal::prelude::*;
use rustls::ClientConfig;
use rustls::pki_types::ServerName;
use tokio::net::TcpStream;
use tokio_rustls::TlsConnector;
use tracing::{debug, info, warn};

use crate::config::SpikeDetectionConfig;
use crate::types::IngestorEvent;
use crate::types::market::{BinanceDepth, BinanceTick, DataSource, PriceLevel, SpikeInfo};

use super::spike::SpikeDetector;

// ─── Constants ────────────────────────────────────────────────────────────────

/// Binance combined stream path suffix for BTC depth + ticker.
const BTCUSDT_STREAM: &str = "/stream?streams=btcusdt@depth20@100ms/btcusdt@ticker";

/// Reconnection backoff: initial delay (ms).
const BACKOFF_INITIAL_MS: u64 = 1_000;

/// Reconnection backoff: maximum delay (ms).
const BACKOFF_MAX_MS: u64 = 30_000;

// ─── Binance Gateway ─────────────────────────────────────────────────────────

/// Streams Binance spot WebSocket feeds for BTC/USDT.
///
/// Connects to the Binance combined stream, parses `@depth20@100ms` and
/// `@ticker` frames, runs inline spike detection, and emits
/// [`IngestorEvent::BinanceDepth`] and [`IngestorEvent::BinanceTick`] events to
/// the Engine layer.
///
/// # Spike output protocol
/// When a spike is confirmed, the gateway emits a synthetic `BinanceTick` whose
/// `timestamp_ms` equals the spike's origin timestamp (i.e. when the price first
/// exceeded the ATR threshold). The bid/ask on that tick are taken from the most
/// recent depth snapshot. The Engine correlates this tick's timestamp with
/// `MarketState.last_spike` to understand when the signal occurred. A separate
/// `SpikeInfo` is stored in `MarketState.last_spike` by the engine after it
/// processes the spike. This synthetic tick is emitted **before** the current
/// depth event so it arrives in temporal order.
pub struct BinanceGateway {
    ws_url: String,
    spike_config: SpikeDetectionConfig,
}

impl BinanceGateway {
    pub fn new(ws_url: String, spike_config: SpikeDetectionConfig) -> Self {
        Self {
            ws_url,
            spike_config,
        }
    }

    /// Run the gateway forever, reconnecting on failure with exponential backoff.
    ///
    /// `stale_threshold_ms`: discard any event where
    /// `now_ms - event.timestamp_ms > stale_threshold_ms`.
    /// PRD default: 500 ms (Section 5.4).
    pub async fn run(&self, tx: Sender<IngestorEvent>, stale_threshold_ms: u64) -> Result<()> {
        let mut backoff_ms = BACKOFF_INITIAL_MS;
        let mut detector = SpikeDetector::new(&self.spike_config);

        loop {
            info!(url = %self.ws_url, "connecting to Binance combined stream");

            match self
                .connect_and_stream(&tx, &mut detector, stale_threshold_ms)
                .await
            {
                Ok(()) => {
                    info!("Binance WebSocket closed cleanly — scheduling reconnect");
                }
                Err(e) => {
                    warn!(
                        error = %e,
                        backoff_ms,
                        "Binance WebSocket error — scheduling reconnect"
                    );
                }
            }

            // Emit disconnection status to Engine.
            let _ = tx.try_send(IngestorEvent::WsStatus {
                source: DataSource::Binance,
                connected: false,
            });

            tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
            backoff_ms = (backoff_ms * 2).min(BACKOFF_MAX_MS);
        }
    }

    /// Connect and stream until the connection is lost.
    async fn connect_and_stream(
        &self,
        tx: &Sender<IngestorEvent>,
        detector: &mut SpikeDetector,
        stale_threshold_ms: u64,
    ) -> Result<()> {
        let mut ws = self.tls_connect().await?;

        let _ = tx.try_send(IngestorEvent::WsStatus {
            source: DataSource::Binance,
            connected: true,
        });
        info!("Binance WebSocket connected successfully");

        loop {
            let frame = ws
                .read_frame()
                .await
                .context("Binance WS read_frame error")?;

            match frame.opcode {
                OpCode::Text | OpCode::Binary => {
                    let json = std::str::from_utf8(&frame.payload)
                        .context("Binance WS payload is not valid UTF-8")?;
                    if let Err(e) = self.handle_message(json, tx, detector, stale_threshold_ms) {
                        debug!(error = %e, "Binance message handling error (non-fatal)");
                    }
                }
                OpCode::Ping => {
                    let pong = Frame::pong(frame.payload);
                    ws.write_frame(pong)
                        .await
                        .context("failed to send WebSocket pong")?;
                }
                OpCode::Close => {
                    info!("Binance server sent Close frame");
                    return Ok(());
                }
                _ => {}
            }
        }
    }

    /// Dispatch a single JSON message from the Binance combined stream.
    fn handle_message(
        &self,
        json: &str,
        tx: &Sender<IngestorEvent>,
        detector: &mut SpikeDetector,
        stale_threshold_ms: u64,
    ) -> Result<()> {
        let wrapper: CombinedStreamWrapper =
            serde_json::from_str(json).context("combined stream wrapper parse error")?;

        let now_ms = now_epoch_ms();
        let stream = wrapper.stream.as_str();

        if stream.contains("@ticker") {
            // ── Ticker ────────────────────────────────────────────────
            let tick = parse_ticker_from_value(wrapper.data).context("ticker data parse error")?;

            if is_stale(tick.timestamp_ms, now_ms, stale_threshold_ms) {
                let age = now_ms.saturating_sub(tick.timestamp_ms);
                debug!(age_ms = age, "discarding stale BinanceTick");
                detector.record_stale(now_ms);
                return Ok(());
            }

            if tx.try_send(IngestorEvent::BinanceTick(tick)).is_err() {
                warn!("ingestor channel full — BinanceTick dropped");
            }
        } else if stream.contains("@depth") {
            // ── Depth snapshot ────────────────────────────────────────
            let depth = parse_depth_from_value(wrapper.data).context("depth data parse error")?;

            if is_stale(depth.timestamp_ms, now_ms, stale_threshold_ms) {
                let age = now_ms.saturating_sub(depth.timestamp_ms);
                debug!(age_ms = age, "discarding stale BinanceDepth");
                detector.record_stale(now_ms);
                return Ok(());
            }

            // ── Spike detection ───────────────────────────────────────
            if let Some(mid) = depth.mid_price() {
                let mid_f64 = mid.to_f64().unwrap_or(0.0);
                if let Some(spike) = detector.update(mid_f64, depth.timestamp_ms) {
                    // Emit confirmed spike as a first-class event (no encoding tricks).
                    if tx
                        .try_send(IngestorEvent::SpikeConfirmed(SpikeInfo {
                            direction: spike.direction,
                            magnitude: spike.magnitude,
                            sustained_ms: spike.sustained_ms,
                            timestamp_ms: spike.timestamp_ms,
                        }))
                        .is_err()
                    {
                        warn!("ingestor channel full — SpikeConfirmed dropped");
                    }

                    debug!(
                        direction = ?spike.direction,
                        magnitude_pct = %(spike.magnitude.to_f64().unwrap_or(0.0) * 100.0),
                        sustained_ms = spike.sustained_ms,
                        spike_ts_ms = spike.timestamp_ms,
                        "Binance price spike confirmed"
                    );
                }
            }

            if tx.try_send(IngestorEvent::BinanceDepth(depth)).is_err() {
                warn!("ingestor channel full — BinanceDepth dropped");
            }
        } else {
            debug!(stream, "unknown Binance combined stream type — ignoring");
        }

        Ok(())
    }

    /// Open a TLS WebSocket connection to the Binance combined stream URL.
    ///
    /// `fastwebsockets::handshake::client` always returns
    /// `WebSocket<TokioIo<Upgraded>>` regardless of the underlying stream type.
    /// The TLS stream is passed directly (it implements `tokio::io::AsyncRead +
    /// AsyncWrite`) and hyper upgrades it internally.
    async fn tls_connect(&self) -> Result<WebSocket<TokioIo<Upgraded>>> {
        let url_str = format!("{}{}", self.ws_url, BTCUSDT_STREAM);
        let uri: Uri = url_str.parse().context("invalid Binance WS URL")?;

        let host = uri
            .host()
            .context("Binance WS URL has no host")?
            .to_string();
        let port = uri.port_u16().unwrap_or(443);
        let addr = format!("{host}:{port}");

        // TCP.
        let tcp = TcpStream::connect(&addr)
            .await
            .with_context(|| format!("TCP connect to {addr} failed"))?;
        tcp.set_nodelay(true).context("failed to set TCP_NODELAY")?;

        // TLS — pass the raw TlsStream; fastwebsockets wraps it internally.
        let tls_config = build_tls_config()?;
        let connector = TlsConnector::from(Arc::new(tls_config));
        let server_name = ServerName::try_from(host.as_str())
            .map_err(|e| anyhow!("invalid TLS server name '{}': {e}", host))?
            .to_owned();
        let tls_stream = connector
            .connect(server_name, tcp)
            .await
            .context("TLS handshake failed")?;

        // WebSocket upgrade.
        let path_and_query = uri.path_and_query().map(|pq| pq.as_str()).unwrap_or("/");

        let request = Request::builder()
            .method("GET")
            .uri(path_and_query)
            .header("Host", &host)
            .header(UPGRADE, "websocket")
            .header(CONNECTION, "upgrade")
            .header("Sec-WebSocket-Key", handshake::generate_key())
            .header("Sec-WebSocket-Version", "13")
            .body(Empty::<Bytes>::new())
            .context("failed to build WS upgrade request")?;

        // handshake::client takes `S: tokio::io::AsyncRead + AsyncWrite + Send + Unpin + 'static`
        // and always returns `WebSocket<TokioIo<Upgraded>>`.
        let (ws, _resp) = handshake::client(&SpawnExecutor, request, tls_stream)
            .await
            .context("WebSocket handshake failed")?;

        Ok(ws)
    }
}

// ─── JSON parsing helpers ─────────────────────────────────────────────────────

/// Wraps a single event from a Binance combined stream.
/// `{"stream": "btcusdt@depth20@100ms", "data": { ... }}`
#[derive(serde::Deserialize)]
struct CombinedStreamWrapper {
    stream: String,
    data: serde_json::Value,
}

/// Deserialise the `data` portion of a `@ticker` combined stream event.
fn parse_ticker_from_value(data: serde_json::Value) -> Result<BinanceTick> {
    #[derive(serde::Deserialize)]
    struct RawTicker {
        #[serde(rename = "s")]
        symbol: String,
        /// Best bid price.
        #[serde(rename = "b")]
        bid_price: String,
        /// Best bid quantity.
        #[serde(rename = "B")]
        bid_qty: String,
        /// Best ask price.
        #[serde(rename = "a")]
        ask_price: String,
        /// Best ask quantity.
        #[serde(rename = "A")]
        ask_qty: String,
        /// Event timestamp (epoch ms).
        #[serde(rename = "E")]
        event_time: u64,
    }

    let raw: RawTicker = serde_json::from_value(data).context("ticker RawTicker deserialise")?;
    Ok(BinanceTick {
        symbol: raw.symbol,
        bid_price: raw
            .bid_price
            .parse::<Decimal>()
            .context("bid_price Decimal")?,
        bid_qty: raw.bid_qty.parse::<Decimal>().context("bid_qty Decimal")?,
        ask_price: raw
            .ask_price
            .parse::<Decimal>()
            .context("ask_price Decimal")?,
        ask_qty: raw.ask_qty.parse::<Decimal>().context("ask_qty Decimal")?,
        timestamp_ms: raw.event_time,
    })
}

/// Deserialise the `data` portion of a `@depth20@100ms` combined stream event.
fn parse_depth_from_value(data: serde_json::Value) -> Result<BinanceDepth> {
    #[derive(serde::Deserialize)]
    struct RawDepth {
        /// Transaction time (epoch ms). Present in diff depth (`@depth@100ms`)
        /// but absent in partial book depth (`@depth20@100ms`).
        /// Falls back to current wall-clock time when missing.
        #[serde(rename = "T", default)]
        transaction_time: Option<u64>,
        /// Bid levels: `[[price_str, qty_str], ...]`, highest price first.
        #[serde(rename = "bids")]
        bids: Vec<[String; 2]>,
        /// Ask levels: `[[price_str, qty_str], ...]`, lowest price first.
        #[serde(rename = "asks")]
        asks: Vec<[String; 2]>,
    }

    let raw: RawDepth = serde_json::from_value(data).context("depth RawDepth deserialise")?;
    let timestamp_ms = raw.transaction_time.unwrap_or_else(now_epoch_ms);

    let parse_levels = |levels: Vec<[String; 2]>| -> Result<Vec<PriceLevel>> {
        levels
            .into_iter()
            .map(|[p, s]| {
                Ok(PriceLevel {
                    price: p.parse::<Decimal>().context("level price Decimal")?,
                    size: s.parse::<Decimal>().context("level size Decimal")?,
                })
            })
            .collect()
    };

    // Binance already sends bids highest-first and asks lowest-first — matches our invariant.
    Ok(BinanceDepth {
        symbol: "btcusdt".to_string(),
        bids: parse_levels(raw.bids)?,
        asks: parse_levels(raw.asks)?,
        timestamp_ms,
    })
}

// ─── TLS configuration ────────────────────────────────────────────────────────

fn build_tls_config() -> Result<ClientConfig> {
    let mut root_store = rustls::RootCertStore::empty();
    root_store.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
    Ok(ClientConfig::builder()
        .with_root_certificates(root_store)
        .with_no_client_auth())
}

// ─── Helpers ──────────────────────────────────────────────────────────────────

/// Returns `true` if the event timestamp is older than `threshold_ms`.
#[inline]
pub(super) fn is_stale(event_ts_ms: u64, now_ms: u64, threshold_ms: u64) -> bool {
    now_ms.saturating_sub(event_ts_ms) > threshold_ms
}

/// Current wall-clock time as epoch milliseconds.
#[inline]
pub(super) fn now_epoch_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

// ─── fastwebsockets executor adapter ─────────────────────────────────────────

/// Minimal `hyper::rt::Executor` that spawns onto the current tokio runtime.
/// Required by `fastwebsockets::handshake::client`.
struct SpawnExecutor;

impl<Fut> hyper::rt::Executor<Fut> for SpawnExecutor
where
    Fut: std::future::Future + Send + 'static,
    Fut::Output: Send + 'static,
{
    fn execute(&self, fut: Fut) {
        tokio::task::spawn(fut);
    }
}

// ─── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_stale() {
        assert!(is_stale(1000, 1600, 500)); // age = 600 > 500
        assert!(!is_stale(1200, 1600, 500)); // age = 400 < 500
        assert!(!is_stale(1100, 1600, 500)); // age = 500 == 500 (not strictly greater)
    }

    #[test]
    fn test_fastwebsockets_generate_key_length() {
        let key = handshake::generate_key();
        // 16 bytes base64-encoded = 24 characters (with padding).
        assert_eq!(key.len(), 24, "WS key should be 24 chars: '{key}'");
        assert!(key.chars().all(|c| c.is_ascii()));
    }
}
