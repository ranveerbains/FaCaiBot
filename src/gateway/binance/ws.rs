//! Binance SBE WebSocket gateway — Layer 1 Ingestor ("The Ear").
//!
//! Maintains a persistent combined stream connection to Binance's SBE endpoint,
//! decodes binary `@depth20` (50ms cadence) and `@bestBidAsk` (real-time) events,
//! runs inline spike detection with EMA-ATR, and pushes [`IngestorEvent`]s to the
//! Engine via a crossbeam channel.
//!
//! # SBE protocol
//! Messages arrive as binary WebSocket frames containing raw SBE-encoded data
//! (schema `stream_1_0.xml`, schemaId=1, version=0). Each message starts with an
//! 8-byte header: `blockLength(u16) | templateId(u16) | schemaId(u16) | version(u16)`.
//! Subscription confirmations arrive as JSON text frames (logged, not parsed).
//!
//! # Spike detection output
//! Uses speculative Leg 1 posting: emits [`IngestorEvent::SpikeCandidate`] immediately
//! when ATR + magnitude pass, then [`IngestorEvent::SpikeConfirmed`] or
//! [`IngestorEvent::SpikeFailed`] after the sustain + momentum check.
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
use crate::types::market::{BinanceDepth, BinanceTick, DataSource, PriceLevel};

use super::spike::{SpikeDetector, SpikeEvent};

// ─── Constants ────────────────────────────────────────────────────────────────

/// Binance SBE combined stream: depth20 (50ms) + bestBidAsk (real-time).
const BTCUSDT_STREAM: &str = "/stream?streams=btcusdt@depth20/btcusdt@bestBidAsk";

/// SBE message header size (bytes): blockLength(u16) + templateId(u16) + schemaId(u16) + version(u16).
const SBE_HEADER_SIZE: usize = 8;

/// SBE template ID for BestBidAskStreamEvent (from stream_1_0.xml).
const SBE_TEMPLATE_BEST_BID_ASK: u16 = 10001;

/// SBE template ID for DepthSnapshotStreamEvent (from stream_1_0.xml).
const SBE_TEMPLATE_DEPTH_SNAPSHOT: u16 = 10002;

/// Reconnection backoff: initial delay (ms).
const BACKOFF_INITIAL_MS: u64 = 1_000;

/// Reconnection backoff: maximum delay (ms).
const BACKOFF_MAX_MS: u64 = 30_000;

// ─── Binance Gateway ─────────────────────────────────────────────────────────

/// Streams Binance spot SBE WebSocket feeds for BTC/USDT.
///
/// Connects to the Binance SBE combined stream, decodes binary `@depth20` and
/// `@bestBidAsk` frames, runs inline spike detection, and emits
/// [`IngestorEvent::BinanceDepth`] and [`IngestorEvent::BinanceTick`] events to
/// the Engine layer.
pub struct BinanceGateway {
    ws_url: String,
    ed25519_api_key: String,
    spike_config: SpikeDetectionConfig,
}

impl BinanceGateway {
    pub fn new(
        ws_url: String,
        ed25519_api_key: String,
        spike_config: SpikeDetectionConfig,
    ) -> Self {
        Self {
            ws_url,
            ed25519_api_key,
            spike_config,
        }
    }

    /// Run the gateway forever, reconnecting on failure with exponential backoff.
    ///
    /// `stale_threshold_ms`: discard any event where
    /// `now_ms - event.timestamp_ms > stale_threshold_ms`.
    pub async fn run(&self, tx: Sender<IngestorEvent>, stale_threshold_ms: u64) -> Result<()> {
        let mut backoff_ms = BACKOFF_INITIAL_MS;
        let mut detector = SpikeDetector::new(&self.spike_config);

        loop {
            info!(url = %self.ws_url, "connecting to Binance SBE combined stream");

            match self
                .connect_and_stream(&tx, &mut detector, stale_threshold_ms)
                .await
            {
                Ok(()) => {
                    info!("Binance SBE WebSocket closed cleanly — scheduling reconnect");
                }
                Err(e) => {
                    warn!(
                        error = %e,
                        backoff_ms,
                        "Binance SBE WebSocket error — scheduling reconnect"
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
        info!("Binance SBE WebSocket connected successfully");

        loop {
            let frame = ws
                .read_frame()
                .await
                .context("Binance SBE WS read_frame error")?;

            match frame.opcode {
                OpCode::Binary => {
                    // SBE market data arrives as binary frames.
                    if let Err(e) =
                        handle_sbe_message(&frame.payload, tx, detector, stale_threshold_ms)
                    {
                        debug!(error = %e, "SBE message handling error (non-fatal)");
                    }
                }
                OpCode::Text => {
                    // Subscription confirmations arrive as JSON text frames.
                    let text = std::str::from_utf8(&frame.payload).unwrap_or("?");
                    debug!(msg = text, "SBE subscription response");
                }
                OpCode::Ping => {
                    let pong = Frame::pong(frame.payload);
                    ws.write_frame(pong)
                        .await
                        .context("failed to send WebSocket pong")?;
                }
                OpCode::Close => {
                    info!("Binance SBE server sent Close frame");
                    return Ok(());
                }
                _ => {}
            }
        }
    }

    /// Open a TLS WebSocket connection to the Binance SBE combined stream.
    async fn tls_connect(&self) -> Result<WebSocket<TokioIo<Upgraded>>> {
        let url_str = format!("{}{}", self.ws_url, BTCUSDT_STREAM);
        let uri: Uri = url_str.parse().context("invalid Binance SBE WS URL")?;

        let host = uri
            .host()
            .context("Binance SBE WS URL has no host")?
            .to_string();
        let port = uri.port_u16().unwrap_or(443);
        let addr = format!("{host}:{port}");

        // TCP.
        let tcp = TcpStream::connect(&addr)
            .await
            .with_context(|| format!("TCP connect to {addr} failed"))?;
        tcp.set_nodelay(true).context("failed to set TCP_NODELAY")?;

        // TLS.
        let tls_config = build_tls_config()?;
        let connector = TlsConnector::from(Arc::new(tls_config));
        let server_name = ServerName::try_from(host.as_str())
            .map_err(|e| anyhow!("invalid TLS server name '{}': {e}", host))?
            .to_owned();
        let tls_stream = connector
            .connect(server_name, tcp)
            .await
            .context("TLS handshake failed")?;

        // WebSocket upgrade — includes X-MBX-APIKEY for SBE auth.
        let path_and_query = uri.path_and_query().map(|pq| pq.as_str()).unwrap_or("/");

        let request = Request::builder()
            .method("GET")
            .uri(path_and_query)
            .header("Host", &host)
            .header(UPGRADE, "websocket")
            .header(CONNECTION, "upgrade")
            .header("Sec-WebSocket-Key", handshake::generate_key())
            .header("Sec-WebSocket-Version", "13")
            .header("X-MBX-APIKEY", &self.ed25519_api_key)
            .body(Empty::<Bytes>::new())
            .context("failed to build WS upgrade request")?;

        let (ws, _resp) = handshake::client(&SpawnExecutor, request, tls_stream)
            .await
            .context("WebSocket handshake failed")?;

        Ok(ws)
    }
}

// ─── SBE message handling ────────────────────────────────────────────────────

/// Dispatch a single SBE binary message from the Binance combined stream.
fn handle_sbe_message(
    payload: &[u8],
    tx: &Sender<IngestorEvent>,
    detector: &mut SpikeDetector,
    stale_threshold_ms: u64,
) -> Result<()> {
    if payload.len() < SBE_HEADER_SIZE {
        return Err(anyhow!(
            "SBE payload too short: {} bytes (need >= {})",
            payload.len(),
            SBE_HEADER_SIZE
        ));
    }

    // 8-byte SBE message header (little-endian).
    let block_length = u16::from_le_bytes([payload[0], payload[1]]) as usize;
    let template_id = u16::from_le_bytes([payload[2], payload[3]]);
    // payload[4..6] = schemaId, payload[6..8] = version (not needed for dispatch).

    let body = &payload[SBE_HEADER_SIZE..];
    let now_ms = now_epoch_ms();

    match template_id {
        SBE_TEMPLATE_DEPTH_SNAPSHOT => {
            let depth =
                parse_sbe_depth(body, block_length).context("SBE depth snapshot parse error")?;

            if is_stale(depth.timestamp_ms, now_ms, stale_threshold_ms) {
                let age = now_ms.saturating_sub(depth.timestamp_ms);
                debug!(age_ms = age, "discarding stale SBE BinanceDepth");
                detector.record_stale(now_ms);
                return Ok(());
            }

            // ── Spike detection ───────────────────────────────────────
            if let Some(mid) = depth.mid_price() {
                let mid_f64 = mid.to_f64().unwrap_or(0.0);
                match detector.update(mid_f64, depth.timestamp_ms) {
                    SpikeEvent::Candidate(spike) => {
                        debug!(
                            direction = ?spike.direction,
                            magnitude_pct = %(spike.magnitude.to_f64().unwrap_or(0.0) * 100.0),
                            "Binance spike candidate — speculative Leg 1"
                        );
                        if tx.try_send(IngestorEvent::SpikeCandidate(spike)).is_err() {
                            warn!("ingestor channel full — SpikeCandidate dropped");
                        }
                    }
                    SpikeEvent::Confirmed(spike) => {
                        debug!(
                            direction = ?spike.direction,
                            magnitude_pct = %(spike.magnitude.to_f64().unwrap_or(0.0) * 100.0),
                            sustained_ms = spike.sustained_ms,
                            "Binance spike confirmed — sim fill gate open"
                        );
                        if tx.try_send(IngestorEvent::SpikeConfirmed(spike)).is_err() {
                            warn!("ingestor channel full — SpikeConfirmed dropped");
                        }
                    }
                    SpikeEvent::Failed { timestamp_ms } => {
                        debug!(timestamp_ms, "Binance spike failed — cancelling speculative Leg 1");
                        if tx.try_send(IngestorEvent::SpikeFailed { timestamp_ms }).is_err() {
                            warn!("ingestor channel full — SpikeFailed dropped");
                        }
                    }
                    SpikeEvent::None => {}
                }
            }

            if tx.try_send(IngestorEvent::BinanceDepth(depth)).is_err() {
                warn!("ingestor channel full — BinanceDepth dropped");
            }
        }
        SBE_TEMPLATE_BEST_BID_ASK => {
            let tick =
                parse_sbe_best_bid_ask(body, block_length).context("SBE bestBidAsk parse error")?;

            if is_stale(tick.timestamp_ms, now_ms, stale_threshold_ms) {
                let age = now_ms.saturating_sub(tick.timestamp_ms);
                debug!(age_ms = age, "discarding stale SBE BinanceTick");
                detector.record_stale(now_ms);
                return Ok(());
            }

            if tx.try_send(IngestorEvent::BinanceTick(tick)).is_err() {
                warn!("ingestor channel full — BinanceTick dropped");
            }
        }
        other => {
            debug!(template_id = other, "unknown SBE template — ignoring");
        }
    }

    Ok(())
}

// ─── SBE parsing functions ───────────────────────────────────────────────────

/// Convert SBE mantissa + exponent to `rust_decimal::Decimal`.
///
/// Value = mantissa × 10^exponent.  Exponent is negative for decimal places.
/// Single integer copy — no string allocation or parsing.
#[inline]
fn sbe_to_decimal(mantissa: i64, exponent: i8) -> Decimal {
    if exponent <= 0 {
        Decimal::new(mantissa, (-exponent) as u32)
    } else {
        // Positive exponent (rare for prices): mantissa × 10^exp.
        Decimal::new(mantissa, 0) * Decimal::new(10_i64.pow(exponent as u32), 0)
    }
}

/// Read a little-endian `i64` from a byte slice at the given offset.
#[inline]
fn read_i64(buf: &[u8], offset: usize) -> Result<i64> {
    let bytes: [u8; 8] = buf
        .get(offset..offset + 8)
        .context("SBE i64 read out of bounds")?
        .try_into()
        .unwrap();
    Ok(i64::from_le_bytes(bytes))
}

/// Read a little-endian `u16` from a byte slice at the given offset.
#[inline]
fn read_u16(buf: &[u8], offset: usize) -> Result<u16> {
    let bytes: [u8; 2] = buf
        .get(offset..offset + 2)
        .context("SBE u16 read out of bounds")?
        .try_into()
        .unwrap();
    Ok(u16::from_le_bytes(bytes))
}

/// Parse SBE `DepthSnapshotStreamEvent` body (after 8-byte header) into [`BinanceDepth`].
///
/// Root block layout (18 bytes):
///   - `[0..8]`   eventTime: i64 (microseconds)
///   - `[8..16]`  bookUpdateId: i64
///   - `[16]`     priceExponent: i8
///   - `[17]`     qtyExponent: i8
///
/// Then repeating groups (bids, asks) with `groupSize16Encoding`:
///   - Group header: blockLength(u16) + numInGroup(u16) = 4 bytes
///   - Each entry: price(i64) + qty(i64) = 16 bytes
fn parse_sbe_depth(body: &[u8], block_length: usize) -> Result<BinanceDepth> {
    if body.len() < 18 {
        return Err(anyhow!("SBE depth body too short: {} bytes", body.len()));
    }

    let event_time_us = read_i64(body, 0)?;
    // bookUpdateId at [8..16] — not needed.
    let price_exp = body[16] as i8;
    let qty_exp = body[17] as i8;

    // Timestamp: SBE uses microseconds, our types use milliseconds.
    let timestamp_ms = (event_time_us / 1000) as u64;

    // Groups start after the root block.
    let mut offset = block_length;

    let bids = parse_sbe_price_levels(body, &mut offset, price_exp, qty_exp)
        .context("SBE bids group parse error")?;
    let asks = parse_sbe_price_levels(body, &mut offset, price_exp, qty_exp)
        .context("SBE asks group parse error")?;

    Ok(BinanceDepth {
        symbol: "btcusdt".to_string(),
        bids,
        asks,
        timestamp_ms,
    })
}

/// Parse a repeating group of price levels from SBE binary.
///
/// `groupSize16Encoding`: blockLength(u16) + numInGroup(u16) = 4 byte header.
/// Each entry: price mantissa(i64) + qty mantissa(i64).
fn parse_sbe_price_levels(
    body: &[u8],
    offset: &mut usize,
    price_exp: i8,
    qty_exp: i8,
) -> Result<Vec<PriceLevel>> {
    let group_block_length = read_u16(body, *offset)? as usize;
    let num_in_group = read_u16(body, *offset + 2)? as usize;
    *offset += 4; // past group header

    let mut levels = Vec::with_capacity(num_in_group);
    for _ in 0..num_in_group {
        let price_mantissa = read_i64(body, *offset)?;
        let qty_mantissa = read_i64(body, *offset + 8)?;
        levels.push(PriceLevel {
            price: sbe_to_decimal(price_mantissa, price_exp),
            size: sbe_to_decimal(qty_mantissa, qty_exp),
        });
        *offset += group_block_length;
    }

    Ok(levels)
}

/// Parse SBE `BestBidAskStreamEvent` body (after 8-byte header) into [`BinanceTick`].
///
/// Root block layout (50 bytes):
///   - `[0..8]`   eventTime: i64 (microseconds)
///   - `[8..16]`  bookUpdateId: i64
///   - `[16]`     priceExponent: i8
///   - `[17]`     qtyExponent: i8
///   - `[18..26]` bidPrice: i64 mantissa
///   - `[26..34]` bidQty: i64 mantissa
///   - `[34..42]` askPrice: i64 mantissa
///   - `[42..50]` askQty: i64 mantissa
fn parse_sbe_best_bid_ask(body: &[u8], block_length: usize) -> Result<BinanceTick> {
    if body.len() < block_length || block_length < 50 {
        return Err(anyhow!(
            "SBE bestBidAsk body too short: {} bytes (block_length={})",
            body.len(),
            block_length
        ));
    }

    let event_time_us = read_i64(body, 0)?;
    // bookUpdateId at [8..16] — not needed.
    let price_exp = body[16] as i8;
    let qty_exp = body[17] as i8;
    let bid_price = read_i64(body, 18)?;
    let bid_qty = read_i64(body, 26)?;
    let ask_price = read_i64(body, 34)?;
    let ask_qty = read_i64(body, 42)?;

    let timestamp_ms = (event_time_us / 1000) as u64;

    Ok(BinanceTick {
        symbol: "BTCUSDT".to_string(),
        bid_price: sbe_to_decimal(bid_price, price_exp),
        bid_qty: sbe_to_decimal(bid_qty, qty_exp),
        ask_price: sbe_to_decimal(ask_price, price_exp),
        ask_qty: sbe_to_decimal(ask_qty, qty_exp),
        timestamp_ms,
    })
}

// ─── TLS configuration ──────────────────────────────────────────────────────

fn build_tls_config() -> Result<ClientConfig> {
    let mut root_store = rustls::RootCertStore::empty();
    root_store.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
    Ok(ClientConfig::builder()
        .with_root_certificates(root_store)
        .with_no_client_auth())
}

// ─── Helpers ────────────────────────────────────────────────────────────────

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

// ─── Tests ──────────────────────────────────────────────────────────────────

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
        assert_eq!(key.len(), 24, "WS key should be 24 chars: '{key}'");
        assert!(key.chars().all(|c| c.is_ascii()));
    }

    // ── SBE decimal conversion ──────────────────────────────────────

    #[test]
    fn test_sbe_to_decimal_negative_exponent() {
        // 4250050 × 10^-2 = 42500.50
        let d = sbe_to_decimal(4250050, -2);
        assert_eq!(d, Decimal::new(4250050, 2));
        assert_eq!(d.to_string(), "42500.50");
    }

    #[test]
    fn test_sbe_to_decimal_large_negative_exponent() {
        // 123456789 × 10^-5 = 1234.56789
        let d = sbe_to_decimal(123456789, -5);
        assert_eq!(d, Decimal::new(123456789, 5));
        assert_eq!(d.to_string(), "1234.56789");
    }

    #[test]
    fn test_sbe_to_decimal_zero_exponent() {
        let d = sbe_to_decimal(42500, 0);
        assert_eq!(d, Decimal::new(42500, 0));
    }

    #[test]
    fn test_sbe_to_decimal_positive_exponent() {
        // 5 × 10^2 = 500
        let d = sbe_to_decimal(5, 2);
        assert_eq!(d.to_string(), "500");
    }

    // ── SBE BestBidAsk parsing ──────────────────────────────────────

    #[test]
    fn test_parse_sbe_best_bid_ask() {
        // Build synthetic BestBidAskStreamEvent body (50 bytes root block).
        let mut body = vec![0u8; 50];
        let event_time_us: i64 = 1_700_000_000_000_000; // microseconds
        body[0..8].copy_from_slice(&event_time_us.to_le_bytes()); // eventTime
        body[8..16].copy_from_slice(&1_i64.to_le_bytes()); // bookUpdateId
        body[16] = (-2_i8) as u8; // priceExponent
        body[17] = (-5_i8) as u8; // qtyExponent
        body[18..26].copy_from_slice(&9700050_i64.to_le_bytes()); // bidPrice
        body[26..34].copy_from_slice(&150000_i64.to_le_bytes()); // bidQty
        body[34..42].copy_from_slice(&9700100_i64.to_le_bytes()); // askPrice
        body[42..50].copy_from_slice(&200000_i64.to_le_bytes()); // askQty

        let tick = parse_sbe_best_bid_ask(&body, 50).unwrap();

        assert_eq!(tick.symbol, "BTCUSDT");
        assert_eq!(tick.bid_price, Decimal::new(9700050, 2)); // 97000.50
        assert_eq!(tick.bid_qty, Decimal::new(150000, 5)); // 1.50000
        assert_eq!(tick.ask_price, Decimal::new(9700100, 2)); // 97001.00
        assert_eq!(tick.ask_qty, Decimal::new(200000, 5)); // 2.00000
        assert_eq!(tick.timestamp_ms, 1_700_000_000_000); // us → ms
    }

    // ── SBE DepthSnapshot parsing ───────────────────────────────────

    #[test]
    fn test_parse_sbe_depth_snapshot() {
        // Build synthetic DepthSnapshotStreamEvent.
        // Root block = 18 bytes, then bids group (2 levels), then asks group (2 levels).
        let price_exp: i8 = -2;
        let qty_exp: i8 = -5;
        let event_time_us: i64 = 1_700_000_000_000_000;

        let mut body = Vec::new();

        // Root block (18 bytes).
        body.extend_from_slice(&event_time_us.to_le_bytes()); // [0..8] eventTime
        body.extend_from_slice(&42_i64.to_le_bytes()); // [8..16] bookUpdateId
        body.push(price_exp as u8); // [16] priceExponent
        body.push(qty_exp as u8); // [17] qtyExponent

        // Bids group header (groupSize16Encoding: blockLength u16 + numInGroup u16).
        let entry_block_length: u16 = 16; // price(8) + qty(8)
        let num_bids: u16 = 2;
        body.extend_from_slice(&entry_block_length.to_le_bytes());
        body.extend_from_slice(&num_bids.to_le_bytes());

        // Bid 0: price=97001.00, qty=1.50000
        body.extend_from_slice(&9700100_i64.to_le_bytes());
        body.extend_from_slice(&150000_i64.to_le_bytes());
        // Bid 1: price=97000.50, qty=2.00000
        body.extend_from_slice(&9700050_i64.to_le_bytes());
        body.extend_from_slice(&200000_i64.to_le_bytes());

        // Asks group header.
        let num_asks: u16 = 2;
        body.extend_from_slice(&entry_block_length.to_le_bytes());
        body.extend_from_slice(&num_asks.to_le_bytes());

        // Ask 0: price=97001.50, qty=0.50000
        body.extend_from_slice(&9700150_i64.to_le_bytes());
        body.extend_from_slice(&50000_i64.to_le_bytes());
        // Ask 1: price=97002.00, qty=3.00000
        body.extend_from_slice(&9700200_i64.to_le_bytes());
        body.extend_from_slice(&300000_i64.to_le_bytes());

        let depth = parse_sbe_depth(&body, 18).unwrap();

        assert_eq!(depth.symbol, "btcusdt");
        assert_eq!(depth.timestamp_ms, 1_700_000_000_000);
        assert_eq!(depth.bids.len(), 2);
        assert_eq!(depth.asks.len(), 2);

        // Bids: highest first.
        assert_eq!(depth.bids[0].price, Decimal::new(9700100, 2)); // 97001.00
        assert_eq!(depth.bids[0].size, Decimal::new(150000, 5)); // 1.50000
        assert_eq!(depth.bids[1].price, Decimal::new(9700050, 2)); // 97000.50
        assert_eq!(depth.bids[1].size, Decimal::new(200000, 5)); // 2.00000

        // Asks: lowest first.
        assert_eq!(depth.asks[0].price, Decimal::new(9700150, 2)); // 97001.50
        assert_eq!(depth.asks[0].size, Decimal::new(50000, 5)); // 0.50000
        assert_eq!(depth.asks[1].price, Decimal::new(9700200, 2)); // 97002.00
        assert_eq!(depth.asks[1].size, Decimal::new(300000, 5)); // 3.00000
    }

    // ── SBE full message dispatch ───────────────────────────────────

    #[test]
    fn test_handle_sbe_message_depth() {
        let (tx, rx) = crossbeam_channel::bounded(64);
        let spike_cfg = SpikeDetectionConfig::default();
        let mut detector = SpikeDetector::new(&spike_cfg);

        // Build a full SBE message (header + body).
        let block_length: u16 = 18;
        let template_id: u16 = SBE_TEMPLATE_DEPTH_SNAPSHOT;
        let schema_id: u16 = 1;
        let version: u16 = 0;

        let mut payload = Vec::new();
        // Header.
        payload.extend_from_slice(&block_length.to_le_bytes());
        payload.extend_from_slice(&template_id.to_le_bytes());
        payload.extend_from_slice(&schema_id.to_le_bytes());
        payload.extend_from_slice(&version.to_le_bytes());

        // Body — root block.
        let now_us = (now_epoch_ms() as i64) * 1000;
        payload.extend_from_slice(&now_us.to_le_bytes()); // eventTime
        payload.extend_from_slice(&1_i64.to_le_bytes()); // bookUpdateId
        payload.push((-2_i8) as u8); // priceExponent
        payload.push((-5_i8) as u8); // qtyExponent

        // Bids group: 1 level.
        payload.extend_from_slice(&16_u16.to_le_bytes()); // blockLength
        payload.extend_from_slice(&1_u16.to_le_bytes()); // numInGroup
        payload.extend_from_slice(&9700100_i64.to_le_bytes()); // price
        payload.extend_from_slice(&150000_i64.to_le_bytes()); // qty

        // Asks group: 1 level.
        payload.extend_from_slice(&16_u16.to_le_bytes());
        payload.extend_from_slice(&1_u16.to_le_bytes());
        payload.extend_from_slice(&9700200_i64.to_le_bytes());
        payload.extend_from_slice(&50000_i64.to_le_bytes());

        handle_sbe_message(&payload, &tx, &mut detector, 5000).unwrap();

        // Should emit a BinanceDepth event.
        let event = rx.try_recv().expect("expected BinanceDepth event");
        match event {
            IngestorEvent::BinanceDepth(d) => {
                assert_eq!(d.bids.len(), 1);
                assert_eq!(d.asks.len(), 1);
                assert_eq!(d.bids[0].price, Decimal::new(9700100, 2));
            }
            other => panic!("expected BinanceDepth, got {other:?}"),
        }
    }

    #[test]
    fn test_handle_sbe_message_best_bid_ask() {
        let (tx, rx) = crossbeam_channel::bounded(64);
        let spike_cfg = SpikeDetectionConfig::default();
        let mut detector = SpikeDetector::new(&spike_cfg);

        let block_length: u16 = 50;
        let template_id: u16 = SBE_TEMPLATE_BEST_BID_ASK;

        let mut payload = Vec::new();
        // Header.
        payload.extend_from_slice(&block_length.to_le_bytes());
        payload.extend_from_slice(&template_id.to_le_bytes());
        payload.extend_from_slice(&1_u16.to_le_bytes()); // schemaId
        payload.extend_from_slice(&0_u16.to_le_bytes()); // version

        // Body.
        let now_us = (now_epoch_ms() as i64) * 1000;
        payload.extend_from_slice(&now_us.to_le_bytes()); // eventTime
        payload.extend_from_slice(&1_i64.to_le_bytes()); // bookUpdateId
        payload.push((-2_i8) as u8); // priceExponent
        payload.push((-5_i8) as u8); // qtyExponent
        payload.extend_from_slice(&9700050_i64.to_le_bytes()); // bidPrice
        payload.extend_from_slice(&150000_i64.to_le_bytes()); // bidQty
        payload.extend_from_slice(&9700100_i64.to_le_bytes()); // askPrice
        payload.extend_from_slice(&200000_i64.to_le_bytes()); // askQty

        handle_sbe_message(&payload, &tx, &mut detector, 5000).unwrap();

        let event = rx.try_recv().expect("expected BinanceTick event");
        match event {
            IngestorEvent::BinanceTick(t) => {
                assert_eq!(t.bid_price, Decimal::new(9700050, 2));
                assert_eq!(t.ask_price, Decimal::new(9700100, 2));
            }
            other => panic!("expected BinanceTick, got {other:?}"),
        }
    }

    #[test]
    fn test_handle_sbe_message_stale_dropped() {
        let (tx, rx) = crossbeam_channel::bounded(64);
        let spike_cfg = SpikeDetectionConfig::default();
        let mut detector = SpikeDetector::new(&spike_cfg);

        let mut payload = Vec::new();
        payload.extend_from_slice(&50_u16.to_le_bytes()); // blockLength
        payload.extend_from_slice(&SBE_TEMPLATE_BEST_BID_ASK.to_le_bytes());
        payload.extend_from_slice(&1_u16.to_le_bytes());
        payload.extend_from_slice(&0_u16.to_le_bytes());

        // Use a very old timestamp (1s ago with 500ms threshold → stale).
        let old_us = ((now_epoch_ms() - 2000) as i64) * 1000;
        payload.extend_from_slice(&old_us.to_le_bytes());
        payload.extend_from_slice(&1_i64.to_le_bytes());
        payload.push((-2_i8) as u8);
        payload.push((-5_i8) as u8);
        payload.extend_from_slice(&9700050_i64.to_le_bytes());
        payload.extend_from_slice(&150000_i64.to_le_bytes());
        payload.extend_from_slice(&9700100_i64.to_le_bytes());
        payload.extend_from_slice(&200000_i64.to_le_bytes());

        // Stale threshold = 500ms, event is 2s old → should be dropped.
        handle_sbe_message(&payload, &tx, &mut detector, 500).unwrap();

        assert!(rx.try_recv().is_err(), "stale event should be dropped");
    }
}
