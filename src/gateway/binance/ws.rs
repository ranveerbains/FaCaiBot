//! Binance SBE WebSocket gateway — Layer 1 Ingestor ("The Ear").
//!
//! Maintains a persistent combined stream connection to Binance's SBE endpoint,
//! decodes binary `@depth20` (50ms cadence) and `@bestBidAsk` (real-time) events,
//! and pushes [`IngestorEvent`]s to the Engine via a crossbeam channel.
//!
//! # SBE protocol
//! Messages arrive as binary WebSocket frames containing raw SBE-encoded data
//! (schema `stream_1_0.xml`, schemaId=1, version=0). Each message starts with an
//! 8-byte header: `blockLength(u16) | templateId(u16) | schemaId(u16) | version(u16)`.
//! Subscription confirmations arrive as JSON text frames (logged, not parsed).
//!
//! # Thread model
//! This module runs on a **dedicated OS thread** with its own single-threaded
//! tokio runtime (see `main.rs`). It must never touch the main multi-threaded
//! runtime.
//!
//! # Latency target
//! < 50 ms from Binance WS push to crossbeam channel emit (P99).

use std::sync::Arc;
use std::time::Duration;

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
use rustls::pki_types::ServerName;
use tokio::net::TcpStream;
use tokio_rustls::TlsConnector;
use tracing::{debug, info, warn};

use crate::types::IngestorEvent;
use crate::types::market::{BinanceDepth, BinanceTick, DataSource, PriceLevel, SpotTrade};

// ─── Constants ────────────────────────────────────────────────────────────────

/// Binance SBE combined stream: depth20 (50ms) + bestBidAsk (real-time) + trade (per-trade).
const BTCUSDT_STREAM: &str = "/stream?streams=btcusdt@depth20/btcusdt@bestBidAsk/btcusdt@trade";

/// SBE message header size (bytes): blockLength(u16) + templateId(u16) + schemaId(u16) + version(u16).
const SBE_HEADER_SIZE: usize = 8;

/// SBE template ID for BestBidAskStreamEvent (from stream_1_0.xml).
const SBE_TEMPLATE_BEST_BID_ASK: u16 = 10001;

/// SBE template ID for DepthSnapshotStreamEvent (from stream_1_0.xml).
const SBE_TEMPLATE_DEPTH_SNAPSHOT: u16 = 10002;

/// SBE template ID for TradeStreamEvent (from stream_1_0.xml).
const SBE_TEMPLATE_TRADE: u16 = 10000;

/// Reconnection backoff: initial delay (ms).
const BACKOFF_INITIAL_MS: u64 = 1_000;

/// Reconnection backoff: maximum delay (ms).
const BACKOFF_MAX_MS: u64 = 30_000;

// ─── Binance Gateway ─────────────────────────────────────────────────────────

/// Streams Binance spot SBE WebSocket feeds for BTC/USDT.
///
/// Connects to the Binance SBE combined stream, decodes binary `@depth20` and
/// `@bestBidAsk` frames, and emits [`IngestorEvent::BinanceDepth`] and
/// [`IngestorEvent::BinanceTick`] events to the Engine layer.
pub struct BinanceGateway {
    ws_url: String,
    ed25519_api_key: String,
}

impl BinanceGateway {
    pub fn new(
        ws_url: String,
        ed25519_api_key: String,
    ) -> Self {
        Self {
            ws_url,
            ed25519_api_key,
        }
    }

    /// Run the gateway forever, reconnecting on failure with exponential backoff.
    ///
    /// `stale_threshold_ms`: discard any event where
    /// `now_ms - event.timestamp_ms > stale_threshold_ms`.
    pub async fn run(&self, tx: Sender<IngestorEvent>, stale_threshold_ms: u64) -> Result<()> {
        let mut backoff_ms = BACKOFF_INITIAL_MS;

        loop {
            info!(url = %self.ws_url, "connecting to Binance SBE combined stream");

            match self
                .connect_and_stream(&tx, stale_threshold_ms)
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
                        handle_sbe_message(&frame.payload, tx, stale_threshold_ms)
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
                return Ok(());
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
                return Ok(());
            }

            if tx.try_send(IngestorEvent::BinanceTick(tick)).is_err() {
                warn!("ingestor channel full — BinanceTick dropped");
            }
        }
        SBE_TEMPLATE_TRADE => {
            match parse_sbe_trade(body, block_length) {
                Ok(trades) => {
                    for trade in trades {
                        if !is_stale(trade.timestamp_ms, now_ms, stale_threshold_ms) {
                            if tx.try_send(IngestorEvent::SpotTrade(trade)).is_err() {
                                // High frequency — don't warn on every drop.
                                debug!("ingestor channel full — SpotTrade dropped");
                            }
                        } else {
                            let age = now_ms.saturating_sub(trade.timestamp_ms);
                            debug!(age_ms = age, "discarding stale SBE SpotTrade");
                        }
                    }
                }
                Err(e) => {
                    warn!(error = %e, block_length, "SBE trade parse failed — SpotFlow dead");
                }
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
        symbol: "BTCUSDT",
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
        symbol: "BTCUSDT",
        bid_price: sbe_to_decimal(bid_price, price_exp),
        bid_qty: sbe_to_decimal(bid_qty, qty_exp),
        ask_price: sbe_to_decimal(ask_price, price_exp),
        ask_qty: sbe_to_decimal(ask_qty, qty_exp),
        timestamp_ms,
    })
}

/// Parse SBE `TradesStreamEvent` (template 10000) with repeating group of trades into vec of [`SpotTrade`].
///
/// Root block layout (18 bytes):
///   - `[0..8]`   eventTime: i64 (utcTimestampUs)
///   - `[8..16]`  transactTime: i64 (utcTimestampUs)
///   - `[16]`     priceExponent: i8
///   - `[17]`     qtyExponent: i8
/// Group header (4 bytes):
///   - `[18..20]` blockLength: u16 (size per trade entry, little-endian)
///   - `[20..22]` numInGroup: u16 (number of trades, little-endian)
/// Trade entries (repeating, blockLength bytes each, typically 26 bytes):
///   - `[0..8]`   id: i64 (tradeId)
///   - `[8..16]`  price: i64 (mantissa64)
///   - `[16..24]` qty: i64 (mantissa64)
///   - `[24]`     isBuyerMaker: u8 (boolean)
///   - `[25]`     isBestMatch: u8 (boolean, unused)
/// Variable data:
///   - symbol: varString8 (skipped)
fn parse_sbe_trade(body: &[u8], _block_length: usize) -> Result<Vec<SpotTrade>> {
    // Root block: 18 bytes
    if body.len() < 18 {
        return Err(anyhow!(
            "SBE trade root block too short: {} bytes (need 18)",
            body.len()
        ));
    }

    let event_time_us = read_i64(body, 0)?;
    let price_exp = body[16] as i8;
    let qty_exp = body[17] as i8;
    let timestamp_ms = (event_time_us / 1000) as u64;

    // Group header: 4 bytes at [18..22]
    if body.len() < 22 {
        return Err(anyhow!(
            "SBE trade group header too short: {} bytes (need 22)",
            body.len()
        ));
    }

    let trade_block_length = u16::from_le_bytes([body[18], body[19]]) as usize;
    let num_trades = u16::from_le_bytes([body[20], body[21]]) as usize;

    // Verify we have enough data for all trades
    let expected_len = 22 + (num_trades * trade_block_length);
    if body.len() < expected_len {
        return Err(anyhow!(
            "SBE trade body too short: {} bytes (expected {} for {} trades with block_length={})",
            body.len(),
            expected_len,
            num_trades,
            trade_block_length
        ));
    }

    let mut trades = Vec::with_capacity(num_trades);

    // Parse each trade entry in the repeating group
    for i in 0..num_trades {
        let offset = 22 + (i * trade_block_length);

        if body.len() < offset + 26 {
            return Err(anyhow!(
                "SBE trade entry {} too short at offset {}",
                i,
                offset
            ));
        }

        let price = read_i64(body, offset + 8)?;
        let qty = read_i64(body, offset + 16)?;
        let is_buyer_maker = body[offset + 24] != 0;

        trades.push(SpotTrade {
            price: sbe_to_decimal(price, price_exp),
            quantity: sbe_to_decimal(qty, qty_exp),
            is_buyer_maker,
            timestamp_ms,
        });
    }

    Ok(trades)
}

use crate::utils::tls::build_tls_config;

// ─── Helpers ────────────────────────────────────────────────────────────────

/// Returns `true` if the event timestamp is older than `threshold_ms`.
#[inline]
pub(super) fn is_stale(event_ts_ms: u64, now_ms: u64, threshold_ms: u64) -> bool {
    now_ms.saturating_sub(event_ts_ms) > threshold_ms
}

pub(super) use crate::utils::time::epoch_ms as now_epoch_ms;

use crate::utils::tls::SpawnExecutor;

// ─── Tests ──────────────────────────────────────────────────────────────────

#[cfg(test)]
#[path = "tests/ws_tests.rs"]
mod tests;
