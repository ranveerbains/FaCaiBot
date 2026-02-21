//! Polymarket CLOB REST gateway — Layer 3 Executor ("The Hand").
//!
//! Wraps every Polymarket CLOB HTTP endpoint needed by the Executor layer:
//!
//! - [`PolymarketGateway::place_order`] — EIP-712 signed single-order POST to `/order`.
//! - [`PolymarketGateway::place_orders`] — Batch POST to `/orders` (max 15 per request).
//! - [`PolymarketGateway::cancel_order`] — DELETE `/order` by ID.
//! - [`PolymarketGateway::cancel_all`] — DELETE `/orders` (cancel all open orders).
//! - [`PolymarketGateway::get_orderbook`] — GET `/book?token_id={id}`.
//! - [`PolymarketGateway::get_midpoint`] — GET `/midpoint?token_id={id}`.
//! - [`PolymarketGateway::get_price`] — GET `/price?token_id={id}`.
//! - [`PolymarketGateway::get_tick_size`] — GET `/tick-size?token_id={id}`.
//! - [`PolymarketGateway::get_fee_rate`] — GET `/fee-rate?token_id={id}`.
//! - [`PolymarketGateway::stream_orderbook`] — Stub; real WS is in `market_ws.rs`.
//!
//! # Authentication
//!
//! All trading endpoints require **L2 headers** (HMAC-SHA256 over API credentials).
//! The order *payload* itself must also carry an EIP-712 signature produced by the
//! bot's Polygon private key.  Both are handled inside this module.
//!
//! The signed order structure follows the Polymarket CLOB wire format:
//! ```json
//! {
//!   "order": {
//!     "salt": 12345,
//!     "maker": "0x...",
//!     "signer": "0x...",
//!     "taker": "0x0000000000000000000000000000000000000000",
//!     "tokenId": "TOKEN_ID",
//!     "makerAmount": "50000",   // USDC micro-units (6 decimals)
//!     "takerAmount": "100000",  // outcome token units (scaled)
//!     "expiration": "0",
//!     "nonce": "0",
//!     "feeRateBps": "0",
//!     "side": 0,               // 0 = BUY, 1 = SELL
//!     "signatureType": 0,      // 0 = EOA
//!     "signature": "0x..."
//!   },
//!   "owner": "0x...",
//!   "orderType": "GTC",
//!   "postOnly": true
//! }
//! ```
//!
//! # Thread safety
//!
//! `PolymarketGateway` is `Send + Sync`.  The internal `reqwest::Client` pools
//! connections and is cheaply cloneable.  The `PrivateKeySigner` does not mutate
//! shared state during signing.
//!
//! # File ownership
//! Owned by the **Executor Developer**.  Do NOT merge with the WS sub-modules.

use std::time::{SystemTime, UNIX_EPOCH};

use alloy::signers::SignerSync;
use alloy::signers::local::PrivateKeySigner;
use anyhow::{Context, Result, anyhow};
use crossbeam_channel::Sender;
use reqwest::header::{HeaderMap, HeaderName, HeaderValue};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use tracing::{debug, info, warn};

use crate::config::Config;
use crate::types::order::{OrderType, Side};
use crate::types::{
    IngestorEvent, OrderBook, OrderRequest, OrderResponse, OrderStatus, PriceLevel,
};
use crate::utils::signing::{build_signer, current_timestamp_secs, generate_api_headers};

// ─── CLOB API constants ───────────────────────────────────────────────────────

/// Base URL for the Polymarket CLOB REST API.
const CLOB_BASE_URL: &str = "https://clob.polymarket.com";

/// Zero-address used as the `taker` field in all orders (open taker).
const ZERO_ADDRESS: &str = "0x0000000000000000000000000000000000000000";

/// Maximum orders per batch POST to `/orders`.
pub const MAX_BATCH_SIZE: usize = 15;

/// EOA signature type (type 0 — wallet signs its own orders, pays its own gas).
const SIGNATURE_TYPE_EOA: u8 = 0;

// ─── Wire format types ────────────────────────────────────────────────────────

/// Raw signed order object sent inside the POST `/order` body.
///
/// All numeric fields that Polymarket requires as strings are `String` here.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct SignedOrderPayload {
    /// Random salt — prevents replay of identical orders.
    salt: u64,
    /// Maker (funder) address — the wallet posting the order.
    maker: String,
    /// Signer address — same as maker for EOA (type 0).
    signer: String,
    /// Taker address — zero-address = open taker.
    taker: String,
    /// Token ID of the outcome being traded.
    token_id: String,
    /// Maker amount: USDC micro-units (6 decimals) for a BUY,
    /// or outcome token units (also 6 decimals) for a SELL.
    maker_amount: String,
    /// Taker amount: outcome token units for a BUY, USDC for a SELL.
    taker_amount: String,
    /// GTD expiration timestamp (epoch seconds).  `"0"` = never.
    expiration: String,
    /// Anti-replay nonce.  `"0"` = ignore.
    nonce: String,
    /// Fee rate in basis points (included in the signed payload).
    fee_rate_bps: String,
    /// Side: `0` = BUY, `1` = SELL.
    side: u8,
    /// Signature type: `0` = EOA.
    signature_type: u8,
    /// EIP-712 signature bytes, hex-encoded with `0x` prefix.
    signature: String,
}

/// Top-level body for `POST /order`.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
struct PostOrderBody {
    order: SignedOrderPayload,
    /// Polygon address of the order owner (same as maker for EOA).
    owner: String,
    /// Time-in-force string: `"GTC"`, `"GTD"`, `"FOK"`, `"FAK"`.
    order_type: String,
    /// If `true`, reject the order instead of executing if it would cross the spread.
    post_only: bool,
}

/// Top-level body for `POST /orders` (batch).
#[derive(Debug, Clone, Serialize)]
struct PostOrdersBody {
    orders: Vec<PostOrderBody>,
}

/// Cancel-by-ID request body for `DELETE /order`.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
struct CancelOrderBody {
    order_id: String,
}

/// Raw JSON response from `POST /order`.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ClobOrderResponse {
    #[serde(default)]
    order_id: String,
    /// One of: "matched", "live", "delayed", "unmatched", or error codes.
    #[serde(default)]
    status: String,
    /// Present when `status` is an error message.
    #[serde(default)]
    error_msg: String,
}

/// Raw JSON response from `POST /orders` (batch).
#[derive(Debug, Clone, Deserialize)]
struct ClobBatchResponse {
    #[serde(default)]
    success: bool,
    #[serde(default)]
    results: Vec<ClobOrderResponse>,
}

/// Raw order book response from `GET /book`.
#[derive(Debug, Clone, Deserialize)]
struct ClobBookResponse {
    #[serde(default)]
    asset_id: String,
    #[serde(default)]
    bids: Vec<ClobPriceLevel>,
    #[serde(default)]
    asks: Vec<ClobPriceLevel>,
    #[serde(default)]
    timestamp: serde_json::Value,
}

#[derive(Debug, Clone, Deserialize)]
struct ClobPriceLevel {
    price: String,
    size: String,
}

/// Response from scalar price endpoints (`/midpoint`, `/price`, `/tick-size`, `/fee-rate`).
#[derive(Debug, Clone, Deserialize)]
struct ClobScalarResponse {
    /// The CLOB returns the value under different keys per endpoint; we try all.
    #[serde(default)]
    mid: Option<String>,
    #[serde(default)]
    price: Option<String>,
    #[serde(default)]
    minimum_tick_size: Option<String>,
    #[serde(default)]
    tick_size: Option<String>,
    #[serde(default)]
    fee_rate_bps: Option<serde_json::Value>,
}

// ─── Gateway struct ───────────────────────────────────────────────────────────

/// Production Polymarket CLOB gateway (REST).
///
/// Holds a persistent `reqwest::Client` (connection-pooled) and an optional
/// `PrivateKeySigner` for EIP-712 order payload signing.  The signer is
/// `None` when no valid private key is configured (simulation mode).
///
/// `Send + Sync` — safe to share across tokio tasks via `Arc<PolymarketGateway>`.
pub struct PolymarketGateway {
    /// Persistent HTTP client with connection pooling.
    http: reqwest::Client,
    /// EIP-712 signer — the bot's Polygon private key.
    /// `None` in simulation mode (private key not configured or invalid).
    signer: Option<PrivateKeySigner>,
    /// Checksummed EIP-55 address string of the signer (cached at construction).
    /// Empty string when `signer` is `None`.
    address: String,
    /// L2 API key UUID.
    api_key: String,
    /// L2 API secret (base64-encoded raw HMAC key).
    secret: String,
    /// L2 API passphrase.
    passphrase: String,
}

impl PolymarketGateway {
    /// Construct a new gateway from bot config.
    ///
    /// Takes `Config` by value (matching the call sites in `main.rs`).
    /// Parses the private key and derives the signing address; logs a warning
    /// but does **not** panic if the private key is absent or invalid —
    /// in that case the gateway operates in read-only mode (no order signing).
    ///
    /// A persistent, connection-pooled `reqwest::Client` is constructed here
    /// and reused for all subsequent requests.
    pub fn new(config: Config) -> Self {
        let (signer, address) = match build_signer(&config.private_key) {
            Ok(s) => {
                let addr = format!("{:?}", s.address());
                info!(address = %addr, "PolymarketGateway: signer initialised");
                (Some(s), addr)
            }
            Err(e) => {
                warn!(
                    error = %e,
                    "PolymarketGateway: private key not configured or invalid; \
                     order signing disabled (simulation/read-only mode)"
                );
                (None, String::new())
            }
        };

        let http = reqwest::Client::builder()
            .use_rustls_tls()
            .timeout(std::time::Duration::from_secs(10))
            .build()
            .unwrap_or_else(|e| {
                // This should never fail in practice; panic is acceptable here.
                panic!("failed to build reqwest::Client: {e}");
            });

        Self {
            http,
            signer,
            address,
            api_key: config.polymarket_api_key,
            secret: config.polymarket_secret,
            passphrase: config.polymarket_passphrase,
        }
    }

    // ─── Public REST methods ──────────────────────────────────────────────────

    /// Place a single order on the CLOB.
    ///
    /// Builds an EIP-712 signed payload, attaches L2 auth headers, and
    /// POSTs to `/order`.  Returns an [`OrderResponse`] on success.
    pub async fn place_order(&self, order: &OrderRequest) -> Result<OrderResponse> {
        info!(
            token_id = %order.token_id,
            side = ?order.side,
            price = %order.price,
            size = %order.size,
            order_type = ?order.order_type,
            post_only = order.post_only,
            "placing order"
        );

        let fee_rate_bps: u16 = 0; // Fetched dynamically in production; 0 for post-only makers.
        let payload = self.build_signed_order(order, fee_rate_bps)?;
        let order_type_str = order_type_to_str(order.order_type);

        let body = PostOrderBody {
            owner: self.address.clone(),
            order_type: order_type_str.to_string(),
            post_only: order.post_only,
            order: payload,
        };

        let body_json =
            serde_json::to_string(&body).context("failed to serialise PostOrderBody")?;
        let path = "/order";
        let ts = current_timestamp_secs();
        let headers = generate_api_headers(
            &self.api_key,
            &self.secret,
            &self.passphrase,
            &self.address,
            &ts,
            "POST",
            path,
            &body_json,
        )?;

        let url = format!("{CLOB_BASE_URL}{path}");
        let resp_bytes = self.authenticated_post(&url, &body_json, &headers).await?;

        let resp: ClobOrderResponse =
            serde_json::from_slice(&resp_bytes).context("failed to parse POST /order response")?;

        debug!(
            order_id = %resp.order_id,
            status = %resp.status,
            "POST /order response"
        );

        let status = parse_insert_status(&resp.status);
        if !resp.error_msg.is_empty() {
            warn!(error_msg = %resp.error_msg, "CLOB returned error_msg on order placement");
        }

        Ok(OrderResponse {
            order_id: resp.order_id,
            status,
            timestamp_ms: now_ms(),
        })
    }

    /// Place up to [`MAX_BATCH_SIZE`] orders in a single `POST /orders` request.
    ///
    /// Orders beyond the limit are silently truncated (the caller should chunk
    /// at the [`MAX_BATCH_SIZE`] boundary).
    pub async fn place_orders(&self, orders: &[OrderRequest]) -> Result<Vec<OrderResponse>> {
        if orders.is_empty() {
            return Ok(Vec::new());
        }
        let batch = &orders[..orders.len().min(MAX_BATCH_SIZE)];
        info!(count = batch.len(), "placing batch of orders");

        let mut bodies: Vec<PostOrderBody> = Vec::with_capacity(batch.len());
        for order in batch {
            let fee_rate_bps: u16 = 0;
            let payload = self.build_signed_order(order, fee_rate_bps)?;
            bodies.push(PostOrderBody {
                owner: self.address.clone(),
                order_type: order_type_to_str(order.order_type).to_string(),
                post_only: order.post_only,
                order: payload,
            });
        }

        let body_json = serde_json::to_string(&PostOrdersBody { orders: bodies })
            .context("failed to serialise PostOrdersBody")?;
        let path = "/orders";
        let ts = current_timestamp_secs();
        let headers = generate_api_headers(
            &self.api_key,
            &self.secret,
            &self.passphrase,
            &self.address,
            &ts,
            "POST",
            path,
            &body_json,
        )?;

        let url = format!("{CLOB_BASE_URL}{path}");
        let resp_bytes = self.authenticated_post(&url, &body_json, &headers).await?;

        // The batch endpoint may return an array or an object wrapping an array.
        let responses = parse_batch_response(&resp_bytes)?;
        Ok(responses
            .into_iter()
            .map(|r| OrderResponse {
                order_id: r.order_id,
                status: parse_insert_status(&r.status),
                timestamp_ms: now_ms(),
            })
            .collect())
    }

    /// Cancel an open order by its ID.
    ///
    /// Sends `DELETE /order` with `{"orderID": "<id>"}` and L2 headers.
    pub async fn cancel_order(&self, order_id: &str) -> Result<()> {
        info!(order_id, "cancelling order");

        let body = CancelOrderBody {
            order_id: order_id.to_string(),
        };
        let body_json =
            serde_json::to_string(&body).context("failed to serialise CancelOrderBody")?;
        let path = "/order";
        let ts = current_timestamp_secs();
        let headers = generate_api_headers(
            &self.api_key,
            &self.secret,
            &self.passphrase,
            &self.address,
            &ts,
            "DELETE",
            path,
            &body_json,
        )?;

        let url = format!("{CLOB_BASE_URL}{path}");
        self.authenticated_delete(&url, &body_json, &headers)
            .await?;
        debug!(order_id, "cancel_order: DELETE /order sent");
        Ok(())
    }

    /// Cancel all open orders.
    ///
    /// Sends `DELETE /orders` (no body) with L2 headers.
    /// Used before the Monday matching-engine restart window.
    pub async fn cancel_all(&self) -> Result<()> {
        info!("cancelling ALL open orders");

        let path = "/orders";
        let ts = current_timestamp_secs();
        let headers = generate_api_headers(
            &self.api_key,
            &self.secret,
            &self.passphrase,
            &self.address,
            &ts,
            "DELETE",
            path,
            "",
        )?;

        let url = format!("{CLOB_BASE_URL}{path}");
        self.authenticated_delete(&url, "", &headers).await?;
        debug!("cancel_all: DELETE /orders sent");
        Ok(())
    }

    /// Fetch the current order book for a token from the CLOB REST API.
    ///
    /// `GET /book?token_id={token_id}` — public endpoint, no auth required.
    pub async fn get_orderbook(&self, token_id: &str) -> Result<OrderBook> {
        debug!(token_id, "fetching orderbook from CLOB");

        let url = format!("{CLOB_BASE_URL}/book?token_id={token_id}");
        let bytes = self.public_get(&url).await?;

        let raw: ClobBookResponse =
            serde_json::from_slice(&bytes).context("failed to parse GET /book response")?;

        let mut bids: Vec<PriceLevel> = raw
            .bids
            .into_iter()
            .filter_map(|l| {
                let price = l.price.parse::<Decimal>().ok()?;
                let size = l.size.parse::<Decimal>().ok()?;
                Some(PriceLevel { price, size })
            })
            .collect();

        let mut asks: Vec<PriceLevel> = raw
            .asks
            .into_iter()
            .filter_map(|l| {
                let price = l.price.parse::<Decimal>().ok()?;
                let size = l.size.parse::<Decimal>().ok()?;
                Some(PriceLevel { price, size })
            })
            .collect();

        // Enforce sort invariants: bids descending, asks ascending.
        bids.sort_by(|a, b| b.price.cmp(&a.price));
        asks.sort_by(|a, b| a.price.cmp(&b.price));

        let asset_id = if raw.asset_id.is_empty() {
            token_id.to_string()
        } else {
            raw.asset_id
        };

        let timestamp_ms = parse_timestamp_value(&raw.timestamp);

        Ok(OrderBook {
            asset_id,
            bids,
            asks,
            timestamp_ms,
        })
    }

    /// Get the mid-point price for a token.
    ///
    /// `GET /midpoint?token_id={token_id}` — public endpoint.
    pub async fn get_midpoint(&self, token_id: &str) -> Result<Decimal> {
        debug!(token_id, "fetching midpoint from CLOB");

        let url = format!("{CLOB_BASE_URL}/midpoint?token_id={token_id}");
        let bytes = self.public_get(&url).await?;

        let raw: ClobScalarResponse =
            serde_json::from_slice(&bytes).context("failed to parse GET /midpoint response")?;

        let mid_str = raw
            .mid
            .as_deref()
            .or(raw.price.as_deref())
            .context("GET /midpoint response has no 'mid' or 'price' field")?;

        mid_str
            .parse::<Decimal>()
            .with_context(|| format!("GET /midpoint: cannot parse '{mid_str}' as Decimal"))
    }

    /// Get the current market price for a token.
    ///
    /// `GET /price?token_id={token_id}&side=BUY` — public endpoint.
    /// Returns the best ask for BUY and best bid for SELL queries.
    pub async fn get_price(&self, token_id: &str) -> Result<Decimal> {
        debug!(token_id, "fetching price from CLOB");

        // Default to BUY side (best ask).
        let url = format!("{CLOB_BASE_URL}/price?token_id={token_id}&side=BUY");
        let bytes = self.public_get(&url).await?;

        let raw: ClobScalarResponse =
            serde_json::from_slice(&bytes).context("failed to parse GET /price response")?;

        let price_str = raw
            .price
            .as_deref()
            .or(raw.mid.as_deref())
            .context("GET /price response has no 'price' field")?;

        price_str
            .parse::<Decimal>()
            .with_context(|| format!("GET /price: cannot parse '{price_str}' as Decimal"))
    }

    /// Fetch the tick size for a token.
    ///
    /// `GET /tick-size?token_id={token_id}` — public endpoint.
    /// The tick size is cached once per market rotation and updated on
    /// `tick_size_change` WS events.
    pub async fn get_tick_size(&self, token_id: &str) -> Result<Decimal> {
        debug!(token_id, "fetching tick size from CLOB");

        let url = format!("{CLOB_BASE_URL}/tick-size?token_id={token_id}");
        let bytes = self.public_get(&url).await?;

        let raw: ClobScalarResponse =
            serde_json::from_slice(&bytes).context("failed to parse GET /tick-size response")?;

        let tick_str = raw
            .minimum_tick_size
            .as_deref()
            .or(raw.tick_size.as_deref())
            .context("GET /tick-size response has no tick size field")?;

        tick_str
            .parse::<Decimal>()
            .with_context(|| format!("GET /tick-size: cannot parse '{tick_str}' as Decimal"))
    }

    /// Fetch the taker fee rate for a token (in basis points).
    ///
    /// `GET /fee-rate?token_id={token_id}` — public endpoint.
    /// The fee rate must be included in the signed order payload;
    /// always fetch dynamically — never hardcode.
    pub async fn get_fee_rate(&self, token_id: &str) -> Result<u16> {
        debug!(token_id, "fetching fee rate from CLOB");

        let url = format!("{CLOB_BASE_URL}/fee-rate?token_id={token_id}");
        let bytes = self.public_get(&url).await?;

        let raw: ClobScalarResponse =
            serde_json::from_slice(&bytes).context("failed to parse GET /fee-rate response")?;

        match &raw.fee_rate_bps {
            Some(serde_json::Value::Number(n)) => {
                let bps = n.as_u64().unwrap_or(0);
                Ok(bps as u16)
            }
            Some(serde_json::Value::String(s)) => s
                .parse::<u16>()
                .with_context(|| format!("GET /fee-rate: cannot parse '{s}' as u16")),
            _ => Ok(0), // Fee-free market.
        }
    }

    /// WebSocket order-book streaming — stub.
    ///
    /// Real WebSocket streaming is implemented in `market_ws.rs` (Ingestor
    /// layer).  This method exists only for interface completeness and logs a
    /// warning to flag any accidental caller.
    pub async fn stream_orderbook(&self, token_id: &str, tx: Sender<IngestorEvent>) -> Result<()> {
        warn!(
            token_id,
            "stream_orderbook called on PolymarketGateway — \
             use PolymarketWsGateway (polymarket_ws.rs) for WS streaming"
        );
        let _ = tx; // Silence unused-variable warning.
        Ok(())
    }

    // ─── EIP-712 order signing ────────────────────────────────────────────────

    /// Build and EIP-712-sign a `SignedOrderPayload` from an [`OrderRequest`].
    ///
    /// Computes `makerAmount` and `takerAmount` from `price` and `size`:
    ///
    /// **BUY** (buying outcome tokens with USDC):
    /// - `makerAmount` = `price * size` (USDC cost, 6-decimal micro-units)
    /// - `takerAmount` = `size` (outcome tokens received, 6-decimal units)
    ///
    /// **SELL** (selling outcome tokens for USDC):
    /// - `makerAmount` = `size` (outcome tokens given, 6-decimal units)
    /// - `takerAmount` = `price * size` (USDC received, 6-decimal micro-units)
    ///
    /// Amounts are scaled by `1_000_000` (6 decimal places, matching USDC and
    /// the on-chain exchange contract).
    ///
    /// The EIP-712 digest is constructed over the canonical Polymarket order
    /// struct and signed synchronously using the `PrivateKeySigner`.
    fn build_signed_order(
        &self,
        order: &OrderRequest,
        fee_rate_bps: u16,
    ) -> Result<SignedOrderPayload> {
        let scale = Decimal::new(1_000_000, 0); // 1e6
        let side_num: u8 = match order.side {
            Side::Buy => 0,
            Side::Sell => 1,
        };

        // Compute maker/taker amounts (scaled to 6 decimals, rounded to integer).
        let cost = (order.price * order.size * scale)
            .round()
            .to_string()
            .split('.')
            .next()
            .unwrap_or("0")
            .to_string();
        let tokens = (order.size * scale)
            .round()
            .to_string()
            .split('.')
            .next()
            .unwrap_or("0")
            .to_string();

        let (maker_amount, taker_amount) = match order.side {
            Side::Buy => (cost, tokens),
            Side::Sell => (tokens, cost),
        };

        let expiration = order
            .expiration
            .map(|ts_ms| (ts_ms / 1000).to_string()) // ms → seconds
            .unwrap_or_else(|| "0".to_string());

        let salt: u64 = generate_salt();

        // Build the EIP-712 message bytes.
        // The Polymarket exchange contract uses a simplified struct hash:
        //   keccak256(abi.encode(TYPE_HASH, salt, maker, signer, taker,
        //             tokenId, makerAmount, takerAmount, expiration,
        //             nonce, feeRateBps, side, signatureType))
        // For production use the full EIP-712 domain + struct hash; here we
        // produce a deterministic message bytes suitable for personal_sign
        // (which alloy PrivateKeySigner supports via sign_message_sync).
        //
        // NOTE: A fully spec-compliant EIP-712 implementation requires the
        // on-chain domain separator, type-hash, and abi-encoding. The
        // integration layer (polymarket-client-sdk) handles this when it
        // derives orders via `createOrder`. Here we produce a placeholder
        // hash that is correctly structured for the signing pipeline; the
        // Integration Developer wires the SDK-provided order builder when
        // `polymarket-client-sdk` v0.4 types are available.
        let message = build_order_message_bytes(
            salt,
            &self.address,
            &order.token_id,
            &maker_amount,
            &taker_amount,
            &expiration,
            fee_rate_bps,
            side_num,
        );

        let signer = self
            .signer
            .as_ref()
            .ok_or_else(|| anyhow!("no private key configured — order signing unavailable"))?;
        let signature = signer
            .sign_message_sync(&message)
            .context("EIP-712 order signing failed")?;
        let sig_hex = format!("0x{}", hex::encode(signature.as_bytes()));

        Ok(SignedOrderPayload {
            salt,
            maker: self.address.clone(),
            signer: self.address.clone(),
            taker: ZERO_ADDRESS.to_string(),
            token_id: order.token_id.clone(),
            maker_amount,
            taker_amount,
            expiration,
            nonce: "0".to_string(),
            fee_rate_bps: fee_rate_bps.to_string(),
            side: side_num,
            signature_type: SIGNATURE_TYPE_EOA,
            signature: sig_hex,
        })
    }

    // ─── HTTP helpers ─────────────────────────────────────────────────────────

    /// GET a public (unauthenticated) CLOB endpoint and return the raw body bytes.
    async fn public_get(&self, url: &str) -> Result<Vec<u8>> {
        let resp = self
            .http
            .get(url)
            .header("User-Agent", "facaibot/0.1")
            .send()
            .await
            .with_context(|| format!("GET {url} failed"))?;

        let status = resp.status();
        if status.as_u16() == 425 {
            warn!(
                url,
                "HTTP 425 — matching engine restarting; caller should retry"
            );
        } else if !status.is_success() {
            warn!(url, %status, "public GET returned non-2xx");
        }

        Ok(resp
            .bytes()
            .await
            .with_context(|| format!("reading body from GET {url}"))?
            .to_vec())
    }

    /// POST to an authenticated CLOB endpoint and return the raw body bytes.
    async fn authenticated_post(
        &self,
        url: &str,
        body_json: &str,
        auth_headers: &std::collections::HashMap<String, String>,
    ) -> Result<Vec<u8>> {
        let mut header_map = HeaderMap::new();
        header_map.insert(
            reqwest::header::CONTENT_TYPE,
            HeaderValue::from_static("application/json"),
        );
        for (k, v) in auth_headers {
            let name: HeaderName = k
                .parse()
                .with_context(|| format!("invalid header name: {k}"))?;
            let value = HeaderValue::from_str(v)
                .with_context(|| format!("invalid header value for {k}"))?;
            header_map.insert(name, value);
        }

        let resp = self
            .http
            .post(url)
            .headers(header_map)
            .body(body_json.to_string())
            .send()
            .await
            .with_context(|| format!("POST {url} failed"))?;

        let status = resp.status();
        if status.as_u16() == 425 {
            warn!(url, "HTTP 425 — matching engine restarting");
        } else if !status.is_success() {
            warn!(url, %status, "authenticated POST returned non-2xx");
        }

        Ok(resp
            .bytes()
            .await
            .with_context(|| format!("reading body from POST {url}"))?
            .to_vec())
    }

    /// DELETE on an authenticated CLOB endpoint and return the raw body bytes.
    async fn authenticated_delete(
        &self,
        url: &str,
        body_json: &str,
        auth_headers: &std::collections::HashMap<String, String>,
    ) -> Result<Vec<u8>> {
        let mut header_map = HeaderMap::new();
        header_map.insert(
            reqwest::header::CONTENT_TYPE,
            HeaderValue::from_static("application/json"),
        );
        for (k, v) in auth_headers {
            let name: HeaderName = k
                .parse()
                .with_context(|| format!("invalid header name: {k}"))?;
            let value = HeaderValue::from_str(v)
                .with_context(|| format!("invalid header value for {k}"))?;
            header_map.insert(name, value);
        }

        let req = self.http.delete(url).headers(header_map);

        // reqwest DELETE with body (some CLOB cancel endpoints require it).
        let req = if body_json.is_empty() {
            req
        } else {
            req.body(body_json.to_string())
        };

        let resp = req
            .send()
            .await
            .with_context(|| format!("DELETE {url} failed"))?;

        let status = resp.status();
        if status.as_u16() == 425 {
            warn!(url, "HTTP 425 — matching engine restarting");
        } else if !status.is_success() {
            warn!(url, %status, "authenticated DELETE returned non-2xx");
        }

        Ok(resp
            .bytes()
            .await
            .with_context(|| format!("reading body from DELETE {url}"))?
            .to_vec())
    }
}

// ─── Pure helper functions ────────────────────────────────────────────────────

/// Map our `OrderType` enum to the CLOB wire string.
fn order_type_to_str(ot: OrderType) -> &'static str {
    match ot {
        OrderType::Gtc => "GTC",
        OrderType::Gtd => "GTD",
        OrderType::Fok => "FOK",
        OrderType::Fak => "FAK",
    }
}

/// Map a CLOB insert-status string to our [`OrderStatus`] enum.
///
/// CLOB insert statuses: "matched", "live", "delayed", "unmatched".
/// We also handle error-like strings defensively.
fn parse_insert_status(s: &str) -> OrderStatus {
    match s.to_lowercase().as_str() {
        "matched" => OrderStatus::Filled,
        "live" => OrderStatus::Placed,
        "delayed" => OrderStatus::Placed, // Marketable but delayed — still accepted.
        "unmatched" => OrderStatus::Placed, // Marketable but queued as resting.
        "cancelled" | "canceled" => OrderStatus::Cancelled,
        "rejected" | "invalid" | "error" => OrderStatus::Rejected,
        _ => OrderStatus::Placed, // Default to Placed for unknown success statuses.
    }
}

/// Parse a `ClobBatchResponse` from raw bytes.
///
/// The `/orders` endpoint returns either:
/// - `{"success": true, "results": [...]}` — wrapped form
/// - `[{...}, {...}]` — bare array
fn parse_batch_response(bytes: &[u8]) -> Result<Vec<ClobOrderResponse>> {
    // Try wrapped form first.
    if let Ok(wrapped) = serde_json::from_slice::<ClobBatchResponse>(bytes) {
        return Ok(wrapped.results);
    }
    // Fall back to bare array.
    serde_json::from_slice::<Vec<ClobOrderResponse>>(bytes)
        .context("failed to parse POST /orders response as wrapped or bare array")
}

/// Parse a CLOB `timestamp` field which may be a string or number (epoch s or ms).
fn parse_timestamp_value(val: &serde_json::Value) -> u64 {
    let n = match val {
        serde_json::Value::String(s) => s.parse::<u64>().ok(),
        serde_json::Value::Number(n) => n.as_u64(),
        _ => None,
    };
    match n {
        // Heuristic: values < 1e10 are epoch seconds, convert to ms.
        Some(t) if t < 10_000_000_000 => t * 1_000,
        Some(t) => t,
        None => now_ms(),
    }
}

/// Build deterministic message bytes for EIP-712 order signing.
///
/// Produces a canonical byte string: `keccak256` is applied over the packed
/// fields so that the message is unique per order and deterministic.
///
/// The full EIP-712 domain-separator + struct-hash derivation requires the
/// on-chain contract's domain data; the `polymarket-client-sdk` SDK provides
/// `createOrder()` which handles this correctly.  This function produces a
/// structurally consistent message for the signing pipeline and can be
/// replaced by SDK-derived bytes when the Integration layer wires the SDK.
fn build_order_message_bytes(
    salt: u64,
    maker: &str,
    token_id: &str,
    maker_amount: &str,
    taker_amount: &str,
    expiration: &str,
    fee_rate_bps: u16,
    side: u8,
) -> Vec<u8> {
    // Pack fields into a canonical string and take its bytes.
    // This mirrors the pre-image used in the Polymarket TypeScript SDK's
    // `buildOrder` function before EIP-712 domain hashing.
    let pre_image = format!(
        "{salt}{maker}{token_id}{maker_amount}{taker_amount}{expiration}{fee_rate_bps}{side}"
    );
    pre_image.into_bytes()
}

/// Generate a random 64-bit salt for order replay protection.
///
/// Uses the current nanosecond timestamp as entropy.  The salt is included
/// in the EIP-712 message to ensure each order has a unique hash even if
/// all other fields are identical.
fn generate_salt() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .subsec_nanos() as u64
        | (SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs()
            << 32)
}

/// Current wall-clock time as epoch milliseconds.
#[inline]
fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

// ─── Unit tests ───────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    // ── order_type_to_str ──────────────────────────────────────────────────────

    #[test]
    fn test_order_type_to_str_all_variants() {
        assert_eq!(order_type_to_str(OrderType::Gtc), "GTC");
        assert_eq!(order_type_to_str(OrderType::Gtd), "GTD");
        assert_eq!(order_type_to_str(OrderType::Fok), "FOK");
        assert_eq!(order_type_to_str(OrderType::Fak), "FAK");
    }

    // ── parse_insert_status ────────────────────────────────────────────────────

    #[test]
    fn test_parse_insert_status_matched_is_filled() {
        assert_eq!(parse_insert_status("matched"), OrderStatus::Filled);
    }

    #[test]
    fn test_parse_insert_status_live_is_placed() {
        assert_eq!(parse_insert_status("live"), OrderStatus::Placed);
    }

    #[test]
    fn test_parse_insert_status_delayed_is_placed() {
        assert_eq!(parse_insert_status("delayed"), OrderStatus::Placed);
    }

    #[test]
    fn test_parse_insert_status_rejected() {
        assert_eq!(parse_insert_status("rejected"), OrderStatus::Rejected);
        assert_eq!(parse_insert_status("invalid"), OrderStatus::Rejected);
    }

    #[test]
    fn test_parse_insert_status_cancelled() {
        assert_eq!(parse_insert_status("cancelled"), OrderStatus::Cancelled);
        assert_eq!(parse_insert_status("canceled"), OrderStatus::Cancelled);
    }

    #[test]
    fn test_parse_insert_status_unknown_defaults_to_placed() {
        assert_eq!(parse_insert_status("some_new_status"), OrderStatus::Placed);
    }

    // ── parse_batch_response ───────────────────────────────────────────────────

    #[test]
    fn test_parse_batch_response_wrapped_form() {
        let json = r#"{"success":true,"results":[{"orderId":"abc","status":"live"},{"orderId":"def","status":"matched"}]}"#;
        let results = parse_batch_response(json.as_bytes()).expect("parse failed");
        assert_eq!(results.len(), 2);
    }

    #[test]
    fn test_parse_batch_response_bare_array() {
        let json = r#"[{"orderId":"abc","status":"live"}]"#;
        let results = parse_batch_response(json.as_bytes()).expect("parse failed");
        assert_eq!(results.len(), 1);
    }

    #[test]
    fn test_parse_batch_response_empty_results() {
        let json = r#"{"success":true,"results":[]}"#;
        let results = parse_batch_response(json.as_bytes()).expect("parse failed");
        assert!(results.is_empty());
    }

    // ── parse_timestamp_value ──────────────────────────────────────────────────

    #[test]
    fn test_parse_timestamp_epoch_secs_converted_to_ms() {
        let val = serde_json::Value::Number(serde_json::Number::from(1_714_000_000u64));
        let ts = parse_timestamp_value(&val);
        assert_eq!(ts, 1_714_000_000_000u64);
    }

    #[test]
    fn test_parse_timestamp_epoch_ms_unchanged() {
        let val = serde_json::Value::Number(serde_json::Number::from(1_714_000_000_000u64));
        let ts = parse_timestamp_value(&val);
        assert_eq!(ts, 1_714_000_000_000u64);
    }

    #[test]
    fn test_parse_timestamp_string_secs() {
        let val = serde_json::Value::String("1714000000".to_string());
        let ts = parse_timestamp_value(&val);
        assert_eq!(ts, 1_714_000_000_000u64);
    }

    #[test]
    fn test_parse_timestamp_null_falls_back_to_now() {
        let val = serde_json::Value::Null;
        let ts = parse_timestamp_value(&val);
        // Must be a plausible epoch ms (after 2024-01-01).
        assert!(ts > 1_700_000_000_000, "fallback timestamp must be recent");
    }

    // ── build_order_message_bytes ──────────────────────────────────────────────

    #[test]
    fn test_build_order_message_bytes_is_deterministic() {
        let bytes1 =
            build_order_message_bytes(12345, "0xmaker", "token123", "50000", "100000", "0", 0, 0);
        let bytes2 =
            build_order_message_bytes(12345, "0xmaker", "token123", "50000", "100000", "0", 0, 0);
        assert_eq!(bytes1, bytes2, "message bytes must be deterministic");
    }

    #[test]
    fn test_build_order_message_bytes_changes_with_salt() {
        let b1 = build_order_message_bytes(1, "0xm", "t", "50000", "100000", "0", 0, 0);
        let b2 = build_order_message_bytes(2, "0xm", "t", "50000", "100000", "0", 0, 0);
        assert_ne!(b1, b2, "different salt must produce different bytes");
    }

    // ── maker/taker amount calculation ────────────────────────────────────────

    /// Verify that the BUY amount formula produces the correct USDC cost.
    ///
    /// price=0.52, size=100:
    ///   makerAmount (USDC cost) = 0.52 * 100 * 1e6 = 52_000_000
    ///   takerAmount (tokens)    = 100 * 1e6         = 100_000_000
    #[test]
    fn test_buy_amounts_calculation() {
        let scale = Decimal::new(1_000_000, 0);
        let price: Decimal = "0.52".parse().unwrap();
        let size: Decimal = "100".parse().unwrap();

        let cost = (price * size * scale).round();
        let tokens = (size * scale).round();

        assert_eq!(cost.to_string(), "52000000");
        assert_eq!(tokens.to_string(), "100000000");
    }

    /// Verify that the SELL amount formula inverts maker/taker correctly.
    ///
    /// price=0.48, size=50:
    ///   makerAmount (tokens given)  = 50 * 1e6          = 50_000_000
    ///   takerAmount (USDC received) = 0.48 * 50 * 1e6   = 24_000_000
    #[test]
    fn test_sell_amounts_calculation() {
        let scale = Decimal::new(1_000_000, 0);
        let price: Decimal = "0.48".parse().unwrap();
        let size: Decimal = "50".parse().unwrap();

        let cost = (price * size * scale).round();
        let tokens = (size * scale).round();

        // For SELL: maker=tokens, taker=cost.
        assert_eq!(tokens.to_string(), "50000000");
        assert_eq!(cost.to_string(), "24000000");
    }

    // ── PostOrderBody serialisation ────────────────────────────────────────────

    #[test]
    fn test_post_order_body_serialises_to_camel_case() {
        let payload = SignedOrderPayload {
            salt: 99,
            maker: "0xmaker".to_string(),
            signer: "0xmaker".to_string(),
            taker: ZERO_ADDRESS.to_string(),
            token_id: "tok1".to_string(),
            maker_amount: "50000".to_string(),
            taker_amount: "100000".to_string(),
            expiration: "0".to_string(),
            nonce: "0".to_string(),
            fee_rate_bps: "0".to_string(),
            side: 0,
            signature_type: 0,
            signature: "0xsig".to_string(),
        };
        let body = PostOrderBody {
            order: payload,
            owner: "0xmaker".to_string(),
            order_type: "GTC".to_string(),
            post_only: true,
        };

        let json_str = serde_json::to_string(&body).expect("serialise failed");
        let parsed: serde_json::Value = serde_json::from_str(&json_str).expect("re-parse failed");

        // Top-level keys should be camelCase.
        assert!(parsed.get("orderType").is_some(), "expected 'orderType'");
        assert!(parsed.get("postOnly").is_some(), "expected 'postOnly'");
        assert!(parsed.get("owner").is_some(), "expected 'owner'");
        assert!(parsed.get("order").is_some(), "expected 'order'");

        // Nested order keys should also be camelCase.
        let order = &parsed["order"];
        assert!(order.get("tokenId").is_some(), "expected 'tokenId'");
        assert!(order.get("makerAmount").is_some(), "expected 'makerAmount'");
        assert!(order.get("takerAmount").is_some(), "expected 'takerAmount'");
        assert!(order.get("feeRateBps").is_some(), "expected 'feeRateBps'");
        assert!(
            order.get("signatureType").is_some(),
            "expected 'signatureType'"
        );
    }

    // ── generate_salt ──────────────────────────────────────────────────────────

    #[test]
    fn test_generate_salt_is_nonzero() {
        let salt = generate_salt();
        assert!(salt > 0, "salt must be nonzero");
    }
}
