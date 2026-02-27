//! Polymarket CLOB REST gateway — Layer 3 Executor ("The Hand").
//!
//! Wraps every Polymarket CLOB HTTP endpoint needed by the Executor layer:
//!
//! - [`PolymarketGateway::place_order`] — SDK-signed order POST to `/order`.
//! - [`PolymarketGateway::cancel_order`] — Cancel by ID via SDK.
//! - [`PolymarketGateway::cancel_all`] — Cancel all open orders via SDK.
//! - [`PolymarketGateway::get_orderbook`] — GET `/book?token_id={id}`.
//! - [`PolymarketGateway::get_midpoint`] — GET `/midpoint?token_id={id}`.
//! - [`PolymarketGateway::get_price`] — GET `/price?token_id={id}`.
//! - [`PolymarketGateway::get_tick_size`] — GET `/tick-size?token_id={id}`.
//! - [`PolymarketGateway::stream_orderbook`] — Stub; real WS is in `market_ws.rs`.
//!
//! # Authentication
//!
//! Order placement uses the official `polymarket-client-sdk` for correct EIP-712
//! signing, automatic fee rate fetching, and tick size validation. Cancel operations
//! also go through the SDK's authenticated client.
//!
//! Public read endpoints (orderbook, midpoint, etc.) use a lightweight `reqwest`
//! client directly — no auth required.
//!
//! # Thread safety
//!
//! `PolymarketGateway` is `Send + Sync`.  The SDK client uses `Arc` internally
//! and is cheaply cloneable.
//!
//! # File ownership
//! Owned by the **Executor Developer**.  Do NOT merge with the WS sub-modules.

use std::str::FromStr;
use std::time::{SystemTime, UNIX_EPOCH};

use alloy::primitives::U256;
use alloy::signers::Signer as _;
use alloy::signers::local::PrivateKeySigner;
use anyhow::{Context, Result, anyhow};
use crossbeam_channel::Sender;
use polymarket_client_sdk::POLYGON;
use polymarket_client_sdk::auth::state::Authenticated;
use polymarket_client_sdk::auth::{Credentials, Normal};
use polymarket_client_sdk::clob::types::response::PostOrderResponse;
use polymarket_client_sdk::clob::types::{
    OrderStatusType, OrderType as SdkOrderType, Side as SdkSide,
};
use polymarket_client_sdk::clob::{Client as SdkClient, Config as SdkConfig};
use rust_decimal::Decimal;
use serde::Deserialize;
use tracing::{debug, info, warn};
use uuid::Uuid;

use crate::config::Config;
use crate::types::order::{OrderType, Side};
use crate::types::{
    IngestorEvent, OrderBook, OrderRequest, OrderResponse, OrderStatus, PriceLevel,
};
use crate::utils::signing::build_signer;

// ─── CLOB API constants ───────────────────────────────────────────────────────

/// Base URL for the Polymarket CLOB REST API (used for public GET endpoints).
const CLOB_BASE_URL: &str = "https://clob.polymarket.com";

// ─── Wire format types (public GET endpoints only) ───────────────────────────

/// Raw order book response from `GET /book`.
#[allow(dead_code)] // used by REST accessors (live mode market param fetching)
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

#[allow(dead_code)] // used by REST accessors (live mode market param fetching)
#[derive(Debug, Clone, Deserialize)]
struct ClobPriceLevel {
    price: String,
    size: String,
}

/// Response from scalar price endpoints (`/midpoint`, `/price`, `/tick-size`).
#[allow(dead_code)] // used by REST accessors (live mode market param fetching)
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
}

// ─── Gateway struct ───────────────────────────────────────────────────────────

/// Production Polymarket CLOB gateway (REST).
///
/// Uses the official `polymarket-client-sdk` for authenticated operations
/// (order placement, signing, cancellation) and a lightweight `reqwest::Client`
/// for public read endpoints.
///
/// `Send + Sync` — safe to share across tokio tasks via `Arc<PolymarketGateway>`.
pub struct PolymarketGateway {
    /// Persistent HTTP client for public (unauthenticated) GET endpoints.
    http: reqwest::Client,
    /// EIP-712 signer with chain_id=137 (Polygon) set. Used for SDK `sign()` calls.
    /// `None` when no valid private key is configured (simulation mode).
    signer: Option<PrivateKeySigner>,
    /// Checksummed EIP-55 address string of the signer (cached at construction).
    /// Empty string when `signer` is `None`.
    #[allow(dead_code)] // used for logging
    address: String,
    /// Authenticated SDK CLOB client. Handles EIP-712 signing, fee rate caching,
    /// tick size validation, and L2 HMAC auth internally.
    /// `None` when credentials are not configured (simulation mode).
    sdk_client: Option<SdkClient<Authenticated<Normal>>>,
}

impl PolymarketGateway {
    /// Construct a new gateway from bot config.
    ///
    /// **Async** — initializes the SDK CLOB client with pre-existing L2 credentials.
    /// If the private key or API credentials are invalid/missing, the gateway
    /// operates in read-only mode (no order signing or placement).
    pub async fn new(config: Config) -> Self {
        // Build signer with chain_id for Polygon mainnet.
        let (signer, address) = match build_signer(&config.private_key) {
            Ok(s) => {
                let addr = format!("{:?}", s.address());
                info!(address = %addr, "PolymarketGateway: signer initialised");
                (Some(s.with_chain_id(Some(POLYGON))), addr)
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

        // Initialize SDK CLOB client (only if signer + credentials are available).
        let sdk_client = if let Some(ref signer) = signer {
            match init_sdk_client(signer, &config).await {
                Ok(client) => {
                    info!("PolymarketGateway: SDK CLOB client authenticated");
                    Some(client)
                }
                Err(e) => {
                    warn!(
                        error = %e,
                        "PolymarketGateway: SDK CLOB client init failed — \
                         order placement disabled (read-only mode)"
                    );
                    None
                }
            }
        } else {
            None
        };

        let http = reqwest::Client::builder()
            .use_rustls_tls()
            .timeout(std::time::Duration::from_secs(10))
            .build()
            .unwrap_or_else(|e| {
                panic!("failed to build reqwest::Client: {e}");
            });

        Self {
            http,
            signer,
            address,
            sdk_client,
        }
    }

    // ─── Public REST methods ──────────────────────────────────────────────────

    /// Place a single order on the CLOB.
    ///
    /// Uses the SDK to build, sign, and post the order. The SDK handles:
    /// - EIP-712 typed data signing (correct domain separator for CTF/NegRisk exchange)
    /// - Automatic fee rate fetching and caching per token
    /// - Tick size and lot size validation
    /// - Maker/taker amount calculation
    pub async fn place_order(&self, order: &OrderRequest) -> Result<OrderResponse> {
        info!(
            token_id = %order.token_id,
            side = ?order.side,
            price = %order.price,
            size = %order.size,
            order_type = ?order.order_type,
            post_only = order.post_only,
            "placing order via SDK"
        );

        let sdk = self
            .sdk_client
            .as_ref()
            .ok_or_else(|| anyhow!("SDK client not initialized — order placement unavailable"))?;
        let signer = self
            .signer
            .as_ref()
            .ok_or_else(|| anyhow!("no private key configured — order signing unavailable"))?;

        // Parse token_id as U256 (large decimal string → U256).
        let token_id = U256::from_str(&order.token_id)
            .with_context(|| format!("failed to parse token_id '{}' as U256", order.token_id))?;

        let sdk_side = to_sdk_side(order.side);
        let sdk_order_type = to_sdk_order_type(order.order_type);

        // Build: SDK fetches tick_size (and fee rate internally) from CLOB, cached per token.
        let mut builder = sdk
            .limit_order()
            .token_id(token_id)
            .side(sdk_side)
            .price(order.price)
            .size(order.size)
            .order_type(sdk_order_type)
            .post_only(order.post_only);

        // Set expiration for GTD orders.
        if let Some(exp_ms) = order.expiration {
            if let Some(dt) = polymarket_client_sdk::types::DateTime::from_timestamp((exp_ms / 1000) as i64, 0) {
                builder = builder.expiration(dt);
            }
        }

        let signable = builder
            .build()
            .await
            .map_err(|e| anyhow!("SDK order build failed: {e}"))?;

        // Sign: EIP-712 typed data with correct domain separator (auto-detects neg_risk).
        let signed = sdk
            .sign(signer, signable)
            .await
            .map_err(|e| anyhow!("SDK order signing failed: {e}"))?;

        // Post: sends authenticated request to CLOB with L2 HMAC headers.
        let resp: PostOrderResponse = sdk
            .post_order(signed)
            .await
            .map_err(|e| anyhow!("SDK post_order failed: {e}"))?;

        debug!(
            order_id = %resp.order_id,
            status = ?resp.status,
            success = resp.success,
            "POST /order response"
        );

        let status = if !resp.success {
            OrderStatus::Rejected
        } else {
            map_sdk_status(&resp.status)
        };

        if let Some(ref msg) = resp.error_msg {
            if !msg.is_empty() {
                warn!(error_msg = %msg, "CLOB returned error_msg on order placement");
            }
        }

        Ok(OrderResponse {
            order_id: resp.order_id,
            status,
            timestamp_ms: now_ms(),
        })
    }

    /// Cancel an open order by its ID.
    ///
    /// Uses the SDK's authenticated `cancel_order` method (DELETE `/order`).
    pub async fn cancel_order(&self, order_id: &str) -> Result<()> {
        info!(order_id, "cancelling order via SDK");

        let sdk = self
            .sdk_client
            .as_ref()
            .ok_or_else(|| anyhow!("SDK client not initialized — cancel unavailable"))?;

        let resp = sdk
            .cancel_order(order_id)
            .await
            .map_err(|e| anyhow!("cancel_order failed: {e}"))?;

        debug!(order_id, ?resp.canceled, "cancel_order response");
        Ok(())
    }

    /// Cancel all open orders.
    ///
    /// Uses the SDK's authenticated `cancel_all_orders` method (DELETE `/cancel-all`).
    pub async fn cancel_all(&self) -> Result<()> {
        info!("cancelling ALL open orders via SDK");

        let sdk = self
            .sdk_client
            .as_ref()
            .ok_or_else(|| anyhow!("SDK client not initialized — cancel unavailable"))?;

        let resp = sdk
            .cancel_all_orders()
            .await
            .map_err(|e| anyhow!("cancel_all_orders failed: {e}"))?;

        debug!(?resp.canceled, "cancel_all response");
        Ok(())
    }

    /// Fetch the current order book for a token from the CLOB REST API.
    ///
    /// `GET /book?token_id={token_id}` — public endpoint, no auth required.
    #[allow(dead_code)] // live mode market param fetching
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
    #[allow(dead_code)] // live mode market param fetching
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
    /// `GET /price?token_id={id}&side=BUY` — public endpoint.
    #[allow(dead_code)] // live mode market param fetching
    pub async fn get_price(&self, token_id: &str) -> Result<Decimal> {
        debug!(token_id, "fetching price from CLOB");

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
    #[allow(dead_code)] // live mode market param fetching
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

    /// WebSocket order-book streaming — stub.
    ///
    /// Real WebSocket streaming is implemented in `market_ws.rs` (Ingestor layer).
    #[allow(dead_code)] // interface completeness — real streaming in market_ws.rs
    pub async fn stream_orderbook(&self, token_id: &str, tx: Sender<IngestorEvent>) -> Result<()> {
        warn!(
            token_id,
            "stream_orderbook called on PolymarketGateway — \
             use PolymarketWsGateway (polymarket_ws.rs) for WS streaming"
        );
        let _ = tx;
        Ok(())
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
}

// ─── SDK client initialization ───────────────────────────────────────────────

/// Initialize an authenticated SDK CLOB client using pre-existing L2 credentials.
///
/// Skips the `create_or_derive_api_key` network call by passing credentials directly.
async fn init_sdk_client(
    signer: &PrivateKeySigner,
    config: &Config,
) -> Result<SdkClient<Authenticated<Normal>>> {
    let uuid: Uuid = config
        .polymarket_api_key
        .parse()
        .context("POLYMARKET_API_KEY must be a valid UUID")?;

    let creds = Credentials::new(
        uuid,
        config.polymarket_secret.clone(),
        config.polymarket_passphrase.clone(),
    );

    let client = SdkClient::new("https://clob.polymarket.com", SdkConfig::default())
        .map_err(|e| anyhow!("failed to create SDK client: {e}"))?
        .authentication_builder(signer)
        .credentials(creds)
        .authenticate()
        .await
        .map_err(|e| anyhow!("SDK authentication failed: {e}"))?;

    Ok(client)
}

// ─── Pure helper functions ────────────────────────────────────────────────────

/// Map our `Side` enum to the SDK's `Side`.
fn to_sdk_side(side: Side) -> SdkSide {
    match side {
        Side::Buy => SdkSide::Buy,
        Side::Sell => SdkSide::Sell,
    }
}

/// Map our `OrderType` enum to the SDK's `OrderType`.
fn to_sdk_order_type(ot: OrderType) -> SdkOrderType {
    match ot {
        OrderType::Gtc => SdkOrderType::GTC,
        OrderType::Gtd => SdkOrderType::GTD,
        OrderType::Fok => SdkOrderType::FOK,
        OrderType::Fak => SdkOrderType::FAK,
    }
}

/// Map SDK `OrderStatusType` to our `OrderStatus`.
fn map_sdk_status(s: &OrderStatusType) -> OrderStatus {
    match s {
        OrderStatusType::Matched => OrderStatus::Filled,
        OrderStatusType::Live => OrderStatus::Placed,
        OrderStatusType::Delayed => OrderStatus::Placed,
        OrderStatusType::Unmatched => OrderStatus::Placed,
        OrderStatusType::Canceled => OrderStatus::Cancelled,
        OrderStatusType::Unknown(_) | _ => OrderStatus::Placed,
    }
}

/// Parse a CLOB `timestamp` field which may be a string or number (epoch s or ms).
#[allow(dead_code)] // used by get_orderbook (live mode)
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

    // ── map_sdk_status ───────────────────────────────────────────────────────

    #[test]
    fn test_map_sdk_status_matched_is_filled() {
        assert_eq!(map_sdk_status(&OrderStatusType::Matched), OrderStatus::Filled);
    }

    #[test]
    fn test_map_sdk_status_live_is_placed() {
        assert_eq!(map_sdk_status(&OrderStatusType::Live), OrderStatus::Placed);
    }

    #[test]
    fn test_map_sdk_status_delayed_is_placed() {
        assert_eq!(map_sdk_status(&OrderStatusType::Delayed), OrderStatus::Placed);
    }

    #[test]
    fn test_map_sdk_status_canceled_is_cancelled() {
        assert_eq!(map_sdk_status(&OrderStatusType::Canceled), OrderStatus::Cancelled);
    }

    #[test]
    fn test_map_sdk_status_unknown_defaults_to_placed() {
        assert_eq!(
            map_sdk_status(&OrderStatusType::Unknown("new_status".to_string())),
            OrderStatus::Placed
        );
    }

    // ── to_sdk_side ──────────────────────────────────────────────────────────

    #[test]
    fn test_to_sdk_side() {
        assert!(matches!(to_sdk_side(Side::Buy), SdkSide::Buy));
        assert!(matches!(to_sdk_side(Side::Sell), SdkSide::Sell));
    }

    // ── to_sdk_order_type ────────────────────────────────────────────────────

    #[test]
    fn test_to_sdk_order_type() {
        assert!(matches!(to_sdk_order_type(OrderType::Gtc), SdkOrderType::GTC));
        assert!(matches!(to_sdk_order_type(OrderType::Gtd), SdkOrderType::GTD));
        assert!(matches!(to_sdk_order_type(OrderType::Fok), SdkOrderType::FOK));
        assert!(matches!(to_sdk_order_type(OrderType::Fak), SdkOrderType::FAK));
    }

    // ── parse_timestamp_value ────────────────────────────────────────────────

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
        assert!(ts > 1_700_000_000_000, "fallback timestamp must be recent");
    }

    // ── maker/taker amount calculation ───────────────────────────────────────

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

        assert_eq!(tokens.to_string(), "50000000");
        assert_eq!(cost.to_string(), "24000000");
    }
}
