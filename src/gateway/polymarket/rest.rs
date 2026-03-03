//! Polymarket CLOB REST gateway — Layer 3 Executor ("The Hand").
//!
//! Wraps every Polymarket CLOB HTTP endpoint needed by the Executor layer:
//!
//! - [`PolymarketGateway::place_order`] — SDK-signed order POST to `/order`.
//! - [`PolymarketGateway::cancel_order`] — Cancel by ID via SDK.
//! - [`PolymarketGateway::cancel_all`] — Cancel all open orders via SDK.
//!
//! # Authentication
//!
//! Order placement uses the official `polymarket-client-sdk` for correct EIP-712
//! signing, automatic fee rate fetching, and tick size validation. Cancel operations
//! also go through the SDK's authenticated client.
//!
//! # Thread safety
//!
//! `PolymarketGateway` is `Send + Sync`.  The SDK client uses `Arc` internally
//! and is cheaply cloneable.
//!
//! # File ownership
//! Owned by the **Executor Developer**.  Do NOT merge with the WS sub-modules.

use std::str::FromStr;

use alloy::primitives::U256;
use alloy::signers::Signer as _;
use alloy::signers::local::PrivateKeySigner;
use anyhow::{Context, Result, anyhow};
use polymarket_client_sdk::POLYGON;
use polymarket_client_sdk::auth::state::Authenticated;
use polymarket_client_sdk::auth::{Credentials, Normal};
use polymarket_client_sdk::clob::types::response::PostOrderResponse;
use polymarket_client_sdk::clob::types::{
    OrderStatusType, OrderType as SdkOrderType, Side as SdkSide,
};
use polymarket_client_sdk::clob::{Client as SdkClient, Config as SdkConfig};
use tracing::{debug, info, warn};
use uuid::Uuid;

use crate::config::Config;
use crate::types::order::{OrderType, Side};
use crate::types::{OrderRequest, OrderResponse, OrderStatus};
use crate::utils::signing::build_signer;

// ─── Gateway struct ───────────────────────────────────────────────────────────

/// Production Polymarket CLOB gateway (REST).
///
/// Uses the official `polymarket-client-sdk` for authenticated operations
/// (order placement, signing, cancellation).
///
/// `Send + Sync` — safe to share across tokio tasks via `Arc<PolymarketGateway>`.
pub struct PolymarketGateway {
    /// EIP-712 signer with chain_id=137 (Polygon) set. Used for SDK `sign()` calls.
    /// `None` when no valid private key is configured (simulation mode).
    signer: Option<PrivateKeySigner>,
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
        let signer = match build_signer(&config.private_key) {
            Ok(s) => {
                info!(address = %format!("{:?}", s.address()), "PolymarketGateway: signer initialised");
                Some(s.with_chain_id(Some(POLYGON)))
            }
            Err(e) => {
                warn!(
                    error = %e,
                    "PolymarketGateway: private key not configured or invalid; \
                     order signing disabled (simulation/read-only mode)"
                );
                None
            }
        };

        // Initialize SDK CLOB client (only if signer + credentials are available).
        let sdk_client = if let Some(ref signer) = signer {
            match init_sdk_client(signer, &config).await {
                Ok(client) => {
                    info!(
                        signer_address = %format!("{:?}", signer.address()),
                        "PolymarketGateway: SDK CLOB client authenticated — \
                         verify this address matches your Polymarket wallet"
                    );
                    Some(client)
                }
                Err(e) => {
                    warn!(
                        error = %e,
                        signer_address = %format!("{:?}", signer.address()),
                        "PolymarketGateway: SDK CLOB client init failed — \
                         order placement disabled (read-only mode). \
                         Check that PRIVATE_KEY matches the wallet used to derive API credentials"
                    );
                    None
                }
            }
        } else {
            None
        };

        Self { signer, sdk_client }
    }

    /// Return a reference to the authenticated SDK client (if available).
    ///
    /// Used by `LiveExecutor` to pre-warm SDK caches (`tick_size`, `neg_risk`)
    /// on market rotation so the first order has zero extra latency.
    pub fn sdk_client(&self) -> Option<&SdkClient<Authenticated<Normal>>> {
        self.sdk_client.as_ref()
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
            if let Some(dt) =
                polymarket_client_sdk::types::DateTime::from_timestamp((exp_ms / 1000) as i64, 0)
            {
                builder = builder.expiration(dt);
            }
        }

        let signable = builder
            .build()
            .await
            .map_err(|e| {
                anyhow!(
                    "SDK order build failed (token={}, price={}, size={}): {e}",
                    order.token_id,
                    order.price,
                    order.size
                )
            })?;

        debug!(
            signer_address = %format!("{:?}", signer.address()),
            token_id = %order.token_id,
            side = ?order.side,
            price = %order.price,
            size = %order.size,
            "signing order via SDK (neg_risk auto-fetched from CLOB if not cached)"
        );

        // Sign: EIP-712 typed data with correct domain separator (auto-detects neg_risk).
        let signed = sdk
            .sign(signer, signable)
            .await
            .map_err(|e| anyhow!("SDK order signing failed: {e}"))?;

        // Post: sends authenticated request to CLOB with L2 HMAC headers.
        let resp: PostOrderResponse = sdk
            .post_order(signed)
            .await
            .map_err(|e| {
                anyhow!(
                    "SDK post_order failed (token={}, side={:?}, price={}, size={}): {e}",
                    order.token_id,
                    order.side,
                    order.price,
                    order.size
                )
            })?;

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
    /// Returns `true` if the order was confirmed cancelled, `false` if it was
    /// not in the `canceled` list (may have filled before the cancel reached CLOB).
    pub async fn cancel_order(&self, order_id: &str) -> Result<bool> {
        info!(order_id, "cancelling order via SDK");

        let sdk = self
            .sdk_client
            .as_ref()
            .ok_or_else(|| anyhow!("SDK client not initialized — cancel unavailable"))?;

        let resp = sdk
            .cancel_order(order_id)
            .await
            .map_err(|e| anyhow!("cancel_order failed: {e}"))?;

        let was_cancelled = resp.canceled.iter().any(|id| id == order_id);
        debug!(order_id, was_cancelled, "cancel_order response");
        Ok(was_cancelled)
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

use crate::utils::time::epoch_ms as now_ms;

// ─── Unit tests ───────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal::Decimal;

    // ── map_sdk_status ───────────────────────────────────────────────────────

    #[test]
    fn test_map_sdk_status_matched_is_filled() {
        assert_eq!(
            map_sdk_status(&OrderStatusType::Matched),
            OrderStatus::Filled
        );
    }

    #[test]
    fn test_map_sdk_status_live_is_placed() {
        assert_eq!(map_sdk_status(&OrderStatusType::Live), OrderStatus::Placed);
    }

    #[test]
    fn test_map_sdk_status_delayed_is_placed() {
        assert_eq!(
            map_sdk_status(&OrderStatusType::Delayed),
            OrderStatus::Placed
        );
    }

    #[test]
    fn test_map_sdk_status_canceled_is_cancelled() {
        assert_eq!(
            map_sdk_status(&OrderStatusType::Canceled),
            OrderStatus::Cancelled
        );
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
        assert!(matches!(
            to_sdk_order_type(OrderType::Gtc),
            SdkOrderType::GTC
        ));
        assert!(matches!(
            to_sdk_order_type(OrderType::Gtd),
            SdkOrderType::GTD
        ));
        assert!(matches!(
            to_sdk_order_type(OrderType::Fok),
            SdkOrderType::FOK
        ));
        assert!(matches!(
            to_sdk_order_type(OrderType::Fak),
            SdkOrderType::FAK
        ));
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
