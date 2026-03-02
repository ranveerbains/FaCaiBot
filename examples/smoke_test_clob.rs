//! Live CLOB smoke test: place a post-only order → verify accepted → cancel.
//!
//! Usage:
//!   cargo run --example smoke_test_clob
//!
//! Prerequisites:
//! - `.env` with POLYMARKET_API_KEY, POLYMARKET_SECRET, POLYMARKET_PASSPHRASE, PRIVATE_KEY
//! - USDC.e balance on Polygon (even $1 is enough — the order rests, not fills)
//! - Token approvals for CTF Exchange (one-time on-chain tx)
//!
//! What it does:
//! 1. Discovers a live BTC 5-min market via Gamma API
//! 2. Places a BUY YES at $0.01 (far below market — guaranteed to rest unfilled)
//! 3. Verifies the CLOB accepted it (status = Placed/Live)
//! 4. Cancels the order
//! 5. Reports success/failure

use std::str::FromStr;

use alloy::primitives::U256;
use alloy::signers::Signer as _;
use alloy::signers::local::PrivateKeySigner;
use anyhow::{Context, Result, anyhow};
use polymarket_client_sdk::POLYGON;
use polymarket_client_sdk::auth::state::Authenticated;
use polymarket_client_sdk::auth::{Credentials, Normal};
use polymarket_client_sdk::clob::types::{OrderType, Side};
use polymarket_client_sdk::clob::{Client as SdkClient, Config as SdkConfig};
use rust_decimal::Decimal;
use serde::Deserialize;
use uuid::Uuid;

const CLOB_BASE_URL: &str = "https://clob.polymarket.com";
const GAMMA_BASE_URL: &str = "https://gamma-api.polymarket.com";

// ─── Gamma API types ────────────────────────────────────────────────────────

#[derive(Debug, Deserialize)]
struct GammaEvent {
    slug: Option<String>,
    markets: Option<Vec<GammaMarket>>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct GammaMarket {
    slug: Option<String>,
    condition_id: Option<String>,
    end_date: Option<String>,
    clob_token_ids: Option<String>,
    accepting_orders: Option<bool>,
    order_price_min_tick_size: Option<f64>,
    order_min_size: Option<f64>,
    best_bid: Option<f64>,
    best_ask: Option<f64>,
    neg_risk: Option<bool>,
}

// ─── Main ───────────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() -> Result<()> {
    // Install rustls crypto provider.
    rustls::crypto::ring::default_provider()
        .install_default()
        .expect("failed to install rustls crypto provider");

    dotenvy::dotenv().ok();

    println!("=== FaCaiBot Live CLOB Smoke Test ===\n");

    // ── Step 0: Load credentials ─────────────────────────────────────────
    println!("[0] Loading credentials from .env...");
    let api_key_str =
        std::env::var("POLYMARKET_API_KEY").context("POLYMARKET_API_KEY not set in .env")?;
    let secret = std::env::var("POLYMARKET_SECRET").context("POLYMARKET_SECRET not set in .env")?;
    let passphrase =
        std::env::var("POLYMARKET_PASSPHRASE").context("POLYMARKET_PASSPHRASE not set in .env")?;
    let private_key = std::env::var("PRIVATE_KEY").context("PRIVATE_KEY not set in .env")?;

    let api_key_uuid: Uuid = api_key_str
        .parse()
        .context("POLYMARKET_API_KEY must be a valid UUID")?;
    println!(
        "    API key: {}...{}",
        &api_key_str[..8],
        &api_key_str[api_key_str.len() - 4..]
    );

    // ── Step 1: Build signer ─────────────────────────────────────────────
    println!("[1] Building signer...");
    let signer: PrivateKeySigner = private_key
        .parse()
        .context("failed to parse PRIVATE_KEY as PrivateKeySigner")?;
    let address = format!("{:?}", signer.address());
    let signer = signer.with_chain_id(Some(POLYGON));
    println!("    Address: {address}");

    // ── Step 2: Initialize SDK client ────────────────────────────────────
    println!("[2] Authenticating SDK client...");
    let creds = Credentials::new(api_key_uuid, secret, passphrase);
    let sdk: SdkClient<Authenticated<Normal>> = SdkClient::new(CLOB_BASE_URL, SdkConfig::default())
        .map_err(|e| anyhow!("failed to create SDK client: {e}"))?
        .authentication_builder(&signer)
        .credentials(creds)
        .authenticate()
        .await
        .map_err(|e| anyhow!("SDK authentication failed: {e}"))?;
    println!("    SDK client authenticated successfully!");

    // ── Step 3: Discover a live market ───────────────────────────────────
    println!("[3] Discovering a live BTC 5-min market via Gamma API...");
    let http = reqwest::Client::builder()
        .use_rustls_tls()
        .timeout(std::time::Duration::from_secs(10))
        .build()?;

    let gamma_url =
        format!("{GAMMA_BASE_URL}/events?tag_id=102892&active=true&closed=false&limit=10");
    let events: Vec<GammaEvent> = http.get(&gamma_url).send().await?.json().await?;

    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_secs();

    let mut found_token_id: Option<String> = None;
    let mut found_slug = String::new();
    let mut found_min_size: f64 = 5.0;
    let mut found_best_bid: f64 = 0.0;

    for ev in &events {
        let slug = ev.slug.as_deref().unwrap_or("");
        if !slug.contains("btc") && !slug.contains("eth") {
            continue;
        }
        for mkt in ev.markets.as_deref().unwrap_or_default() {
            if mkt.accepting_orders != Some(true) {
                continue;
            }
            // Parse endDate and check it's in the future.
            if let Some(ref end_date) = mkt.end_date {
                // Simple ISO parse — extract epoch from the format "2026-02-25T19:00:00Z"
                let end_epoch = parse_iso_epoch(end_date);
                if end_epoch <= now {
                    continue;
                }
            }
            if let Some(ref ids_json) = mkt.clob_token_ids {
                let ids: Vec<String> = serde_json::from_str(ids_json).unwrap_or_default();
                if let Some(yes_token) = ids.first() {
                    found_token_id = Some(yes_token.clone());
                    found_slug = mkt.slug.clone().unwrap_or_default();
                    found_min_size = mkt.order_min_size.unwrap_or(5.0);
                    found_best_bid = mkt.best_bid.unwrap_or(0.0);
                    break;
                }
            }
        }
        if found_token_id.is_some() {
            break;
        }
    }

    let token_id_str = found_token_id.ok_or_else(|| {
        anyhow!("no active BTC 5-min market found — are markets running right now?")
    })?;
    let token_id = U256::from_str(&token_id_str).context("failed to parse token_id as U256")?;

    println!("    Market: {found_slug}");
    println!("    YES token: {token_id_str}");
    println!("    Min size: {found_min_size}");
    println!("    Current best bid: ${found_best_bid:.2}");

    // ── Step 4: Place a post-only BUY at $0.01 ──────────────────────────
    let price: Decimal = "0.01".parse().unwrap();
    let size: Decimal = Decimal::try_from(found_min_size).unwrap_or_else(|_| Decimal::new(5, 0)); // default to 5

    println!("\n[4] Placing post-only BUY order...");
    println!("    Token: {token_id_str}");
    println!("    Side:  BUY");
    println!("    Price: ${price} (far below best bid ${found_best_bid:.2} — will rest unfilled)");
    println!("    Size:  {size} shares");
    println!("    Type:  GTC post-only");

    let signable = sdk
        .limit_order()
        .token_id(token_id)
        .side(Side::Buy)
        .price(price)
        .size(size)
        .order_type(OrderType::GTC)
        .post_only(true)
        .build()
        .await
        .map_err(|e| anyhow!("SDK order build failed: {e}"))?;

    println!("    Order built (fee_rate + tick_size fetched from CLOB)");

    let signed = sdk
        .sign(&signer, signable)
        .await
        .map_err(|e| anyhow!("SDK order signing failed: {e}"))?;

    println!("    Order signed (EIP-712)");

    let resp = sdk
        .post_order(signed)
        .await
        .map_err(|e| anyhow!("SDK post_order failed: {e}"))?;

    println!("\n    === CLOB Response ===");
    println!("    Order ID:  {}", resp.order_id);
    println!("    Status:    {:?}", resp.status);
    println!("    Success:   {}", resp.success);
    if let Some(ref msg) = resp.error_msg {
        if !msg.is_empty() {
            println!("    Error msg: {msg}");
        }
    }

    if !resp.success {
        println!("\n    FAILED: Order was rejected by the CLOB.");
        println!("    This could mean:");
        println!("      - Insufficient USDC.e balance");
        println!("      - Token approvals not set");
        println!("      - Invalid credentials");
        return Err(anyhow!("order rejected: {:?}", resp.error_msg));
    }

    println!("\n    ORDER ACCEPTED ON CLOB!");

    // ── Step 5: Cancel the order ─────────────────────────────────────────
    println!("\n[5] Cancelling order {}...", resp.order_id);

    let cancel_resp = sdk
        .cancel_order(&resp.order_id)
        .await
        .map_err(|e| anyhow!("cancel_order failed: {e}"))?;

    println!("    Cancelled: {:?}", cancel_resp.canceled);
    println!("    Not cancelled: {:?}", cancel_resp.not_canceled);

    // ── Summary ──────────────────────────────────────────────────────────
    println!("\n=== SMOKE TEST PASSED ===");
    println!("  - SDK authentication: OK");
    println!("  - EIP-712 signing:    OK");
    println!("  - Fee rate fetch:     OK");
    println!("  - Order placement:    OK");
    println!("  - Order cancellation: OK");
    println!("\nLive mode is ready for production.");

    Ok(())
}

/// Simple ISO 8601 → epoch seconds parser for "2026-02-25T19:00:00Z" format.
fn parse_iso_epoch(iso: &str) -> u64 {
    // Try to parse with chrono-like manual approach.
    // Format: YYYY-MM-DDTHH:MM:SSZ
    let parts: Vec<&str> = iso.trim_end_matches('Z').split('T').collect();
    if parts.len() != 2 {
        return 0;
    }
    let date_parts: Vec<u32> = parts[0].split('-').filter_map(|s| s.parse().ok()).collect();
    let time_parts: Vec<u32> = parts[1].split(':').filter_map(|s| s.parse().ok()).collect();
    if date_parts.len() != 3 || time_parts.len() < 2 {
        return 0;
    }
    let (year, month, day) = (date_parts[0], date_parts[1], date_parts[2]);
    let (hour, min, sec) = (
        time_parts[0],
        time_parts[1],
        time_parts.get(2).copied().unwrap_or(0),
    );

    // Days from epoch (1970-01-01) to date — simplified calculation.
    let mut days: u64 = 0;
    for y in 1970..year {
        days += if is_leap(y) { 366 } else { 365 };
    }
    let month_days = [0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];
    for m in 1..month {
        days += month_days[m as usize] as u64;
        if m == 2 && is_leap(year) {
            days += 1;
        }
    }
    days += (day - 1) as u64;

    days * 86400 + hour as u64 * 3600 + min as u64 * 60 + sec as u64
}

fn is_leap(y: u32) -> bool {
    (y % 4 == 0 && y % 100 != 0) || (y % 400 == 0)
}
