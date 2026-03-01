//! Wallet management: balance checks, Polymarket positions, and CTF token redemption.

use alloy::network::EthereumWallet;
use alloy::primitives::{Address, FixedBytes, U256, address};
use alloy::providers::{Provider, ProviderBuilder};
use alloy::sol;
use anyhow::{Context, Result};
use tokio_rustls::TlsConnector;
use tracing::warn;

use crate::reporting::telegram::post_telegram_message;
use crate::utils::signing::build_signer;

// ─── Constants ───────────────────────────────────────────────────────────────

/// USDC.e on Polygon (bridged USDC, 6 decimals).
const USDC_E: Address = address!("2791Bca1f2de4661ED88A30C99A7a9449Aa84174");

/// Conditional Tokens Framework (CTF) contract on Polygon.
const CTF: Address = address!("4D97DCd97eC945f40cF65F87097ACe5EA0476045");

/// Fallback Polygon RPC if `POLYGON_RPC_URL` is not set.
const POLYGON_RPC_FALLBACK: &str = "https://rpc.ankr.com/polygon";

/// Polymarket data API base URL.
const DATA_API: &str = "https://data-api.polymarket.com";

// ─── Solidity Interfaces ─────────────────────────────────────────────────────

sol! {
    #[sol(rpc)]
    interface IERC20 {
        function balanceOf(address account) external view returns (uint256);
    }

    #[sol(rpc)]
    interface ICTF {
        function redeemPositions(
            address collateralToken,
            bytes32 parentCollectionId,
            bytes32 conditionId,
            uint256[] indexSets
        ) external;
    }
}

// ─── Helpers ─────────────────────────────────────────────────────────────────

fn get_wallet_info() -> Result<(alloy::signers::local::PrivateKeySigner, Address)> {
    let pk = std::env::var("PRIVATE_KEY").context("PRIVATE_KEY not set in env")?;
    let signer = build_signer(&pk)?;
    let address = signer.address();
    Ok((signer, address))
}

fn get_rpc_url() -> String {
    std::env::var("POLYGON_RPC_URL").unwrap_or_else(|_| POLYGON_RPC_FALLBACK.to_string())
}

/// Format a token amount with given decimals (e.g., 6 for USDC.e).
fn format_token(raw: U256, decimals: u32) -> String {
    let divisor = U256::from(10u64.pow(decimals));
    let whole = raw / divisor;
    let frac = raw % divisor;
    format!("{}.{:0>width$}", whole, frac, width = decimals as usize)
}

/// Format a native token amount (18 decimals) truncated to `display_decimals` places.
fn format_wei(raw: U256, display_decimals: u32) -> String {
    let divisor = U256::from(10u64.pow(18));
    let frac_divisor = U256::from(10u64.pow(18 - display_decimals));
    let whole = raw / divisor;
    let frac = (raw % divisor) / frac_divisor;
    format!(
        "{}.{:0>width$}",
        whole,
        frac,
        width = display_decimals as usize
    )
}

fn short_id(id: &str) -> String {
    if id.len() > 10 {
        format!("{}...{}", &id[..6], &id[id.len() - 4..])
    } else {
        id.to_string()
    }
}

// ─── Command Handlers ────────────────────────────────────────────────────────

/// `/balance` — Show EOA wallet USDC.e + POL balance on Polygon.
pub async fn handle_balance() -> String {
    match balance_inner().await {
        Ok(msg) => msg,
        Err(e) => format!("Balance check failed: {e}"),
    }
}

async fn balance_inner() -> Result<String> {
    let (_signer, address) = get_wallet_info()?;
    let rpc_url = get_rpc_url();
    let provider = ProviderBuilder::new().connect_http(rpc_url.parse().context("invalid RPC URL")?);

    // Native POL balance.
    let pol_balance = provider
        .get_balance(address)
        .await
        .context("failed to get POL balance")?;

    // USDC.e balance (ERC20).
    let usdc_contract = IERC20::new(USDC_E, &provider);
    let usdc_balance = usdc_contract
        .balanceOf(address)
        .call()
        .await
        .context("failed to get USDC.e balance")?;

    Ok(format!(
        "Wallet: {addr}\nUSDC.e: ${usdc}\nPOL: {pol}",
        addr = address,
        usdc = format_token(usdc_balance, 6),
        pol = format_wei(pol_balance, 4),
    ))
}

/// `/polybalance` — Show Polymarket positions and total value.
pub async fn handle_polybalance() -> String {
    match polybalance_inner().await {
        Ok(msg) => msg,
        Err(e) => format!("Polybalance check failed: {e}"),
    }
}

async fn polybalance_inner() -> Result<String> {
    let (_signer, address) = get_wallet_info()?;
    let client = reqwest::Client::new();

    // Fetch positions.
    let positions_url = format!("{DATA_API}/positions?user={address}");
    let positions_resp: serde_json::Value = client
        .get(&positions_url)
        .send()
        .await
        .context("failed to fetch positions")?
        .json()
        .await
        .context("failed to parse positions response")?;

    let position_count = positions_resp.as_array().map(|a| a.len()).unwrap_or(0);

    // Fetch total value.
    let value_url = format!("{DATA_API}/value?user={address}");
    let value_resp: serde_json::Value = client
        .get(&value_url)
        .send()
        .await
        .context("failed to fetch value")?
        .json()
        .await
        .context("failed to parse value response")?;

    let total_value = value_resp
        .as_f64()
        .or_else(|| value_resp.get("value").and_then(|v| v.as_f64()))
        .unwrap_or(0.0);

    Ok(format!(
        "Wallet: {addr}\nPositions: {count}\nTotal value: ${value:.2}",
        addr = address,
        count = position_count,
        value = total_value,
    ))
}

/// `/redeem` — Manually trigger redemption of resolved positions.
pub async fn handle_redeem() -> String {
    match redeem_inner().await {
        Ok(msg) => msg,
        Err(e) => format!("Redemption failed: {e}"),
    }
}

async fn redeem_inner() -> Result<String> {
    let (signer, address) = get_wallet_info()?;
    let rpc_url = get_rpc_url();
    let client = reqwest::Client::new();

    // Fetch current positions to find condition IDs.
    let positions_url = format!("{DATA_API}/positions?user={address}");
    let positions_resp: serde_json::Value = client
        .get(&positions_url)
        .send()
        .await
        .context("failed to fetch positions")?
        .json()
        .await
        .context("failed to parse positions response")?;

    let positions = positions_resp
        .as_array()
        .context("positions response is not an array")?;

    if positions.is_empty() {
        return Ok("No positions found — nothing to redeem.".into());
    }

    // Extract unique condition IDs.
    let mut condition_ids: Vec<String> = positions
        .iter()
        .filter_map(|p| {
            p.get("conditionId")
                .or_else(|| p.get("condition_id"))
                .and_then(|v| v.as_str())
                .map(|s| s.to_string())
        })
        .collect();
    condition_ids.sort();
    condition_ids.dedup();

    if condition_ids.is_empty() {
        return Ok("No condition IDs found in positions — nothing to redeem.".into());
    }

    // Build signing provider.
    let wallet = EthereumWallet::from(signer);
    let provider = ProviderBuilder::new()
        .wallet(wallet)
        .connect_http(rpc_url.parse().context("invalid RPC URL")?);
    let ctf = ICTF::new(CTF, &provider);

    let parent_collection_id = FixedBytes::<32>::ZERO;
    let index_sets = vec![U256::from(1), U256::from(2)];

    let mut redeemed = 0u32;
    let mut skipped = 0u32;
    let mut errors: Vec<String> = Vec::new();

    for cid_hex in &condition_ids {
        let condition_id: FixedBytes<32> = match cid_hex.parse() {
            Ok(id) => id,
            Err(e) => {
                errors.push(format!("invalid conditionId {}: {e}", short_id(cid_hex)));
                skipped += 1;
                continue;
            }
        };

        match ctf
            .redeemPositions(USDC_E, parent_collection_id, condition_id, index_sets.clone())
            .send()
            .await
        {
            Ok(pending) => match pending.get_receipt().await {
                Ok(receipt) => {
                    if receipt.status() {
                        redeemed += 1;
                    } else {
                        skipped += 1;
                        errors.push(format!(
                            "{}: tx reverted (market not resolved?)",
                            short_id(cid_hex)
                        ));
                    }
                }
                Err(e) => {
                    skipped += 1;
                    errors.push(format!("{}: receipt error: {e}", short_id(cid_hex)));
                }
            },
            Err(e) => {
                skipped += 1;
                errors.push(format!("{}: {e}", short_id(cid_hex)));
            }
        }
    }

    let mut msg = format!("Redemption complete: {redeemed} redeemed, {skipped} skipped");
    if !errors.is_empty() {
        msg.push_str("\n\nErrors:");
        for e in &errors {
            msg.push_str(&format!("\n- {e}"));
        }
    }

    Ok(msg)
}

// ─── Auto-Redeem Background Task ────────────────────────────────────────────

/// Runs every 24 hours, redeems resolved positions, notifies via Telegram.
/// Never panics — fully fire-and-forget.
pub async fn auto_redeem_loop(tls_connector: TlsConnector, bot_token: String, chat_id: String) {
    loop {
        tokio::time::sleep(std::time::Duration::from_secs(86_400)).await;

        let result = handle_redeem().await;
        let msg = format!("Auto-redeem: {result}");

        if let Err(e) = post_telegram_message(&tls_connector, &bot_token, &chat_id, &msg).await {
            warn!(error = %e, "failed to send auto-redeem notification");
        }
    }
}

// ─── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_format_token_normal() {
        let raw = U256::from(1_500_000u64); // 1.5 USDC.e
        assert_eq!(format_token(raw, 6), "1.500000");
    }

    #[test]
    fn test_format_token_zero() {
        assert_eq!(format_token(U256::ZERO, 6), "0.000000");
    }

    #[test]
    fn test_format_token_large() {
        let raw = U256::from(123_456_789u64); // 123.456789 USDC.e
        assert_eq!(format_token(raw, 6), "123.456789");
    }

    #[test]
    fn test_format_wei() {
        // 1.5 POL = 1_500_000_000_000_000_000 wei
        let raw = U256::from(1_500_000_000_000_000_000u64);
        assert_eq!(format_wei(raw, 4), "1.5000");
    }
}
