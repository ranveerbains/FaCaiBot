//! Wallet management: balance checks, Polymarket positions, and CTF token redemption.

use std::sync::Arc;
use std::time::Duration;

use alloy::network::EthereumWallet;
use alloy::primitives::{Address, FixedBytes, U256, address};
use alloy::providers::{Provider, ProviderBuilder};
use alloy::sol;
use anyhow::{Context, Result};
use tokio_rustls::TlsConnector;
use tracing::warn;

use crate::reporting::telegram::{TelegramReporter, post_telegram_message};
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

/// Path to the persistent redemption file (relative to working directory).
const REDEEMS_FILE: &str = "redeems.txt";

/// Append a condition ID to `redeems.txt` (sync I/O — called from engine thread).
/// Deduplicates: skips if the ID is already present in the file.
pub fn append_condition_id_sync(condition_id: &str) {
    use std::io::Write;

    // Read existing content to check for duplicates.
    let existing = std::fs::read_to_string(REDEEMS_FILE).unwrap_or_default();
    if existing.lines().any(|line| line.trim() == condition_id) {
        return;
    }

    match std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(REDEEMS_FILE)
    {
        Ok(mut f) => {
            if let Err(e) = writeln!(f, "{condition_id}") {
                tracing::warn!(error = %e, "failed to append condition ID to redeems.txt");
            }
        }
        Err(e) => {
            tracing::warn!(error = %e, "failed to open redeems.txt for append");
        }
    }
}

/// Read condition IDs from `redeems.txt` (async). Returns deduplicated, validated IDs.
async fn read_condition_ids_from_file() -> Vec<String> {
    let content = match tokio::fs::read_to_string(REDEEMS_FILE).await {
        Ok(c) => c,
        Err(_) => return Vec::new(),
    };

    let mut ids: Vec<String> = content
        .lines()
        .map(|l| l.trim().to_string())
        .filter(|l| l.len() == 66 && l.starts_with("0x"))
        .collect();
    ids.sort();
    ids.dedup();
    ids
}

/// Remove successfully redeemed IDs from `redeems.txt` (async, atomic write-temp-rename).
async fn remove_redeemed_ids(redeemed: &[String]) {
    if redeemed.is_empty() {
        return;
    }

    let content = match tokio::fs::read_to_string(REDEEMS_FILE).await {
        Ok(c) => c,
        Err(e) => {
            tracing::warn!(error = %e, "failed to read redeems.txt for cleanup");
            return;
        }
    };

    // Normalize redeemed IDs to lowercase for safe comparison.
    let redeemed_lower: Vec<String> = redeemed.iter().map(|s| s.to_lowercase()).collect();

    let remaining: Vec<String> = content
        .lines()
        .map(|l| l.trim().to_string())
        .filter(|l| !l.is_empty() && !redeemed_lower.contains(&l.to_lowercase()))
        .collect();

    let tmp = format!("{REDEEMS_FILE}.tmp");
    if remaining.is_empty() {
        // All redeemed — remove the file entirely.
        if let Err(e) = tokio::fs::remove_file(REDEEMS_FILE).await {
            tracing::warn!(error = %e, "failed to remove redeems.txt after full redemption");
        }
        let _ = tokio::fs::remove_file(&tmp).await;
    } else {
        let new_content = remaining.join("\n") + "\n";
        match tokio::fs::write(&tmp, new_content.as_bytes()).await {
            Ok(()) => {
                if let Err(e) = tokio::fs::rename(&tmp, REDEEMS_FILE).await {
                    tracing::warn!(error = %e, "failed to rename temp file to redeems.txt");
                }
            }
            Err(e) => {
                tracing::warn!(error = %e, "failed to write temp file for redeems.txt cleanup");
            }
        }
    }
}

// ─── Command Handlers ────────────────────────────────────────────────────────

/// `/balance` — Show EOA wallet USDC.e + POL balance on Polygon.
pub async fn handle_balance() -> String {
    match tokio::time::timeout(Duration::from_secs(10), balance_inner()).await {
        Ok(Ok(msg)) => msg,
        Ok(Err(e)) => format!("Balance check failed: {e}"),
        Err(_) => "Balance check timed out (10s).".into(),
    }
}

async fn balance_inner() -> Result<String> {
    let (_signer, address) = get_wallet_info()?;
    let rpc_url = get_rpc_url();

    let provider = ProviderBuilder::new()
        .connect_http(rpc_url.parse().context("invalid RPC URL")?);

    // Native POL balance.
    let pol_balance = provider
        .get_balance(address)
        .await
        .context("failed to get POL balance")?;

    // USDC.e balance via alloy contract call (correct ABI encoding).
    let usdc = IERC20::new(USDC_E, &provider);
    let usdc_balance = usdc
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
    match tokio::time::timeout(Duration::from_secs(10), polybalance_inner()).await {
        Ok(Ok(msg)) => msg,
        Ok(Err(e)) => format!("Polybalance check failed: {e}"),
        Err(_) => "Polybalance check timed out (10s).".into(),
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

    // API returns `[{"user":"...","value":N}]` — extract from first array element.
    let total_value = value_resp
        .as_array()
        .and_then(|arr| arr.first())
        .and_then(|obj| obj.get("value"))
        .and_then(|v| v.as_f64())
        .unwrap_or(0.0);

    Ok(format!(
        "Wallet: {addr}\nPositions: {count}\nMarket value: ${value:.2}",
        addr = address,
        count = position_count,
        value = total_value,
    ))
}

/// `/redeem` — Manually trigger redemption of resolved positions.
pub async fn handle_redeem() -> String {
    match tokio::time::timeout(Duration::from_secs(120), redeem_inner()).await {
        Ok(Ok(msg)) => msg,
        Ok(Err(e)) => format!("Redemption failed: {e}"),
        Err(_) => "Redemption timed out (120s).".into(),
    }
}

async fn redeem_inner() -> Result<String> {
    let (signer, address) = get_wallet_info()?;
    let rpc_url = get_rpc_url();
    let client = reqwest::Client::new();

    // Source 1: Data API positions (graceful — don't fail if API errors).
    let mut condition_ids: Vec<String> = Vec::new();
    match client
        .get(format!("{DATA_API}/positions?user={address}"))
        .send()
        .await
    {
        Ok(resp) => {
            if let Ok(positions_resp) = resp.json::<serde_json::Value>().await
                && let Some(positions) = positions_resp.as_array()
            {
                for p in positions {
                    if let Some(cid) = p
                        .get("conditionId")
                        .or_else(|| p.get("condition_id"))
                        .and_then(|v| v.as_str())
                    {
                        condition_ids.push(cid.to_string());
                    }
                }
            }
        }
        Err(e) => {
            warn!(error = %e, "Data API fetch failed — continuing with file IDs only");
        }
    }

    // Source 2: Persistent file of condition IDs from completed trades.
    let file_ids = read_condition_ids_from_file().await;
    condition_ids.extend(file_ids);

    // Dedup.
    condition_ids.sort();
    condition_ids.dedup();

    if condition_ids.is_empty() {
        return Ok("No positions found — nothing to redeem.".into());
    }

    // Build signing provider with cached nonce management (prevents "nonce too low" on rapid txs).
    // ProviderBuilder::new() includes SimpleNonceManager which queries the RPC every send —
    // between rapid sequential txs, the RPC returns the same nonce. CachedNonceManager
    // tracks nonces locally and increments after each submission, overriding the default.
    let wallet = EthereumWallet::from(signer);
    let provider = ProviderBuilder::new()
        .with_cached_nonce_management()
        .wallet(wallet)
        .connect_http(rpc_url.parse().context("invalid RPC URL")?);
    let ctf = ICTF::new(CTF, &provider);

    let parent_collection_id = FixedBytes::<32>::ZERO;
    let index_sets = vec![U256::from(1), U256::from(2)];

    let mut redeemed = 0u32;
    let mut skipped = 0u32;
    let mut errors: Vec<String> = Vec::new();
    let mut redeemed_ids: Vec<String> = Vec::new();

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
            Ok(pending) => {
                let tx_hash = *pending.tx_hash();
                // Per-tx receipt timeout — don't let slow RPC starve remaining positions.
                match tokio::time::timeout(Duration::from_secs(8), pending.get_receipt()).await {
                    Ok(Ok(receipt)) => {
                        if receipt.status() {
                            redeemed += 1;
                            redeemed_ids.push(cid_hex.clone());
                        } else {
                            skipped += 1;
                            errors.push(format!(
                                "{}: tx reverted (market not resolved?)",
                                short_id(cid_hex)
                            ));
                        }
                    }
                    Ok(Err(e)) if e.to_string().contains("null response") => {
                        // Tx was broadcast — RPC just lost the receipt. Count as success.
                        redeemed += 1;
                        redeemed_ids.push(cid_hex.clone());
                    }
                    Ok(Err(e)) => {
                        skipped += 1;
                        errors.push(format!("{}: receipt error: {e}", short_id(cid_hex)));
                    }
                    Err(_) => {
                        skipped += 1;
                        warn!(tx = %tx_hash, "receipt poll timed out");
                        errors.push(format!("{}: receipt timed out", short_id(cid_hex)));
                    }
                }
            }
            Err(e) => {
                skipped += 1;
                errors.push(format!("{}: {e}", short_id(cid_hex)));
            }
        }
    }

    // Remove successfully redeemed IDs from the persistent file.
    remove_redeemed_ids(&redeemed_ids).await;

    let mut msg = format!("Redemption complete: {redeemed} redeemed, {skipped} skipped");
    if !errors.is_empty() {
        msg.push_str("\n\nErrors:");
        for e in &errors {
            msg.push_str(&format!("\n- {e}"));
        }
    }

    Ok(msg)
}

/// `/redeem <condition_id>` — Redeem a specific condition ID.
pub async fn handle_redeem_specific(condition_id: String) -> String {
    match tokio::time::timeout(Duration::from_secs(30), redeem_specific_inner(&condition_id)).await {
        Ok(Ok(msg)) => msg,
        Ok(Err(e)) => format!("Redemption failed: {e}"),
        Err(_) => "Redemption timed out (30s).".into(),
    }
}

async fn redeem_specific_inner(cid_hex: &str) -> Result<String> {
    let cid_hex = cid_hex.trim();
    if cid_hex.len() != 66 || !cid_hex.starts_with("0x") {
        anyhow::bail!("Invalid condition ID format. Expected 0x-prefixed 32-byte hex (66 chars).");
    }

    let condition_id: FixedBytes<32> = cid_hex
        .parse()
        .context("failed to parse condition ID as bytes32")?;

    let (signer, _address) = get_wallet_info()?;
    let rpc_url = get_rpc_url();

    let wallet = EthereumWallet::from(signer);
    let provider = ProviderBuilder::new()
        .with_cached_nonce_management()
        .wallet(wallet)
        .connect_http(rpc_url.parse().context("invalid RPC URL")?);
    let ctf = ICTF::new(CTF, &provider);

    let parent_collection_id = FixedBytes::<32>::ZERO;
    let index_sets = vec![U256::from(1), U256::from(2)];

    let pending = ctf
        .redeemPositions(USDC_E, parent_collection_id, condition_id, index_sets)
        .send()
        .await
        .context("redeemPositions call failed")?;

    let tx_hash = *pending.tx_hash();

    match tokio::time::timeout(Duration::from_secs(8), pending.get_receipt()).await {
        Ok(Ok(receipt)) => {
            if receipt.status() {
                remove_redeemed_ids(&[cid_hex.to_string()]).await;
                Ok(format!("Redeemed {}: tx {tx_hash}", short_id(cid_hex)))
            } else {
                Ok(format!(
                    "Tx reverted for {} (market not resolved?): tx {tx_hash}",
                    short_id(cid_hex)
                ))
            }
        }
        Ok(Err(e)) if e.to_string().contains("null response") => {
            remove_redeemed_ids(&[cid_hex.to_string()]).await;
            Ok(format!(
                "Redeemed {} (receipt lost, tx broadcast): tx {tx_hash}",
                short_id(cid_hex)
            ))
        }
        Ok(Err(e)) => Ok(format!("Receipt error for {}: {e}", short_id(cid_hex))),
        Err(_) => Ok(format!(
            "Receipt timed out for {}: tx {tx_hash}",
            short_id(cid_hex)
        )),
    }
}

// ─── Auto-Redeem Background Task ────────────────────────────────────────────

/// Runs once per market rotation, ~60s after rotation for UMA resolution.
/// Failures are graceful — logged and retried next cycle.
pub async fn auto_redeem_loop(
    tls_connector: TlsConnector,
    bot_token: String,
    chat_id: String,
    rotation_notify: Arc<tokio::sync::Notify>,
    reporter: TelegramReporter,
) {
    loop {
        rotation_notify.notified().await;
        tokio::time::sleep(std::time::Duration::from_secs(60)).await;

        let result = handle_redeem().await;

        // Only notify when something was actually redeemed (skip "0 redeemed, 0 skipped" noise).
        let is_noop = result.contains("nothing to redeem")
            || result.contains("No positions found")
            || result.contains("No condition IDs")
            || result.starts_with("Redemption complete: 0 redeemed, 0 skipped");
        if is_noop {
            continue;
        }

        let msg = format!("Auto-redeem: {result}");
        match post_telegram_message(&tls_connector, &bot_token, &chat_id, &msg).await {
            Ok(Some(id)) => reporter.track_msg_id(id).await,
            Ok(None) => {}
            Err(e) => warn!(error = %e, "failed to send auto-redeem notification"),
        }
    }
}

// ─── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
#[path = "tests/wallet_tests.rs"]
mod tests;
