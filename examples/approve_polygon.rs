//! One-time on-chain USDC.e approval for Polymarket CLOB.
//! Approves both CTF Exchange and Neg Risk Adapter — required before trading.
//!
//! Usage:
//!   cargo run --example approve_polygon
//!
//! Prerequisites:
//! - PRIVATE_KEY set in .env
//! - Small amount of MATIC/POL on the wallet for gas (~$0.01 worth)

use alloy::network::EthereumWallet;
use alloy::primitives::{Address, U256, address};
use alloy::providers::ProviderBuilder;
use alloy::signers::local::PrivateKeySigner;
use alloy::sol;
use anyhow::{Result, anyhow};

const POLYGON_RPC_FALLBACK: &str = "https://rpc.ankr.com/polygon";

const USDC_E: Address       = address!("2791Bca1f2de4661ED88A30C99A7a9449Aa84174");
const CTF_EXCHANGE: Address  = address!("4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E");
const NEG_RISK_ADAPTER: Address = address!("d91E80cF2E7be2e162c6513ceD06f1dD0dA35296");

sol! {
    #[sol(rpc)]
    interface IERC20 {
        function approve(address spender, uint256 amount) external returns (bool);
        function allowance(address owner, address spender) external view returns (uint256);
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    rustls::crypto::ring::default_provider()
        .install_default()
        .ok();

    dotenvy::dotenv().ok();

    let private_key = std::env::var("PRIVATE_KEY")
        .map_err(|_| anyhow!("PRIVATE_KEY not set in .env"))?;

    let signer: PrivateKeySigner = private_key
        .parse()
        .map_err(|e| anyhow!("failed to parse PRIVATE_KEY: {e}"))?;
    let wallet_address = signer.address();
    println!("Wallet: {wallet_address:?}");

    let rpc_url = std::env::var("POLYGON_RPC_URL")
        .unwrap_or_else(|_| POLYGON_RPC_FALLBACK.to_string());

    let wallet = EthereumWallet::from(signer);
    let provider = ProviderBuilder::new()
        .wallet(wallet)
        .connect_http(rpc_url.parse()?);

    let usdc = IERC20::new(USDC_E, &provider);
    let max = U256::MAX;

    // ── CTF Exchange ────────────────────────────────────────────────────────
    let current = usdc.allowance(wallet_address, CTF_EXCHANGE).call().await?;
    if current == max {
        println!("CTF Exchange already approved — skipping.");
    } else {
        println!("Approving CTF Exchange ({CTF_EXCHANGE:?})...");
        let receipt = usdc
            .approve(CTF_EXCHANGE, max)
            .send()
            .await?
            .get_receipt()
            .await?;
        println!(
            "  tx: {:?}  status: {:?}",
            receipt.transaction_hash,
            receipt.status()
        );
    }

    // ── Neg Risk Adapter ────────────────────────────────────────────────────
    let current = usdc.allowance(wallet_address, NEG_RISK_ADAPTER).call().await?;
    if current == max {
        println!("Neg Risk Adapter already approved — skipping.");
    } else {
        println!("Approving Neg Risk Adapter ({NEG_RISK_ADAPTER:?})...");
        let receipt = usdc
            .approve(NEG_RISK_ADAPTER, max)
            .send()
            .await?
            .get_receipt()
            .await?;
        println!(
            "  tx: {:?}  status: {:?}",
            receipt.transaction_hash,
            receipt.status()
        );
    }

    println!("\nDone. Run smoke_test_clob to verify.");
    Ok(())
}
