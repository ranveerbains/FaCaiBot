use alloy::signers::local::PrivateKeySigner;
use anyhow::{Context, Result};

/// Build a local signer from a hex-encoded private key.
///
/// The key may optionally start with "0x".
pub fn build_signer(private_key_hex: &str) -> Result<PrivateKeySigner> {
    let signer: PrivateKeySigner = private_key_hex
        .parse()
        .context("failed to parse private key")?;
    Ok(signer)
}
