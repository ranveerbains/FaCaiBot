//! EIP-712 signing helper.
//!
//! Provides [`build_signer`] — construct an alloy `PrivateKeySigner` from a hex private key.

use alloy::signers::local::PrivateKeySigner;
use anyhow::{Context, Result};

/// Build a local alloy signer from a hex-encoded private key.
///
/// The key may optionally include the `"0x"` prefix.
///
/// # Errors
/// Returns an error if the hex string is malformed or the key is invalid.
pub fn build_signer(private_key_hex: &str) -> Result<PrivateKeySigner> {
    let signer: PrivateKeySigner = private_key_hex
        .parse()
        .context("failed to parse private key as PrivateKeySigner")?;
    Ok(signer)
}

#[cfg(test)]
#[path = "tests/signing_tests.rs"]
mod tests;
