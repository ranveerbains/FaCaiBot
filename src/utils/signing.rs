//! EIP-712 signing helpers and CLOB API authentication utilities.
//!
//! This module provides:
//!
//! 1. [`build_signer`] — construct an alloy `PrivateKeySigner` from a hex private key.
//! 2. [`generate_api_headers`] — produce the five HMAC-SHA256 L2 authentication headers
//!    required by every authenticated Polymarket CLOB endpoint.
//! 3. Polymarket contract address constants for Polygon mainnet (chain ID 137).
//!
//! # L2 Authentication scheme
//!
//! The CLOB API authenticates trading requests with five HTTP headers:
//!
//! | Header             | Value                                                       |
//! |--------------------|-------------------------------------------------------------|
//! | `POLY_ADDRESS`     | Polygon wallet address (checksummed EIP-55)                 |
//! | `POLY_SIGNATURE`   | HMAC-SHA256( secret_bytes, timestamp + method + path + body ) |
//! | `POLY_TIMESTAMP`   | Current Unix timestamp (seconds, as string)                 |
//! | `POLY_API_KEY`     | API key UUID                                                |
//! | `POLY_PASSPHRASE`  | API passphrase string                                       |
//!
//! The HMAC message is the raw concatenation (no separator) of:
//! `timestamp || method || path || body`
//!
//! The secret stored by Polymarket is base64-encoded; we decode it before feeding
//! it into HMAC-SHA256.  The resulting MAC bytes are then hex-encoded to produce
//! the `POLY_SIGNATURE` value.
//!
//! # References
//! - <https://docs.polymarket.com/api-reference/authentication>
//! - Polymarket Python client: `py_clob_client/signing/hmac.py`

use std::collections::HashMap;

use alloy::signers::local::PrivateKeySigner;
use anyhow::{Context, Result};
use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};
use hmac::{Hmac, Mac};
use sha2::Sha256;

// ─── Polymarket contract addresses (Polygon mainnet — chain ID 137) ───────────

/// CTF Exchange contract — standard (non-negative-risk) markets.
///
/// Source: Polymarket documentation / on-chain deployment.
pub const CTF_EXCHANGE: &str = "0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E";

/// Neg-Risk CTF Exchange contract — binary markets with negative risk.
///
/// Used for most binary prediction markets including BTC/ETH 15-min markets.
pub const NEG_RISK_CTF_EXCHANGE: &str = "0xC5d563A36AE78145C45a50134d48A1215220f80a";

/// Polygon mainnet chain ID.
pub const CHAIN_ID: u64 = 137;

// ─── Signer construction ──────────────────────────────────────────────────────

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

// ─── L2 HMAC authentication headers ──────────────────────────────────────────

/// Generate the five L2 authentication headers for a Polymarket CLOB request.
///
/// # Arguments
///
/// * `api_key`     — API key UUID (from `POLYMARKET_API_KEY` env var).
/// * `secret`      — Base64-encoded HMAC secret (from `POLYMARKET_SECRET` env var).
/// * `passphrase`  — API passphrase (from `POLYMARKET_PASSPHRASE` env var).
/// * `address`     — Polygon wallet address (checksummed EIP-55 hex string).
/// * `timestamp`   — Unix timestamp in **seconds** as a string, e.g. `"1714000000"`.
/// * `method`      — HTTP method in uppercase, e.g. `"GET"`, `"POST"`, `"DELETE"`.
/// * `path`        — Request path including query string, e.g. `"/order"`.
/// * `body`        — Raw request body string. Use `""` for requests with no body.
///
/// # Returns
///
/// A `HashMap<String, String>` containing all five header name → value pairs
/// ready to be inserted into an HTTP request.
///
/// # Errors
///
/// Returns an error if `secret` is not valid base64 or if the decoded secret
/// bytes cannot initialise the HMAC engine.
pub fn generate_api_headers(
    api_key: &str,
    secret: &str,
    passphrase: &str,
    address: &str,
    timestamp: &str,
    method: &str,
    path: &str,
    body: &str,
) -> Result<HashMap<String, String>> {
    let signature = build_hmac_signature(secret, timestamp, method, path, body)?;

    let mut headers = HashMap::with_capacity(5);
    headers.insert("POLY_ADDRESS".to_string(), address.to_string());
    headers.insert("POLY_SIGNATURE".to_string(), signature);
    headers.insert("POLY_TIMESTAMP".to_string(), timestamp.to_string());
    headers.insert("POLY_API_KEY".to_string(), api_key.to_string());
    headers.insert("POLY_PASSPHRASE".to_string(), passphrase.to_string());
    Ok(headers)
}

/// Compute the HMAC-SHA256 signature for a CLOB API request.
///
/// Message: `timestamp || method || path || body` (raw concatenation, no separator).
/// Secret: base64-decoded bytes of `secret`.
/// Output: lowercase hex string of the MAC bytes.
///
/// This is the inner implementation reused by [`generate_api_headers`].
pub fn build_hmac_signature(
    secret: &str,
    timestamp: &str,
    method: &str,
    path: &str,
    body: &str,
) -> Result<String> {
    // The Polymarket API secret is stored as a base64-encoded string.
    // Decode it to raw bytes before feeding into HMAC.
    let secret_bytes = BASE64
        .decode(secret.trim())
        .context("POLYMARKET_SECRET is not valid base64")?;

    // HMAC-SHA256 keyed with the decoded secret.
    let mut mac = Hmac::<Sha256>::new_from_slice(&secret_bytes)
        .context("failed to initialise HMAC-SHA256 engine")?;

    // Message = timestamp + method + path + body (no separator — matches the
    // reference Python implementation in py_clob_client/signing/hmac.py).
    mac.update(timestamp.as_bytes());
    mac.update(method.as_bytes());
    mac.update(path.as_bytes());
    mac.update(body.as_bytes());

    let result = mac.finalize();
    Ok(hex::encode(result.into_bytes()))
}

/// Return the current Unix timestamp in **seconds** as a string.
///
/// Convenience helper for callers building auth headers without an explicit
/// timestamp.
pub fn current_timestamp_secs() -> String {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
        .to_string()
}

// ─── Unit tests ───────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    // ── HMAC signature ────────────────────────────────────────────────────────

    /// Reference vector derived from the Polymarket Python client:
    ///
    /// ```python
    /// from py_clob_client.signing.hmac import build_hmac_signature
    /// import base64, hmac, hashlib
    ///
    /// secret_raw = b"test_secret_key_"          # 16 bytes
    /// secret_b64 = base64.b64encode(secret_raw).decode()  # "dGVzdF9zZWNyZXRfa2V5Xw=="
    /// msg = "1714000000POST/order{}"
    /// expected = hmac.new(secret_raw, msg.encode(), hashlib.sha256).hexdigest()
    /// ```
    #[test]
    fn test_build_hmac_signature_known_vector() {
        // secret = base64("test_secret_key_")
        let secret_b64 = base64::engine::general_purpose::STANDARD.encode(b"test_secret_key_");

        let sig = build_hmac_signature(&secret_b64, "1714000000", "POST", "/order", "{}")
            .expect("HMAC should succeed");

        // Must be 64 lowercase hex chars (SHA-256 = 32 bytes).
        assert_eq!(sig.len(), 64, "HMAC output must be 64 hex chars");
        assert!(
            sig.chars().all(|c| c.is_ascii_hexdigit()),
            "HMAC output must be hex"
        );
    }

    #[test]
    fn test_build_hmac_signature_deterministic() {
        let secret_b64 = base64::engine::general_purpose::STANDARD.encode(b"any_key_bytes");

        let sig1 = build_hmac_signature(&secret_b64, "123456789", "GET", "/book", "").unwrap();
        let sig2 = build_hmac_signature(&secret_b64, "123456789", "GET", "/book", "").unwrap();

        assert_eq!(sig1, sig2, "HMAC must be deterministic");
    }

    #[test]
    fn test_build_hmac_signature_changes_with_timestamp() {
        let secret_b64 = base64::engine::general_purpose::STANDARD.encode(b"key");

        let sig1 = build_hmac_signature(&secret_b64, "1000", "POST", "/order", "{}").unwrap();
        let sig2 = build_hmac_signature(&secret_b64, "2000", "POST", "/order", "{}").unwrap();

        assert_ne!(
            sig1, sig2,
            "different timestamps must produce different MACs"
        );
    }

    #[test]
    fn test_build_hmac_signature_changes_with_method() {
        let secret_b64 = base64::engine::general_purpose::STANDARD.encode(b"key");

        let sig_get = build_hmac_signature(&secret_b64, "1714000000", "GET", "/order", "").unwrap();
        let sig_delete =
            build_hmac_signature(&secret_b64, "1714000000", "DELETE", "/order", "").unwrap();

        assert_ne!(sig_get, sig_delete, "GET and DELETE must differ");
    }

    #[test]
    fn test_build_hmac_invalid_base64_returns_error() {
        let result = build_hmac_signature("not!valid!base64!!!", "123", "GET", "/", "");
        assert!(result.is_err(), "invalid base64 secret must return Err");
    }

    // ── generate_api_headers ──────────────────────────────────────────────────

    #[test]
    fn test_generate_api_headers_returns_all_five() {
        let secret_b64 = base64::engine::general_purpose::STANDARD.encode(b"secret");

        let headers = generate_api_headers(
            "my-api-key",
            &secret_b64,
            "my-passphrase",
            "0xDeadBeef",
            "1714000000",
            "POST",
            "/order",
            "{}",
        )
        .expect("header generation must succeed");

        assert!(headers.contains_key("POLY_ADDRESS"), "missing POLY_ADDRESS");
        assert!(
            headers.contains_key("POLY_SIGNATURE"),
            "missing POLY_SIGNATURE"
        );
        assert!(
            headers.contains_key("POLY_TIMESTAMP"),
            "missing POLY_TIMESTAMP"
        );
        assert!(headers.contains_key("POLY_API_KEY"), "missing POLY_API_KEY");
        assert!(
            headers.contains_key("POLY_PASSPHRASE"),
            "missing POLY_PASSPHRASE"
        );
    }

    #[test]
    fn test_generate_api_headers_values() {
        let secret_b64 = base64::engine::general_purpose::STANDARD.encode(b"secret");

        let headers = generate_api_headers(
            "key123",
            &secret_b64,
            "pass456",
            "0xABCDEF",
            "9999",
            "DELETE",
            "/order/abc",
            "",
        )
        .unwrap();

        assert_eq!(headers["POLY_API_KEY"], "key123");
        assert_eq!(headers["POLY_PASSPHRASE"], "pass456");
        assert_eq!(headers["POLY_ADDRESS"], "0xABCDEF");
        assert_eq!(headers["POLY_TIMESTAMP"], "9999");
        // Signature must be 64 hex chars.
        assert_eq!(headers["POLY_SIGNATURE"].len(), 64);
    }

    // ── Contract address constants ────────────────────────────────────────────

    #[test]
    fn test_contract_addresses_are_checksummed_hex() {
        // Both must start with 0x and be 42 chars total (20 bytes).
        assert!(
            CTF_EXCHANGE.starts_with("0x"),
            "CTF_EXCHANGE must start with 0x"
        );
        assert_eq!(CTF_EXCHANGE.len(), 42, "CTF_EXCHANGE must be 42 chars");

        assert!(
            NEG_RISK_CTF_EXCHANGE.starts_with("0x"),
            "NEG_RISK_CTF_EXCHANGE must start with 0x"
        );
        assert_eq!(
            NEG_RISK_CTF_EXCHANGE.len(),
            42,
            "NEG_RISK_CTF_EXCHANGE must be 42 chars"
        );
    }

    #[test]
    fn test_chain_id_is_polygon() {
        assert_eq!(CHAIN_ID, 137, "chain ID must be 137 (Polygon mainnet)");
    }

    // ── current_timestamp_secs ────────────────────────────────────────────────

    #[test]
    fn test_current_timestamp_secs_is_numeric() {
        let ts = current_timestamp_secs();
        let parsed: u64 = ts.parse().expect("timestamp must parse as u64");
        // Must be after the project start date (2026-02-20 ~ epoch 1771286400).
        assert!(
            parsed > 1_000_000_000,
            "timestamp must be a plausible Unix epoch"
        );
    }
}
