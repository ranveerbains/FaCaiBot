use super::*;

#[test]
fn test_build_signer_valid_hex() {
    // A valid 32-byte hex private key (64 hex chars).
    let key = "0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80";
    let signer = build_signer(key);
    assert!(signer.is_ok(), "valid hex key must produce Ok");
}

#[test]
fn test_build_signer_invalid_hex() {
    let result = build_signer("not_a_valid_key");
    assert!(result.is_err(), "invalid key must return Err");
}
