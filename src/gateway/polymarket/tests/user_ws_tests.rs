use super::*;

#[test]
fn test_parse_trade_status_matched() {
    let status = parse_trade_status("MATCHED").expect("parse failed");
    assert_eq!(status, TradeStatus::Matched);
}

#[test]
fn test_parse_trade_status_confirmed() {
    let status = parse_trade_status("CONFIRMED").expect("parse failed");
    assert_eq!(status, TradeStatus::Confirmed);
}

#[test]
fn test_parse_trade_status_failed() {
    let status = parse_trade_status("FAILED").expect("parse failed");
    assert_eq!(status, TradeStatus::Failed);
}

#[test]
fn test_parse_trade_status_retrying() {
    let status = parse_trade_status("RETRYING").expect("parse failed");
    assert_eq!(status, TradeStatus::Retrying);
}

#[test]
fn test_parse_trade_status_case_insensitive() {
    let status = parse_trade_status("matched").expect("lowercase parse");
    assert_eq!(status, TradeStatus::Matched);
}

#[test]
fn test_parse_trade_status_canceled() {
    let status = parse_trade_status("CANCELED").expect("parse failed");
    assert_eq!(status, TradeStatus::Canceled);
}

#[test]
fn test_parse_trade_status_cancelled_british() {
    let status = parse_trade_status("CANCELLED").expect("parse failed");
    assert_eq!(status, TradeStatus::Canceled);
}

#[test]
fn test_parse_trade_status_canceled_lowercase() {
    let status = parse_trade_status("canceled").expect("lowercase parse");
    assert_eq!(status, TradeStatus::Canceled);
}

#[test]
fn test_parse_trade_status_unknown_errors() {
    let result = parse_trade_status("PENDING_QUEUE");
    assert!(result.is_err(), "unknown status should return Err");
}

#[test]
fn test_build_user_auth_msg_format() {
    let msg = build_user_auth_msg("my-api-key", "my-secret", "my-passphrase");
    let parsed: serde_json::Value = serde_json::from_str(&msg).expect("valid JSON");

    assert_eq!(parsed["type"], "user");
    assert_eq!(parsed["operation"], "subscribe");
    assert_eq!(parsed["initial_dump"], true);
    assert!(parsed["markets"].as_array().unwrap().is_empty());
    assert!(parsed["asset_ids"].as_array().unwrap().is_empty());

    let auth = &parsed["auth"];
    assert_eq!(auth["apiKey"], "my-api-key");
    assert_eq!(auth["secret"], "my-secret");
    assert_eq!(auth["passphrase"], "my-passphrase");
}
