use super::*;

#[test]
fn test_parse_agg_trade() {
    let json = r#"{"stream":"btcusdt@aggTrade","data":{"e":"aggTrade","E":1234567890123,"s":"BTCUSDT","a":1234,"p":"50000.00","q":"0.5","f":100,"l":100,"T":1234567890123,"m":false}}"#;
    let wrapper: StreamWrapper = serde_json::from_str(json).unwrap();
    assert!(wrapper.stream.ends_with("@aggTrade"));
    let t: AggTradePayload = serde_json::from_value(wrapper.data).unwrap();
    assert_eq!(t.p, "50000.00");
    assert_eq!(t.q, "0.5");
    assert!(!t.m);
}

#[test]
fn test_parse_book_ticker() {
    let json = r#"{"stream":"btcusdt@bookTicker","data":{"e":"bookTicker","u":1234,"s":"BTCUSDT","b":"49999.50","B":"1.2","a":"50000.50","A":"0.8","T":1234567890123,"E":1234567890123}}"#;
    let wrapper: StreamWrapper = serde_json::from_str(json).unwrap();
    assert!(wrapper.stream.ends_with("@bookTicker"));
    let t: BookTickerPayload = serde_json::from_value(wrapper.data).unwrap();
    assert_eq!(t.b, "49999.50");
    assert_eq!(t.a, "50000.50");
}

#[test]
fn test_parse_force_order() {
    let json = r#"{"stream":"btcusdt@forceOrder","data":{"e":"forceOrder","E":1234567890123,"o":{"s":"BTCUSDT","S":"SELL","o":"LIMIT","f":"IOC","q":"0.1","p":"49500.00","ap":"49500.00","X":"FILLED","l":"0.1","z":"0.1","T":1234567890123}}}"#;
    let wrapper: StreamWrapper = serde_json::from_str(json).unwrap();
    assert!(wrapper.stream.ends_with("@forceOrder"));
    let t: ForceOrderPayload = serde_json::from_value(wrapper.data).unwrap();
    assert_eq!(t.o.side, "SELL");
    assert_eq!(t.o.p, "49500.00");
    assert_eq!(t.o.q, "0.1");
}
