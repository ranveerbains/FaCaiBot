-- FaCaiBot — Common QuestDB Analytics Queries
-- Run via QuestDB web console at localhost:9000

-- ============================================================
-- Overall performance (last 24h)
-- ============================================================
SELECT
    count(*) AS trades,
    avg(profit_pct) AS avg_net_pct,
    sum(net_profit) AS total_net,
    avg(taker_fee) AS avg_taker_fee,
    sum(CASE WHEN leg2_was_taker THEN 1 ELSE 0 END) AS emergency_taker_count
FROM simulated_trades
WHERE timestamp > dateadd('h', -24, now());

-- ============================================================
-- Fill rate analysis (last 24h)
-- ============================================================
SELECT
    count(*) AS total_signals,
    sum(CASE WHEN action = 'entered' THEN 1 ELSE 0 END) AS leg1_fills,
    sum(CASE WHEN action = 'unfilled_postonly' THEN 1 ELSE 0 END) AS unfilled,
    sum(CASE WHEN action = 'aborted_spread' THEN 1 ELSE 0 END) AS aborted_spread,
    sum(CASE WHEN action = 'aborted_liquidity' THEN 1 ELSE 0 END) AS aborted_liquidity,
    round(sum(CASE WHEN action = 'entered' THEN 1.0 ELSE 0 END) / count(*) * 100, 1) AS fill_rate_pct
FROM trade_signals
WHERE timestamp > dateadd('h', -24, now());

-- ============================================================
-- Performance by confidence tier (last 24h)
-- ============================================================
SELECT
    profit_target_tier,
    count(*) AS trades,
    avg(confidence) AS avg_confidence,
    avg(profit_pct) AS avg_net_pct,
    sum(net_profit) AS total_net,
    avg(erosion_steps) AS avg_erosion_steps,
    avg(alloc_amount) AS avg_alloc
FROM simulated_trades
WHERE timestamp > dateadd('h', -24, now())
GROUP BY profit_target_tier;

-- ============================================================
-- Emergency taker fee impact by price range
-- ============================================================
SELECT
    CASE
        WHEN leg2_price BETWEEN 0.4 AND 0.6 THEN 'mid-range (0.4-0.6)'
        ELSE 'extreme (<0.4 or >0.6)'
    END AS zone,
    avg(taker_fee) AS avg_fee,
    avg(profit_pct) AS avg_net_pct,
    count(*) AS n
FROM simulated_trades
WHERE leg2_was_taker = true
    AND timestamp > dateadd('h', -24, now())
GROUP BY zone;

-- ============================================================
-- Hourly signal rate and fill rate
-- ============================================================
SELECT
    timestamp AS hour,
    count(*) AS signals,
    sum(CASE WHEN action = 'entered' THEN 1 ELSE 0 END) AS fills
FROM trade_signals
WHERE timestamp > dateadd('h', -24, now())
SAMPLE BY 1h;

-- ============================================================
-- Polymarket book spread over time (5s snapshots)
-- ============================================================
SELECT
    timestamp,
    token_id,
    best_bid,
    best_ask,
    spread,
    bid_depth,
    ask_depth
FROM poly_book_snapshots
WHERE timestamp > dateadd('h', -1, now())
ORDER BY timestamp DESC;

-- ============================================================
-- Binance tick volatility (1-min ATR proxy)
-- ============================================================
SELECT
    timestamp AS minute,
    symbol,
    max(mid) - min(mid) AS range,
    avg(mid) AS avg_mid
FROM (
    SELECT timestamp, symbol, (bid_price + ask_price) / 2 AS mid
    FROM binance_ticks
    WHERE timestamp > dateadd('h', -1, now())
)
SAMPLE BY 1m;

-- ============================================================
-- Pruning (run hourly via automated task)
-- ============================================================
-- ALTER TABLE binance_ticks DROP PARTITION WHERE timestamp < dateadd('h', -24, now());
-- ALTER TABLE poly_book_snapshots DROP PARTITION WHERE timestamp < dateadd('h', -24, now());
-- ALTER TABLE trade_signals DROP PARTITION WHERE timestamp < dateadd('h', -24, now());
-- ALTER TABLE simulated_trades DROP PARTITION WHERE timestamp < dateadd('h', -24, now());
