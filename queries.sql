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
    profit_tier,
    count(*) AS trades,
    avg(confidence) AS avg_confidence,
    avg(profit_pct) AS avg_net_pct,
    sum(net_profit) AS total_net,
    avg(erosion_steps) AS avg_erosion_steps,
    avg(alloc_amount) AS avg_alloc
FROM simulated_trades
WHERE timestamp > dateadd('h', -24, now())
GROUP BY profit_tier;

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
FROM binance_ticks
WHERE timestamp > dateadd('h', -1, now())
SAMPLE BY 1m;

-- ════════════════════════════════════════════════════════════
-- TUNING QUERIES — map loss causes to config parameters
-- ════════════════════════════════════════════════════════════

-- ============================================================
-- 8. Loss attribution by exit reason (last 24h)
-- THE most important tuning query. Answers: "What is causing losses?"
--   BreakEvenBreach    → Phase 2 entry guard (BE breach at transition)
--   Phase2Timeout      → Phase 2 resting timed out
--   Phase2PriceBreach  → ask rose above Phase 2 posted price
--   Phase1Breach       → pair cost exceeded threshold during Phase 1
--   MarketExpiry       → tune entry_guards.entry_cutoff_secs
--   FavorableTaker     → usually a win (opportunistic take)
--   WhipsawReversal    → opposite spike → immediate FOK
--   NormalHedge        → happy path
-- ============================================================
SELECT
    exit_reason,
    count(*) AS trades,
    sum(CASE WHEN net_profit > 0 THEN 1 ELSE 0 END) AS wins,
    sum(CASE WHEN net_profit <= 0 THEN 1 ELSE 0 END) AS losses,
    round(sum(CASE WHEN net_profit > 0 THEN 1.0 ELSE 0 END) / count(*) * 100, 1) AS win_rate_pct,
    round(avg(net_profit), 6) AS avg_net_profit,
    round(sum(net_profit), 6) AS total_net_profit,
    round(avg(taker_fee), 6) AS avg_taker_fee,
    round(avg(pair_cost), 4) AS avg_pair_cost,
    round(avg(erosion_steps), 1) AS avg_erosion_steps
FROM simulated_trades
WHERE timestamp > dateadd('h', -24, now())
GROUP BY exit_reason
ORDER BY total_net_profit ASC;

-- ============================================================
-- 9. Spike quality vs. outcome (last 24h)
-- Answers: "Am I trading on weak spikes?"
-- Tune: spike_detection.min_magnitude_pct, spike_detection.multiplier
-- ============================================================
SELECT
    CASE
        WHEN spike_magnitude < 0.001 THEN '<0.10%'
        WHEN spike_magnitude < 0.002 THEN '0.10-0.20%'
        WHEN spike_magnitude < 0.005 THEN '0.20-0.50%'
        ELSE '>=0.50%'
    END AS spike_bucket,
    count(*) AS trades,
    round(sum(CASE WHEN net_profit > 0 THEN 1.0 ELSE 0 END) / count(*) * 100, 1) AS win_rate_pct,
    round(avg(net_profit), 6) AS avg_net_profit,
    round(sum(net_profit), 6) AS total_net_profit,
    round(avg(confidence), 3) AS avg_confidence
FROM simulated_trades
WHERE timestamp > dateadd('h', -24, now())
GROUP BY spike_bucket
ORDER BY spike_bucket;

-- ============================================================
-- 10. Confidence threshold calibration (last 24h)
-- Answers: "Where should HIGH/MED/LOW cutoffs be?"
-- Tune: confidence.high_threshold, confidence.med_threshold
-- ============================================================
SELECT
    CASE
        WHEN confidence < 0.2 THEN '0.00-0.20'
        WHEN confidence < 0.3 THEN '0.20-0.30'
        WHEN confidence < 0.4 THEN '0.30-0.40'
        WHEN confidence < 0.5 THEN '0.40-0.50'
        WHEN confidence < 0.6 THEN '0.50-0.60'
        WHEN confidence < 0.7 THEN '0.60-0.70'
        ELSE '0.70+'
    END AS conf_bucket,
    count(*) AS trades,
    round(sum(CASE WHEN net_profit > 0 THEN 1.0 ELSE 0 END) / count(*) * 100, 1) AS win_rate_pct,
    round(avg(net_profit), 6) AS avg_net_profit,
    round(sum(net_profit), 6) AS total_net_profit
FROM simulated_trades
WHERE timestamp > dateadd('h', -24, now())
GROUP BY conf_bucket
ORDER BY conf_bucket;

-- ============================================================
-- 11. Erosion step distribution vs. outcome (last 24h)
-- Answers: "Are trades filling too late in the cascade?"
-- Tune: confidence.*_target_pct, risk.erosion_base_interval_ms
-- ============================================================
SELECT
    erosion_steps,
    exit_reason,
    count(*) AS trades,
    round(sum(CASE WHEN net_profit > 0 THEN 1.0 ELSE 0 END) / count(*) * 100, 1) AS win_rate_pct,
    round(avg(net_profit), 6) AS avg_net_profit,
    round(avg(pair_cost), 4) AS avg_pair_cost,
    round(avg(taker_fee), 6) AS avg_taker_fee
FROM simulated_trades
WHERE timestamp > dateadd('h', -24, now())
GROUP BY erosion_steps, exit_reason
ORDER BY erosion_steps;

-- ============================================================
-- 12. Time remaining at entry vs. outcome (last 24h)
-- Answers: "Am I entering too late in the 5-min window?"
-- Tune: entry_guards.entry_cutoff_secs
-- ============================================================
SELECT
    CASE
        WHEN time_remaining < 300 THEN '<5min'
        WHEN time_remaining < 600 THEN '5-10min'
        ELSE '10-15min'
    END AS entry_window,
    count(*) AS signals,
    sum(CASE WHEN action = 'entered' THEN 1 ELSE 0 END) AS entered,
    round(sum(CASE WHEN action = 'entered' THEN 1.0 ELSE 0 END) / count(*) * 100, 1) AS entry_rate_pct
FROM trade_signals
WHERE timestamp > dateadd('h', -24, now())
GROUP BY entry_window
ORDER BY entry_window;

-- ============================================================
-- 13. Emergency taker vs. maker exit comparison (last 24h)
-- Answers: "How much are emergency fees eating profits?"
-- Tune: risk.emergency_deadline_ms (lower = faster FOK fallback, fewer maker fills)
-- ============================================================
SELECT
    CASE
        WHEN leg2_was_taker THEN 'taker (FOK)'
        WHEN emergency_maker THEN 'maker (post-only emergency)'
        ELSE 'maker (normal erosion)'
    END AS fill_type,
    count(*) AS trades,
    round(avg(taker_fee), 6) AS avg_fee,
    round(sum(taker_fee), 6) AS total_fees,
    round(avg(net_profit), 6) AS avg_net_profit,
    round(sum(net_profit), 6) AS total_net_profit,
    round(sum(CASE WHEN net_profit > 0 THEN 1.0 ELSE 0 END) / count(*) * 100, 1) AS win_rate_pct
FROM simulated_trades
WHERE timestamp > dateadd('h', -24, now())
GROUP BY fill_type;

-- ============================================================
-- 14. Signal funnel breakdown (last 24h)
-- Answers: "Where in the funnel am I losing opportunities?"
-- Tune: entry_guards.* based on which rejection dominates
-- ============================================================
SELECT
    action,
    count(*) AS signals,
    round(count(*) * 100.0 / (SELECT count(*) FROM trade_signals WHERE timestamp > dateadd('h', -24, now())), 1) AS pct_of_total,
    round(avg(confidence), 3) AS avg_confidence,
    round(avg(spike_magnitude), 6) AS avg_spike_mag
FROM trade_signals
WHERE timestamp > dateadd('h', -24, now())
GROUP BY action
ORDER BY signals DESC;

-- ============================================================
-- 15. Adverse movement deep-dive (last 24h)
-- Answers: "What pair costs trigger adverse exits?"
-- Tune: risk.adverse_threshold
-- ============================================================
SELECT
    round(pair_cost, 2) AS pair_cost_bucket,
    count(*) AS trades,
    round(avg(net_profit), 6) AS avg_net_profit,
    round(avg(taker_fee), 6) AS avg_fee,
    round(avg(erosion_steps), 1) AS avg_steps
FROM simulated_trades
WHERE exit_reason = 'AdverseMovement'
    AND timestamp > dateadd('h', -24, now())
GROUP BY pair_cost_bucket
ORDER BY pair_cost_bucket;

-- ============================================================
-- 16. Trades where pair cost exceeded $1.00 (hedge infeasibility at fill time)
-- Maps to: entry_guards.max_spread
-- ============================================================
SELECT
    direction,
    leg1_price,
    leg2_price,
    pair_cost,
    net_profit,
    exit_reason,
    spike_magnitude,
    timestamp
FROM simulated_trades
WHERE pair_cost > 1.0
ORDER BY timestamp DESC
LIMIT 50;

-- ════════════════════════════════════════════════════════════
-- LIVE MODE QUERIES — executed_trades (same schema as sim)
-- ════════════════════════════════════════════════════════════

-- ============================================================
-- 17. Live mode: overall performance (last 24h)
-- ============================================================
SELECT
    count(*) AS trades,
    avg(profit_pct) AS avg_net_pct,
    sum(net_profit) AS total_net,
    avg(taker_fee) AS avg_taker_fee,
    sum(CASE WHEN leg2_was_taker THEN 1 ELSE 0 END) AS emergency_taker_count
FROM executed_trades
WHERE timestamp > dateadd('h', -24, now());

-- ============================================================
-- 18. Live mode: loss attribution by exit reason (last 24h)
-- ============================================================
SELECT
    exit_reason,
    count(*) AS trades,
    sum(CASE WHEN net_profit > 0 THEN 1 ELSE 0 END) AS wins,
    sum(CASE WHEN net_profit <= 0 THEN 1 ELSE 0 END) AS losses,
    round(sum(CASE WHEN net_profit > 0 THEN 1.0 ELSE 0 END) / count(*) * 100, 1) AS win_rate_pct,
    round(avg(net_profit), 6) AS avg_net_profit,
    round(sum(net_profit), 6) AS total_net_profit,
    round(avg(taker_fee), 6) AS avg_taker_fee,
    round(avg(pair_cost), 4) AS avg_pair_cost,
    round(avg(erosion_steps), 1) AS avg_erosion_steps
FROM executed_trades
WHERE timestamp > dateadd('h', -24, now())
GROUP BY exit_reason
ORDER BY total_net_profit ASC;

-- ============================================================
-- 19. Live mode: emergency taker vs. maker exit (last 24h)
-- ============================================================
SELECT
    CASE
        WHEN leg2_was_taker THEN 'taker (FOK)'
        WHEN emergency_maker THEN 'maker (post-only emergency)'
        ELSE 'maker (normal erosion)'
    END AS fill_type,
    count(*) AS trades,
    round(avg(taker_fee), 6) AS avg_fee,
    round(sum(taker_fee), 6) AS total_fees,
    round(avg(net_profit), 6) AS avg_net_profit,
    round(sum(net_profit), 6) AS total_net_profit,
    round(sum(CASE WHEN net_profit > 0 THEN 1.0 ELSE 0 END) / count(*) * 100, 1) AS win_rate_pct
FROM executed_trades
WHERE timestamp > dateadd('h', -24, now())
GROUP BY fill_type;

-- ============================================================
-- 20. Live mode: spike quality vs. outcome (last 24h)
-- ============================================================
SELECT
    CASE
        WHEN spike_magnitude < 0.001 THEN '<0.10%'
        WHEN spike_magnitude < 0.002 THEN '0.10-0.20%'
        WHEN spike_magnitude < 0.005 THEN '0.20-0.50%'
        ELSE '>=0.50%'
    END AS spike_bucket,
    count(*) AS trades,
    round(sum(CASE WHEN net_profit > 0 THEN 1.0 ELSE 0 END) / count(*) * 100, 1) AS win_rate_pct,
    round(avg(net_profit), 6) AS avg_net_profit,
    round(sum(net_profit), 6) AS total_net_profit,
    round(avg(confidence), 3) AS avg_confidence
FROM executed_trades
WHERE timestamp > dateadd('h', -24, now())
GROUP BY spike_bucket
ORDER BY spike_bucket;

-- ============================================================
-- ════════════════════════════════════════════════════════════
-- METRIC TUNING QUERIES — use after 30min session
-- ════════════════════════════════════════════════════════════
-- Run these to understand metric normalization & freshness
-- to tune config.toml saturation and freshness gates.

-- ============================================================
-- 21. Metric normalization distribution (recent signals)
-- Shows if metrics are saturating at 1.0 or stuck at 0.0
-- Tune: saturation (if all 1.0) or min_threshold (if all 0.0)
-- ============================================================
SELECT
    'CVD' AS metric,
    count(*) AS total_signals,
    round(min(cvd_norm), 3) AS min_val,
    round(max(cvd_norm), 3) AS max_val,
    round(avg(cvd_norm), 3) AS avg_val,
    sum(CASE WHEN cvd_norm = 0.0 THEN 1 ELSE 0 END) AS count_zero,
    sum(CASE WHEN cvd_norm = 1.0 THEN 1 ELSE 0 END) AS count_max,
    sum(CASE WHEN cvd_norm BETWEEN 0.0 AND 0.3 THEN 1 ELSE 0 END) AS count_weak,
    sum(CASE WHEN cvd_norm BETWEEN 0.3 AND 0.7 THEN 1 ELSE 0 END) AS count_medium,
    sum(CASE WHEN cvd_norm BETWEEN 0.7 AND 1.0 THEN 1 ELSE 0 END) AS count_strong
FROM trade_signals
WHERE timestamp > dateadd('m', -30, now())
UNION ALL
SELECT
    'Basis' AS metric,
    count(*) AS total_signals,
    round(min(basis_norm), 3) AS min_val,
    round(max(basis_norm), 3) AS max_val,
    round(avg(basis_norm), 3) AS avg_val,
    sum(CASE WHEN basis_norm = 0.0 THEN 1 ELSE 0 END) AS count_zero,
    sum(CASE WHEN basis_norm = 1.0 THEN 1 ELSE 0 END) AS count_max,
    sum(CASE WHEN basis_norm BETWEEN 0.0 AND 0.3 THEN 1 ELSE 0 END) AS count_weak,
    sum(CASE WHEN basis_norm BETWEEN 0.3 AND 0.7 THEN 1 ELSE 0 END) AS count_medium,
    sum(CASE WHEN basis_norm BETWEEN 0.7 AND 1.0 THEN 1 ELSE 0 END) AS count_strong
FROM trade_signals
WHERE timestamp > dateadd('m', -30, now())
UNION ALL
SELECT
    'SpotFlow' AS metric,
    count(*) AS total_signals,
    round(min(spot_flow_norm), 3) AS min_val,
    round(max(spot_flow_norm), 3) AS max_val,
    round(avg(spot_flow_norm), 3) AS avg_val,
    sum(CASE WHEN spot_flow_norm = 0.0 THEN 1 ELSE 0 END) AS count_zero,
    sum(CASE WHEN spot_flow_norm = 1.0 THEN 1 ELSE 0 END) AS count_max,
    sum(CASE WHEN spot_flow_norm BETWEEN 0.0 AND 0.3 THEN 1 ELSE 0 END) AS count_weak,
    sum(CASE WHEN spot_flow_norm BETWEEN 0.3 AND 0.7 THEN 1 ELSE 0 END) AS count_medium,
    sum(CASE WHEN spot_flow_norm BETWEEN 0.7 AND 1.0 THEN 1 ELSE 0 END) AS count_strong
FROM trade_signals
WHERE timestamp > dateadd('m', -30, now())
UNION ALL
SELECT
    'OBI' AS metric,
    count(*) AS total_signals,
    round(min(obi_norm), 3) AS min_val,
    round(max(obi_norm), 3) AS max_val,
    round(avg(obi_norm), 3) AS avg_val,
    sum(CASE WHEN obi_norm = 0.0 THEN 1 ELSE 0 END) AS count_zero,
    sum(CASE WHEN obi_norm = 1.0 THEN 1 ELSE 0 END) AS count_max,
    sum(CASE WHEN obi_norm BETWEEN 0.0 AND 0.3 THEN 1 ELSE 0 END) AS count_weak,
    sum(CASE WHEN obi_norm BETWEEN 0.3 AND 0.7 THEN 1 ELSE 0 END) AS count_medium,
    sum(CASE WHEN obi_norm BETWEEN 0.7 AND 1.0 THEN 1 ELSE 0 END) AS count_strong
FROM trade_signals
WHERE timestamp > dateadd('m', -30, now())
UNION ALL
SELECT
    'Liq' AS metric,
    count(*) AS total_signals,
    round(min(liq_norm), 3) AS min_val,
    round(max(liq_norm), 3) AS max_val,
    round(avg(liq_norm), 3) AS avg_val,
    sum(CASE WHEN liq_norm = 0.0 THEN 1 ELSE 0 END) AS count_zero,
    sum(CASE WHEN liq_norm = 1.0 THEN 1 ELSE 0 END) AS count_max,
    sum(CASE WHEN liq_norm BETWEEN 0.0 AND 0.3 THEN 1 ELSE 0 END) AS count_weak,
    sum(CASE WHEN liq_norm BETWEEN 0.3 AND 0.7 THEN 1 ELSE 0 END) AS count_medium,
    sum(CASE WHEN liq_norm BETWEEN 0.7 AND 1.0 THEN 1 ELSE 0 END) AS count_strong
FROM trade_signals
WHERE timestamp > dateadd('m', -30, now())
UNION ALL
SELECT
    'ATR' AS metric,
    count(*) AS total_signals,
    round(min(atr_norm), 3) AS min_val,
    round(max(atr_norm), 3) AS max_val,
    round(avg(atr_norm), 3) AS avg_val,
    sum(CASE WHEN atr_norm = 0.0 THEN 1 ELSE 0 END) AS count_zero,
    sum(CASE WHEN atr_norm = 1.0 THEN 1 ELSE 0 END) AS count_max,
    sum(CASE WHEN atr_norm BETWEEN 0.0 AND 0.3 THEN 1 ELSE 0 END) AS count_weak,
    sum(CASE WHEN atr_norm BETWEEN 0.3 AND 0.7 THEN 1 ELSE 0 END) AS count_medium,
    sum(CASE WHEN atr_norm BETWEEN 0.7 AND 1.0 THEN 1 ELSE 0 END) AS count_strong
FROM trade_signals
WHERE timestamp > dateadd('m', -30, now());

-- ============================================================
-- 22. Metric freshness distribution (recent signals)
-- Shows age of metrics at signal time (ms since last update)
-- If age > freshness_X_ms, metric was stale — increase gate
-- ============================================================
SELECT
    'CVD' AS metric,
    count(*) AS total_signals,
    round(min(cvd_age_ms), 0) AS min_age_ms,
    round(max(cvd_age_ms), 0) AS max_age_ms,
    round(avg(cvd_age_ms), 0) AS avg_age_ms,
    sum(CASE WHEN cvd_age_ms > 150 THEN 1 ELSE 0 END) AS stale_count,
    '150' AS freshness_gate_ms
FROM trade_signals
WHERE timestamp > dateadd('m', -30, now())
UNION ALL
SELECT
    'Basis' AS metric,
    count(*) AS total_signals,
    round(min(basis_age_ms), 0) AS min_age_ms,
    round(max(basis_age_ms), 0) AS max_age_ms,
    round(avg(basis_age_ms), 0) AS avg_age_ms,
    sum(CASE WHEN basis_age_ms > 150 THEN 1 ELSE 0 END) AS stale_count,
    '150' AS freshness_gate_ms
FROM trade_signals
WHERE timestamp > dateadd('m', -30, now())
UNION ALL
SELECT
    'SpotFlow' AS metric,
    count(*) AS total_signals,
    round(min(spot_flow_age_ms), 0) AS min_age_ms,
    round(max(spot_flow_age_ms), 0) AS max_age_ms,
    round(avg(spot_flow_age_ms), 0) AS avg_age_ms,
    sum(CASE WHEN spot_flow_age_ms > 150 THEN 1 ELSE 0 END) AS stale_count,
    '150' AS freshness_gate_ms
FROM trade_signals
WHERE timestamp > dateadd('m', -30, now())
UNION ALL
SELECT
    'OBI' AS metric,
    count(*) AS total_signals,
    round(min(obi_age_ms), 0) AS min_age_ms,
    round(max(obi_age_ms), 0) AS max_age_ms,
    round(avg(obi_age_ms), 0) AS avg_age_ms,
    sum(CASE WHEN obi_age_ms > 120 THEN 1 ELSE 0 END) AS stale_count,
    '120' AS freshness_gate_ms
FROM trade_signals
WHERE timestamp > dateadd('m', -30, now())
UNION ALL
SELECT
    'Liq' AS metric,
    count(*) AS total_signals,
    round(min(liq_age_ms), 0) AS min_age_ms,
    round(max(liq_age_ms), 0) AS max_age_ms,
    round(avg(liq_age_ms), 0) AS avg_age_ms,
    sum(CASE WHEN liq_age_ms > 2500 THEN 1 ELSE 0 END) AS stale_count,
    '2500' AS freshness_gate_ms
FROM trade_signals
WHERE timestamp > dateadd('m', -30, now())
UNION ALL
SELECT
    'ATR' AS metric,
    count(*) AS total_signals,
    round(min(atr_age_ms), 0) AS min_age_ms,
    round(max(atr_age_ms), 0) AS max_age_ms,
    round(avg(atr_age_ms), 0) AS avg_age_ms,
    sum(CASE WHEN atr_age_ms > 120 THEN 1 ELSE 0 END) AS stale_count,
    '120' AS freshness_gate_ms
FROM trade_signals
WHERE timestamp > dateadd('m', -30, now());

-- Pruning (run hourly via automated task)
-- ============================================================
-- ALTER TABLE binance_ticks DROP PARTITION WHERE timestamp < dateadd('h', -24, now());
-- ALTER TABLE poly_book_snapshots DROP PARTITION WHERE timestamp < dateadd('h', -24, now());
-- ALTER TABLE trade_signals DROP PARTITION WHERE timestamp < dateadd('h', -24, now());
-- ALTER TABLE simulated_trades DROP PARTITION WHERE timestamp < dateadd('h', -24, now());
-- ALTER TABLE executed_trades DROP PARTITION WHERE timestamp < dateadd('h', -24, now());
