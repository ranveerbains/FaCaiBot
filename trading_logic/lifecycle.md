# Trade Lifecycle (Sections 9-12)

---

## 9. Trade Completion & State Reset

### Detection

**Simulation:** `advance_simulation()` detects both legs Filled after Leg 2 fill.

**Live:** Two detection paths:
1. **User WS fill**: The main engine loop checks after processing each event -- if both `leg1_state` and `leg2_state` are Filled, calls `on_trade_complete()`. Leg 1 fills arrive via User WS `TradeStatusUpdate` (MATCHED event with hex order hash). Leg 2 fills arrive via User WS or sync FOK.
2. **Sync FOK fill**: When `OrderPosted` feedback has `already_filled=true` (FOK returned `Filled` synchronously from REST, or post-only filled synchronously -- rare), the engine transitions the leg directly to `OrderState::Filled` in `on_order_posted()`. The main loop detects both legs filled immediately in the feedback drain iteration and triggers `on_trade_complete()`. This prevents the double-fill bug where the engine keeps evaluating and dispatching additional FOK signals while waiting for a User WS MATCHED event that never arrives for synchronous fills.

### State reset

On completion: `leg1_state`, `leg2_state`, and `hedge` all reset to None. `cumulative_used` persists (capital stays allocated within this market). After reset, the engine can immediately accept a new buildup signal.

### PnL computation (simulation)

- **Pair cost** = leg1_price + leg2_price (per share)
- **Paired size** = min(leg1_size, leg2_size) -- the quantity actually hedged
- **Gross profit** = (1.0 - pair_cost) x paired_size
- **Maker rebate** = sum of both legs' `compute_maker_rebate(price, size)` (= `compute_taker_fee() x 0.20`; zero for taker fills)
- **Net profit** = gross_profit - taker_fee + maker_rebate
- **Total cost** = leg1_price x leg1_size + leg2_price x leg2_size (actual capital deployed -- not pair_cost x size)
- **Profit %** = net_profit / total_cost x 100
- **Partial fill handling**: When leg sizes differ (e.g., `clob_safe_fok_size()` reduces FOK size), `was_partial = true` is set on the larger leg's `SimFill`. Telegram shows unhedged shares pending resolution

---

## 10. Market Rotation

### Timeline

```
T-180s   Cutoff window: no new Leg 1 entries (buildups dropped)
T-180s   Pre-warm: discover Market B via Gamma API, fetch books
T-0      Instant switch: emit pre-warmed MarketRotation + books
         Market WS resubscribes to new token IDs in parallel
         Quiet period starts (rotation_quiet_ms = 30s, no new entries)
T+30s    Quiet period ends -- trading enabled
```

### Market discovery

Gamma API `GET /events?tag_id=102892&closed=false&order=endDate&ascending=true&limit=100`. Tag 102892 = "5M" markets. Filter by slug prefix `btc-updown-5m-`. Note: `clobTokenIds` is a JSON-encoded string (not an array) -- index 0 = YES, index 1 = NO.

Pre-warming at T-180s: query for markets ending after the current one, skipping Market A to find Market B. Pre-fetch both YES and NO books via REST.

**Fallback:** If pre-warming failed, falls back to immediate Gamma poll within 5s of expiry.

**Delivery:** `MarketRotation` uses blocking `send()` to guarantee delivery. Book events use `try_send()` (expendable -- WS will provide updates).

### Rotation emergency protection

If Leg 1 is Filled but Leg 2 incomplete when rotation arrives, the engine builds an emergency FOK signal using the OLD market's token IDs and books BEFORE resetting state. The main loop drains this buffer before sending the rotation command to the executor.

**Drainage order:**
1. Drain `rotation_emergency_buffer` -> send emergency signals to executor
2. Send `MarketRotation` -> executor resets

**Telegram alert (live mode):** When `leg1_filled && !leg2_filled` at rotation, the engine sends a `fire_critical()` Telegram alert with position details (direction, entry price, size, whether a FOK was submitted). This ensures abandoned positions are never silent -- you always know when a trade was open at market expiry.

### Engine state reset on rotation

All market-specific state resets: active token IDs updated, books cleared, buildup state cleared, leg states cleared, `cumulative_used` reset to 0, `in_cutoff_window` reset to false, `whipsaw_fok_pending` reset to false, `in_quiet_period` set to true (starts `rotation_quiet_ms` quiet period), `in_trade_cooldown` reset to false (new market shouldn't inherit stale cooldown), `pending_leg1_cancel` cleared.

### Executor cleanup

**Live:** `cancel_all()` open CLOB orders, reset active order tracking.

**Simulation:** Force-close any open positions (record as full loss), lock `AwaitingResolution` positions for UMA resolution tracking, reset per-market counters, increment `markets_observed`.

---

## 11. Cutoff Window

### Detection

Checked on every event -- the cutoff is detected promptly regardless of event type. When `time_remaining_secs < entry_cutoff_secs` (`entry_cutoff_secs`), the engine sets `in_cutoff_window = true`.

### Effects

| Action | During cutoff? |
|--------|---------------|
| New Leg 1 entries | **Blocked** -- buildups dropped |
| Existing Leg 2 hedge | **Continues** -- no cutoff check in Leg 2 evaluation |
| Emergency exits | **Continue** -- Phase 1 breach, break-even, Phase 2 timeout, whipsaw, flow reversal/collapse, favorable all active |
| Market summary | **Sent** -- `MarketCutoff` triggers Telegram summary (sim) |

When first entering cutoff with an open position, a log notes "Leg 2 will continue until rotation" -- informational only, no special action taken.

---

## 11b. Rotation Quiet Period

### Detection

Checked on every event. When `MarketRotation` fires, `in_quiet_period = true` and `rotation_ms = now_ms`. On each subsequent event, if `now_ms - rotation_ms >= rotation_quiet_ms` (default 30000ms), `in_quiet_period` clears.

### Effects

| Action | During quiet period? |
|--------|---------------------|
| New Leg 1 entries | **Blocked** -- buildup signals dropped |
| Existing Leg 2 hedge | N/A -- no position exists at rotation start |
| Emergency exits | N/A |

### Rationale

After market rotation, the Polymarket book takes ~20-30s to fully reprice. Entries during this window have stale reference prices, leading to losses clustering near rotation boundaries. The quiet period prevents this by waiting for market makers to establish fresh liquidity.

### Diagnostic

`diag_buildups_dropped_quiet` counter, visible in `/diag` Telegram output.

---

## 11c. Trade Cooldown

### Detection

Checked on every event (in `update_phase()`). When `on_trade_complete()` fires, `in_trade_cooldown = true` and `last_trade_complete_ms = now_ms`. On each subsequent event, if `now_ms - last_trade_complete_ms >= trade_cooldown_ms` (default 5000ms), `in_trade_cooldown` clears.

### Effects

| Action | During cooldown? |
|--------|-----------------|
| New Leg 1 entries | **Blocked** -- buildups dropped in `evaluate()` |
| Signal detection | **Continues** -- buildup events flow through normally, only entry is rejected |

### Rationale

Session analysis showed 6 trades in ~2 minutes on the same market. First 3 won (+$0.97), last 3 lost (-$2.38). The bot re-enters too quickly after completing a trade when the market is still volatile from the previous move. A 5s cooldown prevents rapid-fire re-entry.

### Reset

Cleared on `MarketRotation` -- a new market shouldn't inherit a stale cooldown from the previous market.

### Diagnostic

`diag_buildups_dropped_cooldown` counter, visible in 60s terminal log and `/diag` Telegram output.

---

## 12. Capital Management

### Per-trade allocation

`alloc = max(round(max_alloc_per_trade x tier_pct), $1)`. `max_alloc_per_trade` is the sole capital control.

### Per-market budget

`cumulative_used` tracks total USDC allocated in the current 5-minute market. Reset to 0 on rotation. No explicit per-market cap guard -- the single-trade-at-a-time constraint plus `max_alloc_per_trade` naturally bound exposure.

### Session tracking (simulation only)

| Field | Updates |
|-------|---------|
| `virtual_balance` | -cost on Leg 1 fill, +(cost + net_profit) on trade close |
| `locked_in_resolution` | +cost when position locked for UMA |
| `total_pnl` | +net_profit on each trade close |
| `total_taker_fees_paid` | +fee on each taker fill |
| `total_maker_rebates_earned` | +maker_rebate on each trade close |

### Live capital

In live mode, the wallet USDC.e balance is the real constraint. No virtual balance tracking -- the CLOB rejects orders that exceed available funds.
