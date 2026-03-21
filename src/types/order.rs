use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use crate::engine::position::MarketSide;

// ─── Side ────────────────────────────────────────────────────────────────────

/// Side of a trade on the Polymarket CLOB (Buy or Sell).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Side {
    Buy,
    Sell,
}

// ─── Order Type ──────────────────────────────────────────────────────────────

/// Order time-in-force / execution type for the Polymarket CLOB.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum OrderType {
    Gtc,
    Gtd,
    Fok,
}

// ─── Order Request ───────────────────────────────────────────────────────────

/// Request submitted to the Polymarket CLOB.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderRequest {
    pub token_id: String,
    pub side: Side,
    pub price: Decimal,
    pub size: Decimal,
    pub order_type: OrderType,
    pub post_only: bool,
    pub expiration: Option<u64>,
}

impl OrderRequest {
    pub fn post_only_gtc(token_id: String, side: Side, price: Decimal, size: Decimal) -> Self {
        Self {
            token_id,
            side,
            price,
            size,
            order_type: OrderType::Gtc,
            post_only: true,
            expiration: None,
        }
    }

    pub fn emergency_fok(token_id: String, side: Side, price: Decimal, size: Decimal) -> Self {
        Self {
            token_id,
            side,
            price,
            size,
            order_type: OrderType::Fok,
            post_only: false,
            expiration: None,
        }
    }
}

// ─── Order Response ──────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderResponse {
    pub order_id: String,
    pub status: OrderStatus,
    pub timestamp_ms: u64,
    pub size_matched: Decimal,
}

// ─── Order Status ────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum OrderStatus {
    Placed,
    Filled,
    Cancelled,
    Rejected,
}

// ─── V2 Executor Command ────────────────────────────────────────────────────

/// Commands sent from the V2 engine to the executor.
#[derive(Debug, Clone)]
pub enum V2ExecutorCommand {
    /// Post a maker order on one side.
    PostOrder {
        side: MarketSide,
        token_id: String,
        price: Decimal,
        size: Decimal,
    },
    /// Cancel a resting order.
    CancelOrder {
        side: MarketSide,
        order_id: String,
    },
    /// CLOSING phase: aggressive FOK to pair position.
    ClosingFok {
        side: MarketSide,
        token_id: String,
        price: Decimal,
        size: Decimal,
    },
    /// Taker rebalance order to correct inventory imbalance.
    RebalanceTaker {
        side: MarketSide,
        token_id: String,
        price: Decimal,
        size: Decimal,
    },
    /// Market rotation — executor should reset state.
    MarketRotation {
        condition_id: String,
        yes_token_id: String,
        no_token_id: String,
        tick_size: Decimal,
    },
    /// Tick size changed mid-market.
    TickSizeChanged {
        yes_token_id: String,
        no_token_id: String,
        new_tick_size: Decimal,
    },
}

// ─── V2 Executor Feedback ───────────────────────────────────────────────────

/// Feedback from the executor back to the V2 engine.
#[derive(Debug, Clone)]
pub enum V2ExecutorFeedback {
    /// Order successfully posted on the CLOB.
    OrderPosted {
        side: MarketSide,
        order_id: String,
        price: Decimal,
        size: Decimal,
        already_filled: bool,
    },
    /// Order placement failed.
    OrderFailed {
        side: MarketSide,
    },
    /// Cancel result with authoritative fill size.
    CancelResult {
        side: MarketSide,
        order_id: String,
        size_matched: Option<Decimal>,
    },
    /// Closing FOK result.
    ClosingFokResult {
        side: MarketSide,
        filled: bool,
        size_matched: Decimal,
        price: Decimal,
    },
    /// Taker rebalance result.
    RebalanceResult {
        side: MarketSide,
        filled: bool,
        size_matched: Decimal,
        price: Decimal,
    },
}
