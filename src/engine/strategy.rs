use rust_decimal::Decimal;
use tracing::info;

use crate::types::{IngestorEvent, MarketState, TradeSignal};

/// Evaluates ingestor events against the current market state and produces trade signals.
pub struct StrategyEngine {
    state: MarketState,
}

impl StrategyEngine {
    pub fn new() -> Self {
        Self {
            state: MarketState::new(),
        }
    }

    /// Process an inbound event and update internal state.
    pub fn on_event(&mut self, event: IngestorEvent) {
        match event {
            IngestorEvent::PolymarketBook(book) => {
                self.state.last_update_ms = book.timestamp_ms;
                self.state.poly_book = Some(book);
            }
            IngestorEvent::BinanceTick(tick) => {
                self.state.last_update_ms = tick.timestamp_ms;
                // Use midpoint of Binance bid/ask as reference price.
                self.state.binance_price = Some((tick.bid_price + tick.ask_price) / Decimal::TWO);
            }
            IngestorEvent::MarketRotation {
                condition_id,
                token_id,
            } => {
                info!(%condition_id, %token_id, "market rotated to new 15m window");
                self.state.active_condition_id = Some(condition_id);
                self.state.active_token_id = Some(token_id);
                // Clear stale book on rotation.
                self.state.poly_book = None;
            }
        }
    }

    /// Evaluate current state and optionally emit a trade signal.
    ///
    /// Core arbitrage logic:
    /// - Compare Polymarket YES token price against Binance-implied probability.
    /// - If the CLOB price is significantly mispriced relative to the reference,
    ///   generate a signal.
    pub fn evaluate(&self) -> Option<TradeSignal> {
        let book = self.state.poly_book.as_ref()?;
        let _reference = self.state.binance_price?;
        let token_id = self.state.active_token_id.as_ref()?;
        let best_ask = book.best_ask()?;
        let best_bid = book.best_bid()?;

        // TODO: Implement actual arbitrage evaluation.
        // Placeholder: detect trivially wide spreads as a signal.
        let spread = best_ask.price - best_bid.price;
        let _threshold = Decimal::new(5, 2); // 0.05

        // Skeleton: no signal emitted until strategy is implemented.
        let _ = (spread, token_id);
        None
    }

    pub fn state(&self) -> &MarketState {
        &self.state
    }
}

impl Default for StrategyEngine {
    fn default() -> Self {
        Self::new()
    }
}
