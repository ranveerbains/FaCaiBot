use rust_decimal::Decimal;

/// Which side of the binary market (YES or NO token).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MarketSide {
    Yes,
    No,
}

impl MarketSide {
    pub fn label(&self) -> &'static str {
        match self {
            MarketSide::Yes => "YES",
            MarketSide::No => "NO",
        }
    }
}

/// A single fill record for reporting.
#[derive(Debug, Clone)]
pub struct AccFill {
    pub price: Decimal,
    pub size: Decimal,
    pub timestamp_ms: u64,
    pub was_taker: bool,
    pub fee: Decimal, // positive = taker fee paid, negative = maker rebate earned
}

/// Accumulated shares on one side (YES or NO).
#[derive(Debug, Clone)]
pub struct SidePosition {
    pub total_shares: Decimal,
    pub total_cost: Decimal,      // sum of (price * shares) across fills
    pub total_rebate: Decimal,    // estimated maker rebates earned
    pub total_taker_fee: Decimal, // taker fees paid
    pub fill_count: u32,
    pub fills: Vec<AccFill>,
}

impl SidePosition {
    pub fn new() -> Self {
        Self {
            total_shares: Decimal::ZERO,
            total_cost: Decimal::ZERO,
            total_rebate: Decimal::ZERO,
            total_taker_fee: Decimal::ZERO,
            fill_count: 0,
            fills: Vec::new(),
        }
    }

    /// Volume-weighted average fill price.
    pub fn avg_price(&self) -> Decimal {
        if self.total_shares.is_zero() {
            Decimal::ZERO
        } else {
            self.total_cost / self.total_shares
        }
    }

    pub fn record_fill(&mut self, price: Decimal, size: Decimal, timestamp_ms: u64, was_taker: bool, fee: Decimal) {
        self.total_shares += size;
        self.total_cost += price * size;
        if was_taker {
            self.total_taker_fee += fee;
        } else {
            self.total_rebate += fee.abs(); // maker rebate stored as positive
        }
        self.fill_count += 1;
        self.fills.push(AccFill {
            price,
            size,
            timestamp_ms,
            was_taker,
            fee,
        });
    }

    pub fn reset(&mut self) {
        self.total_shares = Decimal::ZERO;
        self.total_cost = Decimal::ZERO;
        self.total_rebate = Decimal::ZERO;
        self.total_taker_fee = Decimal::ZERO;
        self.fill_count = 0;
        self.fills.clear();
    }
}

/// Bilateral position tracking: YES + NO shares accumulated during one market.
#[derive(Debug, Clone)]
pub struct BilateralPosition {
    pub yes: SidePosition,
    pub no: SidePosition,
}

impl BilateralPosition {
    pub fn new() -> Self {
        Self {
            yes: SidePosition::new(),
            no: SidePosition::new(),
        }
    }

    pub fn side_mut(&mut self, side: MarketSide) -> &mut SidePosition {
        match side {
            MarketSide::Yes => &mut self.yes,
            MarketSide::No => &mut self.no,
        }
    }

    pub fn record_fill(
        &mut self,
        side: MarketSide,
        price: Decimal,
        size: Decimal,
        timestamp_ms: u64,
        was_taker: bool,
        fee: Decimal,
    ) {
        self.side_mut(side).record_fill(price, size, timestamp_ms, was_taker, fee);
    }

    /// Minimum of YES and NO shares — these are fully paired.
    pub fn paired_shares(&self) -> Decimal {
        self.yes.total_shares.min(self.no.total_shares)
    }

    /// Average pair cost per share: yes_avg + no_avg.
    /// Only meaningful when both sides have fills.
    pub fn avg_pair_cost(&self) -> Decimal {
        self.yes.avg_price() + self.no.avg_price()
    }

    /// Locked profit on paired shares: (1.00 - avg_pair_cost) x paired_shares.
    pub fn locked_profit(&self) -> Decimal {
        let paired = self.paired_shares();
        if paired.is_zero() {
            return Decimal::ZERO;
        }
        (Decimal::ONE - self.avg_pair_cost()) * paired
    }

    /// Unpaired shares on YES side.
    pub fn unpaired_yes(&self) -> Decimal {
        self.yes.total_shares - self.paired_shares()
    }

    /// Unpaired shares on NO side.
    pub fn unpaired_no(&self) -> Decimal {
        self.no.total_shares - self.paired_shares()
    }

    /// Unpaired shares for a given side.
    pub fn unpaired(&self, side: MarketSide) -> Decimal {
        match side {
            MarketSide::Yes => self.unpaired_yes(),
            MarketSide::No => self.unpaired_no(),
        }
    }

    /// USDC exposure on the unpaired side (unpaired_shares x avg_price of that side).
    pub fn unpaired_usdc(&self) -> Decimal {
        let up_yes = self.unpaired_yes();
        let up_no = self.unpaired_no();
        if up_yes > up_no {
            up_yes * self.yes.avg_price()
        } else {
            up_no * self.no.avg_price()
        }
    }

    /// Total capital deployed across both sides.
    pub fn total_capital_deployed(&self) -> Decimal {
        self.yes.total_cost + self.no.total_cost
    }

    /// Total maker rebates earned across both sides.
    pub fn total_rebates(&self) -> Decimal {
        self.yes.total_rebate + self.no.total_rebate
    }

    /// Total taker fees paid across both sides.
    pub fn total_taker_fees(&self) -> Decimal {
        self.yes.total_taker_fee + self.no.total_taker_fee
    }

    pub fn total_fills(&self) -> u32 {
        self.yes.fill_count + self.no.fill_count
    }

    pub fn reset(&mut self) {
        self.yes.reset();
        self.no.reset();
    }
}

impl Default for BilateralPosition {
    fn default() -> Self {
        Self::new()
    }
}

impl Default for SidePosition {
    fn default() -> Self {
        Self::new()
    }
}

// ─── Tests ──────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn dec(s: &str) -> Decimal {
        s.parse().unwrap()
    }

    #[test]
    fn test_side_position_avg_price() {
        let mut pos = SidePosition::new();
        pos.record_fill(dec("0.45"), dec("20"), 1000, false, dec("-0.01"));
        pos.record_fill(dec("0.40"), dec("30"), 2000, false, dec("-0.015"));
        // VWAP = (0.45*20 + 0.40*30) / 50 = (9 + 12) / 50 = 0.42
        assert_eq!(pos.avg_price(), dec("0.42"));
        assert_eq!(pos.total_shares, dec("50"));
        assert_eq!(pos.fill_count, 2);
    }

    #[test]
    fn test_bilateral_pairing() {
        let mut bp = BilateralPosition::new();
        bp.record_fill(MarketSide::Yes, dec("0.42"), dec("30"), 1000, false, dec("-0.01"));
        bp.record_fill(MarketSide::No, dec("0.48"), dec("20"), 2000, false, dec("-0.01"));

        assert_eq!(bp.paired_shares(), dec("20"));
        assert_eq!(bp.unpaired_yes(), dec("10"));
        assert_eq!(bp.unpaired_no(), dec("0"));
        assert_eq!(bp.avg_pair_cost(), dec("0.90")); // 0.42 + 0.48
        assert_eq!(bp.locked_profit(), dec("2.0")); // (1.00 - 0.90) * 20
    }

    #[test]
    fn test_bilateral_no_fills() {
        let bp = BilateralPosition::new();
        assert_eq!(bp.paired_shares(), Decimal::ZERO);
        assert_eq!(bp.locked_profit(), Decimal::ZERO);
        assert_eq!(bp.total_capital_deployed(), Decimal::ZERO);
    }

    #[test]
    fn test_bilateral_single_side() {
        let mut bp = BilateralPosition::new();
        bp.record_fill(MarketSide::Yes, dec("0.50"), dec("100"), 1000, false, dec("-0.02"));
        assert_eq!(bp.paired_shares(), Decimal::ZERO);
        assert_eq!(bp.unpaired_yes(), dec("100"));
        assert_eq!(bp.unpaired_no(), Decimal::ZERO);
    }

    #[test]
    fn test_bilateral_equal_sides() {
        let mut bp = BilateralPosition::new();
        bp.record_fill(MarketSide::Yes, dec("0.43"), dec("50"), 1000, false, dec("0"));
        bp.record_fill(MarketSide::No, dec("0.47"), dec("50"), 2000, false, dec("0"));
        assert_eq!(bp.paired_shares(), dec("50"));
        assert_eq!(bp.unpaired_yes(), Decimal::ZERO);
        assert_eq!(bp.unpaired_no(), Decimal::ZERO);
        // profit = (1.00 - 0.90) * 50 = 5.00
        assert_eq!(bp.locked_profit(), dec("5.0"));
    }

    #[test]
    fn test_capital_deployed() {
        let mut bp = BilateralPosition::new();
        bp.record_fill(MarketSide::Yes, dec("0.45"), dec("20"), 1000, false, dec("0"));
        bp.record_fill(MarketSide::No, dec("0.50"), dec("10"), 2000, true, dec("0.05"));
        // 0.45*20 + 0.50*10 = 9.0 + 5.0 = 14.0
        assert_eq!(bp.total_capital_deployed(), dec("14.0"));
        assert_eq!(bp.total_taker_fees(), dec("0.05"));
    }

    #[test]
    fn test_unpaired_usdc() {
        let mut bp = BilateralPosition::new();
        bp.record_fill(MarketSide::Yes, dec("0.42"), dec("50"), 1000, false, dec("0"));
        bp.record_fill(MarketSide::No, dec("0.48"), dec("20"), 2000, false, dec("0"));
        // Unpaired: 30 YES at avg 0.42 = $12.60
        assert_eq!(bp.unpaired_usdc(), dec("12.60"));
    }

    #[test]
    fn test_reset() {
        let mut bp = BilateralPosition::new();
        bp.record_fill(MarketSide::Yes, dec("0.50"), dec("100"), 1000, false, dec("0"));
        bp.reset();
        assert_eq!(bp.yes.total_shares, Decimal::ZERO);
        assert_eq!(bp.total_fills(), 0);
    }

    #[test]
    fn test_multiple_fills_same_side() {
        let mut bp = BilateralPosition::new();
        bp.record_fill(MarketSide::Yes, dec("0.40"), dec("10"), 1000, false, dec("-0.01"));
        bp.record_fill(MarketSide::Yes, dec("0.44"), dec("10"), 2000, false, dec("-0.01"));
        bp.record_fill(MarketSide::Yes, dec("0.48"), dec("10"), 3000, false, dec("-0.01"));
        bp.record_fill(MarketSide::No, dec("0.50"), dec("25"), 4000, false, dec("-0.01"));
        // YES avg = (4.0 + 4.4 + 4.8) / 30 = 13.2 / 30 = 0.44
        assert_eq!(bp.yes.avg_price(), dec("0.44"));
        assert_eq!(bp.paired_shares(), dec("25"));
        assert_eq!(bp.unpaired_yes(), dec("5"));
    }
}
