package com.kotsin.execution.quant.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

/**
 * QuantPositionSizer - Calculates position sizes for quant signals.
 *
 * Uses risk-based position sizing:
 * - Fixed percentage of account at risk per trade
 * - Adjusts based on signal confidence
 * - Applies position size multiplier from signal
 * - Respects maximum position value
 */
@Service
@Slf4j
public class QuantPositionSizer {

    @Value("${quant.sizing.risk-per-trade:0.01}")
    private double riskPerTrade;  // 1% risk per trade

    @Value("${quant.sizing.max-position-value:500000}")
    private double maxPositionValue;

    @Value("${quant.sizing.min-quantity:1}")
    private int minQuantity;

    @Value("${quant.sizing.default-lot-size:25}")
    private int defaultLotSize;

    /**
     * Calculate position quantity based on risk parameters
     *
     * @param entryPrice Entry price
     * @param stopLoss Stop loss price
     * @param confidence Signal confidence (0-1)
     * @param multiplier Position size multiplier from signal
     * @return Position quantity
     */
    public int calculateQuantity(double entryPrice, double stopLoss,
                                  double confidence, double multiplier) {
        if (entryPrice <= 0 || stopLoss <= 0) {
            log.warn("quant_sizer_invalid_prices entry={} sl={}", entryPrice, stopLoss);
            return minQuantity;
        }

        // Get available capital
        double availableCapital = getAvailableCapital();
        if (availableCapital <= 0) {
            log.warn("quant_sizer_no_capital available={}", availableCapital);
            return minQuantity;
        }

        // Calculate risk per share
        double riskPerShare = Math.abs(entryPrice - stopLoss);
        if (riskPerShare <= 0) {
            log.warn("quant_sizer_no_risk entry={} sl={}", entryPrice, stopLoss);
            return minQuantity;
        }

        // Calculate maximum risk amount
        double maxRiskAmount = availableCapital * riskPerTrade;

        // Adjust for confidence (0.5 - 1.0 scaling)
        double confidenceAdjustment = 0.5 + (Math.min(1.0, Math.max(0.0, confidence)) * 0.5);
        maxRiskAmount *= confidenceAdjustment;

        // Apply multiplier
        maxRiskAmount *= Math.min(2.0, Math.max(0.5, multiplier));

        // Calculate quantity based on risk
        int quantity = (int) Math.floor(maxRiskAmount / riskPerShare);

        // Apply maximum position value constraint
        double positionValue = quantity * entryPrice;
        if (positionValue > maxPositionValue) {
            quantity = (int) Math.floor(maxPositionValue / entryPrice);
            log.debug("quant_sizer_max_value_applied qty={} value={}",
                    quantity, quantity * entryPrice);
        }

        // Round to lot size if applicable
        if (quantity >= defaultLotSize) {
            int lots = quantity / defaultLotSize;
            quantity = lots * defaultLotSize;
        }

        // Ensure minimum
        quantity = Math.max(minQuantity, quantity);

        log.debug("quant_sizer_calculated entry={:.2f} sl={:.2f} risk={:.2f} conf={:.2f} mult={:.2f} qty={}",
                entryPrice, stopLoss, riskPerShare, confidence, multiplier, quantity);

        return quantity;
    }

    /**
     * Calculate quantity for options with lot size consideration
     *
     * @param entryPrice Entry price
     * @param stopLoss Stop loss price
     * @param lotSize Lot size for the option
     * @param confidence Signal confidence
     * @param multiplier Position size multiplier
     * @return Number of lots (multiply by lotSize for quantity)
     */
    public int calculateLots(double entryPrice, double stopLoss, int lotSize,
                              double confidence, double multiplier) {
        int quantity = calculateQuantity(entryPrice, stopLoss, confidence, multiplier);
        int lots = Math.max(1, quantity / lotSize);
        return lots;
    }

    /**
     * Get available capital for trading
     * Uses configured default capital since WalletService doesn't track margins
     */
    private double getAvailableCapital() {
        // Use configured max position value as proxy for available capital
        // In production, this would query a margin service
        return maxPositionValue * 2;  // Allow 2 concurrent positions worth of capital
    }

    /**
     * Calculate risk amount for a given position
     */
    public double calculateRiskAmount(double entryPrice, double stopLoss, int quantity) {
        double riskPerShare = Math.abs(entryPrice - stopLoss);
        return riskPerShare * quantity;
    }

    /**
     * Calculate R-multiple for a trade outcome
     */
    public double calculateRMultiple(double entryPrice, double stopLoss,
                                      double exitPrice, boolean isLong) {
        double riskPerShare = Math.abs(entryPrice - stopLoss);
        if (riskPerShare <= 0) return 0;

        double pnlPerShare = isLong ? (exitPrice - entryPrice) : (entryPrice - exitPrice);
        return pnlPerShare / riskPerShare;
    }
}
