package com.kotsin.execution.wallet.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDate;
import java.time.LocalDateTime;

/**
 * Core Wallet Entity for managing trading capital, margin, and limits.
 * Supports both virtual and live trading modes.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class WalletEntity {

    private String walletId;
    private String userId;
    private WalletMode mode; // VIRTUAL or LIVE

    // Capital Management
    private double initialCapital;
    private double currentBalance;
    private double usedMargin;
    private double availableMargin;
    private double reservedMargin; // For pending orders

    // P&L Tracking
    private double realizedPnl;
    private double unrealizedPnl;
    private double totalPnl;
    private double dayPnl;
    private double weekPnl;
    private double monthPnl;

    // Daily Tracking (resets each day)
    private LocalDate tradingDate;
    private double dayStartBalance;
    private double dayRealizedPnl;
    private double dayUnrealizedPnl;
    private int dayTradeCount;
    private int dayWinCount;
    private int dayLossCount;

    // Risk Limits
    private double maxDailyLoss;           // Max loss allowed per day (absolute)
    private double maxDailyLossPercent;    // Max loss as % of capital
    private double maxDrawdown;            // Max total drawdown allowed
    private double maxDrawdownPercent;     // Max drawdown as % of peak
    private int maxOpenPositions;          // Max concurrent positions
    private int maxPositionsPerSymbol;     // Max positions per symbol
    private double maxPositionSize;        // Max position value
    private double maxPositionSizePercent; // Max position as % of capital

    // Circuit Breaker State
    private boolean circuitBreakerTripped;
    private String circuitBreakerReason;
    private LocalDateTime circuitBreakerTrippedAt;
    private LocalDateTime circuitBreakerResetsAt;

    // Statistics
    private int totalTradeCount;
    private int totalWinCount;
    private int totalLossCount;
    private double winRate;
    private double avgWin;
    private double avgLoss;
    private double profitFactor;
    private double peakBalance;
    private double maxDrawdownHit;

    // Timestamps
    private LocalDateTime createdAt;
    private LocalDateTime updatedAt;
    private long version;

    public enum WalletMode {
        VIRTUAL,
        LIVE
    }

    /**
     * Calculate available margin after accounting for used and reserved
     */
    public double getEffectiveAvailableMargin() {
        return currentBalance - usedMargin - reservedMargin;
    }

    /**
     * Check if wallet can take a new position
     */
    public boolean canTakePosition(double requiredMargin, int currentOpenPositions) {
        if (circuitBreakerTripped) {
            return false;
        }
        if (currentOpenPositions >= maxOpenPositions) {
            return false;
        }
        if (requiredMargin > getEffectiveAvailableMargin()) {
            return false;
        }
        if (maxPositionSize > 0 && requiredMargin > maxPositionSize) {
            return false;
        }
        if (maxPositionSizePercent > 0 && requiredMargin > (currentBalance * maxPositionSizePercent / 100)) {
            return false;
        }
        return true;
    }

    /**
     * Check if daily loss limit is breached
     */
    public boolean isDailyLossLimitBreached() {
        double currentDayLoss = -dayRealizedPnl - dayUnrealizedPnl;
        if (maxDailyLoss > 0 && currentDayLoss >= maxDailyLoss) {
            return true;
        }
        if (maxDailyLossPercent > 0 && currentDayLoss >= (dayStartBalance * maxDailyLossPercent / 100)) {
            return true;
        }
        return false;
    }

    /**
     * Check if drawdown limit is breached
     */
    public boolean isDrawdownLimitBreached() {
        double currentDrawdown = peakBalance - currentBalance;
        if (maxDrawdown > 0 && currentDrawdown >= maxDrawdown) {
            return true;
        }
        if (maxDrawdownPercent > 0 && peakBalance > 0) {
            double drawdownPercent = (currentDrawdown / peakBalance) * 100;
            if (drawdownPercent >= maxDrawdownPercent) {
                return true;
            }
        }
        return false;
    }

    /**
     * Update peak balance if new high
     */
    public void updatePeakBalance() {
        if (currentBalance > peakBalance) {
            peakBalance = currentBalance;
        }
    }

    /**
     * Reset daily counters (call at start of trading day)
     */
    public void resetDailyCounters(LocalDate newTradingDate) {
        this.tradingDate = newTradingDate;
        this.dayStartBalance = currentBalance;
        this.dayRealizedPnl = 0;
        this.dayUnrealizedPnl = 0;
        this.dayPnl = 0;
        this.dayTradeCount = 0;
        this.dayWinCount = 0;
        this.dayLossCount = 0;

        // Reset circuit breaker if it was date-based
        if (circuitBreakerTripped && circuitBreakerResetsAt != null
                && LocalDateTime.now().isAfter(circuitBreakerResetsAt)) {
            circuitBreakerTripped = false;
            circuitBreakerReason = null;
            circuitBreakerTrippedAt = null;
            circuitBreakerResetsAt = null;
        }
    }

    /**
     * Create default virtual wallet
     */
    public static WalletEntity createDefaultVirtual(String walletId, double initialCapital) {
        LocalDateTime now = LocalDateTime.now();
        return WalletEntity.builder()
                .walletId(walletId)
                .userId("default")
                .mode(WalletMode.VIRTUAL)
                .initialCapital(initialCapital)
                .currentBalance(initialCapital)
                .availableMargin(initialCapital)
                .usedMargin(0)
                .reservedMargin(0)
                .peakBalance(initialCapital)
                .tradingDate(LocalDate.now())
                .dayStartBalance(initialCapital)
                // Default risk limits
                .maxDailyLoss(initialCapital * 0.03) // 3% daily loss limit
                .maxDailyLossPercent(3.0)
                .maxDrawdown(initialCapital * 0.10) // 10% max drawdown
                .maxDrawdownPercent(10.0)
                .maxOpenPositions(10)
                .maxPositionsPerSymbol(2)
                .maxPositionSizePercent(20.0) // Max 20% per position
                .circuitBreakerTripped(false)
                .createdAt(now)
                .updatedAt(now)
                .version(0)
                .build();
    }
}
