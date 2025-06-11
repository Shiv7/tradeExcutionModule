package com.kotsin.execution.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;
import java.util.Map;

/**
 * Portfolio Summary Model
 * 
 * Comprehensive portfolio overview including:
 * - Financial metrics (value, P&L, exposure)
 * - Trade statistics (win rate, counts)
 * - Risk metrics (drawdown, exposure percentage)
 * - Position distribution analysis
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class PortfolioSummary {
    
    // Core Financial Metrics
    private Double portfolioValue;
    private Double initialCapital;
    private Double totalRealizedPnL;
    private Double totalUnrealizedPnL;
    private Double totalExposure;
    private Double exposurePercentage;
    
    // Trade Statistics
    private Integer activeTradesCount;
    private Integer totalCompletedTrades;
    private Integer winningTrades;
    private Double winRate;
    
    // Risk Metrics
    private Double maxDrawdown;
    private Double sharpeRatio;
    
    // Position Analysis
    private Map<String, Double> positionsByScript;     // Script -> Position %
    private Map<String, Double> exposureByStrategy;    // Strategy -> Exposure Amount
    
    // Metadata
    private LocalDateTime lastUpdated;
    
    /**
     * Calculate total return percentage
     */
    public Double getTotalReturnPercentage() {
        if (initialCapital != null && portfolioValue != null && initialCapital > 0) {
            return ((portfolioValue - initialCapital) / initialCapital) * 100;
        }
        return 0.0;
    }
    
    /**
     * Calculate total P&L (realized + unrealized)
     */
    public Double getTotalPnL() {
        double realized = totalRealizedPnL != null ? totalRealizedPnL : 0.0;
        double unrealized = totalUnrealizedPnL != null ? totalUnrealizedPnL : 0.0;
        return realized + unrealized;
    }
    
    /**
     * Calculate losing trades count
     */
    public Integer getLosingTrades() {
        if (totalCompletedTrades != null && winningTrades != null) {
            return totalCompletedTrades - winningTrades;
        }
        return 0;
    }
    
    /**
     * Get formatted portfolio summary
     */
    public String getFormattedSummary() {
        return String.format(
            "Portfolio Summary:\n" +
            "Value: ₹%.2f (%.2f%% return)\n" +
            "P&L: ₹%.2f (Realized: ₹%.2f, Unrealized: ₹%.2f)\n" +
            "Exposure: ₹%.2f (%.2f%% of portfolio)\n" +
            "Trades: %d active, %d completed (%.2f%% win rate)\n" +
            "Risk: %.2f%% max drawdown, %.2f Sharpe ratio",
            portfolioValue, getTotalReturnPercentage(),
            getTotalPnL(), totalRealizedPnL, totalUnrealizedPnL,
            totalExposure, exposurePercentage,
            activeTradesCount, totalCompletedTrades, winRate,
            maxDrawdown, sharpeRatio
        );
    }
} 