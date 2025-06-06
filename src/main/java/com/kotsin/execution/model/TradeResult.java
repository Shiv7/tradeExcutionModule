package com.kotsin.execution.model;

import lombok.Data;
import lombok.Builder;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;

/**
 * Represents the final result of a completed trade.
 * This will be published to the profit/loss Kafka topic.
 */
@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class TradeResult {
    
    // Trade Identification
    private String tradeId;
    private String scripCode;
    private String companyName;
    private String exchange;
    private String exchangeType;
    private String strategyName;
    
    // Signal Information
    private String signalType; // "BULLISH" or "BEARISH"
    private LocalDateTime signalTime;
    private String originalSignalId;
    
    // Entry Details
    private Double entryPrice;
    private LocalDateTime entryTime;
    private Integer positionSize;
    
    // Exit Details
    private Double exitPrice;
    private LocalDateTime exitTime;
    private String exitReason;
    
    // Trade Performance
    private Double profitLoss;
    private Double roi; // Return on Investment %
    private Boolean successful;
    private Double riskAdjustedReturn;
    
    // Target Analysis
    private Boolean target1Hit;
    private Boolean target2Hit;
    private LocalDateTime target1HitTime;
    private LocalDateTime target2HitTime;
    
    // Risk Management
    private Double initialStopLoss;
    private Double finalStopLoss; // If trailing stop was used
    private Double riskAmount;
    private Double maxDrawdown;
    
    // Trade Duration
    private Long durationMinutes;
    private Integer daysHeld;
    private Boolean isMultiDayTrade;
    
    // Market Context
    private Double highSinceEntry;
    private Double lowSinceEntry;
    private Double maxFavorableExcursion; // Best price reached in favor
    private Double maxAdverseExcursion;   // Worst price reached against
    
    // Performance Metrics
    private Map<String, Double> riskRewardRatios;
    private Double sharpeRatio;
    private Double calmarRatio;
    
    // Additional Metadata
    @Builder.Default
    private Map<String, Object> metadata = new HashMap<>();
    
    // System Information
    private LocalDateTime resultGeneratedTime;
    private String systemVersion;
    
    /**
     * Add metadata entry
     */
    public void addMetadata(String key, Object value) {
        if (metadata == null) {
            metadata = new HashMap<>();
        }
        metadata.put(key, value);
    }
    
    /**
     * Get metadata value
     */
    public Object getMetadata(String key) {
        return metadata != null ? metadata.get(key) : null;
    }
    
    /**
     * Calculate profit/loss based on entry/exit prices and position size
     */
    public void calculateProfitLoss() {
        if (entryPrice == null || exitPrice == null || positionSize == null) {
            return;
        }
        
        boolean isBullish = "BULLISH".equalsIgnoreCase(signalType);
        
        // Calculate raw P&L
        double pnl = isBullish ? 
            (exitPrice - entryPrice) * positionSize : 
            (entryPrice - exitPrice) * positionSize;
        
        this.profitLoss = roundToTwoDecimals(pnl);
        
        // Calculate ROI
        double investment = entryPrice * positionSize;
        this.roi = investment > 0 ? roundToTwoDecimals((pnl / investment) * 100) : 0.0;
        
        // Determine if trade was successful
        this.successful = pnl > 0;
        
        // Calculate risk-adjusted return
        if (riskAmount != null && riskAmount > 0) {
            this.riskAdjustedReturn = roundToTwoDecimals(pnl / riskAmount);
        }
        
        // Calculate maximum favorable and adverse excursions
        if (highSinceEntry != null && lowSinceEntry != null) {
            if (isBullish) {
                this.maxFavorableExcursion = roundToTwoDecimals((highSinceEntry - entryPrice) * positionSize);
                this.maxAdverseExcursion = roundToTwoDecimals((entryPrice - lowSinceEntry) * positionSize);
            } else {
                this.maxFavorableExcursion = roundToTwoDecimals((entryPrice - lowSinceEntry) * positionSize);
                this.maxAdverseExcursion = roundToTwoDecimals((highSinceEntry - entryPrice) * positionSize);
            }
        }
    }
    
    /**
     * Calculate trade duration in minutes
     */
    public void calculateDuration() {
        if (entryTime != null && exitTime != null) {
            this.durationMinutes = java.time.Duration.between(entryTime, exitTime).toMinutes();
        }
    }
    
    /**
     * Utility method to round to 2 decimal places
     */
    private double roundToTwoDecimals(double value) {
        return Math.round(value * 100.0) / 100.0;
    }
    
    /**
     * Generate a summary string for logging
     */
    public String getSummary() {
        return String.format("Trade %s: %s %s - Entry: %.2f, Exit: %.2f, P&L: %.2f, ROI: %.2f%%, Duration: %d min, Reason: %s",
                tradeId, signalType, scripCode, entryPrice, exitPrice, profitLoss, roi, durationMinutes, exitReason);
    }
    
    /**
     * Check if this was a winning trade
     */
    public boolean isWinner() {
        return successful != null && successful;
    }
    
    /**
     * Check if this was a losing trade
     */
    public boolean isLoser() {
        return successful != null && !successful;
    }
    
    /**
     * Get the trade direction
     */
    public boolean isBullish() {
        return "BULLISH".equalsIgnoreCase(signalType);
    }
    
    /**
     * Get profit/loss (alias for profitLoss field)
     */
    public Double getPnL() {
        return profitLoss;
    }
    
    /**
     * Set profit/loss (alias for profitLoss field)
     */
    public void setPnL(Double pnl) {
        this.profitLoss = pnl;
    }
} 