package com.kotsin.execution.model;

import lombok.Data;
import lombok.Builder;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;

/**
 * Represents an active trade being monitored by the execution engine.
 * Based on the trade simulation logic from KotsinBackTestBE.
 */
@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class ActiveTrade {
    
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
    private Boolean entryTriggered;
    private Integer positionSize;
    
    // Exit Levels
    private Double stopLoss;
    private Double target1;
    private Double target2;
    private Double target3;
    private Double target4;
    
    // Trade State
    private TradeStatus status;
    private Boolean target1Hit;
    private Boolean target2Hit;
    private Double trailingStopLoss;
    private Boolean useTrailingStop;
    
    // Tracking Variables
    private Double highSinceEntry;
    private Double lowSinceEntry;
    private LocalDateTime maxHoldingTime;
    private Integer daysHeld;
    
    // Risk Management
    private Double riskAmount;
    private Double riskPerShare;
    private Map<String, Double> riskRewardRatios;
    
    // Market Data Context
    private Double currentPrice;
    private LocalDateTime lastUpdateTime;
    
    // Metadata for additional information
    @Builder.Default
    private Map<String, Object> metadata = new HashMap<>();
    
    // Exit Information (populated when trade closes)
    private Double exitPrice;
    private LocalDateTime exitTime;
    private String exitReason;
    
    public enum TradeStatus {
        WAITING_FOR_ENTRY,    // Signal received, waiting for entry conditions
        ACTIVE,               // In trade, monitoring for exits
        PARTIAL_EXIT,         // Hit first target, partial position closed
        CLOSED_PROFIT,        // Closed at target
        CLOSED_LOSS,          // Closed at stop loss
        CLOSED_TIME,          // Closed due to time limit
        CANCELLED             // Trade cancelled before entry
    }
    
    /**
     * Check if this trade is bullish
     */
    public boolean isBullish() {
        return "BULLISH".equalsIgnoreCase(signalType);
    }
    
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
     * Update current price and tracking variables
     */
    public void updatePrice(double newPrice, LocalDateTime timestamp) {
        this.currentPrice = newPrice;
        this.lastUpdateTime = timestamp;
        
        if (entryTriggered && status == TradeStatus.ACTIVE) {
            // Update high/low since entry
            if (highSinceEntry == null || newPrice > highSinceEntry) {
                highSinceEntry = newPrice;
            }
            if (lowSinceEntry == null || newPrice < lowSinceEntry) {
                lowSinceEntry = newPrice;
            }
        }
    }
    
    /**
     * Check if stop loss is hit at current price
     */
    public boolean isStopLossHit() {
        if (currentPrice == null || stopLoss == null || status != TradeStatus.ACTIVE) {
            return false;
        }
        
        if (isBullish()) {
            return currentPrice <= (trailingStopLoss != null ? trailingStopLoss : stopLoss);
        } else {
            return currentPrice >= (trailingStopLoss != null ? trailingStopLoss : stopLoss);
        }
    }
    
    /**
     * Check if target1 is hit at current price
     */
    public boolean isTarget1Hit() {
        if (currentPrice == null || target1 == null || target1Hit || status != TradeStatus.ACTIVE) {
            return false;
        }
        
        if (isBullish()) {
            return currentPrice >= target1 && target1 > entryPrice;
        } else {
            return currentPrice <= target1 && target1 < entryPrice;
        }
    }
    
    /**
     * Check if target2 is hit at current price
     */
    public boolean isTarget2Hit() {
        if (currentPrice == null || target2 == null || target2Hit || !target1Hit || status != TradeStatus.ACTIVE) {
            return false;
        }
        
        if (isBullish()) {
            return currentPrice >= target2 && target2 > entryPrice;
        } else {
            return currentPrice <= target2 && target2 < entryPrice;
        }
    }
    
    /**
     * Update trailing stop loss based on current conditions
     */
    public void updateTrailingStop() {
        if (!useTrailingStop || !entryTriggered || status != TradeStatus.ACTIVE) {
            return;
        }
        
        if (target1Hit && highSinceEntry != null && lowSinceEntry != null) {
            double initialStopDistance = Math.abs(entryPrice - stopLoss);
            
            if (isBullish()) {
                // Move stop up for bullish trades
                double newTrailingStop = highSinceEntry - (initialStopDistance * 0.5);
                if (trailingStopLoss == null || newTrailingStop > trailingStopLoss) {
                    trailingStopLoss = newTrailingStop;
                }
            } else {
                // Move stop down for bearish trades
                double newTrailingStop = lowSinceEntry + (initialStopDistance * 0.5);
                if (trailingStopLoss == null || newTrailingStop < trailingStopLoss) {
                    trailingStopLoss = newTrailingStop;
                }
            }
        }
    }
    
    /**
     * Calculate current profit/loss based on current market price
     */
    public double getCurrentPnL() {
        if (entryPrice == null || currentPrice == null || positionSize == null || !entryTriggered) {
            return 0.0;
        }
        
        // Calculate unrealized P&L
        if (isBullish()) {
            return (currentPrice - entryPrice) * positionSize;
        } else {
            return (entryPrice - currentPrice) * positionSize;
        }
    }
    
    /**
     * Calculate current ROI percentage
     */
    public double getCurrentROI() {
        if (entryPrice == null || positionSize == null || !entryTriggered) {
            return 0.0;
        }
        
        double currentPnL = getCurrentPnL();
        double investment = entryPrice * positionSize;
        
        return investment > 0 ? (currentPnL / investment) * 100.0 : 0.0;
    }
    
    /**
     * Get current trade status for display
     */
    public String getDisplayStatus() {
        if (status == null) return "UNKNOWN";
        
        switch (status) {
            case WAITING_FOR_ENTRY:
                return "Waiting for Entry";
            case ACTIVE:
                if (target1Hit && target2Hit) {
                    return "Active (T1+T2 Hit)";
                } else if (target1Hit) {
                    return "Active (T1 Hit)";
                } else {
                    return "Active";
                }
            case PARTIAL_EXIT:
                return "Partial Exit";
            case CLOSED_PROFIT:
                return "Closed at Target";
            case CLOSED_LOSS:
                return "Stopped Out";
            case CLOSED_TIME:
                return "Time Exit";
            case CANCELLED:
                return "Cancelled";
            default:
                return status.toString();
        }
    }
    
    /**
     * Get risk-reward ratio for target 1
     */
    public double getRiskRewardRatio() {
        if (entryPrice == null || stopLoss == null || target1 == null) {
            return 0.0;
        }
        
        // Additional validation for corrupted data
        if (entryPrice <= 0 || stopLoss <= 0 || target1 <= 0) {
            return 0.0;
        }
        
        double riskPerShare = Math.abs(entryPrice - stopLoss);
        double rewardPerShare = Math.abs(target1 - entryPrice);
        
        return riskPerShare > 0 ? rewardPerShare / riskPerShare : 0.0;
    }
} 