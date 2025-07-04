package com.kotsin.execution.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Strategy signal model for trade execution module.
 * Matches the StrategySignal structure from strategy module.
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class StrategySignal {
    
    private static final Logger log = LoggerFactory.getLogger(StrategySignal.class);
    
    // Basic signal information
    private String scripCode;
    private String companyName;
    private String strategy;              // Strategy name (e.g., "ENHANCED_30M")
    private String timeframe;             // Timeframe (e.g., "3m", "30m")
    private String signal;                // "BUY", "SELL", "BULLISH", "BEARISH"
    private String reason;                // Reason for the signal
    private long timestamp;               // Signal generation timestamp
    
    // Price levels
    private double entryPrice;            // Entry price
    private double stopLoss;              // Stop loss level
    private double target1;               // First target
    private double target2;               // Second target (optional)
    private double target3;               // Third target (optional)
    
    // Risk management
    private double riskReward;            // Risk to reward ratio
    private double riskAmount;            // Risk amount per share
    private double rewardAmount;          // Potential reward per share
    
    // Pivot explanations
    private String stopLossExplanation;   // Stop loss pivot explanation
    private String target1Explanation;    // Target 1 pivot explanation
    
    // Signal confidence and quality
    private String confidence;            // "HIGH", "MEDIUM", "LOW"
    private double signalStrength;        // Signal strength score (0-100)
    
    // Indicator values at signal time
    private double supertrendValue;
    private String supertrendSignal;
    private String previousSupertrendSignal;  // For direction change detection
    private double bbUpper;
    private double bbMiddle;
    private double bbLower;
    private double rsi;
    private double vwap;
    
    // Additional metadata
    private String exchange;
    private String exchangeType;
    private int volume;
    private boolean backtest;           // Whether this is a backtest signal (matches JSON field name)
    
    /**
     * Check if this is a bullish signal
     */
    public boolean isBullish() {
        return "BUY".equals(signal) || "BULLISH".equals(signal);
    }
    
    /**
     * Check if this is a bearish signal
     */
    public boolean isBearish() {
        return "SELL".equals(signal) || "BEARISH".equals(signal);
    }
    
    /**
     * Get normalized signal type for trade execution
     */
    public String getNormalizedSignal() {
        if (isBullish()) return "BULLISH";
        if (isBearish()) return "BEARISH";
        return signal;
    }
    
    @Override
    public String toString() {
        return String.format("%s %s signal for %s at %.2f (SL: %.2f, T1: %.2f, R:R: 1:%.2f)", 
                            strategy, signal, companyName, entryPrice, stopLoss, target1, riskReward);
    }
} 