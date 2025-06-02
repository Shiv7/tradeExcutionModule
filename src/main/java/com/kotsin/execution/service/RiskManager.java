package com.kotsin.execution.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.Map;

/**
 * Risk management service for calculating position sizes, stop losses, and targets
 */
@Service
@Slf4j
@RequiredArgsConstructor
public class RiskManager {
    
    @Value("${app.trading.risk.max-risk-per-trade:1.0}")
    private double maxRiskPerTrade;
    
    @Value("${app.trading.risk.default-position-size:1000}")
    private int defaultPositionSize;
    
    @Value("${app.trading.risk.max-position-size:10000}")
    private int maxPositionSize;
    
    @Value("${app.trading.targets.target1-multiplier:1.5}")
    private double target1Multiplier;
    
    @Value("${app.trading.targets.target2-multiplier:2.5}")
    private double target2Multiplier;
    
    @Value("${app.trading.targets.target3-multiplier:4.0}")
    private double target3Multiplier;
    
    @Value("${app.trading.targets.target4-multiplier:6.0}")
    private double target4Multiplier;
    
    /**
     * Calculate comprehensive trade levels including stop loss and targets
     */
    public Map<String, Double> calculateTradeLevels(Map<String, Object> signalData, String signalType) {
        Map<String, Double> tradeLevels = new HashMap<>();
        
        try {
            // Extract price information from signal
            Double closePrice = extractDoubleValue(signalData, "closePrice");
            Double supertrendValue = extractDoubleValue(signalData, "supertrend");
            
            if (closePrice == null || supertrendValue == null) {
                log.warn("Missing price data for trade level calculation: close={}, supertrend={}", closePrice, supertrendValue);
                return tradeLevels;
            }
            
            boolean isBullish = "BULLISH".equalsIgnoreCase(signalType);
            
            // Calculate stop loss based on SuperTrend
            double stopLoss = calculateStopLoss(closePrice, supertrendValue, isBullish);
            
            // Calculate risk per share
            double riskPerShare = Math.abs(closePrice - stopLoss);
            
            // Calculate position size based on risk
            int positionSize = calculatePositionSize(closePrice, riskPerShare);
            
            // Calculate risk amount
            double riskAmount = riskPerShare * positionSize;
            
            // Calculate targets
            double target1 = calculateTarget(closePrice, riskPerShare, target1Multiplier, isBullish);
            double target2 = calculateTarget(closePrice, riskPerShare, target2Multiplier, isBullish);
            double target3 = calculateTarget(closePrice, riskPerShare, target3Multiplier, isBullish);
            double target4 = calculateTarget(closePrice, riskPerShare, target4Multiplier, isBullish);
            
            // Store calculated values
            tradeLevels.put("stopLoss", stopLoss);
            tradeLevels.put("target1", target1);
            tradeLevels.put("target2", target2);
            tradeLevels.put("target3", target3);
            tradeLevels.put("target4", target4);
            tradeLevels.put("riskPerShare", riskPerShare);
            tradeLevels.put("riskAmount", riskAmount);
            tradeLevels.put("positionSize", (double) positionSize);
            tradeLevels.put("entryPrice", closePrice);
            
            log.info("📊 Calculated trade levels for {} trade at {}: Stop={}, Targets=[{}, {}, {}, {}], Position={}, Risk={}",
                    signalType, closePrice, stopLoss, target1, target2, target3, target4, positionSize, riskAmount);
            
        } catch (Exception e) {
            log.error("Error calculating trade levels: {}", e.getMessage(), e);
        }
        
        return tradeLevels;
    }
    
    /**
     * Calculate stop loss based on SuperTrend
     */
    private double calculateStopLoss(double entryPrice, double supertrendValue, boolean isBullish) {
        if (isBullish) {
            // For bullish trades, stop loss should be below entry, typically at SuperTrend
            return Math.min(supertrendValue, entryPrice * 0.98); // Maximum 2% stop
        } else {
            // For bearish trades, stop loss should be above entry
            return Math.max(supertrendValue, entryPrice * 1.02); // Maximum 2% stop
        }
    }
    
    /**
     * Calculate target based on risk-reward ratio
     */
    private double calculateTarget(double entryPrice, double riskPerShare, double multiplier, boolean isBullish) {
        double rewardPerShare = riskPerShare * multiplier;
        
        if (isBullish) {
            return entryPrice + rewardPerShare;
        } else {
            return entryPrice - rewardPerShare;
        }
    }
    
    /**
     * Calculate position size based on risk management rules
     */
    private int calculatePositionSize(double entryPrice, double riskPerShare) {
        // Calculate based on maximum risk per trade
        double investment = 100000; // Assume 1 lakh investment (can be configurable)
        double maxRiskAmount = investment * (maxRiskPerTrade / 100.0);
        
        // Calculate position size based on risk
        int calculatedSize = (int) (maxRiskAmount / riskPerShare);
        
        // Apply constraints
        calculatedSize = Math.max(calculatedSize, 1); // Minimum 1 unit
        calculatedSize = Math.min(calculatedSize, maxPositionSize); // Maximum limit
        
        // If calculated size is very small, use default
        if (calculatedSize < defaultPositionSize / 10) {
            calculatedSize = defaultPositionSize;
        }
        
        return calculatedSize;
    }
    
    /**
     * Validate trade levels for reasonableness
     */
    public boolean validateTradeLevels(Map<String, Double> tradeLevels, String signalType) {
        try {
            Double entryPrice = tradeLevels.get("entryPrice");
            Double stopLoss = tradeLevels.get("stopLoss");
            Double target1 = tradeLevels.get("target1");
            Double riskAmount = tradeLevels.get("riskAmount");
            
            if (entryPrice == null || stopLoss == null || target1 == null || riskAmount == null) {
                log.warn("Missing required trade level data");
                return false;
            }
            
            boolean isBullish = "BULLISH".equalsIgnoreCase(signalType);
            
            // Validate stop loss is in correct direction
            if (isBullish && stopLoss >= entryPrice) {
                log.warn("Invalid stop loss for bullish trade: stop={}, entry={}", stopLoss, entryPrice);
                return false;
            }
            
            if (!isBullish && stopLoss <= entryPrice) {
                log.warn("Invalid stop loss for bearish trade: stop={}, entry={}", stopLoss, entryPrice);
                return false;
            }
            
            // Validate target is in correct direction
            if (isBullish && target1 <= entryPrice) {
                log.warn("Invalid target for bullish trade: target={}, entry={}", target1, entryPrice);
                return false;
            }
            
            if (!isBullish && target1 >= entryPrice) {
                log.warn("Invalid target for bearish trade: target={}, entry={}", target1, entryPrice);
                return false;
            }
            
            // Validate risk amount is reasonable
            if (riskAmount > 10000) { // Maximum 10k risk
                log.warn("Risk amount too high: {}", riskAmount);
                return false;
            }
            
            return true;
            
        } catch (Exception e) {
            log.error("Error validating trade levels: {}", e.getMessage());
            return false;
        }
    }
    
    /**
     * Extract double value from signal data
     */
    private Double extractDoubleValue(Map<String, Object> data, String key) {
        Object value = data.get(key);
        if (value instanceof Number) {
            return ((Number) value).doubleValue();
        }
        if (value instanceof String) {
            try {
                return Double.parseDouble((String) value);
            } catch (NumberFormatException e) {
                return null;
            }
        }
        return null;
    }
} 