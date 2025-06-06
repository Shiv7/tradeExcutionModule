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
            // Extract price information from signal - check multiple field names
            Double closePrice = extractDoubleValue(signalData, "entryPrice"); // Strategy uses entryPrice
            if (closePrice == null) {
                closePrice = extractDoubleValue(signalData, "closePrice"); // Fallback
            }
            
            Double supertrendValue = extractDoubleValue(signalData, "supertrendValue");
            if (supertrendValue == null) {
                supertrendValue = extractDoubleValue(signalData, "supertrend"); // Fallback
            }
            
            // Additional fallbacks for Bollinger band strategies
            Double bbUpper = extractDoubleValue(signalData, "bbUpper");
            Double bbLower = extractDoubleValue(signalData, "bbLower");
            Double bbMiddle = extractDoubleValue(signalData, "bbMiddle");
            
            log.debug("🔍 Signal data analysis: entryPrice={}, supertrend={}, bbUpper={}, bbLower={}, bbMiddle={}", 
                     closePrice, supertrendValue, bbUpper, bbLower, bbMiddle);
            
            if (closePrice == null) {
                log.error("❌ Missing entry/close price in signal data. Available fields: {}", signalData.keySet());
                return tradeLevels;
            }
            
            // For BB strategies, use BB middle as backup stop if SuperTrend is missing
            if (supertrendValue == null && bbMiddle != null) {
                supertrendValue = bbMiddle;
                log.info("📊 Using BB middle as SuperTrend backup: {}", supertrendValue);
            }
            
            if (supertrendValue == null) {
                log.warn("⚠️ Missing SuperTrend data, using price-based stop calculation");
                boolean isBullish = "BULLISH".equalsIgnoreCase(signalType) || "BUY".equalsIgnoreCase(signalType);
                supertrendValue = isBullish ? closePrice * 0.97 : closePrice * 1.03; // 3% stop
            }
            
            boolean isBullish = determineBullishSignal(signalType, signalData);
            log.info("🎯 Calculating trade levels for {} signal at price {}", isBullish ? "BULLISH" : "BEARISH", closePrice);
            
            // Calculate stop loss based on SuperTrend or BB levels
            double stopLoss = calculateStopLoss(closePrice, supertrendValue, isBullish, signalData);
            
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
            tradeLevels.put("closePrice", closePrice); // Add both field names
            tradeLevels.put("supertrend", supertrendValue);
            
            log.info("✅ Calculated trade levels for {} trade at {}: Stop={}, Targets=[{}, {}, {}, {}], Position={}, Risk={}",
                    signalType, closePrice, stopLoss, target1, target2, target3, target4, positionSize, riskAmount);
            
        } catch (Exception e) {
            log.error("🚨 Error calculating trade levels: {}", e.getMessage(), e);
        }
        
        return tradeLevels;
    }
    
    /**
     * Determine if signal is bullish based on signal type and data
     */
    private boolean determineBullishSignal(String signalType, Map<String, Object> signalData) {
        // Check explicit signal field first
        String signal = extractStringValue(signalData, "signal");
        if ("BUY".equalsIgnoreCase(signal) || "BULLISH".equalsIgnoreCase(signal)) {
            return true;
        }
        if ("SELL".equalsIgnoreCase(signal) || "BEARISH".equalsIgnoreCase(signal)) {
            return false;
        }
        
        // Fallback to signal type
        return "BULLISH".equalsIgnoreCase(signalType) || "BUY".equalsIgnoreCase(signalType);
    }
    
    /**
     * Calculate stop loss with enhanced logic for different strategies
     */
    private double calculateStopLoss(double entryPrice, double supertrendValue, boolean isBullish, Map<String, Object> signalData) {
        // For BB strategies, consider BB levels
        Double bbUpper = extractDoubleValue(signalData, "bbUpper");
        Double bbLower = extractDoubleValue(signalData, "bbLower");
        Double bbMiddle = extractDoubleValue(signalData, "bbMiddle");
        
        if (isBullish) {
            // For bullish trades, stop loss should be below entry
            double stopLoss = Math.min(supertrendValue, entryPrice * 0.975); // Maximum 2.5% stop
            
            // For BB strategies, consider BB middle or lower as additional reference
            if (bbMiddle != null && bbMiddle < entryPrice) {
                stopLoss = Math.max(stopLoss, bbMiddle); // Use higher of SuperTrend or BB middle
            }
            
            return stopLoss;
        } else {
            // For bearish trades, stop loss should be above entry
            double stopLoss = Math.max(supertrendValue, entryPrice * 1.025); // Maximum 2.5% stop
            
            // For BB strategies, consider BB middle or upper as additional reference
            if (bbMiddle != null && bbMiddle > entryPrice) {
                stopLoss = Math.min(stopLoss, bbMiddle); // Use lower of SuperTrend or BB middle
            }
            
            return stopLoss;
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
    
    /**
     * Extract string value from signal data
     */
    private String extractStringValue(Map<String, Object> data, String key) {
        Object value = data.get(key);
        if (value instanceof String) {
            return (String) value;
        }
        return null;
    }
} 