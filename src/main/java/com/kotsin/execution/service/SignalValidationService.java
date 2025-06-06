package com.kotsin.execution.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.Map;

/**
 * Signal Validation Service - implements kotsinBackTestBE validation logic
 * This is where the actual validation logic from kotsinBackTestBE should be implemented
 * 
 * Validation Rules (DYNAMIC - checked with each websocket price update):
 * 1. Filter by Minimum Move - Targets must be >= 2% away from current market price
 * 2. Filter Stops by Distance - Stop loss can't be more than 2% away from current market price
 * 3. Risk-Reward Validation - Must meet >= 1.5:1 ratio with current market price
 * 
 * NEW ARCHITECTURE: Continuous validation with live market data
 */
@Service
@Slf4j
@RequiredArgsConstructor
public class SignalValidationService {
    
    // Constants from kotsinBackTestBE
    private static final double MIN_RR_REQUIRED = 1.5;   // ≥ 1.5 : 1 risk-to-reward
    private static final double MIN_MOVE = 0.02;         // +/-2% filter for first target
    private static final double MAX_STOP_DISTANCE = 0.02; // Not more than 2% away
    
    /**
     * DYNAMIC VALIDATION: Validate signal with live market price from websocket
     * This is called continuously with each price update until all conditions are met
     */
    public SignalValidationResult validateSignalWithLivePrice(Map<String, Object> signalData, 
                                                             double currentMarketPrice) {
        try {
            log.debug("🔍 [DynamicValidation] Validating signal with live price: {}", currentMarketPrice);
            
            // Extract signal data (targets calculated by Strategy Module)
            Double stopLoss = extractDoubleValue(signalData, "stopLoss"); 
            Double target1 = extractDoubleValue(signalData, "target1");
            Double target2 = extractDoubleValue(signalData, "target2");
            Double target3 = extractDoubleValue(signalData, "target3");
            String signalType = extractStringValue(signalData, "signal");
            
            if (stopLoss == null || target1 == null) {
                return SignalValidationResult.rejected("Missing required data: stopLoss or target1");
            }
            
            boolean isBullish = "BUY".equalsIgnoreCase(signalType);
            
            log.debug("🎯 [DynamicValidation] {} signal validation: Price={}, SL={}, T1={}, T2={}, T3={}", 
                     isBullish ? "BULLISH" : "BEARISH", currentMarketPrice, stopLoss, target1, target2, target3);
            
            // RULE 1: Validate stop loss distance with current market price
            if (!validateStopDistanceWithLivePrice(currentMarketPrice, stopLoss, isBullish)) {
                double stopDistance = Math.abs((stopLoss - currentMarketPrice) / currentMarketPrice) * 100;
                return SignalValidationResult.rejected(
                    String.format("Stop loss too far from current price: %.2f%% (max: %.1f%%)", 
                                stopDistance, MAX_STOP_DISTANCE * 100));
            }
            
            // RULE 2: Validate minimum move for target1 with current market price
            if (!validateMinimumMoveWithLivePrice(currentMarketPrice, target1, isBullish)) {
                double moveDistance = Math.abs((target1 - currentMarketPrice) / currentMarketPrice) * 100;
                return SignalValidationResult.rejected(
                    String.format("Target1 too close to current price: %.2f%% (min: %.1f%%)", 
                                moveDistance, MIN_MOVE * 100));
            }
            
            // RULE 3: Validate risk-reward ratio with current market price
            double riskReward = calculateRiskRewardWithLivePrice(currentMarketPrice, stopLoss, target1, isBullish);
            if (riskReward < MIN_RR_REQUIRED) {
                return SignalValidationResult.rejected(
                    String.format("Risk-reward too low with current price: %.2f (min: %.1f)", 
                                riskReward, MIN_RR_REQUIRED));
            }
            
            // ALL VALIDATIONS PASSED WITH LIVE PRICE!
            log.info("✅ [DynamicValidation] Signal PASSED all validations with live price {}: " +
                    "Stop: {:.2f}%, Target: {:.2f}%, R:R: {:.2f}", 
                    currentMarketPrice,
                    Math.abs((stopLoss - currentMarketPrice) / currentMarketPrice) * 100,
                    Math.abs((target1 - currentMarketPrice) / currentMarketPrice) * 100,
                    riskReward);
            
            return SignalValidationResult.approved(riskReward);
            
        } catch (Exception e) {
            log.error("🚨 [DynamicValidation] Error during live validation: {}", e.getMessage(), e);
            return SignalValidationResult.rejected("Dynamic validation error: " + e.getMessage());
        }
    }
    
    /**
     * Validate stop loss distance with live market price - not more than 2% away
     */
    private boolean validateStopDistanceWithLivePrice(double currentPrice, double stopLoss, boolean isBullish) {
        double stopDistance = Math.abs(currentPrice - stopLoss);
        double maxAllowedDistance = currentPrice * MAX_STOP_DISTANCE;
        
        boolean isValid = stopDistance <= maxAllowedDistance;
        
        log.debug("📏 [DynamicValidation] Stop distance (live): {} <= {} = {}", 
                 String.format("%.4f", stopDistance),
                 String.format("%.4f", maxAllowedDistance), 
                 isValid ? "PASS" : "FAIL");
        
        return isValid;
    }
    
    /**
     * Validate minimum move with live market price - target must be >= 2% away
     */
    private boolean validateMinimumMoveWithLivePrice(double currentPrice, double target1, boolean isBullish) {
        double requiredMinMove = currentPrice * MIN_MOVE;
        double actualMove = Math.abs(target1 - currentPrice);
        
        boolean isValid = actualMove >= requiredMinMove;
        
        log.debug("📐 [DynamicValidation] Min move (live): {} >= {} = {}", 
                 String.format("%.4f", actualMove),
                 String.format("%.4f", requiredMinMove), 
                 isValid ? "PASS" : "FAIL");
        
        return isValid;
    }
    
    /**
     * Calculate risk-reward ratio with live market price
     */
    private double calculateRiskRewardWithLivePrice(double currentPrice, double stopLoss, double target1, boolean isBullish) {
        double riskAmount = Math.abs(currentPrice - stopLoss);
        double rewardAmount = Math.abs(target1 - currentPrice);
        
        if (riskAmount == 0) {
            return 0.0; // Invalid
        }
        
        double riskReward = rewardAmount / riskAmount;
        
        log.debug("💰 [DynamicValidation] Risk-reward (live): {} / {} = {:.2f}", 
                 String.format("%.4f", rewardAmount),
                 String.format("%.4f", riskAmount), 
                 riskReward);
        
        return riskReward;
    }

    /**
     * Static validation (original method) - used for initial signal screening
     * This is kept for backward compatibility and initial signal analysis
     */
    public SignalValidationResult validateSignal(Map<String, Object> signalData) {
        try {
            log.info("🔍 [StaticValidation] Initial signal validation");
            
            // Extract signal data
            Double entryPrice = extractDoubleValue(signalData, "entryPrice");
            Double stopLoss = extractDoubleValue(signalData, "stopLoss"); 
            Double target1 = extractDoubleValue(signalData, "target1");
            Double target2 = extractDoubleValue(signalData, "target2");
            Double target3 = extractDoubleValue(signalData, "target3");
            String signalType = extractStringValue(signalData, "signal");
            
            if (entryPrice == null || stopLoss == null || target1 == null) {
                return SignalValidationResult.rejected("Missing required price data: entry, stopLoss, or target1");
            }
            
            // For static validation, use the signal's entry price as reference
            return validateSignalWithLivePrice(signalData, entryPrice);
            
        } catch (Exception e) {
            log.error("🚨 [StaticValidation] Error during validation: {}", e.getMessage(), e);
            return SignalValidationResult.rejected("Static validation error: " + e.getMessage());
        }
    }
    
    /**
     * Validate all targets for proper progression and minimum moves
     */
    public boolean validateTargetProgression(double entryPrice, Double target1, Double target2, 
                                           Double target3, boolean isBullish) {
        try {
            // Validate target1 minimum move using the live price validation method
            if (target1 != null && !validateMinimumMoveWithLivePrice(entryPrice, target1, isBullish)) {
                return false;
            }
            
            // Validate target progression (each target should be further than the previous)
            if (target2 != null && target1 != null) {
                if (isBullish && target2 <= target1) {
                    log.warn("❌ [SignalValidation] Target2 not greater than Target1 for bullish signal");
                    return false;
                }
                if (!isBullish && target2 >= target1) {
                    log.warn("❌ [SignalValidation] Target2 not less than Target1 for bearish signal");
                    return false;
                }
            }
            
            if (target3 != null && target2 != null) {
                if (isBullish && target3 <= target2) {
                    log.warn("❌ [SignalValidation] Target3 not greater than Target2 for bullish signal");
                    return false;
                }
                if (!isBullish && target3 >= target2) {
                    log.warn("❌ [SignalValidation] Target3 not less than Target2 for bearish signal");
                    return false;
                }
            }
            
            log.debug("✅ [SignalValidation] Target progression validation passed");
            return true;
            
        } catch (Exception e) {
            log.error("🚨 [SignalValidation] Error validating target progression: {}", e.getMessage());
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
                log.warn("⚠️ [SignalValidation] Could not parse double value for key {}: {}", key, value);
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
        return value instanceof String ? (String) value : null;
    }
    
    /**
     * Signal validation result class
     */
    public static class SignalValidationResult {
        private final boolean approved;
        private final String reason;
        private final Double riskReward;
        
        private SignalValidationResult(boolean approved, String reason, Double riskReward) {
            this.approved = approved;
            this.reason = reason;
            this.riskReward = riskReward;
        }
        
        public static SignalValidationResult approved(double riskReward) {
            return new SignalValidationResult(true, "Signal approved", riskReward);
        }
        
        public static SignalValidationResult rejected(String reason) {
            return new SignalValidationResult(false, reason, null);
        }
        
        public boolean isApproved() { return approved; }
        public String getReason() { return reason; }
        public Double getRiskReward() { return riskReward; }
        
        @Override
        public String toString() {
            return String.format("ValidationResult{approved=%s, reason='%s', riskReward=%s}", 
                                approved, reason, riskReward);
        }
    }
} 