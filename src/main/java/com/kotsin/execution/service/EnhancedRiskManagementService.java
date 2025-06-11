package com.kotsin.execution.service;

import com.kotsin.execution.model.PendingSignal;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.Map;
import java.util.HashMap;

/**
 * Enhanced Risk Management Service for Trade Execution Module
 * Implements strict risk controls and position sizing with 1.5:1 minimum risk-reward ratio
 */
@Service
@Slf4j
@RequiredArgsConstructor
public class EnhancedRiskManagementService {
    
    // Risk Management Configuration
    private static final double MIN_RISK_REWARD_RATIO = 1.5; // Minimum 1.5:1 reward to risk
    private static final double MAX_POSITION_SIZE = 5000.0; // Maximum units per position
    private static final double MAX_PORTFOLIO_EXPOSURE = 0.08; // Maximum 8% portfolio exposure
    private static final double MAX_DAILY_LOSS_PERCENTAGE = 0.05; // Maximum 5% daily loss
    
    private final PortfolioManagementService portfolioManagementService;
    
    /**
     * Validate signal against enhanced risk management rules
     */
    public RiskValidationResult validateSignal(PendingSignal signal, double currentPrice) {
        try {
            log.info("🔍 [RiskValidation] Validating signal {} at price {}", signal.getSignalId(), currentPrice);
            
            // 1. Risk-Reward Ratio Validation
            RiskValidationResult riskRewardResult = validateRiskRewardRatio(signal, currentPrice);
            if (!riskRewardResult.isValid()) {
                return riskRewardResult;
            }
            
            // 2. Position Size Validation
            RiskValidationResult positionSizeResult = validatePositionSize(signal, currentPrice);
            if (!positionSizeResult.isValid()) {
                return positionSizeResult;
            }
            
            // 3. Portfolio Exposure Validation
            RiskValidationResult exposureResult = validatePortfolioExposure(signal, currentPrice);
            if (!exposureResult.isValid()) {
                return exposureResult;
            }
            
            // 4. Daily Loss Limit Validation
            RiskValidationResult dailyLossResult = validateDailyLossLimits();
            if (!dailyLossResult.isValid()) {
                return dailyLossResult;
            }
            
            log.info("✅ [RiskValidation] Signal {} passed all risk validations", signal.getSignalId());
            
            return RiskValidationResult.builder()
                    .valid(true)
                    .riskRewardRatio(riskRewardResult.getRiskRewardRatio())
                    .recommendedPositionSize(positionSizeResult.getRecommendedPositionSize())
                    .maxExposure(exposureResult.getMaxExposure())
                    .message("Signal passed all risk validations")
                    .build();
                    
        } catch (Exception e) {
            log.error("🚨 [RiskValidation] Error validating signal {}: {}", signal.getSignalId(), e.getMessage(), e);
            return RiskValidationResult.builder()
                    .valid(false)
                    .message("Risk validation failed due to error: " + e.getMessage())
                    .build();
        }
    }
    
    /**
     * Validate minimum risk-reward ratio (1.5:1)
     */
    private RiskValidationResult validateRiskRewardRatio(PendingSignal signal, double currentPrice) {
        try {
            Double stopLoss = signal.getStopLoss();
            Double target1 = signal.getTarget1();
            
            if (stopLoss == null || target1 == null) {
                return RiskValidationResult.builder()
                        .valid(false)
                        .message("Missing stop loss or target prices")
                        .build();
            }
            
            double riskPerShare = Math.abs(currentPrice - stopLoss);
            double rewardPerShare = Math.abs(target1 - currentPrice);
            
            if (riskPerShare <= 0) {
                return RiskValidationResult.builder()
                        .valid(false)
                        .message("Invalid risk calculation - stop loss too close to entry")
                        .build();
            }
            
            double riskRewardRatio = rewardPerShare / riskPerShare;
            
            if (riskRewardRatio < MIN_RISK_REWARD_RATIO) {
                return RiskValidationResult.builder()
                        .valid(false)
                        .riskRewardRatio(riskRewardRatio)
                        .message(String.format("Risk-reward ratio %.2f is below minimum %.2f", 
                               riskRewardRatio, MIN_RISK_REWARD_RATIO))
                        .build();
            }
            
            log.info("✅ [RiskReward] Signal {} has good risk-reward ratio: {:.2f}", 
                    signal.getSignalId(), riskRewardRatio);
            
            return RiskValidationResult.builder()
                    .valid(true)
                    .riskRewardRatio(riskRewardRatio)
                    .message("Risk-reward ratio validation passed")
                    .build();
                    
        } catch (Exception e) {
            log.error("🚨 [RiskReward] Error validating risk-reward ratio: {}", e.getMessage(), e);
            return RiskValidationResult.builder()
                    .valid(false)
                    .message("Risk-reward validation failed: " + e.getMessage())
                    .build();
        }
    }
    
    /**
     * Validate position size limits
     */
    private RiskValidationResult validatePositionSize(PendingSignal signal, double currentPrice) {
        try {
            Double stopLoss = signal.getStopLoss();
            if (stopLoss == null) {
                return RiskValidationResult.builder()
                        .valid(false)
                        .message("Missing stop loss for position sizing")
                        .build();
            }
            
            double riskPerShare = Math.abs(currentPrice - stopLoss);
            double maxRiskAmount = portfolioManagementService.getMaxRiskPerTrade();
            
            // Calculate safe position size
            double recommendedPositionSize = maxRiskAmount / riskPerShare;
            
            // Apply maximum position size limit
            if (recommendedPositionSize > MAX_POSITION_SIZE) {
                recommendedPositionSize = MAX_POSITION_SIZE;
            }
            
            if (recommendedPositionSize < 1) {
                return RiskValidationResult.builder()
                        .valid(false)
                        .message("Position size too small - insufficient capital or high risk")
                        .build();
            }
            
            log.info("✅ [PositionSize] Signal {} recommended position size: {:.0f units", 
                    signal.getSignalId(), recommendedPositionSize);
            
            return RiskValidationResult.builder()
                    .valid(true)
                    .recommendedPositionSize(recommendedPositionSize)
                    .message("Position size validation passed")
                    .build();
                    
        } catch (Exception e) {
            log.error("🚨 [PositionSize] Error validating position size: {}", e.getMessage(), e);
            return RiskValidationResult.builder()
                    .valid(false)
                    .message("Position size validation failed: " + e.getMessage())
                    .build();
        }
    }
    
    /**
     * Validate portfolio exposure limits
     */
    private RiskValidationResult validatePortfolioExposure(PendingSignal signal, double currentPrice) {
        try {
            double currentPortfolioValue = portfolioManagementService.getCurrentPortfolioValue();
            double maxExposureAmount = currentPortfolioValue * MAX_PORTFOLIO_EXPOSURE;
            
            double tradeValue = currentPrice * MAX_POSITION_SIZE; // Use max position size for worst case
            
            if (tradeValue > maxExposureAmount) {
                return RiskValidationResult.builder()
                        .valid(false)
                        .maxExposure(MAX_PORTFOLIO_EXPOSURE)
                        .message(String.format("Trade exposure %.2f exceeds maximum %.2f%% of portfolio", 
                               (tradeValue / currentPortfolioValue) * 100, MAX_PORTFOLIO_EXPOSURE * 100))
                        .build();
            }
            
            log.info("✅ [Exposure] Signal {} within portfolio exposure limits: {:.1f}% of portfolio", 
                    signal.getSignalId(), (tradeValue / currentPortfolioValue) * 100);
            
            return RiskValidationResult.builder()
                    .valid(true)
                    .maxExposure(MAX_PORTFOLIO_EXPOSURE)
                    .message("Portfolio exposure validation passed")
                    .build();
                    
        } catch (Exception e) {
            log.error("🚨 [Exposure] Error validating portfolio exposure: {}", e.getMessage(), e);
            return RiskValidationResult.builder()
                    .valid(false)
                    .message("Portfolio exposure validation failed: " + e.getMessage())
                    .build();
        }
    }
    
    /**
     * Validate daily loss limits
     */
    private RiskValidationResult validateDailyLossLimits() {
        try {
            double todayPnL = portfolioManagementService.getTodayPnL();
            double portfolioValue = portfolioManagementService.getCurrentPortfolioValue();
            
            double dailyLossPercentage = Math.abs(todayPnL) / portfolioValue;
            
            if (todayPnL < 0 && dailyLossPercentage >= MAX_DAILY_LOSS_PERCENTAGE) {
                return RiskValidationResult.builder()
                        .valid(false)
                        .message(String.format("Daily loss limit reached: {:.2f}%% (Max: {:.2f}%%)", 
                               dailyLossPercentage * 100, MAX_DAILY_LOSS_PERCENTAGE * 100))
                        .build();
            }
            
            return RiskValidationResult.builder()
                    .valid(true)
                    .message("Daily loss limits validation passed")
                    .build();
                    
        } catch (Exception e) {
            log.error("🚨 [DailyLoss] Error validating daily loss limits: {}", e.getMessage(), e);
            return RiskValidationResult.builder()
                    .valid(false)
                    .message("Daily loss validation failed: " + e.getMessage())
                    .build();
        }
    }
    
    /**
     * Calculate safe position size based on risk management rules
     */
    public double calculateSafePositionSize(double entryPrice, double stopLoss, double maxRiskAmount) {
        double riskPerShare = Math.abs(entryPrice - stopLoss);
        if (riskPerShare <= 0) {
            return 0;
        }
        
        double positionSize = maxRiskAmount / riskPerShare;
        return Math.min(positionSize, MAX_POSITION_SIZE);
    }
    
    /**
     * Get risk management configuration
     */
    public Map<String, Object> getRiskConfiguration() {
        Map<String, Object> config = new HashMap<>();
        config.put("minRiskRewardRatio", MIN_RISK_REWARD_RATIO);
        config.put("maxPositionSize", MAX_POSITION_SIZE);
        config.put("maxPortfolioExposure", MAX_PORTFOLIO_EXPOSURE);
        config.put("maxDailyLossPercentage", MAX_DAILY_LOSS_PERCENTAGE);
        return config;
    }
    
    /**
     * Get risk management statistics
     */
    public Map<String, Object> getRiskManagementStats() {
        try {
            Map<String, Object> stats = new HashMap<>();
            
            // Current configuration
            stats.put("minRiskRewardRatio", MIN_RISK_REWARD_RATIO);
            stats.put("maxPositionSize", MAX_POSITION_SIZE);
            stats.put("maxPortfolioExposure", MAX_PORTFOLIO_EXPOSURE);
            stats.put("maxDailyLossPercentage", MAX_DAILY_LOSS_PERCENTAGE);
            
            // Current portfolio metrics
            double portfolioValue = portfolioManagementService.getCurrentPortfolioValue();
            double maxRiskPerTrade = portfolioManagementService.getMaxRiskPerTrade();
            double todayPnL = portfolioManagementService.getTodayPnL();
            
            stats.put("portfolioValue", portfolioValue);
            stats.put("maxRiskPerTrade", maxRiskPerTrade);
            stats.put("todayPnL", todayPnL);
            stats.put("dailyLossUsed", Math.abs(todayPnL) / portfolioValue * 100);
            
            // Risk utilization
            stats.put("riskUtilizationPercentage", (maxRiskPerTrade / portfolioValue) * 100);
            
            stats.put("timestamp", LocalDateTime.now());
            
            return stats;
            
        } catch (Exception e) {
            log.error("🚨 [RiskStats] Error getting risk management stats: {}", e.getMessage(), e);
            Map<String, Object> errorStats = new HashMap<>();
            errorStats.put("error", "Unable to calculate risk stats");
            errorStats.put("timestamp", LocalDateTime.now());
            return errorStats;
        }
    }
    
    /**
     * Validate signal with strict risk controls (alternative method signature)
     */
    public RiskValidationResult validateSignalWithStrictRiskControls(Map<String, Object> signalData, double currentPrice) {
        try {
            // Create a temporary PendingSignal from signal data for validation
            PendingSignal tempSignal = PendingSignal.builder()
                    .signalId("TEMP_" + System.currentTimeMillis())
                    .scripCode((String) signalData.get("scripCode"))
                    .signalType((String) signalData.get("signalType"))
                    .stopLoss(extractDouble(signalData, "stopLoss"))
                    .target1(extractDouble(signalData, "target1"))
                    .target2(extractDouble(signalData, "target2"))
                    .target3(extractDouble(signalData, "target3"))
                    .build();
            
            return validateSignal(tempSignal, currentPrice);
            
        } catch (Exception e) {
            log.error("🚨 [RiskValidation] Error validating signal with strict controls: {}", e.getMessage(), e);
            return RiskValidationResult.builder()
                    .valid(false)
                    .message("Risk validation failed: " + e.getMessage())
                    .build();
        }
    }
    
    /**
     * Calculate simple targets based on risk-reward ratio
     */
    public Map<String, Double> calculateSimpleTargets(double entryPrice, double stopLoss, boolean isBullish) {
        try {
            Map<String, Double> targets = new HashMap<>();
            
            double riskAmount = Math.abs(entryPrice - stopLoss);
            
            if (isBullish) {
                targets.put("target1", entryPrice + (riskAmount * MIN_RISK_REWARD_RATIO));
                targets.put("target2", entryPrice + (riskAmount * MIN_RISK_REWARD_RATIO * 2));
                targets.put("target3", entryPrice + (riskAmount * MIN_RISK_REWARD_RATIO * 3));
            } else {
                targets.put("target1", entryPrice - (riskAmount * MIN_RISK_REWARD_RATIO));
                targets.put("target2", entryPrice - (riskAmount * MIN_RISK_REWARD_RATIO * 2));
                targets.put("target3", entryPrice - (riskAmount * MIN_RISK_REWARD_RATIO * 3));
            }
            
            targets.put("riskAmount", riskAmount);
            targets.put("riskRewardRatio", MIN_RISK_REWARD_RATIO);
            
            return targets;
            
        } catch (Exception e) {
            log.error("🚨 [SimpleTargets] Error calculating simple targets: {}", e.getMessage(), e);
            return new HashMap<>();
        }
    }
    
    /**
     * Reset to safe risk parameters
     */
    public Map<String, Object> resetToSafeRiskParameters() {
        try {
            Map<String, Object> result = new HashMap<>();
            
            // Log the reset action
            log.warn("🔄 [RiskReset] Resetting to safe risk parameters");
            
            // Return current safe configuration
            result.put("minRiskRewardRatio", MIN_RISK_REWARD_RATIO);
            result.put("maxPositionSize", MAX_POSITION_SIZE);
            result.put("maxPortfolioExposure", MAX_PORTFOLIO_EXPOSURE);
            result.put("maxDailyLossPercentage", MAX_DAILY_LOSS_PERCENTAGE);
            result.put("message", "Risk parameters reset to safe defaults");
            result.put("timestamp", LocalDateTime.now());
            
            return result;
            
        } catch (Exception e) {
            log.error("🚨 [RiskReset] Error resetting risk parameters: {}", e.getMessage(), e);
            Map<String, Object> errorResult = new HashMap<>();
            errorResult.put("error", "Failed to reset risk parameters");
            errorResult.put("timestamp", LocalDateTime.now());
            return errorResult;
        }
    }
    
    /**
     * Extract double value from map with null safety
     */
    private Double extractDouble(Map<String, Object> data, String key) {
        Object value = data.get(key);
        if (value == null) {
            return null;
        }
        
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
     * Risk Validation Result Model
     */
    public static class RiskValidationResult {
        private boolean valid;
        private String message;
        private double riskRewardRatio;
        private double recommendedPositionSize;
        private double maxExposure;
        
        public static RiskValidationResultBuilder builder() {
            return new RiskValidationResultBuilder();
        }
        
        // Getters
        public boolean isValid() { return valid; }
        public String getMessage() { return message; }
        public double getRiskRewardRatio() { return riskRewardRatio; }
        public double getRecommendedPositionSize() { return recommendedPositionSize; }
        public double getMaxExposure() { return maxExposure; }
        
        // Setters
        public void setValid(boolean valid) { this.valid = valid; }
        public void setMessage(String message) { this.message = message; }
        public void setRiskRewardRatio(double riskRewardRatio) { this.riskRewardRatio = riskRewardRatio; }
        public void setRecommendedPositionSize(double recommendedPositionSize) { this.recommendedPositionSize = recommendedPositionSize; }
        public void setMaxExposure(double maxExposure) { this.maxExposure = maxExposure; }
        
        public static class RiskValidationResultBuilder {
            private RiskValidationResult result = new RiskValidationResult();
            
            public RiskValidationResultBuilder valid(boolean valid) {
                result.setValid(valid);
                return this;
            }
            
            public RiskValidationResultBuilder message(String message) {
                result.setMessage(message);
                return this;
            }
            
            public RiskValidationResultBuilder riskRewardRatio(double ratio) {
                result.setRiskRewardRatio(ratio);
                return this;
            }
            
            public RiskValidationResultBuilder recommendedPositionSize(double size) {
                result.setRecommendedPositionSize(size);
                return this;
            }
            
            public RiskValidationResultBuilder maxExposure(double exposure) {
                result.setMaxExposure(exposure);
                return this;
            }
            
            public RiskValidationResult build() {
                return result;
            }
        }
    }
} 