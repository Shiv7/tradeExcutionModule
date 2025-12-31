package com.kotsin.execution.validation;

import com.kotsin.execution.model.StrategySignal;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

/**
 * CRITICAL FIX #4: Comprehensive signal validation
 * Validates all signal parameters before trade execution
 */
@Component
@Slf4j
public class SignalValidator {

    private static final double MIN_CONFIDENCE = 0.0;
    private static final double MAX_CONFIDENCE = 1.0;
    private static final double MIN_STRUCTURAL_BIAS = -1.0;
    private static final double MAX_STRUCTURAL_BIAS = 1.0;
    private static final double MIN_POSITION_MULTIPLIER = 0.1;
    private static final double MAX_POSITION_MULTIPLIER = 3.0;
    private static final double MIN_RISK_REWARD = 0.5;
    private static final long MAX_TIMESTAMP_AGE_MS = 86400_000L; // 24 hours

    /**
     * Comprehensive validation of strategy signal
     */
    public ValidationResult validate(StrategySignal signal) {
        ValidationResult result = new ValidationResult();

        if (signal == null) {
            result.addError("Signal is null");
            return result;
        }

        // 1. CRITICAL: Validate scripCode format
        validateScripCode(signal, result);

        // 2. CRITICAL: Validate price levels and relationships
        validatePriceLevels(signal, result);

        // 3. Validate score bounds
        validateScoreBounds(signal, result);

        // 4. Validate timestamp
        validateTimestamp(signal, result);

        // 5. Validate signal consistency
        validateSignalConsistency(signal, result);

        // 6. Validate numeric fields for NaN/Infinity
        validateNumericFields(signal, result);

        return result;
    }

    private void validateScripCode(StrategySignal signal, ValidationResult result) {
        if (signal.getScripCode() == null || signal.getScripCode().isBlank()) {
            result.addError("scripCode is required");
            return;
        }

        // Expected format: "N:D:49812" (Exchange:Type:Code)
        String[] parts = signal.getScripCode().split(":");
        if (parts.length != 3) {
            result.addError("scripCode must be in format 'Exchange:Type:Code' (e.g., 'N:D:49812')");
            return;
        }

        // Validate exchange
        String exchange = parts[0];
        if (!exchange.matches("[NBM]")) {
            result.addError("Exchange must be N (NSE), B (BSE), or M (MCX)");
        }

        // Validate exchange type
        String exchType = parts[1];
        if (!exchType.matches("[CD]")) {
            result.addError("ExchangeType must be C (Cash) or D (Derivatives)");
        }

        // Validate numeric code
        String numericCode = parts[2];
        if (!numericCode.matches("\\d+")) {
            result.addError("scripCode numeric part must be a valid integer");
        }
    }

    private void validatePriceLevels(StrategySignal signal, ValidationResult result) {
        double entry = signal.getEntryPrice();
        double sl = signal.getStopLoss();
        double t1 = signal.getTarget1();
        double t2 = signal.getTarget2();

        // Basic positivity checks
        if (entry <= 0) result.addError("entryPrice must be > 0");
        if (sl <= 0) result.addError("stopLoss must be > 0");
        if (t1 <= 0) result.addError("target1 must be > 0");
        if (t2 > 0 && t2 <= t1) {
            result.addError("target2 must be > target1 if specified");
        }

        // CRITICAL: Relationship validation based on direction
        boolean isBullish = signal.isBullish();
        boolean isBearish = signal.isBearish();

        if (isBullish) {
            // LONG: stopLoss < entryPrice < target1 < target2
            if (sl >= entry) {
                result.addError("LONG signal must have stopLoss < entryPrice (SL=" + sl + ", Entry=" + entry + ")");
            }
            if (t1 <= entry) {
                result.addError("LONG signal must have target1 > entryPrice (T1=" + t1 + ", Entry=" + entry + ")");
            }
            if (t2 > 0 && t2 <= entry) {
                result.addError("LONG signal must have target2 > entryPrice");
            }
        } else if (isBearish) {
            // SHORT: target2 < target1 < entryPrice < stopLoss
            if (sl <= entry) {
                result.addError("SHORT signal must have stopLoss > entryPrice (SL=" + sl + ", Entry=" + entry + ")");
            }
            if (t1 >= entry) {
                result.addError("SHORT signal must have target1 < entryPrice (T1=" + t1 + ", Entry=" + entry + ")");
            }
            if (t2 > 0 && t2 >= entry) {
                result.addError("SHORT signal must have target2 < entryPrice");
            }
        } else {
            result.addError("Signal must be either BULLISH or BEARISH");
        }

        // Validate minimum risk/reward distance (prevent too-tight stops)
        double risk = Math.abs(entry - sl);
        double reward = Math.abs(t1 - entry);
        double riskPercent = (risk / entry) * 100;

        if (riskPercent < 0.1) {
            result.addWarning("Risk is very tight (<0.1% of entry price) - may result in whipsaws");
        }
        if (riskPercent > 10.0) {
            result.addWarning("Risk is very wide (>10% of entry price) - check if intended");
        }

        // Validate R:R ratio
        if (risk > 0) {
            double calculatedRR = reward / risk;
            if (calculatedRR < MIN_RISK_REWARD) {
                result.addError("Risk-reward ratio too low: " + String.format("%.2f", calculatedRR) +
                                " (minimum: " + MIN_RISK_REWARD + ")");
            }
        }
    }

    private void validateScoreBounds(StrategySignal signal, ValidationResult result) {
        // Confidence: [0, 1]
        validateRange(signal.getConfidence(), MIN_CONFIDENCE, MAX_CONFIDENCE, "confidence", result);

        // Structural bias: [-1, 1]
        validateRange(signal.getStructuralBias(), MIN_STRUCTURAL_BIAS, MAX_STRUCTURAL_BIAS,
                     "structuralBias", result);

        // VCP scores: [0, 1]
        validateRange(signal.getVcpCombinedScore(), 0.0, 1.0, "vcpCombinedScore", result);
        validateRange(signal.getSupportScore(), 0.0, 1.0, "supportScore", result);
        validateRange(signal.getResistanceScore(), 0.0, 1.0, "resistanceScore", result);

        // IPU scores: [0, 1]
        validateRange(signal.getIpuFinalScore(), 0.0, 1.0, "ipuFinalScore", result);
        validateRange(signal.getExhaustionScore(), 0.0, 1.0, "exhaustionScore", result);

        // Position size multiplier: [0.1, 3.0]
        if (signal.getPositionSizeMultiplier() != 0.0) {
            validateRange(signal.getPositionSizeMultiplier(), MIN_POSITION_MULTIPLIER,
                         MAX_POSITION_MULTIPLIER, "positionSizeMultiplier", result);
        }

        // Risk-reward ratio: > 0
        if (signal.getRiskRewardRatio() != 0.0) {
            if (signal.getRiskRewardRatio() <= 0 ||
                Double.isNaN(signal.getRiskRewardRatio()) ||
                Double.isInfinite(signal.getRiskRewardRatio())) {
                result.addError("riskRewardRatio must be a positive finite number");
            }
        }

        // ATR must be positive if specified
        if (signal.getAtr() != 0.0 && signal.getAtr() <= 0) {
            result.addError("ATR must be > 0 if specified");
        }
    }

    private void validateRange(double value, double min, double max, String fieldName,
                               ValidationResult result) {
        if (Double.isNaN(value)) {
            result.addError(fieldName + " is NaN");
        } else if (Double.isInfinite(value)) {
            result.addError(fieldName + " is Infinity");
        } else if (value < min || value > max) {
            result.addError(fieldName + " must be in range [" + min + ", " + max +
                          "] but was " + value);
        }
    }

    private void validateTimestamp(StrategySignal signal, ValidationResult result) {
        long timestamp = signal.getTimestamp();
        long now = System.currentTimeMillis();

        // Check if timestamp is plausible (between 2020 and now + 1 hour)
        long year2020 = 1577836800000L; // 2020-01-01
        long futureThreshold = now + 3600_000L; // 1 hour from now

        if (timestamp < year2020) {
            result.addError("timestamp is before 2020 - likely invalid");
        }

        if (timestamp > futureThreshold) {
            result.addError("timestamp cannot be in the future");
        }

        // Warning for old signals
        long age = now - timestamp;
        if (age > MAX_TIMESTAMP_AGE_MS) {
            result.addWarning("Signal is more than 24 hours old");
        }
    }

    private void validateSignalConsistency(StrategySignal signal, ValidationResult result) {
        // Cannot have both long and short signals
        if (signal.isLongSignal() && signal.isShortSignal()) {
            result.addError("Cannot have both longSignal and shortSignal set to true");
        }

        // Must have at least one signal direction
        if (!signal.isLongSignal() && !signal.isShortSignal()) {
            result.addError("Must have either longSignal or shortSignal set to true");
        }

        // Direction should match signal flags
        String direction = signal.getDirection();
        if (direction != null) {
            if (signal.isLongSignal() && "BEARISH".equalsIgnoreCase(direction)) {
                result.addWarning("longSignal=true but direction=BEARISH - inconsistent");
            }
            if (signal.isShortSignal() && "BULLISH".equalsIgnoreCase(direction)) {
                result.addWarning("shortSignal=true but direction=BULLISH - inconsistent");
            }
        }
    }

    private void validateNumericFields(StrategySignal signal, ValidationResult result) {
        // Check all double fields for NaN/Infinity
        checkFinite(signal.getEntryPrice(), "entryPrice", result);
        checkFinite(signal.getStopLoss(), "stopLoss", result);
        checkFinite(signal.getTarget1(), "target1", result);
        checkFinite(signal.getCurrentPrice(), "currentPrice", result);

        if (signal.getTarget2() != 0.0) {
            checkFinite(signal.getTarget2(), "target2", result);
        }
    }

    private void checkFinite(double value, String fieldName, ValidationResult result) {
        if (value != 0.0) {  // Only check if field is set
            if (Double.isNaN(value)) {
                result.addError(fieldName + " is NaN");
            } else if (Double.isInfinite(value)) {
                result.addError(fieldName + " is Infinity");
            }
        }
    }
}
