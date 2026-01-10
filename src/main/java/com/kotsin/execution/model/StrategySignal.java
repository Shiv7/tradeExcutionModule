package com.kotsin.execution.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

/**
 * StrategySignal - Aligned with StreamingCandle's TradingSignal
 * 
 * This model matches the output from the 16-module quant framework:
 * - VCP (Volume Cluster Profile): Support/resistance clusters
 * - IPU (Institutional Participation Unit): Momentum, urgency, exhaustion
 * 
 * Topic: trading-signals
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class StrategySignal {

    // ========== Metadata ==========
    private String scripCode;           // Format: "N:D:49812" (Exchange:Type:Code)
    private String companyName;
    private long timestamp;

    // ========== Primary Action Signal ==========
    private String signal;              // SignalType enum as string
    private double confidence;          // 0-1 scale
    private String rationale;           // Human-readable reason

    // ========== VCP Component (Volume Cluster Profile) ==========
    private double vcpCombinedScore;    // Overall cluster score
    private double supportScore;        // Strength of support levels
    private double resistanceScore;     // Strength of resistance levels
    private double structuralBias;      // -1 (bearish) to +1 (bullish)
    private double runwayScore;         // Clear path for price movement
    private List<VCPCluster> clusters;  // Price clusters

    // ========== IPU Component (Institutional Participation) ==========
    private double ipuFinalScore;       // Overall IPU score
    private double instProxy;           // Institutional activity proxy
    private double momentumContext;     // Momentum strength
    private double validatedMomentum;   // Confirmed momentum
    private double exhaustionScore;     // Trend exhaustion level
    private double urgencyScore;        // Signal urgency
    private String momentumState;       // ACCELERATING, DECELERATING, FLAT
    private String urgencyLevel;        // AGGRESSIVE, ELEVATED, PATIENT, PASSIVE
    private String direction;           // BULLISH, BEARISH, NEUTRAL
    private double directionalConviction;
    private double flowMomentumAgreement; // 0-1, how aligned flow and momentum are
    private boolean exhaustionWarning;  // True if exhaustion detected
    private boolean xfactorFlag;        // Rare strong signal

    // ========== Current Market Context ==========
    private double currentPrice;
    private double atr;                 // Average True Range
    private double microprice;          // Order book derived fair price

    // ========== Position Sizing Recommendations ==========
    private double positionSizeMultiplier; // 0.5 to 1.5
    private double trailAtrMultiplier;     // ATR multiplier for trailing stop

    // ========== Trade Execution Parameters (CRITICAL) ==========
    private double entryPrice;          // Suggested entry price
    private double stopLoss;            // Stop loss level
    private double target1;             // First target (2:1 R:R)
    private double target2;             // Second target (3:1 R:R)
    private double riskRewardRatio;     // Calculated R:R
    private double riskPercentage;      // Risk as % of entry

    // ========== Signal Flags ==========
    private boolean longSignal;         // True if actionable LONG
    private boolean shortSignal;        // True if actionable SHORT
    private boolean warningSignal;      // True if warning (reduce exposure)
    private double trailingStopDistance;

    // ========== Pattern Recognition (Phase 4 SMTIS) ==========
    private String patternId;           // Pattern template ID (e.g., REVERSAL_FROM_SUPPORT)
    private String sequenceId;          // Unique sequence instance ID
    private String patternCategory;     // REVERSAL, CONTINUATION, BREAKOUT, SQUEEZE
    private String tradingHorizon;      // SCALP, SWING, POSITIONAL
    private double patternConfidence;   // Pattern-specific confidence 0-1
    private double historicalSuccessRate; // Historical win rate for this pattern
    private double historicalExpectedValue; // Historical EV for this pattern
    private int historicalSampleSize;   // Number of past occurrences
    private List<String> matchedEvents; // Events that completed the pattern
    private List<String> matchedBoosters; // Booster events that confirmed
    private List<String> entryReasons;  // Human-readable entry reasons
    private List<String> predictedEvents; // Predicted follow-on events
    private List<String> invalidationWatch; // Events to watch for invalidation
    private String narrative;           // Full pattern explanation
    private double priceMoveDuringPattern; // Price movement during pattern formation
    private long patternDurationMs;     // How long pattern took to complete
    private double target3;             // Extended target for pattern signals

    // ========== Exchange Info (parsed from scripCode) ==========
    private String exchange;            // N (NSE), B (BSE), M (MCX)
    private String exchangeType;        // C (Cash), D (Derivatives)

    // ========== Derived Helpers ==========
    
    public boolean isBullish() {
        return "BULLISH".equalsIgnoreCase(direction) || longSignal;
    }

    public boolean isBearish() {
        return "BEARISH".equalsIgnoreCase(direction) || shortSignal;
    }

    public boolean isActionable() {
        return longSignal || shortSignal;
    }

    public boolean isConfirmedSignal() {
        return signal != null && (
            signal.contains("CONFIRMED") ||
            signal.contains("STRONG") ||
            signal.contains("FADE")
        );
    }

    /**
     * Check if this is a pattern-based signal (from Phase 4 SMTIS)
     */
    public boolean isPatternSignal() {
        return patternId != null && !patternId.isEmpty();
    }

    /**
     * Get combined confidence (pattern + historical)
     */
    public double getCombinedConfidence() {
        if (!isPatternSignal()) {
            return confidence;
        }
        // Blend pattern confidence with historical success rate
        if (historicalSampleSize > 30) {
            return patternConfidence * 0.6 + historicalSuccessRate * 0.4;
        }
        return patternConfidence;
    }

    /**
     * Parse scripCode format "N:D:49812" into exchange/exchangeType
     */
    public void parseScripCode() {
        if (scripCode != null && scripCode.contains(":")) {
            String[] parts = scripCode.split(":");
            if (parts.length >= 3) {
                this.exchange = parts[0];
                this.exchangeType = parts[1];
            }
        }
    }

    /**
     * Get just the numeric scripCode (for broker API)
     */
    public String getNumericScripCode() {
        if (scripCode != null && scripCode.contains(":")) {
            String[] parts = scripCode.split(":");
            if (parts.length >= 3) {
                return parts[2];
            }
        }
        return scripCode;
    }

    /**
     * 🛡️ CRITICAL FIX #4: Validate signal before execution
     * Delegates to SignalValidator for comprehensive validation
     */
    public com.kotsin.execution.validation.ValidationResult validate() {
        com.kotsin.execution.validation.SignalValidator validator =
            new com.kotsin.execution.validation.SignalValidator();
        return validator.validate(this);
    }

    // ========== VCP Cluster Inner Class ==========
    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class VCPCluster {
        private double price;
        private double strength;
        private long totalVolume;
        private double ofiBias;
        private double obValidation;
        private double oiAdjustment;
        private double breakoutDifficulty;
        private double proximity;
        private String type;            // SUPPORT or RESISTANCE
        private double distancePercent;
        private int contributingCandles;
        private double compositeScore;
        private boolean aligned;
        private boolean weaklyValidated;
        private double alignmentMultiplier;
        private boolean stronglyValidated;
    }
}
