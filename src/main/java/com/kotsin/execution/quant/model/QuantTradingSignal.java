package com.kotsin.execution.quant.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * QuantTradingSignal - Trading signal from QuantScore system.
 *
 * Matches the QuantTradingSignal from streamingcandle module.
 * Contains full execution parameters including entry/exit prices,
 * position sizing, and hedging recommendations.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class QuantTradingSignal {

    // Identification
    private String signalId;
    private String scripCode;
    private String familyId;         // Added for unified signal format
    private String symbol;
    private String companyName;
    private long timestamp;
    private String timeframe;
    private String humanReadableTime;
    private String exchange;         // FIX: Added exchange field (N=NSE, B=BSE, M=MCX)

    // Enrichment note from SMTIS pipeline
    private String enrichmentNote;

    // Pattern identification (Phase 6)
    private String patternId;
    private String sequenceId;
    private String category;         // REVERSAL, CONTINUATION, BREAKOUT, etc.
    private String horizon;          // SCALP, SWING, etc.

    // Signal Core
    private double quantScore;
    private String quantLabel;
    private SignalType signalType;
    private String direction;        // LONG/SHORT
    private double directionalStrength;
    private double confidence;
    private double patternConfidence;       // Pattern-specific confidence
    private double historicalSuccessRate;   // Historical success rate
    private int qualityScore;               // Overall quality score (0-100)
    private String rationale;

    // Entry Parameters
    private double entryPrice;
    private double entryRangeHigh;
    private double entryRangeLow;

    // Exit Parameters
    private double stopLoss;
    private double stopLossDistance;
    private double stopLossPercent;
    private double target1;
    private double target2;
    private double target3;
    private double riskRewardRatio;

    // Position Sizing
    private PositionSizing sizing;

    // Trailing Stop
    private TrailingStopConfig trailingStop;

    // Hedging Recommendation
    private HedgingRecommendation hedging;

    // Time Constraints
    private TimeConstraints timeConstraints;

    // Greeks Summary (for display)
    private GreeksSummary greeksSummary;

    // Status
    private boolean actionable;
    private String actionableReason;

    // ========== CONTEXT-AWARE ENHANCEMENTS (SMTIS v2.0) ==========

    // Session Context
    private Double sessionPosition;              // Position in session range (0-100)
    private String sessionPositionDesc;          // AT_SESSION_LOW, MIDDLE, AT_SESSION_HIGH
    private Boolean vBottomDetected;             // V-bottom reversal detected
    private Boolean vTopDetected;                // V-top distribution detected
    private Integer failedBreakoutCount;         // Resistance holding strength
    private Integer failedBreakdownCount;        // Support holding strength
    private String currentSession;               // MCX_MORNING, MCX_EVENING, etc.

    // Family Context (Multi-Instrument Analysis)
    private String familyBias;                   // BULLISH, BEARISH, WEAK_BULLISH, WEAK_BEARISH, NEUTRAL
    private Double familyAlignment;              // 0-100 percentage
    private Boolean fullyAligned;                // All instruments aligned
    private Boolean hasDivergence;               // Options vs price divergence
    private java.util.List<String> divergences;  // Divergence details
    private Boolean shortSqueezeSetup;           // Short squeeze detected
    private Boolean longSqueezeSetup;            // Long squeeze detected
    private String familyInterpretation;         // Human-readable context

    // Event Tracking (Adaptive Learning)
    private java.util.List<String> triggerEvents;    // Events that triggered this signal
    private Integer eventCount;                      // Total events detected
    private java.util.List<String> matchedEvents;    // Pattern/setup matched events
    private Integer confirmedEventsCount;            // Previously confirmed events
    private Integer failedEventsCount;               // Previously failed events
    private Double eventConfirmationRate;            // Historical confirmation rate (0-100)

    // Adaptive Modifiers
    private Double contextModifier;                  // Combined modifier applied to confidence
    private String modifierBreakdown;                // Explanation of modifiers
    private Double originalConfidence;               // Pre-modifier confidence
    private java.util.List<String> modifierReasons;  // List of modifier reasons

    // Technical Context
    private String superTrendDirection;              // BULLISH, BEARISH
    private Boolean superTrendFlip;                  // Just flipped
    private Double bbPercentB;                       // Bollinger %B position
    private Boolean bbSqueeze;                       // BB squeeze detected
    private Double nearestSupport;                   // Nearest support level
    private Double nearestResistance;                // Nearest resistance level
    private Double dailyPivot;                       // Daily pivot level
    private Double maxPainLevel;                     // Options max pain
    private Double gammaFlipLevel;                   // GEX flip level
    private String gexRegime;                        // BULLISH_GAMMA, BEARISH_GAMMA, NEUTRAL

    // Invalidation Monitoring
    private java.util.List<InvalidationCondition> invalidationWatch;  // Conditions to monitor
    private Double invalidationPrice;                // Price that invalidates signal
    private String invalidationReason;               // Why it would be invalidated

    // Predictions
    private java.util.List<String> predictedEvents;  // Expected follow-on events
    private String expectedPriceAction;              // Expected price behavior

    /**
     * Check if signal is actionable for trading
     */
    public boolean isActionable() {
        return actionable && signalType != null &&
               entryPrice > 0 && stopLoss > 0 && target1 > 0;
    }

    /**
     * Get exchange - uses explicit exchange field first, then derives from scripCode
     */
    public String getExchange() {
        // FIX: Use explicit exchange field if set
        if (exchange != null && !exchange.isEmpty()) {
            return exchange;
        }
        // Fallback: derive from scripCode
        if (scripCode == null) return "N";
        if (scripCode.contains("_M_") || scripCode.startsWith("M_") || scripCode.contains("MCX")) return "M";
        if (scripCode.contains("_N_") || scripCode.startsWith("N_")) return "N";
        if (scripCode.contains("_B_") || scripCode.startsWith("B_")) return "B";
        return "N";
    }

    /**
     * Check if this is a long signal
     */
    public boolean isLong() {
        return "LONG".equalsIgnoreCase(direction) ||
               (directionalStrength > 0 && quantScore >= 65);
    }

    /**
     * Check if this is a short signal
     */
    public boolean isShort() {
        return "SHORT".equalsIgnoreCase(direction) ||
               (directionalStrength < 0 && quantScore >= 65);
    }

    // ========== Nested Classes ==========

    public enum SignalType {
        // Original types
        GAMMA_SQUEEZE_LONG,
        GAMMA_SQUEEZE_SHORT,
        IV_CRUSH_ENTRY,
        IV_EXPANSION_ENTRY,
        BREAKOUT_RETEST,
        BREAKDOWN_RETEST,
        VPIN_DIVERGENCE,
        OFI_MOMENTUM,
        WYCKOFF_ACCUMULATION,
        WYCKOFF_DISTRIBUTION,
        PCR_EXTREME,
        MULTI_CATEGORY_CONFLUENCE,
        CUSTOM,
        // New types from Phase 6 SignalGenerator
        PATTERN,
        SETUP,
        FORECAST,
        INTELLIGENCE,
        MANUAL,
        // Signal categories (may also be used as types)
        REVERSAL,
        CONTINUATION,
        BREAKOUT,
        BREAKDOWN,
        MEAN_REVERSION,
        SQUEEZE,
        EXHAUSTION,
        MOMENTUM,
        // FIX: Added missing types from TradingSignalPublisher.mapSourceToSignalType()
        // These are sent by streamingcandle but were not in this enum!
        FLOW_REVERSAL_LONG,
        FLOW_REVERSAL_SHORT,
        CONFLUENCE_BREAKOUT,
        CONFLUENCE_BREAKDOWN,
        SMART_MONEY_ACCUMULATION,
        MULTI_TIMEFRAME_ALIGNMENT,
        REVERSAL_PATTERN
    }

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class PositionSizing {
        private int quantity;
        private int lots;
        private int lotSize;
        private double positionValue;
        private double riskAmount;
        private double riskPercent;
        private double positionSizeMultiplier;
        private String sizingMethod;
    }

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class TrailingStopConfig {
        private boolean enabled;
        private String type;         // FIXED/PCT
        private double value;
        private double step;
        private double activationPrice;
        private double activationPercent;
    }

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class HedgingRecommendation {
        private boolean recommended;
        private String hedgeType;
        private String hedgeInstrument;
        private double hedgeRatio;
        private int hedgeQuantity;
        private double hedgePrice;
        private String hedgeRationale;
        private String hedgePriority;
        private double hedgeCost;
    }

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class TimeConstraints {
        private String preferredSession;
        private boolean avoidExpiry;
        private int daysToExpiry;
        private boolean intraday;
        private long maxHoldingMinutes;
        private boolean marketHoursOnly;
    }

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class GreeksSummary {
        private double totalDelta;
        private double totalGamma;
        private double totalVega;
        private double totalTheta;
        private boolean gammaSqueezeRisk;
        private double gammaSqueezeDistance;
        private double maxGammaStrike;
        private String deltaBias;
        private String vegaStructure;
        private double riskScore;
    }

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class InvalidationCondition {
        private String condition;           // Description of the condition
        private String monitorType;         // PRICE, TIME, EVENT, VOLUME
        private Double threshold;           // Numeric threshold if applicable
        private String action;              // What to do if triggered (EXIT, REDUCE, ALERT)
        
        /**
         * FIX: Handle string-only deserialization gracefully.
         * When the producer sends a plain string (e.g., "Trend breaks down"),
         * Jackson will use this factory method to create an InvalidationCondition.
         * 
         * @param value The plain string condition
         * @return InvalidationCondition with the string as the condition field
         */
        @com.fasterxml.jackson.annotation.JsonCreator
        public static InvalidationCondition fromString(String value) {
            return InvalidationCondition.builder()
                    .condition(value)
                    .monitorType("GENERAL")
                    .action("ALERT")
                    .build();
        }
    }
}
