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
    private String symbol;
    private String companyName;
    private long timestamp;
    private String timeframe;
    private String humanReadableTime;

    // Signal Core
    private double quantScore;
    private String quantLabel;
    private SignalType signalType;
    private String direction;        // LONG/SHORT
    private double directionalStrength;
    private double confidence;
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

    /**
     * Check if signal is actionable for trading
     */
    public boolean isActionable() {
        return actionable && signalType != null &&
               entryPrice > 0 && stopLoss > 0 && target1 > 0;
    }

    /**
     * Get exchange from scripCode
     */
    public String getExchange() {
        if (scripCode == null) return "N";
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
        MOMENTUM
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
}
