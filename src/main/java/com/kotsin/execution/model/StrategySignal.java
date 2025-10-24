package com.kotsin.execution.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Normalized strategy signal used by the execution pipeline.
 * - signal: "BULLISH" or "BEARISH" (normalized by SignalConsumer)
 * - timestamp: epoch millis (producer time if available)
 * - exchange: "N" (NSE) or "B" (BSE)
 * - exchangeType: "C" (Cash) or "D" (Derivatives)
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class StrategySignal {

    private String scripCode;
    private String companyName;

    /** "BULLISH" or "BEARISH" (uppercase) */
    private String signal;

    private double entryPrice;
    private double stopLoss;
    private double target1;
    private double target2;
    private double target3;

    /** event time from producer if present (epoch millis), else 0 */
    private long timestamp;

    /** "N" or "B" */
    private String exchange;

    /** "C" or "D" */
    private String exchangeType;

    /** optional metadata */
    private String strategy;   // e.g., "INTELLIGENT_CONFIRMATION"
    private String timeframe;  // e.g., "30m"

    // Execution instrument overrides (option-only execution)
    // If present, execution should place orders on these instead of the underlying scripCode.
    private String orderScripCode;
    private String orderExchange;
    private String orderExchangeType;
    private Double orderLimitPrice;       // back-compat
    private Double orderLimitPriceEntry;
    private Double orderLimitPriceExit;

    // convenience helpers
    public boolean isBullish() { return "BULLISH".equalsIgnoreCase(signal) || "BUY".equalsIgnoreCase(signal); }
    public boolean isBearish() { return "BEARISH".equalsIgnoreCase(signal) || "SELL".equalsIgnoreCase(signal); }
}
