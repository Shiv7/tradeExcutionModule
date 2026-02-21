package com.kotsin.execution.virtual.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Data;

@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class VirtualPosition {
    public enum Side { LONG, SHORT }

    private String scripCode;
    private Side side;
    private int qtyOpen;
    private double avgEntry;
    private double realizedPnl;
    private Double sl;
    private Double tp1;
    private Double tp2;
    private Double tp1ClosePercent; // 0..1
    private Boolean tp1Hit;

    // Trailing configuration/state
    private String trailingType; // NONE|FIXED|PCT
    private Double trailingValue; // points or percent
    private Double trailingStep;  // points
    private Boolean trailingActive;
    private Double trailingStop;  // current trailing SL level
    private Double trailAnchor;   // last peak/trough reference
    private long openedAt;
    private long updatedAt;

    // Live market data (updated every 500ms in process loop)
    private Double currentPrice;
    private double unrealizedPnl;

    // Exchange (N=NSE, B=BSE, M=MCX, C=Currency)
    private String exchange;

    // Signal linkage for stats tracking
    private String signalId;      // Links back to SignalHistory in StreamingCandle
    private String signalType;    // e.g., "BREAKOUT_RETEST"
    private String signalSource;  // Strategy source: FUDKII, FUKAA, PIVOT, etc.
    private double positionSizeMultiplier;  // From gate chain

    // Display name: e.g. "BDL" for equity, "BDL 1300 CE" for option trades
    private String instrumentSymbol;

    // Fields written by Dashboard StrategyTradeExecutor (shared Redis key space)
    private String strategy;            // Strategy name: FUKAA, FUDKII, PIVOT, etc.
    private String status;              // ACTIVE, CLOSED
    private String instrumentType;      // OPTION, FUTURES
    private Double delta;               // Option delta
    private String underlyingScripCode; // Underlying equity/commodity scripCode
    private Double confidence;          // Signal confidence score

    // Dual-monitoring equity levels (underlying price targets)
    private Double equitySl;
    private Double equityT1;
    private Double equityT2;
    private Double equityT3;
    private Double equityT4;
    private Double equityLtp;           // Live underlying price

    // Option-level targets
    private Double optionSl;
    private Double optionT1;
    private Double optionT2;
    private Double optionT3;
    private Double optionT4;

    // Extended targets and hit tracking
    private Double target3;
    private Double target4;
    private Boolean smartTargets;
    private Boolean t1Hit;
    private Boolean t2Hit;
    private Boolean t3Hit;
    private Boolean t4Hit;
    private Boolean slHit;
    private String exitReason;
}
