package com.kotsin.execution.virtual.model;

import lombok.Data;

@Data
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
    
    // Signal linkage for stats tracking
    private String signalId;      // Links back to SignalHistory in StreamingCandle
    private String signalType;    // e.g., "BREAKOUT_RETEST"
    private double positionSizeMultiplier;  // From gate chain
}
