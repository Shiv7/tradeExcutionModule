package com.kotsin.execution.virtual.model;

import lombok.Data;

@Data
public class VirtualOrder {
    public enum Side { BUY, SELL }
    public enum Type { MARKET, LIMIT }
    public enum Status { NEW, PENDING, FILLED, PARTIAL, CANCELED, COMPLETED, REJECTED }

    private String id;
    private String scripCode;
    private Side side;
    private Type type;
    private int qty;
    private Double limitPrice;
    private Double currentPrice;  // FIX: Market price at time of order (for MARKET order fallback)
    private Double entryPrice; // fill price
    private Double sl;
    private Double tp1;
    private Double tp2;
    private Double tp1ClosePercent; // 0..1
    // Trailing setup (MVP: FIXED or PCT)
    private String trailingType; // NONE|FIXED|PCT
    private Double trailingValue; // points or percent
    private Double trailingStep;  // points step for updates
    private long createdAt;
    private long updatedAt;
    private Status status;
    private String rejectionReason; // FIX: Reason if order was rejected (margin, risk, etc.)

    // Signal metadata (for quant signals)
    private String signalId;
    private String signalType;
    private String signalSource;  // Strategy source: FUDKII, FUKAA, PIVOT, etc.
    private String rationale;
}
