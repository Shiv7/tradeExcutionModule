package com.kotsin.execution.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

/**
 * Result of a completed trade, published to downstream consumers.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class TradeResult {

    private String tradeId;
    private String scripCode;

    private double entryPrice;
    private double exitPrice;

    /** e.g., "TARGET1", "STOP_LOSS", "END_OF_SIM" */
    private String exitReason;

    private LocalDateTime entryTime;
    private LocalDateTime exitTime;

    /** optional realized PnL (positive for profit, negative for loss) */
    private Double realizedPnl;
}
