package com.kotsin.execution.tracking.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/**
 * Tracks complete lifecycle of an order from creation to completion.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class OrderTrackingEntry {

    public enum OrderState {
        CREATED,
        PENDING,
        PARTIALLY_FILLED,
        FILLED,
        REJECTED,
        CANCELED,
        CLOSED
    }

    private String orderId;
    private String scripCode;
    private String symbol;
    private String side;
    private String orderType;
    private int originalQty;
    private int filledQty;
    private int remainingQty;
    private Double limitPrice;
    private Double avgFillPrice;
    private Double stopLoss;
    private Double target1;
    private Double target2;

    private OrderState state;
    private String rejectionReason;

    // Signal reference
    private String signalId;
    private String signalType;

    // Wallet/margin info
    private String walletId;
    private Double marginReserved;
    private Double marginUsed;

    // P&L tracking
    private Double realizedPnl;
    private Double unrealizedPnl;

    // Position reference (after fill)
    private String positionId;

    // Timestamps
    private Instant createdAt;
    private Instant lastUpdatedAt;
    private Instant filledAt;
    private Instant closedAt;

    // Event history
    @Builder.Default
    private List<OrderStatusEvent> eventHistory = new ArrayList<>();

    public void addEvent(OrderStatusEvent event) {
        if (eventHistory == null) {
            eventHistory = new ArrayList<>();
        }
        eventHistory.add(event);
        lastUpdatedAt = Instant.now();
    }

    public void markFilled(double fillPrice, Instant time) {
        this.filledQty = this.originalQty;
        this.remainingQty = 0;
        this.avgFillPrice = fillPrice;
        this.state = OrderState.FILLED;
        this.filledAt = time;
        this.lastUpdatedAt = time;
    }

    public void markPartialFill(int qty, double price) {
        this.filledQty += qty;
        this.remainingQty = this.originalQty - this.filledQty;
        // Update average fill price
        if (this.avgFillPrice == null || this.avgFillPrice == 0) {
            this.avgFillPrice = price;
        } else {
            this.avgFillPrice = ((this.avgFillPrice * (this.filledQty - qty)) + (price * qty)) / this.filledQty;
        }
        this.state = this.remainingQty > 0 ? OrderState.PARTIALLY_FILLED : OrderState.FILLED;
        this.lastUpdatedAt = Instant.now();
    }

    public void markRejected(String reason) {
        this.state = OrderState.REJECTED;
        this.rejectionReason = reason;
        this.lastUpdatedAt = Instant.now();
    }

    public void markClosed(double pnl, Instant time) {
        this.state = OrderState.CLOSED;
        this.realizedPnl = pnl;
        this.closedAt = time;
        this.lastUpdatedAt = time;
    }

    public static OrderTrackingEntry fromOrder(String orderId, String scripCode, String side,
                                                 String orderType, int qty, Double limitPrice,
                                                 String signalId, String walletId) {
        Instant now = Instant.now();
        return OrderTrackingEntry.builder()
                .orderId(orderId)
                .scripCode(scripCode)
                .side(side)
                .orderType(orderType)
                .originalQty(qty)
                .filledQty(0)
                .remainingQty(qty)
                .limitPrice(limitPrice)
                .state(OrderState.CREATED)
                .signalId(signalId)
                .walletId(walletId)
                .realizedPnl(0.0)
                .unrealizedPnl(0.0)
                .createdAt(now)
                .lastUpdatedAt(now)
                .eventHistory(new ArrayList<>())
                .build();
    }
}
