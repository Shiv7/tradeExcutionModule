package com.kotsin.execution.tracking.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;

/**
 * Represents an order status change event for tracking and reconciliation.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class OrderStatusEvent {

    public enum EventType {
        ORDER_CREATED,
        ORDER_PENDING,
        ORDER_FILLED,
        ORDER_PARTIAL_FILL,
        ORDER_REJECTED,
        ORDER_CANCELED,
        POSITION_OPENED,
        POSITION_UPDATED,
        POSITION_CLOSED,
        SL_HIT,
        TP1_HIT,
        TP2_HIT,
        TRAILING_ARMED,
        TRAILING_UPDATE,
        MARGIN_DEDUCTED,
        PNL_CREDITED
    }

    private String eventId;
    private String orderId;
    private String positionId;
    private String scripCode;
    private String symbol;
    private EventType eventType;
    private String status;
    private String side;

    // Price info
    private Double price;
    private Double entryPrice;
    private Double exitPrice;
    private Double stopLoss;
    private Double target1;
    private Double target2;
    private Double trailingStop;

    // Quantity info
    private Integer quantity;
    private Integer filledQty;
    private Integer remainingQty;

    // P&L info
    private Double realizedPnl;
    private Double unrealizedPnl;

    // Wallet info
    private String walletId;
    private Double marginAmount;
    private Double walletBalance;

    // Signal reference
    private String signalId;
    private String signalType;

    // Metadata
    private Instant timestamp;
    private String reason;
    private String source;

    public static OrderStatusEvent orderCreated(String orderId, String scripCode, String side,
                                                  int qty, Double price, String signalId) {
        return OrderStatusEvent.builder()
                .eventId(java.util.UUID.randomUUID().toString())
                .orderId(orderId)
                .scripCode(scripCode)
                .eventType(EventType.ORDER_CREATED)
                .status("NEW")
                .side(side)
                .quantity(qty)
                .price(price)
                .signalId(signalId)
                .timestamp(Instant.now())
                .source("VIRTUAL_ENGINE")
                .build();
    }

    public static OrderStatusEvent orderFilled(String orderId, String scripCode, String side,
                                                 int qty, double fillPrice, String signalId) {
        return OrderStatusEvent.builder()
                .eventId(java.util.UUID.randomUUID().toString())
                .orderId(orderId)
                .scripCode(scripCode)
                .eventType(EventType.ORDER_FILLED)
                .status("FILLED")
                .side(side)
                .quantity(qty)
                .filledQty(qty)
                .remainingQty(0)
                .entryPrice(fillPrice)
                .signalId(signalId)
                .timestamp(Instant.now())
                .source("VIRTUAL_ENGINE")
                .build();
    }

    public static OrderStatusEvent orderPartialFill(String orderId, String scripCode, String side,
                                                      int totalQty, int filledQty, double fillPrice) {
        return OrderStatusEvent.builder()
                .eventId(java.util.UUID.randomUUID().toString())
                .orderId(orderId)
                .scripCode(scripCode)
                .eventType(EventType.ORDER_PARTIAL_FILL)
                .status("PARTIAL")
                .side(side)
                .quantity(totalQty)
                .filledQty(filledQty)
                .remainingQty(totalQty - filledQty)
                .entryPrice(fillPrice)
                .timestamp(Instant.now())
                .source("VIRTUAL_ENGINE")
                .build();
    }

    public static OrderStatusEvent orderRejected(String orderId, String scripCode, String reason) {
        return OrderStatusEvent.builder()
                .eventId(java.util.UUID.randomUUID().toString())
                .orderId(orderId)
                .scripCode(scripCode)
                .eventType(EventType.ORDER_REJECTED)
                .status("REJECTED")
                .reason(reason)
                .timestamp(Instant.now())
                .source("VIRTUAL_ENGINE")
                .build();
    }

    public static OrderStatusEvent positionClosed(String positionId, String scripCode, String side,
                                                    int qty, double entryPrice, double exitPrice,
                                                    double pnl, String exitReason) {
        return OrderStatusEvent.builder()
                .eventId(java.util.UUID.randomUUID().toString())
                .positionId(positionId)
                .scripCode(scripCode)
                .eventType(EventType.POSITION_CLOSED)
                .status("CLOSED")
                .side(side)
                .quantity(qty)
                .entryPrice(entryPrice)
                .exitPrice(exitPrice)
                .realizedPnl(pnl)
                .reason(exitReason)
                .timestamp(Instant.now())
                .source("VIRTUAL_ENGINE")
                .build();
    }

    public static OrderStatusEvent slHit(String positionId, String scripCode, double slPrice, double pnl) {
        return OrderStatusEvent.builder()
                .eventId(java.util.UUID.randomUUID().toString())
                .positionId(positionId)
                .scripCode(scripCode)
                .eventType(EventType.SL_HIT)
                .status("SL_TRIGGERED")
                .stopLoss(slPrice)
                .realizedPnl(pnl)
                .timestamp(Instant.now())
                .source("VIRTUAL_ENGINE")
                .build();
    }

    public static OrderStatusEvent tp1Hit(String positionId, String scripCode, double tpPrice,
                                            int closedQty, double pnl) {
        return OrderStatusEvent.builder()
                .eventId(java.util.UUID.randomUUID().toString())
                .positionId(positionId)
                .scripCode(scripCode)
                .eventType(EventType.TP1_HIT)
                .status("TP1_TRIGGERED")
                .target1(tpPrice)
                .quantity(closedQty)
                .realizedPnl(pnl)
                .timestamp(Instant.now())
                .source("VIRTUAL_ENGINE")
                .build();
    }
}
