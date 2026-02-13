package com.kotsin.execution.tracking.service;

import com.kotsin.execution.tracking.model.OrderStatusEvent;
import com.kotsin.execution.tracking.model.OrderTrackingEntry;
import com.kotsin.execution.tracking.repository.OrderTrackingRepository;
import com.kotsin.execution.virtual.model.VirtualOrder;
import com.kotsin.execution.virtual.model.VirtualPosition;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Service for tracking order status changes and maintaining order lifecycle.
 * Publishes events to Kafka for downstream consumers (dashboard, analytics).
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class OrderStatusTracker {

    private static final String ORDER_STATUS_TOPIC = "order-status-events";

    private final OrderTrackingRepository repository;
    private final KafkaTemplate<String, Object> kafkaTemplate;

    /**
     * Track new order creation
     */
    public OrderTrackingEntry trackOrderCreated(VirtualOrder order, String walletId) {
        log.info("TRACKING_ORDER_CREATED orderId={} scrip={} side={} qty={}",
                order.getId(), order.getScripCode(), order.getSide(), order.getQty());

        OrderTrackingEntry entry = OrderTrackingEntry.fromOrder(
                order.getId(),
                order.getScripCode(),
                order.getSide().toString(),
                order.getType().toString(),
                order.getQty(),
                order.getLimitPrice(),
                order.getSignalId(),
                walletId
        );
        entry.setStopLoss(order.getSl());
        entry.setTarget1(order.getTp1());
        entry.setTarget2(order.getTp2());

        OrderStatusEvent event = OrderStatusEvent.orderCreated(
                order.getId(),
                order.getScripCode(),
                order.getSide().toString(),
                order.getQty(),
                order.getLimitPrice() != null ? order.getLimitPrice() : order.getCurrentPrice(),
                order.getSignalId()
        );

        entry.addEvent(event);
        repository.saveOrderTracking(entry);
        repository.saveStatusEvent(event);
        publishEvent(event);

        return entry;
    }

    /**
     * Track order filled
     */
    public void trackOrderFilled(VirtualOrder order) {
        log.info("TRACKING_ORDER_FILLED orderId={} scrip={} fillPrice={}",
                order.getId(), order.getScripCode(), order.getEntryPrice());

        Optional<OrderTrackingEntry> entryOpt = repository.getOrderTracking(order.getId());
        if (entryOpt.isEmpty()) {
            log.warn("Order tracking not found for filled order: {}", order.getId());
            return;
        }

        OrderTrackingEntry entry = entryOpt.get();
        entry.markFilled(order.getEntryPrice(), Instant.now());

        OrderStatusEvent event = OrderStatusEvent.orderFilled(
                order.getId(),
                order.getScripCode(),
                order.getSide().toString(),
                order.getQty(),
                order.getEntryPrice(),
                order.getSignalId()
        );

        entry.addEvent(event);
        repository.saveOrderTracking(entry);
        repository.saveStatusEvent(event);
        publishEvent(event);
    }

    /**
     * Track partial fill
     */
    public void trackPartialFill(VirtualOrder order, int filledQty, double fillPrice) {
        log.info("TRACKING_PARTIAL_FILL orderId={} scrip={} filled={}/{}",
                order.getId(), order.getScripCode(), filledQty, order.getQty());

        Optional<OrderTrackingEntry> entryOpt = repository.getOrderTracking(order.getId());
        if (entryOpt.isEmpty()) {
            log.warn("Order tracking not found for partial fill: {}", order.getId());
            return;
        }

        OrderTrackingEntry entry = entryOpt.get();
        entry.markPartialFill(filledQty, fillPrice);

        OrderStatusEvent event = OrderStatusEvent.orderPartialFill(
                order.getId(),
                order.getScripCode(),
                order.getSide().toString(),
                order.getQty(),
                entry.getFilledQty(),
                fillPrice
        );

        entry.addEvent(event);
        repository.saveOrderTracking(entry);
        repository.saveStatusEvent(event);
        publishEvent(event);
    }

    /**
     * Track order rejection
     */
    public void trackOrderRejected(VirtualOrder order, String reason) {
        log.info("TRACKING_ORDER_REJECTED orderId={} scrip={} reason={}",
                order.getId(), order.getScripCode(), reason);

        Optional<OrderTrackingEntry> entryOpt = repository.getOrderTracking(order.getId());
        OrderTrackingEntry entry;
        if (entryOpt.isEmpty()) {
            // Create new entry for rejected order
            entry = OrderTrackingEntry.fromOrder(
                    order.getId(),
                    order.getScripCode(),
                    order.getSide().toString(),
                    order.getType().toString(),
                    order.getQty(),
                    order.getLimitPrice(),
                    order.getSignalId(),
                    null
            );
        } else {
            entry = entryOpt.get();
        }

        entry.markRejected(reason);

        OrderStatusEvent event = OrderStatusEvent.orderRejected(
                order.getId(),
                order.getScripCode(),
                reason
        );

        entry.addEvent(event);
        repository.saveOrderTracking(entry);
        repository.saveStatusEvent(event);
        publishEvent(event);
    }

    /**
     * Track position closed (SL/TP/Manual)
     */
    public void trackPositionClosed(VirtualPosition position, double exitPrice,
                                     double pnl, String exitReason) {
        log.info("TRACKING_POSITION_CLOSED scrip={} exit={} pnl={} reason={}",
                position.getScripCode(), exitPrice, pnl, exitReason);

        String positionId = position.getScripCode() + "_" + position.getOpenedAt();

        OrderStatusEvent event = OrderStatusEvent.positionClosed(
                positionId,
                position.getScripCode(),
                position.getSide().toString(),
                position.getQtyOpen(),
                position.getAvgEntry(),
                exitPrice,
                pnl,
                exitReason
        );

        // If linked to a signal, update the order entry
        if (position.getSignalId() != null) {
            List<OrderTrackingEntry> entries = repository.getOrdersBySignalId(position.getSignalId());
            for (OrderTrackingEntry entry : entries) {
                if (entry.getScripCode().equals(position.getScripCode())) {
                    entry.markClosed(pnl, Instant.now());
                    entry.addEvent(event);
                    repository.saveOrderTracking(entry);
                }
            }
        }

        repository.saveStatusEvent(event);
        publishEvent(event);
    }

    /**
     * Track SL hit
     */
    public void trackSlHit(VirtualPosition position, double slPrice, double pnl) {
        log.info("TRACKING_SL_HIT scrip={} sl={} pnl={}", position.getScripCode(), slPrice, pnl);

        String positionId = position.getScripCode() + "_" + position.getOpenedAt();
        OrderStatusEvent event = OrderStatusEvent.slHit(positionId, position.getScripCode(), slPrice, pnl);

        repository.saveStatusEvent(event);
        publishEvent(event);
    }

    /**
     * Track TP1 hit
     */
    public void trackTp1Hit(VirtualPosition position, double tpPrice, int closedQty, double pnl) {
        log.info("TRACKING_TP1_HIT scrip={} tp={} closedQty={} pnl={}",
                position.getScripCode(), tpPrice, closedQty, pnl);

        String positionId = position.getScripCode() + "_" + position.getOpenedAt();
        OrderStatusEvent event = OrderStatusEvent.tp1Hit(positionId, position.getScripCode(),
                tpPrice, closedQty, pnl);

        repository.saveStatusEvent(event);
        publishEvent(event);
    }

    /**
     * Get tracking entry by order ID
     */
    public Optional<OrderTrackingEntry> getOrderTracking(String orderId) {
        return repository.getOrderTracking(orderId);
    }

    /**
     * Get all events for an order
     */
    public List<OrderStatusEvent> getOrderEvents(String orderId) {
        return repository.getOrderEvents(orderId);
    }

    /**
     * Get all active orders
     */
    public List<OrderTrackingEntry> getActiveOrders() {
        return repository.getActiveOrders();
    }

    /**
     * Get orders by signal ID
     */
    public List<OrderTrackingEntry> getOrdersBySignalId(String signalId) {
        return repository.getOrdersBySignalId(signalId);
    }

    /**
     * Get order statistics
     */
    public Map<String, Long> getOrderStats() {
        return repository.getOrderStats();
    }

    /**
     * Publish event to Kafka
     */
    private void publishEvent(OrderStatusEvent event) {
        try {
            kafkaTemplate.send(ORDER_STATUS_TOPIC, event.getScripCode(), event);
        } catch (Exception e) {
            log.error("Failed to publish order status event: {}", e.getMessage());
        }
    }
}
