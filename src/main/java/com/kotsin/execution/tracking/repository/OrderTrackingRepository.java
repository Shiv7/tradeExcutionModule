package com.kotsin.execution.tracking.repository;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.kotsin.execution.tracking.model.OrderStatusEvent;
import com.kotsin.execution.tracking.model.OrderTrackingEntry;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Repository;

import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Redis repository for order tracking entries and status events.
 */
@Repository
@RequiredArgsConstructor
@Slf4j
public class OrderTrackingRepository {

    private static final String ORDER_TRACKING_PREFIX = "order:tracking:";
    private static final String ORDER_EVENTS_PREFIX = "order:events:";
    private static final String ACTIVE_ORDERS_SET = "orders:active";
    private static final String COMPLETED_ORDERS_SET = "orders:completed";
    private static final String SIGNAL_ORDERS_PREFIX = "signal:orders:";
    private static final Duration TTL_COMPLETED = Duration.ofDays(30);

    private final RedisTemplate<String, String> redisTemplate;
    private final ObjectMapper objectMapper;

    /**
     * Save or update an order tracking entry
     */
    public void saveOrderTracking(OrderTrackingEntry entry) {
        try {
            String key = ORDER_TRACKING_PREFIX + entry.getOrderId();
            String json = objectMapper.writeValueAsString(entry);
            redisTemplate.opsForValue().set(key, json);

            // Maintain active/completed sets
            if (entry.getState() == OrderTrackingEntry.OrderState.CLOSED ||
                entry.getState() == OrderTrackingEntry.OrderState.REJECTED ||
                entry.getState() == OrderTrackingEntry.OrderState.CANCELED) {
                redisTemplate.opsForSet().remove(ACTIVE_ORDERS_SET, entry.getOrderId());
                redisTemplate.opsForSet().add(COMPLETED_ORDERS_SET, entry.getOrderId());
                redisTemplate.expire(key, TTL_COMPLETED);
            } else {
                redisTemplate.opsForSet().add(ACTIVE_ORDERS_SET, entry.getOrderId());
            }

            // Maintain signal->orders mapping
            if (entry.getSignalId() != null) {
                redisTemplate.opsForSet().add(SIGNAL_ORDERS_PREFIX + entry.getSignalId(), entry.getOrderId());
            }

        } catch (Exception e) {
            log.error("Failed to save order tracking: {}", e.getMessage());
        }
    }

    /**
     * Get order tracking by orderId
     */
    public Optional<OrderTrackingEntry> getOrderTracking(String orderId) {
        try {
            String key = ORDER_TRACKING_PREFIX + orderId;
            String json = redisTemplate.opsForValue().get(key);
            if (json != null) {
                return Optional.of(objectMapper.readValue(json, OrderTrackingEntry.class));
            }
        } catch (Exception e) {
            log.error("Failed to get order tracking {}: {}", orderId, e.getMessage());
        }
        return Optional.empty();
    }

    /**
     * Store an order status event
     */
    public void saveStatusEvent(OrderStatusEvent event) {
        try {
            String eventsKey = ORDER_EVENTS_PREFIX + event.getOrderId();
            String json = objectMapper.writeValueAsString(event);
            redisTemplate.opsForList().rightPush(eventsKey, json);
            // Keep last 100 events per order
            redisTemplate.opsForList().trim(eventsKey, -100, -1);
        } catch (Exception e) {
            log.error("Failed to save status event: {}", e.getMessage());
        }
    }

    /**
     * Get all events for an order
     */
    public List<OrderStatusEvent> getOrderEvents(String orderId) {
        try {
            String key = ORDER_EVENTS_PREFIX + orderId;
            List<String> jsonList = redisTemplate.opsForList().range(key, 0, -1);
            if (jsonList != null) {
                return jsonList.stream()
                        .map(json -> {
                            try {
                                return objectMapper.readValue(json, OrderStatusEvent.class);
                            } catch (Exception e) {
                                return null;
                            }
                        })
                        .filter(Objects::nonNull)
                        .collect(Collectors.toList());
            }
        } catch (Exception e) {
            log.error("Failed to get order events {}: {}", orderId, e.getMessage());
        }
        return Collections.emptyList();
    }

    /**
     * Get all active orders
     */
    public List<OrderTrackingEntry> getActiveOrders() {
        Set<String> orderIds = redisTemplate.opsForSet().members(ACTIVE_ORDERS_SET);
        if (orderIds == null || orderIds.isEmpty()) {
            return Collections.emptyList();
        }
        return orderIds.stream()
                .map(this::getOrderTracking)
                .filter(Optional::isPresent)
                .map(Optional::get)
                .collect(Collectors.toList());
    }

    /**
     * Get orders by signal ID
     */
    public List<OrderTrackingEntry> getOrdersBySignalId(String signalId) {
        Set<String> orderIds = redisTemplate.opsForSet().members(SIGNAL_ORDERS_PREFIX + signalId);
        if (orderIds == null || orderIds.isEmpty()) {
            return Collections.emptyList();
        }
        return orderIds.stream()
                .map(this::getOrderTracking)
                .filter(Optional::isPresent)
                .map(Optional::get)
                .collect(Collectors.toList());
    }

    /**
     * Get completed orders in time range
     */
    public List<OrderTrackingEntry> getCompletedOrders(Instant from, Instant to) {
        Set<String> orderIds = redisTemplate.opsForSet().members(COMPLETED_ORDERS_SET);
        if (orderIds == null || orderIds.isEmpty()) {
            return Collections.emptyList();
        }
        return orderIds.stream()
                .map(this::getOrderTracking)
                .filter(Optional::isPresent)
                .map(Optional::get)
                .filter(o -> o.getClosedAt() != null &&
                             o.getClosedAt().isAfter(from) &&
                             o.getClosedAt().isBefore(to))
                .collect(Collectors.toList());
    }

    /**
     * Get order count statistics
     */
    public Map<String, Long> getOrderStats() {
        Map<String, Long> stats = new HashMap<>();
        Long activeCount = redisTemplate.opsForSet().size(ACTIVE_ORDERS_SET);
        Long completedCount = redisTemplate.opsForSet().size(COMPLETED_ORDERS_SET);
        stats.put("active", activeCount != null ? activeCount : 0L);
        stats.put("completed", completedCount != null ? completedCount : 0L);
        return stats;
    }

    /**
     * Delete order tracking (for cleanup)
     */
    public void deleteOrderTracking(String orderId) {
        String key = ORDER_TRACKING_PREFIX + orderId;
        String eventsKey = ORDER_EVENTS_PREFIX + orderId;
        redisTemplate.delete(key);
        redisTemplate.delete(eventsKey);
        redisTemplate.opsForSet().remove(ACTIVE_ORDERS_SET, orderId);
        redisTemplate.opsForSet().remove(COMPLETED_ORDERS_SET, orderId);
    }
}
