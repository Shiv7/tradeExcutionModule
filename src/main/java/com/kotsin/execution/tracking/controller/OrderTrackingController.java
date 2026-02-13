package com.kotsin.execution.tracking.controller;

import com.kotsin.execution.tracking.model.OrderStatusEvent;
import com.kotsin.execution.tracking.model.OrderTrackingEntry;
import com.kotsin.execution.tracking.service.OrderStatusTracker;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

/**
 * REST API for order tracking and status queries.
 */
@RestController
@RequestMapping("/api/orders/tracking")
@RequiredArgsConstructor
public class OrderTrackingController {

    private final OrderStatusTracker tracker;

    /**
     * Get tracking details for a specific order
     */
    @GetMapping("/{orderId}")
    public ResponseEntity<OrderTrackingEntry> getOrderTracking(@PathVariable String orderId) {
        return tracker.getOrderTracking(orderId)
                .map(ResponseEntity::ok)
                .orElse(ResponseEntity.notFound().build());
    }

    /**
     * Get all events for an order
     */
    @GetMapping("/{orderId}/events")
    public ResponseEntity<List<OrderStatusEvent>> getOrderEvents(@PathVariable String orderId) {
        List<OrderStatusEvent> events = tracker.getOrderEvents(orderId);
        return ResponseEntity.ok(events);
    }

    /**
     * Get all active orders (not closed/rejected)
     */
    @GetMapping("/active")
    public ResponseEntity<List<OrderTrackingEntry>> getActiveOrders() {
        return ResponseEntity.ok(tracker.getActiveOrders());
    }

    /**
     * Get orders linked to a signal
     */
    @GetMapping("/signal/{signalId}")
    public ResponseEntity<List<OrderTrackingEntry>> getOrdersBySignal(@PathVariable String signalId) {
        return ResponseEntity.ok(tracker.getOrdersBySignalId(signalId));
    }

    /**
     * Get order statistics
     */
    @GetMapping("/stats")
    public ResponseEntity<Map<String, Long>> getOrderStats() {
        return ResponseEntity.ok(tracker.getOrderStats());
    }

    /**
     * Health check for tracking service
     */
    @GetMapping("/health")
    public ResponseEntity<Map<String, Object>> healthCheck() {
        Map<String, Long> stats = tracker.getOrderStats();
        return ResponseEntity.ok(Map.of(
                "status", "UP",
                "activeOrders", stats.getOrDefault("active", 0L),
                "completedOrders", stats.getOrDefault("completed", 0L)
        ));
    }
}
