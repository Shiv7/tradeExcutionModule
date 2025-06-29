package com.kotsin.execution.controller;

import com.kotsin.execution.service.CleanTradeExecutionService;
import com.kotsin.execution.service.CapitalManagementService;
import com.kotsin.execution.consumer.LiveMarketDataConsumer;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

/**
 * Trade Execution Monitoring Controller
 * Provides real-time diagnostics for trade execution system
 */
@RestController
@RequestMapping("/api/trade-execution/monitor")
@Slf4j
@RequiredArgsConstructor
public class TradeExecutionMonitorController {
    
    private final CleanTradeExecutionService cleanTradeExecutionService;
    private final CapitalManagementService capitalManagementService;
    
    private static final DateTimeFormatter TIME_FORMAT = DateTimeFormatter.ofPattern("HH:mm:ss");
    
    /**
     * Get comprehensive trade execution status
     */
    @GetMapping("/status")
    public ResponseEntity<Map<String, Object>> getTradeExecutionStatus() {
        try {
            Map<String, Object> status = new HashMap<>();
            
            // Basic stats
            status.put("timestamp", LocalDateTime.now().format(TIME_FORMAT));
            status.put("activeTradesCount", cleanTradeExecutionService.getActiveTradesCount());
            status.put("activeTradesSummary", cleanTradeExecutionService.getActiveTradesSummary());
            
            // Capital management stats
            Map<String, Object> capitalStats = capitalManagementService.getCapitalStats();
            status.put("capitalStats", capitalStats);
            
            // System health
            status.put("systemHealth", "OPERATIONAL");
            status.put("lastUpdated", LocalDateTime.now().toString());
            
            log.info("📊 [Monitor] Trade execution status requested - Active trades: {}", 
                    cleanTradeExecutionService.getActiveTradesCount());
            
            return ResponseEntity.ok(status);
            
        } catch (Exception e) {
            log.error("🚨 [Monitor] Error getting trade execution status: {}", e.getMessage(), e);
            
            Map<String, Object> errorStatus = new HashMap<>();
            errorStatus.put("error", "Failed to get status: " + e.getMessage());
            errorStatus.put("timestamp", LocalDateTime.now().format(TIME_FORMAT));
            
            return ResponseEntity.internalServerError().body(errorStatus);
        }
    }
    
    /**
     * Get detailed active trades information
     */
    @GetMapping("/active-trades")
    public ResponseEntity<Map<String, Object>> getActiveTradesDetails() {
        try {
            Map<String, Object> response = new HashMap<>();
            
            response.put("timestamp", LocalDateTime.now().format(TIME_FORMAT));
            response.put("activeTradesCount", cleanTradeExecutionService.getActiveTradesCount());
            response.put("activeTradesSummary", cleanTradeExecutionService.getActiveTradesSummary());
            
            // Add more detailed info if available
            response.put("message", "Use /status for comprehensive details");
            
            return ResponseEntity.ok(response);
            
        } catch (Exception e) {
            log.error("🚨 [Monitor] Error getting active trades: {}", e.getMessage(), e);
            return ResponseEntity.internalServerError().body(Map.of("error", e.getMessage()));
        }
    }
    
    /**
     * Manual price update for testing (useful for debugging)
     */
    @PostMapping("/manual-price-update")
    public ResponseEntity<Map<String, String>> manualPriceUpdate(
            @RequestParam String scripCode,
            @RequestParam double price) {
        try {
            LocalDateTime now = LocalDateTime.now();
            
            log.info("🔧 [Monitor] Manual price update: {} @ {} (for testing)", scripCode, price);
            
            // Send price update to trade execution service
            cleanTradeExecutionService.updateTradeWithPrice(scripCode, price, now);
            
            Map<String, String> response = new HashMap<>();
            response.put("status", "success");
            response.put("message", String.format("Price update sent for %s @ %s", scripCode, price));
            response.put("timestamp", now.format(TIME_FORMAT));
            
            return ResponseEntity.ok(response);
            
        } catch (Exception e) {
            log.error("🚨 [Monitor] Error in manual price update: {}", e.getMessage(), e);
            return ResponseEntity.internalServerError().body(Map.of("error", e.getMessage()));
        }
    }
    
    /**
     * Force entry for testing purposes
     */
    @PostMapping("/force-entry/{scripCode}")
    public ResponseEntity<Map<String, String>> forceEntry(@PathVariable String scripCode) {
        try {
            log.info("🔧 [Monitor] Force entry requested for: {}", scripCode);
            
            Map<String, String> response = new HashMap<>();
            response.put("status", "info");
            response.put("message", "Force entry not implemented - use manual price updates to trigger natural entry");
            response.put("scripCode", scripCode);
            response.put("timestamp", LocalDateTime.now().format(TIME_FORMAT));
            
            return ResponseEntity.ok(response);
            
        } catch (Exception e) {
            log.error("🚨 [Monitor] Error in force entry: {}", e.getMessage(), e);
            return ResponseEntity.internalServerError().body(Map.of("error", e.getMessage()));
        }
    }
    
    /**
     * Health check endpoint
     */
    @GetMapping("/health")
    public ResponseEntity<Map<String, String>> healthCheck() {
        Map<String, String> health = new HashMap<>();
        health.put("status", "UP");
        health.put("service", "Trade Execution Monitor");
        health.put("timestamp", LocalDateTime.now().format(TIME_FORMAT));
        health.put("activeTradesCount", String.valueOf(cleanTradeExecutionService.getActiveTradesCount()));
        
        return ResponseEntity.ok(health);
    }
} 