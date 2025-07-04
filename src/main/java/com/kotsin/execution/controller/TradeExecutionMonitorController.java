package com.kotsin.execution.controller;

import com.kotsin.execution.consumer.BulletproofSignalConsumer;
import com.kotsin.execution.model.ActiveTrade;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

/**
 * 🛡️ BULLETPROOF Trade Execution Monitoring Controller
 * Provides real-time diagnostics for the bulletproof trade execution system
 */
@RestController
@RequestMapping("/api/trade-execution/monitor")
@Slf4j
@RequiredArgsConstructor
public class TradeExecutionMonitorController {
    
    private final BulletproofSignalConsumer bulletproofSignalConsumer;
    
    private static final DateTimeFormatter TIME_FORMAT = DateTimeFormatter.ofPattern("HH:mm:ss");
    
    /**
     * 🛡️ BULLETPROOF: Get comprehensive trade execution status
     */
    @GetMapping("/status")
    public ResponseEntity<Map<String, Object>> getTradeExecutionStatus() {
        try {
            Map<String, Object> status = new HashMap<>();
            
            // Basic stats
            status.put("timestamp", LocalDateTime.now().format(TIME_FORMAT));
            
            // 🛡️ BULLETPROOF SYSTEM STATUS
            boolean hasBulletproofTrade = bulletproofSignalConsumer.hasActiveTrade();
            status.put("hasActiveTrade", hasBulletproofTrade);
            
            if (hasBulletproofTrade) {
                ActiveTrade currentTrade = bulletproofSignalConsumer.getCurrentTrade();
                Map<String, Object> tradeInfo = new HashMap<>();
                tradeInfo.put("scripCode", currentTrade.getScripCode());
                tradeInfo.put("signal", currentTrade.getSignalType());
                tradeInfo.put("entryTriggered", currentTrade.getEntryTriggered());
                tradeInfo.put("entryPrice", currentTrade.getEntryPrice());
                tradeInfo.put("stopLoss", currentTrade.getStopLoss());
                tradeInfo.put("target1", currentTrade.getTarget1());
                tradeInfo.put("target1Hit", currentTrade.isTarget1Hit());
                tradeInfo.put("positionSize", currentTrade.getPositionSize());
                tradeInfo.put("status", currentTrade.getStatus());
                status.put("activeTrade", tradeInfo);
            }
            
            // System health
            status.put("systemHealth", "OPERATIONAL");
            status.put("lastUpdated", LocalDateTime.now().toString());
            status.put("systemType", "BULLETPROOF");
            
            log.info("📊 [Monitor] Bulletproof status - Active trade: {}", hasBulletproofTrade);
            
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
     * 🛡️ BULLETPROOF: Get detailed active trade information
     */
    @GetMapping("/active-trades")
    public ResponseEntity<Map<String, Object>> getActiveTradesDetails() {
        try {
            Map<String, Object> response = new HashMap<>();
            
            response.put("timestamp", LocalDateTime.now().format(TIME_FORMAT));
            
            boolean hasActiveTrade = bulletproofSignalConsumer.hasActiveTrade();
            response.put("hasActiveTrade", hasActiveTrade);
            
            if (hasActiveTrade) {
                ActiveTrade currentTrade = bulletproofSignalConsumer.getCurrentTrade();
                response.put("scripCode", currentTrade.getScripCode());
                response.put("signal", currentTrade.getSignalType());
                response.put("status", currentTrade.getStatus());
                response.put("entryTriggered", currentTrade.getEntryTriggered());
            }
            
            response.put("message", "Use /status for comprehensive details");
            
            return ResponseEntity.ok(response);
            
        } catch (Exception e) {
            log.error("🚨 [Monitor] Error getting active trades: {}", e.getMessage(), e);
            return ResponseEntity.internalServerError().body(Map.of("error", e.getMessage()));
        }
    }
    
    /**
     * 🛡️ BULLETPROOF: Manual price update for testing
     */
    @PostMapping("/manual-price-update")
    public ResponseEntity<Map<String, String>> manualPriceUpdate(
            @RequestParam String scripCode,
            @RequestParam double price) {
        try {
            LocalDateTime now = LocalDateTime.now();
            
            log.info("🔧 [Monitor] Manual price update: {} @ {} (bulletproof system)", scripCode, price);
            
            // Send price update to bulletproof system
            bulletproofSignalConsumer.updatePrice(scripCode, price, now);
            
            Map<String, String> response = new HashMap<>();
            response.put("status", "success");
            response.put("message", String.format("Price update sent to bulletproof system for %s @ %s", scripCode, price));
            response.put("timestamp", now.format(TIME_FORMAT));
            response.put("bulletproofActive", String.valueOf(bulletproofSignalConsumer.hasActiveTrade()));
            
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
     * 🛡️ BULLETPROOF: Health check
     */
    @GetMapping("/health")
    public ResponseEntity<Map<String, String>> healthCheck() {
        Map<String, String> health = new HashMap<>();
        health.put("status", "UP");
        health.put("service", "Bulletproof Trade Execution Monitor");
        health.put("timestamp", LocalDateTime.now().format(TIME_FORMAT));
        health.put("bulletproofActiveTrade", String.valueOf(bulletproofSignalConsumer.hasActiveTrade()));
        health.put("systemType", "BULLETPROOF");
        
        return ResponseEntity.ok(health);
    }
} 