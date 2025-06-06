package com.kotsin.execution.controller;

import com.kotsin.execution.service.TradeExecutionService;
import com.kotsin.execution.service.TradeStateManager;
import com.kotsin.execution.service.TradeHistoryService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;
import java.util.HashMap;

/**
 * Test controller for validating trade execution APIs
 */
@RestController
@RequestMapping("/api/v1/test")
@RequiredArgsConstructor
@Slf4j
@CrossOrigin(origins = "*")
public class TestController {
    
    private final TradeExecutionService tradeExecutionService;
    private final TradeStateManager tradeStateManager;
    private final TradeHistoryService tradeHistoryService;
    
    /**
     * Test endpoint to manually create a trade
     * POST /api/v1/test/create-trade
     */
    @PostMapping("/create-trade")
    public ResponseEntity<Map<String, Object>> createTestTrade(
            @RequestParam String scripCode,
            @RequestParam String signal,
            @RequestParam Double entryPrice,
            @RequestParam Double stopLoss,
            @RequestParam Double target1) {
        
        try {
            log.info("🧪 Creating test trade: {} {} @ {}", signal, scripCode, entryPrice);
            
            tradeExecutionService.executeStrategySignal(
                    scripCode, 
                    signal, 
                    entryPrice, 
                    stopLoss, 
                    target1, 
                    "TEST_STRATEGY", 
                    "HIGH"
            );
            
            Map<String, Object> response = new HashMap<>();
            response.put("success", true);
            response.put("message", "Test trade created successfully");
            response.put("scripCode", scripCode);
            response.put("signal", signal);
            response.put("entryPrice", entryPrice);
            
            return ResponseEntity.ok(response);
            
        } catch (Exception e) {
            log.error("🚨 Error creating test trade: {}", e.getMessage(), e);
            return ResponseEntity.internalServerError()
                    .body(Map.of("error", "Failed to create test trade", "message", e.getMessage()));
        }
    }
    
    /**
     * Get system status
     * GET /api/v1/test/status
     */
    @GetMapping("/status")
    public ResponseEntity<Map<String, Object>> getSystemStatus() {
        try {
            Map<String, Object> status = new HashMap<>();
            status.put("status", "RUNNING");
            status.put("activeTrades", tradeStateManager.getActiveTradeCount());
            status.put("completedTrades", tradeHistoryService.getTradeCountStats());
            status.put("timestamp", java.time.LocalDateTime.now());
            
            return ResponseEntity.ok(status);
            
        } catch (Exception e) {
            log.error("🚨 Error getting system status: {}", e.getMessage(), e);
            return ResponseEntity.internalServerError()
                    .body(Map.of("error", "Failed to get system status"));
        }
    }
    
    /**
     * Test signal logging
     * POST /api/v1/test/log-signal
     */
    @PostMapping("/log-signal")
    public ResponseEntity<Map<String, Object>> logTestSignal(
            @RequestParam String scripCode,
            @RequestParam String signal,
            @RequestParam String strategy,
            @RequestParam(defaultValue = "test") String reason,
            @RequestParam(defaultValue = "true") boolean executed) {
        
        try {
            tradeHistoryService.logSignal(scripCode, signal, strategy, reason, executed);
            
            Map<String, Object> response = new HashMap<>();
            response.put("success", true);
            response.put("message", "Signal logged successfully");
            
            return ResponseEntity.ok(response);
            
        } catch (Exception e) {
            log.error("🚨 Error logging test signal: {}", e.getMessage(), e);
            return ResponseEntity.internalServerError()
                    .body(Map.of("error", "Failed to log signal"));
        }
    }
} 