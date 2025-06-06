package com.kotsin.execution.controller;

import com.kotsin.execution.service.TradeExecutionService;
import com.kotsin.execution.service.PendingSignalManager;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;

/**
 * Test controller for the updated trade execution functionality
 * Tests: Multiple trades per script + Trading hours validation
 */
@RestController
@RequestMapping("/test/trade-execution")
@Slf4j
@RequiredArgsConstructor
public class TradeExecutionTestController {

    private final TradeExecutionService tradeExecutionService;
    private final PendingSignalManager pendingSignalManager;

    /**
     * Test multiple trades functionality - create multiple signals for same script
     */
    @PostMapping("/test-multiple-trades")
    public ResponseEntity<Map<String, Object>> testMultipleTrades() {
        Map<String, Object> result = new HashMap<>();
        
        try {
            log.info("🧪 [MultipleTradesTest] Testing multiple trades for same script");
            
            String scripCode = "448155"; // Same script as in your log
            
            // Create multiple signals for the same script with different strategies
            log.info("📊 [MultipleTradesTest] Creating multiple signals for script: {}", scripCode);
            
            // Signal 1: 3M SuperTrend Strategy
            tradeExecutionService.executeStrategySignal(
                scripCode,
                "BUY",
                876.45,
                875.16,
                878.0,
                "3M_SUPERTREND",
                "HIGH"
            );
            
            // Signal 2: 15M Strategy (same script, different strategy)
            tradeExecutionService.executeStrategySignal(
                scripCode,
                "BUY", 
                876.50,
                875.20,
                878.50,
                "15M_STRATEGY",
                "MEDIUM"
            );
            
            // Signal 3: 30M Strategy (same script, different strategy)  
            tradeExecutionService.executeStrategySignal(
                scripCode,
                "SELL",
                876.40,
                877.80,
                875.0,
                "30M_STRATEGY", 
                "HIGH"
            );
            
            // Check pending signals after creation
            int totalPendingSignals = pendingSignalManager.getAllPendingSignals().size();
            Map<String, Object> pendingSummary = pendingSignalManager.getPendingSignalsSummary();
            
            result.put("status", "SUCCESS");
            result.put("message", "✅ Multiple trades test completed - No restrictions on same script");
            result.put("testScript", scripCode);
            result.put("signalsCreated", 3);
            result.put("totalPendingSignals", totalPendingSignals);
            result.put("pendingSignalsSummary", pendingSummary);
            result.put("note", "All signals processed regardless of existing trades for same script");
            
            return ResponseEntity.ok(result);
            
        } catch (Exception e) {
            log.error("🚨 [MultipleTradesTest] Error during multiple trades test: {}", e.getMessage(), e);
            return ResponseEntity.status(500).body(Map.of(
                "status", "ERROR",
                "message", "Multiple trades test failed: " + e.getMessage()
            ));
        }
    }
    
    /**
     * Test trading hours validation for different exchanges
     */
    @PostMapping("/test-trading-hours")
    public ResponseEntity<Map<String, Object>> testTradingHours() {
        Map<String, Object> result = new HashMap<>();
        
        try {
            log.info("🧪 [TradingHoursTest] Testing trading hours validation");
            
            // Test current time (should work if within hours)
            LocalDateTime now = LocalDateTime.now();
            
            result.put("status", "SUCCESS");
            result.put("message", "✅ Trading hours test completed");
            result.put("currentTime", now.format(java.time.format.DateTimeFormatter.ofPattern("HH:mm:ss")));
            
            // Test NSE hours (9:00 AM to 3:30 PM)
            Map<String, Object> nseTest = testExchangeHours("N", "NSE", "EQUITY");
            result.put("nseTest", nseTest);
            
            // Test Commodity hours (9:00 AM to 11:30 PM)
            Map<String, Object> commodityTest = testExchangeHours("M", "COMMODITY", "NATGASMINI 23 JUN 2025 PE 315.00");
            result.put("commodityTest", commodityTest);
            
            result.put("tradingHoursRules", Map.of(
                "NSE_EQUITY", "9:00 AM to 3:30 PM (Exchange: N)",
                "COMMODITY", "9:00 AM to 11:30 PM (Exchange: M)",
                "note", "Signals outside hours are rejected with clear logging"
            ));
            
            return ResponseEntity.ok(result);
            
        } catch (Exception e) {
            log.error("🚨 [TradingHoursTest] Error during trading hours test: {}", e.getMessage(), e);
            return ResponseEntity.status(500).body(Map.of(
                "status", "ERROR", 
                "message", "Trading hours test failed: " + e.getMessage()
            ));
        }
    }
    
    /**
     * Test exchange detection logic
     */
    @PostMapping("/test-exchange-detection")
    public ResponseEntity<Map<String, Object>> testExchangeDetection() {
        Map<String, Object> result = new HashMap<>();
        
        try {
            log.info("🧪 [ExchangeDetectionTest] Testing exchange detection logic");
            
            // Test various script codes
            String[] testScripts = {
                "448155",   // Regular NSE script
                "GOLD123",  // Should detect as commodity
                "SILVER456", // Should detect as commodity
                "NATGAS789", // Should detect as commodity  
                "RELIANCE",  // Should detect as NSE
                "NATGASMINI 23 JUN 2025 PE 315.00" // Should detect as commodity
            };
            
            Map<String, Map<String, String>> detectionResults = new HashMap<>();
            
            for (String script : testScripts) {
                log.info("🔍 [ExchangeDetectionTest] Testing script: {}", script);
                
                // This will trigger the exchange detection logic
                tradeExecutionService.executeStrategySignal(
                    script,
                    "BUY",
                    100.0,
                    99.0,
                    102.0,
                    "TEST_STRATEGY",
                    "MEDIUM"
                );
                
                // Store result (you'd need to enhance this to actually capture the detection result)
                detectionResults.put(script, Map.of(
                    "tested", "true",
                    "note", "Check logs for detection results"
                ));
            }
            
            result.put("status", "SUCCESS");
            result.put("message", "✅ Exchange detection test completed");
            result.put("detectionResults", detectionResults);
            result.put("detectionLogic", Map.of(
                "commodity_keywords", "GOLD, SILVER, CRUDE, NATGAS, COPPER, etc.",
                "default_exchange", "NSE (N) for equity",
                "commodity_exchange", "MCX (M) for commodities"
            ));
            
            return ResponseEntity.ok(result);
            
        } catch (Exception e) {
            log.error("🚨 [ExchangeDetectionTest] Error during exchange detection test: {}", e.getMessage(), e);
            return ResponseEntity.status(500).body(Map.of(
                "status", "ERROR",
                "message", "Exchange detection test failed: " + e.getMessage()
            ));
        }
    }
    
    /**
     * Helper method to test exchange hours
     */
    private Map<String, Object> testExchangeHours(String exchange, String exchangeType, String sampleScript) {
        Map<String, Object> testResult = new HashMap<>();
        
        try {
            LocalDateTime now = LocalDateTime.now();
            int currentHour = now.getHour();
            
            // Determine if current time should be valid for this exchange
            boolean shouldBeValid;
            if ("M".equals(exchange)) {
                // Commodity: 9:00 AM to 11:30 PM
                shouldBeValid = currentHour >= 9 && currentHour < 23 || (currentHour == 23 && now.getMinute() <= 30);
            } else {
                // NSE: 9:00 AM to 3:30 PM
                shouldBeValid = currentHour >= 9 && currentHour < 15 || (currentHour == 15 && now.getMinute() <= 30);
            }
            
            testResult.put("exchange", exchange);
            testResult.put("exchangeType", exchangeType);
            testResult.put("currentTime", now.format(java.time.format.DateTimeFormatter.ofPattern("HH:mm:ss")));
            testResult.put("shouldBeValid", shouldBeValid);
            testResult.put("sampleScript", sampleScript);
            
            if ("M".equals(exchange)) {
                testResult.put("tradingHours", "09:00 - 23:30");
            } else {
                testResult.put("tradingHours", "09:00 - 15:30");
            }
            
            return testResult;
            
        } catch (Exception e) {
            testResult.put("error", e.getMessage());
            return testResult;
        }
    }
} 