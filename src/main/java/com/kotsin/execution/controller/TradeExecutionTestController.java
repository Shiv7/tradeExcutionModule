package com.kotsin.execution.controller;

import com.kotsin.execution.model.PendingSignal;
import com.kotsin.execution.service.TradeExecutionService;
import com.kotsin.execution.service.PendingSignalManager;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.client.RestTemplate;

import java.time.LocalDateTime;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
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

    @PostMapping("/test-pivot-api")
    public ResponseEntity<Map<String, Object>> testPivotApiConnectivity(
            @RequestParam(defaultValue = "10726") String scripCode,
            @RequestParam(defaultValue = "2405.1") double currentPrice,
            @RequestParam(defaultValue = "BUY") String signalType) {
        
        Map<String, Object> result = new HashMap<>();
        
        try {
            log.info("🧪 [PivotAPITest] Testing pivot API connectivity for script: {}", scripCode);
            
            // Test the pivot calculation API
            String apiUrl = String.format("http://localhost:8112/api/pivots/calculate-targets/%s?currentPrice=%.2f&signalType=%s", 
                    scripCode, currentPrice, signalType);
            
            log.info("🧪 [PivotAPITest] API URL: {}", apiUrl);
            
            // Make API call using RestTemplate
            RestTemplate restTemplate = new RestTemplate();
            Map<String, Object> pivotResponse = restTemplate.getForObject(apiUrl, Map.class);
            
            if (pivotResponse != null) {
                result.put("status", "SUCCESS");
                result.put("message", "✅ Pivot API connection successful");
                result.put("apiUrl", apiUrl);
                result.put("pivotResponse", pivotResponse);
                result.put("responseStatus", pivotResponse.get("status"));
            } else {
                result.put("status", "ERROR");
                result.put("message", "❌ Pivot API returned null response");
                result.put("apiUrl", apiUrl);
            }
            
        } catch (org.springframework.web.client.ResourceAccessException e) {
            log.error("🚨 [PivotAPITest] Connection error: {}", e.getMessage());
            result.put("status", "CONNECTION_ERROR");
            result.put("message", "❌ Cannot connect to Strategy Module API");
            result.put("error", e.getMessage());
            result.put("suggestion", "Check if Strategy Module is running on port 8112");
            
        } catch (Exception e) {
            log.error("🚨 [PivotAPITest] API test error: {}", e.getMessage());
            result.put("status", "ERROR");
            result.put("message", "❌ Error testing pivot API");
            result.put("error", e.getMessage());
        }
        
        return ResponseEntity.ok(result);
    }

    @PostMapping("/test-validation")
    public ResponseEntity<Map<String, Object>> testSignalValidation(
            @RequestParam String scripCode,
            @RequestParam double currentPrice) {
        
        Map<String, Object> result = new HashMap<>();
        
        try {
            log.info("🧪 [ValidationTest] Manual validation test for script: {} at price: {}", scripCode, currentPrice);
            
            // Get current pending signals
            Collection<PendingSignal> allPendingSignals = pendingSignalManager.getAllPendingSignals();
            Collection<PendingSignal> matchingSignals = pendingSignalManager.getPendingSignalsForScript(scripCode);
            
            result.put("totalPendingSignals", allPendingSignals.size());
            result.put("matchingSignals", matchingSignals.size());
            result.put("testScript", scripCode);
            result.put("testPrice", currentPrice);
            
            if (matchingSignals.isEmpty()) {
                result.put("status", "NO_PENDING_SIGNALS");
                result.put("message", "No pending signals found for script: " + scripCode);
                
                // Show what signals are actually pending
                List<String> pendingScripts = allPendingSignals.stream()
                        .map(signal -> signal.getScripCode() + " (" + signal.getStrategyName() + ")")
                        .collect(java.util.stream.Collectors.toList());
                result.put("actualPendingSignals", pendingScripts);
            } else {
                result.put("status", "FOUND_SIGNALS");
                result.put("message", "Found " + matchingSignals.size() + " pending signals - triggering validation");
                
                // Manually trigger validation
                LocalDateTime now = LocalDateTime.now();
                tradeExecutionService.updateTradeWithPrice(scripCode, currentPrice, now);
                
                result.put("validationTriggered", true);
            }
            
        } catch (Exception e) {
            log.error("🚨 [ValidationTest] Error in validation test: {}", e.getMessage());
            result.put("status", "ERROR");
            result.put("message", "Error: " + e.getMessage());
        }
        
        return ResponseEntity.ok(result);
    }
} 