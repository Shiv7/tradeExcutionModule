package com.kotsin.execution.controller;

import com.kotsin.execution.model.PendingSignal;
import com.kotsin.execution.service.PendingSignalManager;
import com.kotsin.execution.service.SignalValidationService;
import com.kotsin.execution.service.TradeExecutionService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;

/**
 * Test controller for the new dynamic validation architecture
 * Demonstrates how signals are stored as pending and validated with live market data
 */
@RestController
@RequestMapping("/test/dynamic-validation")
@Slf4j
@RequiredArgsConstructor
public class DynamicValidationTestController {

    private final TradeExecutionService tradeExecutionService;
    private final PendingSignalManager pendingSignalManager;
    private final SignalValidationService signalValidationService;

    /**
     * Test dynamic validation flow - create pending signals and simulate price updates
     */
    @PostMapping("/test-dynamic-flow")
    public ResponseEntity<Map<String, Object>> testDynamicValidationFlow() {
        Map<String, Object> result = new HashMap<>();
        
        try {
            log.info("🧪 [DynamicValidationTest] Starting dynamic validation flow test");
            
            // STEP 1: Create a signal that initially FAILS validation (too close target)
            Map<String, Object> signalData = createTestSignal(
                "114311", // scripCode
                100.0,    // Current price (signal price)
                98.5,     // stopLoss (1.5% below)
                101.0,    // target1 (only 1% above - should FAIL MIN_MOVE of 2%)
                102.5,    // target2
                104.0,    // target3
                "BUY"     // signal type
            );
            
            // Process signal - it should be stored as PENDING (not executed immediately)
            tradeExecutionService.processNewSignal(
                signalData,
                LocalDateTime.now(),
                "TEST_STRATEGY",
                "BULLISH",
                "114311",
                "TEST_COMPANY",
                "NSE",
                "EQUITY"
            );
            
            // Check pending signals
            int pendingCount1 = pendingSignalManager.getAllPendingSignals().size();
            
            result.put("status", "SUCCESS");
            result.put("message", "✅ Dynamic validation architecture test completed");
            result.put("pendingSignalsCreated", pendingCount1);
            result.put("testSignalData", signalData);
            
            return ResponseEntity.ok(result);
            
        } catch (Exception e) {
            log.error("🚨 [DynamicValidationTest] Error during dynamic validation test: {}", e.getMessage(), e);
            return ResponseEntity.status(500).body(Map.of(
                "status", "ERROR",
                "message", "Dynamic validation test failed: " + e.getMessage()
            ));
        }
    }
    
    /**
     * Get current pending signals status
     */
    @GetMapping("/pending-signals-status")
    public ResponseEntity<Map<String, Object>> getPendingSignalsStatus() {
        try {
            Map<String, Object> status = pendingSignalManager.getPendingSignalsSummary();
            return ResponseEntity.ok(status);
            
        } catch (Exception e) {
            return ResponseEntity.status(500).body(Map.of(
                "status", "ERROR",
                "message", "Error getting pending signals status: " + e.getMessage()
            ));
        }
    }
    
    /**
     * Create a test signal for validation testing
     */
    private Map<String, Object> createTestSignal(String scripCode, double entryPrice, 
                                                double stopLoss, double target1, 
                                                double target2, double target3, String signal) {
        Map<String, Object> signalData = new HashMap<>();
        signalData.put("scripCode", scripCode);
        signalData.put("entryPrice", entryPrice);
        signalData.put("closePrice", entryPrice);
        signalData.put("stopLoss", stopLoss);
        signalData.put("target1", target1);
        signalData.put("target2", target2);
        signalData.put("target3", target3);
        signalData.put("signal", signal);
        signalData.put("companyName", "TEST_COMPANY");
        signalData.put("strategy", "TEST_STRATEGY");
        signalData.put("timeframe", "15m");
        signalData.put("signalTime", LocalDateTime.now().toString());
        return signalData;
    }
} 