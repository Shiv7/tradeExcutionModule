package com.kotsin.execution.controller;

import com.kotsin.execution.service.SignalValidationService;
import com.kotsin.execution.service.SignalValidationService.SignalValidationResult;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.HashMap;
import java.util.Map;

/**
 * Test controller for validating the new kotsinBackTestBE validation logic
 * in the Trade Execution Module
 */
@RestController
@RequestMapping("/test/validation")
@Slf4j
@RequiredArgsConstructor
public class ValidationTestController {

    private final SignalValidationService signalValidationService;

    /**
     * Test kotsinBackTestBE validation logic with various scenarios
     */
    @PostMapping("/test-kotsin-validation")
    public ResponseEntity<Map<String, Object>> testKotsinValidation() {
        Map<String, Object> result = new HashMap<>();
        
        try {
            log.info("🧪 [ValidationTest] Starting kotsinBackTestBE validation tests");
            
            // Test Case 1: Valid signal that should PASS all validations
            Map<String, Object> validSignal = createTestSignal(
                7.9,    // entryPrice
                7.4,    // stopLoss (6.3% below entry)
                8.2,    // target1 (3.8% above entry) 
                8.5,    // target2
                8.8,    // target3
                "BUY"   // signal type
            );
            
            SignalValidationResult validResult = signalValidationService.validateSignal(validSignal);
            
            // Test Case 2: Signal with stop loss too far (should FAIL)
            Map<String, Object> stopTooFarSignal = createTestSignal(
                7.9,    // entryPrice
                7.7,    // stopLoss (only 2.5% below - should fail MAX_STOP_DISTANCE of 2%)
                8.2,    // target1
                8.5,    // target2  
                8.8,    // target3
                "BUY"
            );
            
            SignalValidationResult stopTooFarResult = signalValidationService.validateSignal(stopTooFarSignal);
            
            // Test Case 3: Signal with target too close (should FAIL)
            Map<String, Object> targetTooCloseSignal = createTestSignal(
                7.9,    // entryPrice
                7.7,    // stopLoss
                8.0,    // target1 (only 1.3% above - should fail MIN_MOVE of 2%)
                8.2,    // target2
                8.5,    // target3
                "BUY"
            );
            
            SignalValidationResult targetTooCloseResult = signalValidationService.validateSignal(targetTooCloseSignal);
            
            // Test Case 4: Signal with poor risk-reward (should FAIL)
            Map<String, Object> poorRRSignal = createTestSignal(
                7.9,    // entryPrice
                7.7,    // stopLoss (2.5% risk)
                8.0,    // target1 (1.3% reward) - RR = 1.3/2.5 = 0.52 (should fail MIN_RR of 1.5)
                8.2,    // target2
                8.5,    // target3
                "BUY"
            );
            
            SignalValidationResult poorRRResult = signalValidationService.validateSignal(poorRRSignal);
            
            // Test Case 5: Bearish signal that should PASS
            Map<String, Object> validBearishSignal = createTestSignal(
                7.9,    // entryPrice
                8.1,    // stopLoss (2.5% above entry)
                7.6,    // target1 (3.8% below entry)
                7.3,    // target2
                7.0,    // target3
                "SELL"
            );
            
            SignalValidationResult validBearishResult = signalValidationService.validateSignal(validBearishSignal);
            
            // Compile results
            result.put("status", "SUCCESS");
            result.put("message", "✅ kotsinBackTestBE validation testing completed");
            
            result.put("testCase1_validBullish", Map.of(
                "description", "Valid bullish signal (should PASS)",
                "signal", validSignal,
                "result", Map.of(
                    "approved", validResult.isApproved(),
                    "reason", validResult.getReason(),
                    "riskReward", validResult.getRiskReward()
                )
            ));
            
            result.put("testCase2_stopTooFar", Map.of(
                "description", "Stop loss too far (should FAIL)",
                "signal", stopTooFarSignal,
                "result", Map.of(
                    "approved", stopTooFarResult.isApproved(),
                    "reason", stopTooFarResult.getReason()
                )
            ));
            
            result.put("testCase3_targetTooClose", Map.of(
                "description", "Target too close (should FAIL)",
                "signal", targetTooCloseSignal,
                "result", Map.of(
                    "approved", targetTooCloseResult.isApproved(),
                    "reason", targetTooCloseResult.getReason()
                )
            ));
            
            result.put("testCase4_poorRiskReward", Map.of(
                "description", "Poor risk-reward ratio (should FAIL)",
                "signal", poorRRSignal,
                "result", Map.of(
                    "approved", poorRRResult.isApproved(),
                    "reason", poorRRResult.getReason()
                )
            ));
            
            result.put("testCase5_validBearish", Map.of(
                "description", "Valid bearish signal (should PASS)",
                "signal", validBearishSignal,
                "result", Map.of(
                    "approved", validBearishResult.isApproved(),
                    "reason", validBearishResult.getReason(),
                    "riskReward", validBearishResult.getRiskReward()
                )
            ));
            
            result.put("validation_rules", Map.of(
                "MIN_MOVE", "Targets must be >= 2% away from entry",
                "MAX_STOP_DISTANCE", "Stop loss can't be more than 2% away",
                "MIN_RR_REQUIRED", "Must meet >= 1.5:1 risk-reward ratio"
            ));
            
            result.put("architecture_summary", Map.of(
                "strategy_module", "Calculates pivot-based targets using daily/weekly pivots",
                "trade_execution_module", "Validates signals using kotsinBackTestBE logic and executes trades",
                "separation_complete", "✅ Strategy triggers, Trade Execution validates & executes"
            ));
            
            // Summary
            long passedTests = result.entrySet().stream()
                .filter(entry -> entry.getKey().startsWith("testCase"))
                .mapToLong(entry -> {
                    @SuppressWarnings("unchecked")
                    Map<String, Object> testCase = (Map<String, Object>) entry.getValue();
                    @SuppressWarnings("unchecked")
                    Map<String, Object> testResult = (Map<String, Object>) testCase.get("result");
                    return (Boolean) testResult.get("approved") ? 1L : 0L;
                })
                .sum();
            
            result.put("test_summary", Map.of(
                "total_tests", 5,
                "passed_tests", passedTests,
                "failed_tests", 5 - passedTests,
                "expected_passes", 2, // Case 1 and 5 should pass
                "expected_failures", 3 // Cases 2, 3, 4 should fail
            ));
            
            return ResponseEntity.ok(result);
            
        } catch (Exception e) {
            log.error("🚨 [ValidationTest] Error during validation testing: {}", e.getMessage(), e);
            return ResponseEntity.status(500).body(Map.of(
                "status", "ERROR",
                "message", "Validation test failed: " + e.getMessage()
            ));
        }
    }
    
    /**
     * Create a test signal with specified parameters
     */
    private Map<String, Object> createTestSignal(double entryPrice, double stopLoss, 
                                                double target1, double target2, double target3, 
                                                String signal) {
        Map<String, Object> signalData = new HashMap<>();
        signalData.put("entryPrice", entryPrice);
        signalData.put("stopLoss", stopLoss);
        signalData.put("target1", target1);
        signalData.put("target2", target2);
        signalData.put("target3", target3);
        signalData.put("signal", signal);
        signalData.put("scripCode", "114311");
        signalData.put("companyName", "TEST_SIGNAL");
        signalData.put("strategy", "TEST_STRATEGY");
        signalData.put("timeframe", "15m");
        return signalData;
    }
    
    /**
     * Test individual validation rules
     */
    @PostMapping("/test-individual-rules")
    public ResponseEntity<Map<String, Object>> testIndividualRules() {
        Map<String, Object> result = new HashMap<>();
        
        try {
            // Test boundary conditions
            Map<String, Object> boundaryTests = new HashMap<>();
            
            // Test exactly at 2% move (should pass)
            Map<String, Object> exactlyTwoPercentSignal = createTestSignal(
                10.0,   // entryPrice
                9.8,    // stopLoss (2% below)
                10.2,   // target1 (exactly 2% above)
                10.4,   // target2
                10.6,   // target3
                "BUY"
            );
            
            SignalValidationResult exactTwoPercentResult = signalValidationService.validateSignal(exactlyTwoPercentSignal);
            
            boundaryTests.put("exactly_2_percent_move", Map.of(
                "description", "Target exactly 2% away (boundary test)",
                "approved", exactTwoPercentResult.isApproved(),
                "reason", exactTwoPercentResult.getReason(),
                "riskReward", exactTwoPercentResult.getRiskReward()
            ));
            
            // Test exactly at 1.5 RR (should pass)
            Map<String, Object> exactlyOnePointFiveRRSignal = createTestSignal(
                10.0,   // entryPrice
                9.5,    // stopLoss (5% below, risk = 0.5)
                10.75,  // target1 (7.5% above, reward = 0.75, RR = 0.75/0.5 = 1.5)
                11.0,   // target2
                11.25,  // target3
                "BUY"
            );
            
            SignalValidationResult exactRRResult = signalValidationService.validateSignal(exactlyOnePointFiveRRSignal);
            
            boundaryTests.put("exactly_1_5_risk_reward", Map.of(
                "description", "Risk-reward exactly 1.5 (boundary test)",
                "approved", exactRRResult.isApproved(),
                "reason", exactRRResult.getReason(),
                "riskReward", exactRRResult.getRiskReward()
            ));
            
            result.put("status", "SUCCESS");
            result.put("boundary_tests", boundaryTests);
            result.put("validation_constants", Map.of(
                "MIN_MOVE", 0.02,
                "MAX_STOP_DISTANCE", 0.02,
                "MIN_RR_REQUIRED", 1.5
            ));
            
            return ResponseEntity.ok(result);
            
        } catch (Exception e) {
            log.error("🚨 [ValidationTest] Error during boundary testing: {}", e.getMessage(), e);
            return ResponseEntity.status(500).body(Map.of(
                "status", "ERROR",
                "message", "Boundary test failed: " + e.getMessage()
            ));
        }
    }
} 