package com.kotsin.execution.model;

import com.kotsin.execution.validation.ValidationResult;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for StrategySignal model validation
 */
class StrategySignalTest {

    @Test
    @DisplayName("Valid LONG signal should pass validation")
    void testValidLongSignal() {
        StrategySignal signal = StrategySignal.builder()
                .scripCode("N:D:49812")
                .signal("CONFIRMED_BOUNCE_LONG")
                .confidence(0.85)
                .direction("BULLISH")
                .longSignal(true)
                .entryPrice(4120.0)
                .stopLoss(4100.0)
                .target1(4150.0)
                .target2(4180.0)
                .riskRewardRatio(1.5)
                .vcpCombinedScore(0.78)
                .structuralBias(0.6)
                .ipuFinalScore(0.82)
                .timestamp(System.currentTimeMillis())
                .build();

        ValidationResult result = signal.validate();

        assertTrue(result.isValid(), "Valid signal should pass validation");
        assertEquals(0, result.getErrors().size());
    }

    @Test
    @DisplayName("LONG signal with SL > entry should fail validation")
    void testLongSignalInvertedStopLoss() {
        StrategySignal signal = StrategySignal.builder()
                .scripCode("N:D:49812")
                .direction("BULLISH")
                .longSignal(true)
                .entryPrice(4120.0)
                .stopLoss(4150.0)  // ❌ SL above entry for LONG!
                .target1(4180.0)
                .build();

        ValidationResult result = signal.validate();

        assertFalse(result.isValid());
        assertTrue(result.getErrors().stream()
                .anyMatch(e -> e.contains("LONG signal must have stopLoss < entryPrice")));
    }

    @Test
    @DisplayName("LONG signal with target < entry should fail validation")
    void testLongSignalInvertedTarget() {
        StrategySignal signal = StrategySignal.builder()
                .scripCode("N:D:49812")
                .direction("BULLISH")
                .longSignal(true)
                .entryPrice(4120.0)
                .stopLoss(4100.0)
                .target1(4110.0)  // ❌ Target below entry for LONG!
                .build();

        ValidationResult result = signal.validate();

        assertFalse(result.isValid());
        assertTrue(result.getErrors().stream()
                .anyMatch(e -> e.contains("LONG signal must have target1 > entryPrice")));
    }

    @ParameterizedTest
    @ValueSource(doubles = {-0.5, 1.5, 999.0, Double.NaN, Double.POSITIVE_INFINITY})
    @DisplayName("Confidence outside [0,1] should fail validation")
    void testInvalidConfidence(double confidence) {
        StrategySignal signal = StrategySignal.builder()
                .confidence(confidence)
                .build();

        ValidationResult result = signal.validate();

        assertFalse(result.isValid());
        assertTrue(result.getErrors().stream()
                .anyMatch(e -> e.contains("confidence")));
    }

    @ParameterizedTest
    @ValueSource(doubles = {-2.0, 2.0, 999.0, Double.NaN})
    @DisplayName("Structural bias outside [-1,1] should fail validation")
    void testInvalidStructuralBias(double bias) {
        StrategySignal signal = StrategySignal.builder()
                .structuralBias(bias)
                .build();

        ValidationResult result = signal.validate();

        assertFalse(result.isValid());
        assertTrue(result.getErrors().stream()
                .anyMatch(e -> e.contains("structuralBias")));
    }

    @Test
    @DisplayName("Future timestamp should fail validation")
    void testFutureTimestamp() {
        long futureTime = System.currentTimeMillis() + 86400_000 + 3600_000; // > 24h + 1h future buffer

        StrategySignal signal = StrategySignal.builder()
                .timestamp(futureTime)
                .build();

        ValidationResult result = signal.validate();

        assertFalse(result.isValid());
        assertTrue(result.getErrors().contains("timestamp cannot be in the future"));
    }

    @Test
    @DisplayName("Both longSignal and shortSignal true should fail validation")
    void testContradictorySignals() {
        StrategySignal signal = StrategySignal.builder()
                .longSignal(true)
                .shortSignal(true)  // ❌ Both directions!
                .build();

        ValidationResult result = signal.validate();

        assertFalse(result.isValid());
        assertTrue(result.getErrors().stream()
                .anyMatch(e -> e.contains("Cannot have both longSignal and shortSignal")));
    }

    @Test
    @DisplayName("parseScripCode should extract exchange and type correctly")
    void testParseScripCode() {
        StrategySignal signal = StrategySignal.builder()
                .scripCode("N:D:49812")
                .build();

        signal.parseScripCode();

        assertEquals("N", signal.getExchange());
        assertEquals("D", signal.getExchangeType());
        assertEquals("49812", signal.getNumericScripCode());
    }

    @Test
    @DisplayName("Invalid scripCode format should fail validation")
    void testInvalidScripCodeFormat() {
        StrategySignal signal = StrategySignal.builder()
                .scripCode("INVALID_FORMAT")
                .build();

        ValidationResult result = signal.validate();

        assertFalse(result.isValid());
        assertTrue(result.getErrors().stream()
                .anyMatch(e -> e.contains("scripCode must be in format 'Exchange:Type:Code'")));
    }

    @Test
    @DisplayName("Non-numeric scripCode should fail validation")
    void testNonNumericScripCode() {
        StrategySignal signal = StrategySignal.builder()
                .scripCode("N:D:ABC123")  // ❌ Contains letters!
                .build();

        ValidationResult result = signal.validate();

        assertFalse(result.isValid());
        assertTrue(result.getErrors().contains("scripCode numeric part must be a valid integer"));
    }

    @ParameterizedTest
    @CsvSource({
        "0.0, false",      // Zero risk-reward is invalid
        "-1.5, false",     // Negative is invalid
        "1.5, true",       // Positive is valid
        "NaN, false",      // NaN is invalid
        "Infinity, false"  // Infinity is invalid
    })
    @DisplayName("Risk-reward ratio validation")
    void testRiskRewardRatio(String rrStr, boolean expectedValid) {
        double rr = "NaN".equals(rrStr) ? Double.NaN :
                    "Infinity".equals(rrStr) ? Double.POSITIVE_INFINITY :
                    Double.parseDouble(rrStr);

        StrategySignal signal = StrategySignal.builder()
                .riskRewardRatio(rr)
                .build();

        ValidationResult result = signal.validate();

        assertEquals(expectedValid, result.isValid() || !result.getErrors().stream()
                .anyMatch(e -> e.contains("riskRewardRatio")));
    }
}
