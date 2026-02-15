package com.kotsin.execution.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.time.Instant;
import java.util.Optional;

/**
 * Reads ML predictions from Redis (written by Python fastAnalayticsKotsin service).
 *
 * Redis key pattern: ml:prediction:{scripCode}:{interval}
 *
 * Used by TradeManager to add ML quality gate to pivot retest strategy.
 */
@Service
@Slf4j
@RequiredArgsConstructor
public class MLPredictionService {

    private final RedisTemplate<String, String> executionStringRedisTemplate;
    private final ObjectMapper objectMapper = new ObjectMapper();

    private static final Duration MAX_PREDICTION_AGE = Duration.ofMinutes(5);

    /**
     * Get ML prediction for a scrip at a given interval.
     *
     * @param scripCode numeric scrip code (e.g., "1333")
     * @param interval  candle interval (e.g., "15m", "30m")
     * @return MLPrediction if available and fresh, empty otherwise
     */
    public Optional<MLPrediction> getPrediction(String scripCode, String interval) {
        try {
            String key = "ml:prediction:" + scripCode + ":" + interval;
            String json = executionStringRedisTemplate.opsForValue().get(key);

            if (json == null || json.isBlank()) {
                log.debug("ml_prediction_miss scrip={} interval={}", scripCode, interval);
                return Optional.empty();
            }

            JsonNode node = objectMapper.readTree(json);

            // Check freshness
            String timestamp = node.path("timestamp").asText("");
            if (!timestamp.isEmpty()) {
                try {
                    Instant predTime = Instant.parse(timestamp);
                    if (Duration.between(predTime, Instant.now()).compareTo(MAX_PREDICTION_AGE) > 0) {
                        log.debug("ml_prediction_stale scrip={} interval={} age>{}m",
                                scripCode, interval, MAX_PREDICTION_AGE.toMinutes());
                        return Optional.empty();
                    }
                } catch (Exception ignored) {
                    // Timestamp format may be ISO local — still usable
                }
            }

            MLPrediction prediction = MLPrediction.builder()
                    .prediction(node.path("prediction").asText("HOLD"))
                    .confidence(node.path("confidence").asDouble(0.5))
                    .regime(node.path("regime").asText("NEUTRAL_RANGE"))
                    .regimeScore(node.path("regimeScore").asDouble(0.0))
                    .regimeConviction(node.path("regimeConviction").asText("LOW"))
                    .betSignal(node.path("betSignal").asDouble(0.0))
                    .positionSizeMultiplier(node.path("positionSizeMultiplier").asDouble(0.0))
                    .vpinToxicity(node.path("vpinToxicity").asDouble(0.0))
                    .orderFlowImbalance(node.path("orderFlowImbalance").asDouble(0.0))
                    .rsi(node.path("rsi").asDouble(50.0))
                    .atr(node.path("atr").asDouble(0.0))
                    .model(node.path("model").asText("unknown"))
                    .build();

            log.debug("ml_prediction_hit scrip={} interval={} pred={} conf={} regime={}",
                    scripCode, interval, prediction.prediction, prediction.confidence, prediction.regime);

            return Optional.of(prediction);

        } catch (Exception e) {
            log.warn("ml_prediction_error scrip={} interval={} err={}", scripCode, interval, e.getMessage());
            return Optional.empty();
        }
    }

    /**
     * Get the best available ML prediction across multiple intervals.
     * Prefers 15m, then 5m, then 30m.
     */
    public Optional<MLPrediction> getBestPrediction(String scripCode) {
        String[] preferredIntervals = {"15m", "5m", "30m"};
        for (String interval : preferredIntervals) {
            Optional<MLPrediction> pred = getPrediction(scripCode, interval);
            if (pred.isPresent()) {
                return pred;
            }
        }
        return Optional.empty();
    }

    /**
     * Check if ML prediction supports the trade direction.
     *
     * @param prediction ML prediction
     * @param isBullish  true if trade is long, false if short
     * @return true if ML agrees with trade direction or is neutral with high confidence
     */
    public boolean isDirectionAligned(MLPrediction prediction, boolean isBullish) {
        if (isBullish) {
            return "BUY".equals(prediction.prediction);
        } else {
            return "SELL".equals(prediction.prediction);
        }
    }

    /**
     * Check if VPIN indicates toxic flow (adverse selection risk).
     * VPIN > 0.7 suggests informed traders are active — higher risk.
     */
    public boolean isFlowToxic(MLPrediction prediction) {
        return prediction.vpinToxicity > 0.7;
    }

    /**
     * Check if regime is adverse for new entries.
     * Crash/extreme regimes signal caution.
     */
    public boolean isRegimeAdverse(MLPrediction prediction) {
        String regime = prediction.regime;
        return "STRONG_BEARISH".equals(regime) || "STRONG_BULLISH".equals(regime);
    }

    /**
     * ML Prediction data read from Redis.
     */
    @lombok.Data
    @lombok.Builder
    public static class MLPrediction {
        private String prediction;        // BUY, SELL, HOLD
        private double confidence;        // 0.0 - 1.0
        private String regime;            // STRONG_BULLISH, MODERATE_BULLISH, NEUTRAL_RANGE, etc.
        private double regimeScore;       // -1.0 to +1.0
        private String regimeConviction;  // HIGH, MEDIUM, LOW
        private double betSignal;         // -1.0 to +1.0 (Kelly-based)
        private double positionSizeMultiplier; // 0.0 to 0.20
        private double vpinToxicity;      // 0.0 to 1.0
        private double orderFlowImbalance; // -1.0 to +1.0
        private double rsi;
        private double atr;
        private String model;
    }
}
