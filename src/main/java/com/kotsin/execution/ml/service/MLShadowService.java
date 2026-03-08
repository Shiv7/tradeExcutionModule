package com.kotsin.execution.ml.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * ML Shadow Service - Logs ML recommendations alongside rule-based decisions.
 *
 * SHADOW MODE: ML decisions are logged to ml_shadow_log collection but NEVER
 * affect actual trading. This allows comparison of rule-based vs ML performance.
 *
 * Called from signal processing pipeline (SignalBufferService) when ml.shadow.enabled=true.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class MLShadowService {

    private static final ZoneId IST = ZoneId.of("Asia/Kolkata");

    private final BayesianSizer bayesianSizer;
    private final MongoTemplate mongoTemplate;
    private final RestTemplate restTemplate;

    @Value("${ml.shadow.enabled:false}")
    private boolean shadowEnabled;

    @Value("${ml.fastanalytics.url:http://localhost:8002}")
    private String fastAnalyticsUrl;

    /**
     * Evaluate a signal through ML pipeline and log recommendation.
     * Called from SignalBufferService AFTER the rule-based decision is made.
     *
     * @param signalId signal identifier
     * @param strategy strategy name (FUDKII, FUKAA, etc.)
     * @param scripCode scrip code
     * @param direction BULLISH/BEARISH
     * @param entryPrice signal entry price
     * @param stopLoss signal SL
     * @param target1 signal target
     * @param ruleBasedDecision what the rule-based system decided (TRADE/SKIP)
     */
    public void evaluateAndLog(String signalId, String strategy, String scripCode,
                                String direction, double entryPrice, double stopLoss,
                                double target1, String ruleBasedDecision) {
        if (!shadowEnabled) return;

        try {
            // 1. Get Bayesian sizing recommendation
            String regime = getCurrentRegime(scripCode);
            double avgWinLossRatio = 1.5; // Default, will be computed from history later
            BayesianSizer.SizingRecommendation sizing = bayesianSizer.getRecommendation(
                    strategy, regime, avgWinLossRatio);

            // 2. Get quality prediction from FastAnalytics (if available)
            Map<String, Object> qualityPrediction = getQualityPrediction(scripCode);

            // 3. Build shadow log entry
            Map<String, Object> shadowLog = new LinkedHashMap<>();
            shadowLog.put("signalId", signalId);
            shadowLog.put("strategy", strategy);
            shadowLog.put("scripCode", scripCode);
            shadowLog.put("direction", direction);
            shadowLog.put("entryPrice", entryPrice);
            shadowLog.put("stopLoss", stopLoss);
            shadowLog.put("target1", target1);
            shadowLog.put("timestamp", LocalDateTime.now(IST));

            // Rule-based decision
            shadowLog.put("ruleBasedDecision", ruleBasedDecision);

            // ML decision
            Map<String, Object> mlDecision = new LinkedHashMap<>();
            mlDecision.put("shouldTrade", sizing.shouldTrade());
            mlDecision.put("recommendedSize", sizing.recommendedSize());
            mlDecision.put("sampledProbability", sizing.sampledProbability());
            mlDecision.put("expectedWinRate", sizing.expectedWinRate());
            mlDecision.put("totalSamples", sizing.totalSamples());
            mlDecision.put("regime", regime);
            shadowLog.put("mlDecision", mlDecision);

            // Quality prediction
            if (qualityPrediction != null) {
                shadowLog.put("qualityPrediction", qualityPrediction);
            }

            // Agreement check
            boolean mlWouldTrade = sizing.shouldTrade();
            boolean ruleTraded = "TRADE".equals(ruleBasedDecision);
            shadowLog.put("agreement", mlWouldTrade == ruleTraded);
            shadowLog.put("disagreementType",
                    mlWouldTrade == ruleTraded ? "NONE" :
                            (mlWouldTrade ? "ML_TRADE_RULE_SKIP" : "ML_SKIP_RULE_TRADE"));

            // Save to MongoDB
            mongoTemplate.save(shadowLog, "ml_shadow_log");

            log.info("ml_shadow signal={} strategy={} rule={} ml={} agree={} regime={} quality={}",
                    signalId, strategy, ruleBasedDecision,
                    mlWouldTrade ? "TRADE" : "SKIP",
                    mlWouldTrade == ruleTraded,
                    regime,
                    qualityPrediction != null ? qualityPrediction.get("quality") : "N/A");

        } catch (Exception e) {
            log.warn("ml_shadow_error signal={}: {}", signalId, e.getMessage());
        }
    }

    /**
     * Record actual trade outcome for Bayesian update.
     * Called when a trade completes (from VirtualEngineService or StrategyTradeExecutor).
     */
    public void recordOutcome(String strategy, String scripCode, boolean isWin, String exitReason) {
        if (!shadowEnabled) return;

        try {
            String regime = getCurrentRegime(scripCode);
            bayesianSizer.recordOutcome(strategy, regime, isWin);

            // Update shadow log with actual outcome
            Map<String, Object> outcome = new LinkedHashMap<>();
            outcome.put("isWin", isWin);
            outcome.put("exitReason", exitReason);
            // Find and update most recent shadow log for this scrip
            // (async, non-blocking — outcome is also in trade_outcomes collection)

            log.info("ml_outcome strategy={} scrip={} win={} exit={} regime={}",
                    strategy, scripCode, isWin, exitReason, regime);
        } catch (Exception e) {
            log.warn("ml_outcome_error: {}", e.getMessage());
        }
    }

    /**
     * Get current regime from FastAnalytics ML endpoint.
     */
    private String getCurrentRegime(String scripCode) {
        try {
            @SuppressWarnings("unchecked")
            Map<String, Object> response = restTemplate.getForObject(
                    fastAnalyticsUrl + "/api/ml/regime/info", Map.class);
            if (response != null && response.containsKey("regime")) {
                return (String) response.get("regime");
            }
        } catch (Exception e) {
            // FastAnalytics ML not available yet — expected during bootstrap
        }
        return "UNKNOWN";
    }

    /**
     * Get quality prediction from FastAnalytics.
     */
    @SuppressWarnings("unchecked")
    private Map<String, Object> getQualityPrediction(String scripCode) {
        try {
            Map<String, Object> features = new LinkedHashMap<>();
            features.put("scripCode", scripCode);
            return restTemplate.postForObject(
                    fastAnalyticsUrl + "/api/ml/quality/predict", features, Map.class);
        } catch (Exception e) {
            return null;
        }
    }

    public boolean isShadowEnabled() {
        return shadowEnabled;
    }
}
