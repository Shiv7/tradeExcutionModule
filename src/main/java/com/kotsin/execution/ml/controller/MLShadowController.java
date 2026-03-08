package com.kotsin.execution.ml.controller;

import com.kotsin.execution.ml.service.BayesianSizer;
import com.kotsin.execution.ml.service.MLShadowService;
import lombok.RequiredArgsConstructor;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.domain.Sort;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.*;
import java.util.stream.Collectors;

/**
 * REST API for ML Shadow System
 * Provides dashboard with rule-based vs ML comparison data.
 */
@RestController
@RequestMapping("/api/ml")
@RequiredArgsConstructor
public class MLShadowController {

    private static final ZoneId IST = ZoneId.of("Asia/Kolkata");

    private final BayesianSizer bayesianSizer;
    private final MLShadowService shadowService;
    private final MongoTemplate mongoTemplate;

    /**
     * ML system health and status
     */
    @GetMapping("/health")
    public ResponseEntity<Map<String, Object>> health() {
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("shadowEnabled", shadowService.isShadowEnabled());
        result.put("bayesianDistributions", bayesianSizer.getAllDistributions().size());

        // Shadow log count
        long shadowLogs = mongoTemplate.count(new Query(), "ml_shadow_log");
        result.put("totalShadowLogs", shadowLogs);

        return ResponseEntity.ok(result);
    }

    /**
     * Get Bayesian distributions for all strategy-regime pairs
     */
    @GetMapping("/bayesian/distributions")
    public ResponseEntity<Map<String, Object>> getBayesianDistributions() {
        Map<String, BayesianSizer.BetaParams> dists = bayesianSizer.getAllDistributions();

        Map<String, Object> result = new LinkedHashMap<>();
        for (Map.Entry<String, BayesianSizer.BetaParams> entry : dists.entrySet()) {
            BayesianSizer.BetaParams params = entry.getValue();
            Map<String, Object> info = new LinkedHashMap<>();
            info.put("alpha", params.getAlpha());
            info.put("beta", params.getBeta());
            info.put("winRate", String.format("%.1f%%", params.mean() * 100));
            info.put("totalSamples", (int) (params.getAlpha() + params.getBeta() - 2));
            result.put(entry.getKey(), info);
        }

        return ResponseEntity.ok(result);
    }

    /**
     * Get sizing recommendation for a strategy+regime
     */
    @GetMapping("/bayesian/recommend")
    public ResponseEntity<BayesianSizer.SizingRecommendation> getRecommendation(
            @RequestParam String strategy,
            @RequestParam(defaultValue = "UNKNOWN") String regime,
            @RequestParam(defaultValue = "1.5") double avgWinLossRatio) {
        return ResponseEntity.ok(bayesianSizer.getRecommendation(strategy, regime, avgWinLossRatio));
    }

    /**
     * Initialize Bayesian distributions from historical trade data
     */
    @PostMapping("/bayesian/init-from-history")
    public ResponseEntity<Map<String, Object>> initFromHistory() {
        // Query trade_outcomes grouped by strategy
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> outcomes = (List<Map<String, Object>>) (List<?>)
                mongoTemplate.findAll(Map.class, "trade_outcomes");

        Map<String, int[]> strategyStats = new HashMap<>();
        for (Map<String, Object> outcome : outcomes) {
            String strategy = (String) outcome.getOrDefault("signalSource", "UNKNOWN");
            boolean isWin = Boolean.TRUE.equals(outcome.get("isWin"));
            int[] stats = strategyStats.computeIfAbsent(strategy, k -> new int[]{0, 0});
            if (isWin) stats[0]++;
            else stats[1]++;
        }

        Map<String, Object> result = new LinkedHashMap<>();
        for (Map.Entry<String, int[]> entry : strategyStats.entrySet()) {
            String strategy = entry.getKey();
            int wins = entry.getValue()[0];
            int losses = entry.getValue()[1];

            // Initialize for UNKNOWN regime (we'll refine when HMM is ready)
            bayesianSizer.initializeFromHistory(strategy, "UNKNOWN", wins, losses);

            Map<String, Object> info = new LinkedHashMap<>();
            info.put("wins", wins);
            info.put("losses", losses);
            info.put("winRate", String.format("%.1f%%", (double) wins / Math.max(1, wins + losses) * 100));
            result.put(strategy, info);
        }

        return ResponseEntity.ok(result);
    }

    /**
     * Get recent shadow logs (rule-based vs ML comparison)
     */
    @GetMapping("/shadow/logs")
    public ResponseEntity<List<Map>> getShadowLogs(
            @RequestParam(defaultValue = "50") int limit) {
        Query query = new Query()
                .with(Sort.by(Sort.Direction.DESC, "timestamp"))
                .limit(limit);
        @SuppressWarnings("unchecked")
        List<Map> logs = mongoTemplate.find(query, Map.class, "ml_shadow_log");
        // Remove _id for JSON serialization
        logs.forEach(m -> m.remove("_id"));
        return ResponseEntity.ok(logs);
    }

    /**
     * Get agreement/disagreement summary between rule-based and ML
     */
    @GetMapping("/shadow/comparison")
    public ResponseEntity<Map<String, Object>> getComparison(
            @RequestParam(defaultValue = "100") int limit) {
        Query query = new Query()
                .with(Sort.by(Sort.Direction.DESC, "timestamp"))
                .limit(limit);
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> logs = (List<Map<String, Object>>) (List<?>)
                mongoTemplate.find(query, Map.class, "ml_shadow_log");

        int total = logs.size();
        int agree = 0, mlTradeRuleSkip = 0, mlSkipRuleTrade = 0;

        for (Map<String, Object> log : logs) {
            String disagreeType = (String) log.getOrDefault("disagreementType", "NONE");
            switch (disagreeType) {
                case "NONE" -> agree++;
                case "ML_TRADE_RULE_SKIP" -> mlTradeRuleSkip++;
                case "ML_SKIP_RULE_TRADE" -> mlSkipRuleTrade++;
            }
        }

        Map<String, Object> result = new LinkedHashMap<>();
        result.put("totalEvaluated", total);
        result.put("agreement", agree);
        result.put("agreementRate", total > 0 ? String.format("%.1f%%", (double) agree / total * 100) : "N/A");
        result.put("mlTradeRuleSkip", mlTradeRuleSkip);
        result.put("mlSkipRuleTrade", mlSkipRuleTrade);

        return ResponseEntity.ok(result);
    }
}
