package com.kotsin.execution.rl.controller;

import com.kotsin.execution.rl.model.RLExperience;
import com.kotsin.execution.rl.model.RLModuleInsight;
import com.kotsin.execution.rl.model.RLPolicy;
import com.kotsin.execution.rl.model.TradeState;
import com.kotsin.execution.rl.model.TradeAction;
import com.kotsin.execution.rl.repository.RLExperienceRepository;
import com.kotsin.execution.rl.repository.RLModuleInsightRepository;
import com.kotsin.execution.rl.service.ExperienceReplayBuffer;
import com.kotsin.execution.rl.service.RLTrainer;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.*;
import java.util.stream.Collectors;

/**
 * REST API for RL insights and policy management
 */
@RestController
@RequestMapping("/api/rl")
@RequiredArgsConstructor
public class RLController {
    
    private final RLTrainer trainer;
    private final ExperienceReplayBuffer buffer;
    private final RLExperienceRepository experienceRepository;
    private final RLModuleInsightRepository moduleInsightRepository;
    
    /**
     * Get current policy stats
     */
    @GetMapping("/policy")
    public ResponseEntity<RLPolicy> getPolicy() {
        return ResponseEntity.ok(trainer.getCurrentPolicy());
    }
    
    /**
     * Get all module insights (sorted by importance)
     */
    @GetMapping("/modules")
    public ResponseEntity<List<RLModuleInsight>> getModuleInsights() {
        List<RLModuleInsight> insights = moduleInsightRepository.findAll();
        insights.sort((a, b) -> Double.compare(b.getGradientImportance(), a.getGradientImportance()));
        return ResponseEntity.ok(insights);
    }
    
    /**
     * Get single module insight
     */
    @GetMapping("/modules/{name}")
    public ResponseEntity<RLModuleInsight> getModuleInsight(@PathVariable String name) {
        return moduleInsightRepository.findByModuleName(name)
            .map(ResponseEntity::ok)
            .orElse(ResponseEntity.notFound().build());
    }
    
    /**
     * Get comprehensive insights summary
     */
    @GetMapping("/insights")
    public ResponseEntity<Map<String, Object>> getInsightsSummary() {
        RLPolicy policy = trainer.getCurrentPolicy();
        ExperienceReplayBuffer.BufferStats bufferStats = buffer.getStats();
        List<RLModuleInsight> insights = moduleInsightRepository.findAll();
        
        Map<String, Object> summary = new LinkedHashMap<>();
        
        // Summary
        summary.put("policyVersion", policy.getVersion());
        summary.put("totalExperiences", policy.getTotalExperiences());
        summary.put("backtestCount", bufferStats.backtestCount());
        summary.put("liveTradeCount", bufferStats.liveTradeCount());
        summary.put("overallWinRate", String.format("%.1f%%", bufferStats.winRate() * 100));
        summary.put("avgReward", String.format("%.3f", bufferStats.avgReward()));
        
        // Top modules
        List<Map<String, Object>> topModules = insights.stream()
            .filter(i -> i.getVerdict() == RLModuleInsight.ModuleVerdict.STRONG_POSITIVE ||
                        i.getVerdict() == RLModuleInsight.ModuleVerdict.WEAK_POSITIVE)
            .sorted((a, b) -> Double.compare(b.getGradientImportance(), a.getGradientImportance()))
            .limit(5)
            .map(this::formatInsight)
            .collect(Collectors.toList());
        summary.put("topPerformingModules", topModules);
        
        // Worst modules
        List<Map<String, Object>> worstModules = insights.stream()
            .filter(i -> i.getVerdict() == RLModuleInsight.ModuleVerdict.STRONG_NEGATIVE ||
                        i.getVerdict() == RLModuleInsight.ModuleVerdict.WEAK_NEGATIVE)
            .sorted((a, b) -> Double.compare(a.getGradientImportance(), b.getGradientImportance()))
            .limit(3)
            .map(this::formatInsight)
            .collect(Collectors.toList());
        summary.put("underperformingModules", worstModules);
        
        // Actionable insights
        List<String> actionable = new ArrayList<>();
        for (RLModuleInsight insight : insights) {
            if (insight.getRecommendation() != null && 
                insight.getVerdict() != RLModuleInsight.ModuleVerdict.NEUTRAL &&
                insight.getVerdict() != RLModuleInsight.ModuleVerdict.INSUFFICIENT_DATA) {
                actionable.add(insight.getRecommendation());
            }
        }
        summary.put("actionableInsights", actionable);
        
        // Learned adjustments
        Map<String, Double> adjustments = new LinkedHashMap<>();
        adjustments.put("avgShouldTrade", policy.getAvgShouldTrade());
        adjustments.put("avgEntryAdjustment", policy.getAvgEntryAdjustment());
        adjustments.put("avgSlAdjustment", policy.getAvgSlAdjustment());
        adjustments.put("avgTargetAdjustment", policy.getAvgTargetAdjustment());
        summary.put("learnedAdjustments", adjustments);
        
        return ResponseEntity.ok(summary);
    }
    
    /**
     * Evaluate a signal state (get policy recommendation)
     */
    @PostMapping("/evaluate")
    public ResponseEntity<Map<String, Object>> evaluateState(@RequestBody TradeState state) {
        TradeAction action = trainer.getAction(state);
        
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("shouldTrade", action.shouldExecuteTrade());
        result.put("tradeConfidence", String.format("%.1f%%", action.getShouldTrade() * 100));
        result.put("entryAdjustment", String.format("%.2f%%", action.getEntryAdjustment() * 0.5));
        result.put("slAdjustment", String.format("%.2f%%", action.getSlAdjustment() * 1.0));
        result.put("targetAdjustment", String.format("%.2f%%", action.getTargetAdjustment() * 1.0));
        result.put("rawAction", action);
        
        return ResponseEntity.ok(result);
    }
    
    /**
     * Get experience buffer stats
     */
    @GetMapping("/buffer")
    public ResponseEntity<ExperienceReplayBuffer.BufferStats> getBufferStats() {
        return ResponseEntity.ok(buffer.getStats());
    }
    
    /**
     * Get recent experiences
     */
    @GetMapping("/experiences")
    public ResponseEntity<List<Map<String, Object>>> getRecentExperiences(
            @RequestParam(defaultValue = "50") int limit) {
        List<RLExperience> experiences = experienceRepository.findTop1000ByOrderByCreatedAtDesc();
        
        List<Map<String, Object>> result = experiences.stream()
            .limit(limit)
            .map(exp -> {
                Map<String, Object> m = new LinkedHashMap<>();
                m.put("tradeId", exp.getTradeId());
                m.put("scripCode", exp.getScripCode());
                m.put("signalType", exp.getSignalType());
                m.put("source", exp.getSource());
                m.put("reward", exp.getReward());
                m.put("rMultiple", exp.getRMultiple());
                m.put("won", exp.isWon());
                m.put("exitReason", exp.getExitReason());
                m.put("createdAt", exp.getCreatedAt());
                return m;
            })
            .collect(Collectors.toList());
        
        return ResponseEntity.ok(result);
    }
    
    /**
     * Trigger manual training
     */
    @PostMapping("/train")
    public ResponseEntity<Map<String, Object>> triggerTraining(
            @RequestParam(defaultValue = "10") int epochs) {
        trainer.triggerTraining(epochs);
        
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("status", "Training complete");
        result.put("epochs", epochs);
        result.put("policyVersion", trainer.getCurrentPolicy().getVersion());
        result.put("totalExperiences", trainer.getCurrentPolicy().getTotalExperiences());
        
        return ResponseEntity.ok(result);
    }
    
    /**
     * Get performance by signal type
     */
    @GetMapping("/performance/by-signal")
    public ResponseEntity<List<Map<String, Object>>> getPerformanceBySignal() {
        List<RLExperience> experiences = experienceRepository.findTop1000ByOrderByCreatedAtDesc();
        
        Map<String, List<RLExperience>> bySignal = experiences.stream()
            .filter(e -> e.getSignalType() != null)
            .collect(Collectors.groupingBy(RLExperience::getSignalType));
        
        List<Map<String, Object>> result = new ArrayList<>();
        for (Map.Entry<String, List<RLExperience>> entry : bySignal.entrySet()) {
            List<RLExperience> group = entry.getValue();
            long wins = group.stream().filter(RLExperience::isWon).count();
            double avgR = group.stream().mapToDouble(RLExperience::getRMultiple).average().orElse(0);
            
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("signalType", entry.getKey());
            m.put("count", group.size());
            m.put("wins", wins);
            m.put("losses", group.size() - wins);
            m.put("winRate", String.format("%.1f%%", (double) wins / group.size() * 100));
            m.put("avgRMultiple", String.format("%.2f", avgR));
            result.add(m);
        }
        
        result.sort((a, b) -> Double.compare(
            Double.parseDouble(((String)b.get("winRate")).replace("%", "")),
            Double.parseDouble(((String)a.get("winRate")).replace("%", ""))
        ));
        
        return ResponseEntity.ok(result);
    }
    
    private Map<String, Object> formatInsight(RLModuleInsight insight) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("module", insight.getModuleName());
        m.put("category", insight.getCategory());
        m.put("gradientImportance", String.format("%.3f", insight.getGradientImportance()));
        m.put("winRateWhenHigh", String.format("%.1f%%", insight.getWinRateWhenHigh() * 100));
        m.put("winRateWhenLow", String.format("%.1f%%", insight.getWinRateWhenLow() * 100));
        m.put("threshold", String.format("%.2f", insight.getOptimalThreshold()));
        m.put("verdict", insight.getVerdict());
        m.put("recommendation", insight.getRecommendation());
        return m;
    }
}
