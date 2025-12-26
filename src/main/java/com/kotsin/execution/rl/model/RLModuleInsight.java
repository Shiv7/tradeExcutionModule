package com.kotsin.execution.rl.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

import java.time.LocalDateTime;

/**
 * Module-level insights from RL learning
 * Tracks which of the 16 modules contribute to profitable trades
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Document(collection = "rl_module_insights")
public class RLModuleInsight {
    
    @Id
    private String id;
    
    /**
     * Module name (e.g., "supportScore", "xfactor", "urgencyLevel")
     */
    private String moduleName;
    
    /**
     * Module category
     */
    private String category;  // VCP, IPU, FLOW, CONTEXT
    
    /**
     * Gradient-based importance from Actor network
     * Higher = more influential on actions
     */
    private double gradientImportance;
    
    /**
     * Correlation with reward (Pearson coefficient)
     */
    private double rewardCorrelation;
    
    /**
     * Win rate when module value is HIGH (> threshold)
     */
    private double winRateWhenHigh;
    
    /**
     * Win rate when module value is LOW (< threshold)
     */
    private double winRateWhenLow;
    
    /**
     * Optimal threshold learned from data
     */
    private double optimalThreshold;
    
    /**
     * Average R-multiple when module is high
     */
    private double avgRMultipleWhenHigh;
    
    /**
     * Average R-multiple when module is low
     */
    private double avgRMultipleWhenLow;
    
    /**
     * Trade count for statistics
     */
    private int tradesWhenHigh;
    private int tradesWhenLow;
    
    /**
     * Computed verdict
     */
    private ModuleVerdict verdict;
    
    /**
     * Human-readable recommendation
     */
    private String recommendation;
    
    /**
     * Statistical significance (p-value of difference)
     */
    private double pValue;
    
    /**
     * Timestamps
     */
    private LocalDateTime lastUpdated;
    
    public enum ModuleVerdict {
        STRONG_POSITIVE,    // High value → significantly higher win rate
        WEAK_POSITIVE,      // High value → slightly higher win rate
        NEUTRAL,            // No significant difference
        WEAK_NEGATIVE,      // High value → slightly lower win rate
        STRONG_NEGATIVE,    // High value → significantly lower win rate
        INSUFFICIENT_DATA   // Not enough samples
    }
    
    /**
     * Compute verdict from statistics
     */
    public void computeVerdict() {
        int totalTrades = tradesWhenHigh + tradesWhenLow;
        
        if (totalTrades < 30) {
            this.verdict = ModuleVerdict.INSUFFICIENT_DATA;
            this.recommendation = "Need more trades for statistical significance";
            return;
        }
        
        double winDiff = winRateWhenHigh - winRateWhenLow;
        
        if (pValue > 0.1) {
            this.verdict = ModuleVerdict.NEUTRAL;
            this.recommendation = String.format("No significant edge (diff=%.1f%%, p=%.2f)", 
                winDiff * 100, pValue);
        } else if (winDiff > 0.15) {
            this.verdict = ModuleVerdict.STRONG_POSITIVE;
            this.recommendation = String.format("STRONG: Trade when %s > %.2f (%.1f%% win vs %.1f%%)", 
                moduleName, optimalThreshold, winRateWhenHigh * 100, winRateWhenLow * 100);
        } else if (winDiff > 0.05) {
            this.verdict = ModuleVerdict.WEAK_POSITIVE;
            this.recommendation = String.format("Prefer higher %s (%.1f%% vs %.1f%% win rate)", 
                moduleName, winRateWhenHigh * 100, winRateWhenLow * 100);
        } else if (winDiff < -0.15) {
            this.verdict = ModuleVerdict.STRONG_NEGATIVE;
            this.recommendation = String.format("AVOID when %s > %.2f (%.1f%% win vs %.1f%%)", 
                moduleName, optimalThreshold, winRateWhenHigh * 100, winRateWhenLow * 100);
        } else if (winDiff < -0.05) {
            this.verdict = ModuleVerdict.WEAK_NEGATIVE;
            this.recommendation = String.format("Prefer lower %s (%.1f%% vs %.1f%% win rate)", 
                moduleName, winRateWhenHigh * 100, winRateWhenLow * 100);
        } else {
            this.verdict = ModuleVerdict.NEUTRAL;
            this.recommendation = "No significant edge detected";
        }
    }
}
