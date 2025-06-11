package com.kotsin.execution.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;
import java.util.List;

/**
 * Portfolio Insights Model
 * 
 * Provides analytical insights, recommendations, and warnings
 * for portfolio management and risk control
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class PortfolioInsights {
    
    // Analytical Insights
    private List<String> insights;          // Key portfolio observations
    private List<String> recommendations;   // Actionable recommendations
    private List<String> warnings;         // Risk warnings and alerts
    
    // Scoring Metrics
    private Integer riskScore;             // 0-100 (higher = riskier)
    private String performanceGrade;       // A+ to F grade
    private Integer diversificationScore;  // 0-100 (higher = better diversified)
    
    // Metadata
    private LocalDateTime generatedAt;
    
    /**
     * Get risk level description
     */
    public String getRiskLevel() {
        if (riskScore == null) return "Unknown";
        
        if (riskScore <= 20) return "Low";
        if (riskScore <= 40) return "Moderate";
        if (riskScore <= 60) return "Medium";
        if (riskScore <= 80) return "High";
        return "Very High";
    }
    
    /**
     * Get diversification level description
     */
    public String getDiversificationLevel() {
        if (diversificationScore == null) return "Unknown";
        
        if (diversificationScore >= 80) return "Excellent";
        if (diversificationScore >= 60) return "Good";
        if (diversificationScore >= 40) return "Fair";
        if (diversificationScore >= 20) return "Poor";
        return "Very Poor";
    }
    
    /**
     * Check if portfolio has any critical warnings
     */
    public boolean hasCriticalWarnings() {
        return warnings != null && warnings.stream()
                .anyMatch(warning -> warning.toLowerCase().contains("high") || 
                                   warning.toLowerCase().contains("critical") ||
                                   warning.toLowerCase().contains("significant"));
    }
    
    /**
     * Get number of actionable recommendations
     */
    public int getRecommendationCount() {
        return recommendations != null ? recommendations.size() : 0;
    }
    
    /**
     * Get formatted insights summary
     */
    public String getFormattedSummary() {
        StringBuilder summary = new StringBuilder();
        
        summary.append("Portfolio Insights:\n");
        summary.append(String.format("Risk Level: %s (%d/100)\n", getRiskLevel(), riskScore));
        summary.append(String.format("Performance: %s\n", performanceGrade));
        summary.append(String.format("Diversification: %s (%d/100)\n", getDiversificationLevel(), diversificationScore));
        
        if (warnings != null && !warnings.isEmpty()) {
            summary.append(String.format("\n⚠️ %d Warnings:\n", warnings.size()));
            warnings.forEach(warning -> summary.append("- ").append(warning).append("\n"));
        }
        
        if (recommendations != null && !recommendations.isEmpty()) {
            summary.append(String.format("\n💡 %d Recommendations:\n", recommendations.size()));
            recommendations.forEach(rec -> summary.append("- ").append(rec).append("\n"));
        }
        
        return summary.toString();
    }
} 