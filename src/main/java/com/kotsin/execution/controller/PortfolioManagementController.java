package com.kotsin.execution.controller;

import com.kotsin.execution.service.PortfolioManagementService;
import com.kotsin.execution.service.TradeExecutionSelfHealingService;
import com.kotsin.execution.model.PortfolioSummary;
import com.kotsin.execution.model.PortfolioInsights;
import com.kotsin.execution.model.ActiveTrade;
import com.kotsin.execution.service.TradeStateManager;
import com.kotsin.execution.service.TradeHistoryService;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.*;
import java.util.stream.Collectors;

/**
 * 💰 PORTFOLIO MANAGEMENT CONTROLLER
 * 
 * COMPREHENSIVE PORTFOLIO FEATURES:
 * ✅ Portfolio Summary & Insights
 * ✅ Risk Management & Monitoring  
 * ✅ Performance Analytics
 * ✅ Position Management
 * ✅ Exposure Analysis
 * ✅ Self-Healing Monitoring
 * ✅ Emergency Controls
 */
@RestController
@RequestMapping("/api/portfolio")
@RequiredArgsConstructor
@Slf4j
public class PortfolioManagementController {
    
    private final PortfolioManagementService portfolioManagementService;
    private final TradeExecutionSelfHealingService selfHealingService;
    private final TradeStateManager tradeStateManager;
    private final TradeHistoryService tradeHistoryService;
    
    /**
     * 📊 GET PORTFOLIO OVERVIEW
     * Returns comprehensive portfolio summary with all key metrics
     */
    @GetMapping("/overview")
    public ResponseEntity<Map<String, Object>> getPortfolioOverview() {
        try {
            log.info("📊 [PortfolioAPI] Generating portfolio overview...");
            
            PortfolioSummary summary = portfolioManagementService.getPortfolioSummary();
            PortfolioInsights insights = portfolioManagementService.getPortfolioInsights();
            
            Map<String, Object> overview = new HashMap<>();
            overview.put("summary", summary);
            overview.put("insights", insights);
            overview.put("lastUpdated", LocalDateTime.now());
            overview.put("status", "success");
            
            log.info("✅ [PortfolioAPI] Portfolio overview generated successfully");
            return ResponseEntity.ok(overview);
            
        } catch (Exception e) {
            log.error("🚨 [PortfolioAPI] Error: {}", e.getMessage(), e);
            return ResponseEntity.internalServerError().body(
                Map.of("error", "Failed to generate portfolio overview")
            );
        }
    }
    
    /**
     * 💹 GET PORTFOLIO SUMMARY
     * Returns detailed financial metrics and performance data
     */
    @GetMapping("/summary")
    public ResponseEntity<PortfolioSummary> getPortfolioSummary() {
        try {
            log.info("💹 [PortfolioAPI] Fetching portfolio summary...");
            
            PortfolioSummary summary = portfolioManagementService.getPortfolioSummary();
            
            log.info("✅ [PortfolioAPI] Portfolio summary fetched: Value=₹{}, Return={}%, Exposure={}%",
                    summary.getPortfolioValue(), 
                    summary.getTotalReturnPercentage(),
                    summary.getExposurePercentage());
            
            return ResponseEntity.ok(summary);
            
        } catch (Exception e) {
            log.error("🚨 [PortfolioAPI] Error: {}", e.getMessage(), e);
            return ResponseEntity.internalServerError().build();
        }
    }
    
    /**
     * 🧠 GET PORTFOLIO INSIGHTS
     * Returns AI-powered insights, recommendations, and warnings
     */
    @GetMapping("/insights")
    public ResponseEntity<PortfolioInsights> getPortfolioInsights() {
        try {
            log.info("🧠 [PortfolioAPI] Generating portfolio insights...");
            
            PortfolioInsights insights = portfolioManagementService.getPortfolioInsights();
            
            log.info("✅ [PortfolioAPI] Portfolio insights generated: Risk={}, Grade={}, Diversification={}",
                    insights.getRiskLevel(), 
                    insights.getPerformanceGrade(),
                    insights.getDiversificationLevel());
            
            return ResponseEntity.ok(insights);
            
        } catch (Exception e) {
            log.error("🚨 [PortfolioAPI] Error: {}", e.getMessage(), e);
            return ResponseEntity.internalServerError().build();
        }
    }
    
    /**
     * 📈 GET PORTFOLIO PERFORMANCE
     * Returns detailed performance metrics and analytics
     */
    @GetMapping("/performance")
    public ResponseEntity<Map<String, Object>> getPortfolioPerformance() {
        try {
            log.info("📈 [PortfolioAPI] Calculating portfolio performance...");
            
            double portfolioValue = portfolioManagementService.getPortfolioValue();
            double totalExposure = portfolioManagementService.calculateTotalExposure();
            
            // Performance metrics
            Map<String, Object> performance = new HashMap<>();
            performance.put("currentValue", portfolioValue);
            performance.put("totalExposure", totalExposure);
            performance.put("exposurePercentage", (totalExposure / portfolioValue) * 100);
            
            // Active trades performance
            Collection<ActiveTrade> activeTrades = tradeStateManager.getAllActiveTradesAsCollection();
            performance.put("activeTradesCount", activeTrades.size());
            performance.put("averagePositionSize", activeTrades.stream()
                    .mapToInt(trade -> trade.getPositionSize() != null ? trade.getPositionSize() : 0)
                    .average().orElse(0.0));
            
            // Risk metrics
            performance.put("totalRiskAmount", activeTrades.stream()
                    .mapToDouble(trade -> trade.getRiskAmount() != null ? trade.getRiskAmount() : 0.0)
                    .sum());
            
            // Strategy distribution
            Map<String, Long> strategyDistribution = activeTrades.stream()
                    .collect(Collectors.groupingBy(
                        ActiveTrade::getStrategyName, 
                        Collectors.counting()));
            performance.put("strategyDistribution", strategyDistribution);
            
            // Script distribution
            Map<String, Long> scriptDistribution = activeTrades.stream()
                    .collect(Collectors.groupingBy(
                        ActiveTrade::getScripCode, 
                        Collectors.counting()));
            performance.put("scriptDistribution", scriptDistribution);
            
            performance.put("calculatedAt", LocalDateTime.now());
            
            log.info("✅ [PortfolioAPI] Portfolio performance calculated successfully");
            return ResponseEntity.ok(performance);
            
        } catch (Exception e) {
            log.error("🚨 [PortfolioAPI] Error: {}", e.getMessage(), e);
            return ResponseEntity.internalServerError().body(
                Map.of("error", "Failed to calculate portfolio performance")
            );
        }
    }
    
    /**
     * 🎯 GET POSITION ANALYSIS
     * Returns detailed position-level analysis and risk assessment
     */
    @GetMapping("/positions")
    public ResponseEntity<Map<String, Object>> getPositionAnalysis() {
        try {
            log.info("🎯 [PortfolioAPI] Analyzing portfolio positions...");
            
            Collection<ActiveTrade> activeTrades = tradeStateManager.getAllActiveTradesAsCollection();
            
            // Position analysis by script
            Map<String, List<ActiveTrade>> positionsByScript = activeTrades.stream()
                    .collect(Collectors.groupingBy(ActiveTrade::getScripCode));
            
            Map<String, Map<String, Object>> scriptAnalysis = new HashMap<>();
            
            for (Map.Entry<String, List<ActiveTrade>> entry : positionsByScript.entrySet()) {
                String scripCode = entry.getKey();
                List<ActiveTrade> trades = entry.getValue();
                
                Map<String, Object> analysis = new HashMap<>();
                analysis.put("tradesCount", trades.size());
                analysis.put("totalPositionSize", trades.stream()
                        .mapToInt(trade -> trade.getPositionSize() != null ? trade.getPositionSize() : 0)
                        .sum());
                analysis.put("totalRiskAmount", trades.stream()
                        .mapToDouble(trade -> trade.getRiskAmount() != null ? trade.getRiskAmount() : 0.0)
                        .sum());
                analysis.put("averageEntryPrice", trades.stream()
                        .filter(trade -> trade.getEntryPrice() != null)
                        .mapToDouble(ActiveTrade::getEntryPrice)
                        .average().orElse(0.0));
                analysis.put("strategies", trades.stream()
                        .map(ActiveTrade::getStrategyName)
                        .distinct()
                        .collect(Collectors.toList()));
                
                scriptAnalysis.put(scripCode, analysis);
            }
            
            // Overall position metrics
            Map<String, Object> positionMetrics = new HashMap<>();
            positionMetrics.put("totalScripts", positionsByScript.size());
            positionMetrics.put("scriptAnalysis", scriptAnalysis);
            positionMetrics.put("maxPositionsPerScript", positionsByScript.values().stream()
                    .mapToInt(List::size)
                    .max().orElse(0));
            positionMetrics.put("analysisTime", LocalDateTime.now());
            
            log.info("✅ [PortfolioAPI] Position analysis completed for {} scripts", positionsByScript.size());
            return ResponseEntity.ok(positionMetrics);
            
        } catch (Exception e) {
            log.error("🚨 [PortfolioAPI] Error: {}", e.getMessage(), e);
            return ResponseEntity.internalServerError().body(
                Map.of("error", "Failed to analyze positions")
            );
        }
    }
    
    /**
     * ⚠️ GET RISK ASSESSMENT
     * Returns comprehensive risk analysis and warnings
     */
    @GetMapping("/risk-assessment")
    public ResponseEntity<Map<String, Object>> getRiskAssessment() {
        try {
            log.info("⚠️ [PortfolioAPI] Performing comprehensive risk assessment...");
            
            double portfolioValue = portfolioManagementService.getPortfolioValue();
            double totalExposure = portfolioManagementService.calculateTotalExposure();
            Collection<ActiveTrade> activeTrades = tradeStateManager.getAllActiveTradesAsCollection();
            
            Map<String, Object> riskAssessment = new HashMap<>();
            
            // Exposure risk
            double exposurePercentage = (totalExposure / portfolioValue) * 100;
            riskAssessment.put("exposureRisk", Map.of(
                "exposureAmount", totalExposure,
                "exposurePercentage", exposurePercentage,
                "riskLevel", exposurePercentage > 15 ? "HIGH" : exposurePercentage > 8 ? "MEDIUM" : "LOW",
                "recommendation", exposurePercentage > 15 ? "Reduce exposure immediately" : "Exposure within limits"
            ));
            
            // Concentration risk
            Map<String, Long> scriptCounts = activeTrades.stream()
                    .collect(Collectors.groupingBy(ActiveTrade::getScripCode, Collectors.counting()));
            
            long maxScriptCount = scriptCounts.values().stream().mapToLong(Long::longValue).max().orElse(0);
            riskAssessment.put("concentrationRisk", Map.of(
                "maxPositionsPerScript", maxScriptCount,
                "totalScripts", scriptCounts.size(),
                "riskLevel", maxScriptCount > 3 ? "HIGH" : maxScriptCount > 1 ? "MEDIUM" : "LOW",
                "recommendation", maxScriptCount > 3 ? "Diversify positions across more scripts" : "Concentration acceptable"
            ));
            
            // Strategy risk
            Map<String, Long> strategyCounts = activeTrades.stream()
                    .collect(Collectors.groupingBy(ActiveTrade::getStrategyName, Collectors.counting()));
            
            riskAssessment.put("strategyRisk", Map.of(
                "activeStrategies", strategyCounts.size(),
                "strategyDistribution", strategyCounts,
                "riskLevel", strategyCounts.size() == 1 ? "HIGH" : strategyCounts.size() < 3 ? "MEDIUM" : "LOW",
                "recommendation", strategyCounts.size() == 1 ? "Add strategy diversification" : "Strategy mix acceptable"
            ));
            
            // Time risk (positions held too long)
            long oldTrades = activeTrades.stream()
                    .filter(trade -> trade.getEntryTime() != null)
                    .filter(trade -> trade.getEntryTime().isBefore(LocalDateTime.now().minusHours(24)))
                    .count();
            
            riskAssessment.put("timeRisk", Map.of(
                "oldPositions", oldTrades,
                "totalPositions", activeTrades.size(),
                "riskLevel", oldTrades > 3 ? "HIGH" : oldTrades > 1 ? "MEDIUM" : "LOW",
                "recommendation", oldTrades > 3 ? "Review and close old positions" : "Position timing acceptable"
            ));
            
            // Overall risk score
            int overallRiskScore = calculateOverallRiskScore(exposurePercentage, maxScriptCount, 
                                                           strategyCounts.size(), oldTrades);
            riskAssessment.put("overallRisk", Map.of(
                "riskScore", overallRiskScore,
                "riskLevel", overallRiskScore > 70 ? "HIGH" : overallRiskScore > 40 ? "MEDIUM" : "LOW",
                "assessmentTime", LocalDateTime.now()
            ));
            
            log.info("✅ [PortfolioAPI] Risk assessment completed: Overall risk score = {}", overallRiskScore);
            return ResponseEntity.ok(riskAssessment);
            
        } catch (Exception e) {
            log.error("🚨 [PortfolioAPI] Error: {}", e.getMessage(), e);
            return ResponseEntity.internalServerError().body(
                Map.of("error", "Failed to perform risk assessment")
            );
        }
    }
    
    /**
     * 🔧 GET SELF-HEALING STATUS
     * Returns Trade Execution self-healing service statistics
     */
    @GetMapping("/self-healing-status")
    public ResponseEntity<Map<String, Object>> getSelfHealingStatus() {
        try {
            log.info("🔧 [PortfolioAPI] Fetching self-healing status...");
            
            Map<String, Object> healingStats = selfHealingService.getSelfHealingStats();
            
            // Add additional status information
            healingStats.put("systemHealth", "HEALTHY");
            healingStats.put("statusTime", LocalDateTime.now());
            
            log.info("✅ [PortfolioAPI] Self-healing status fetched successfully");
            return ResponseEntity.ok(healingStats);
            
        } catch (Exception e) {
            log.error("🚨 [PortfolioAPI] Error: {}", e.getMessage(), e);
            return ResponseEntity.internalServerError().body(
                Map.of("error", "Failed to fetch self-healing status")
            );
        }
    }
    
    /**
     * 📅 GET HISTORICAL PERFORMANCE
     * Returns historical performance data for specified date range
     */
    @GetMapping("/historical-performance")
    public ResponseEntity<Map<String, Object>> getHistoricalPerformance(
            @RequestParam(defaultValue = "7") int days) {
        try {
            log.info("📅 [PortfolioAPI] Fetching historical performance for {} days...", days);
            
            LocalDate endDate = LocalDate.now();
            LocalDate startDate = endDate.minusDays(days);
            
            // Get completed trades in date range
            List<com.kotsin.execution.model.TradeResult> historicalTrades = 
                tradeHistoryService.getTradeResultsByDateRange(startDate.atStartOfDay(), endDate.atTime(23, 59));
            
            Map<String, Object> historicalData = new HashMap<>();
            historicalData.put("dateRange", Map.of("start", startDate, "end", endDate));
            historicalData.put("totalTrades", historicalTrades.size());
            
            if (!historicalTrades.isEmpty()) {
                double totalPnL = historicalTrades.stream()
                        .mapToDouble(trade -> trade.getProfitLoss() != null ? trade.getProfitLoss() : 0.0)
                        .sum();
                
                long winningTrades = historicalTrades.stream()
                        .filter(trade -> trade.getProfitLoss() != null && trade.getProfitLoss() > 0)
                        .count();
                
                double winRate = (winningTrades * 100.0) / historicalTrades.size();
                
                historicalData.put("totalPnL", totalPnL);
                historicalData.put("winningTrades", winningTrades);
                historicalData.put("winRate", winRate);
                historicalData.put("averagePnLPerTrade", totalPnL / historicalTrades.size());
                
                // Daily breakdown
                Map<LocalDate, Double> dailyPnL = historicalTrades.stream()
                        .filter(trade -> trade.getExitTime() != null)
                        .collect(Collectors.groupingBy(
                            trade -> trade.getExitTime().toLocalDate(),
                            Collectors.summingDouble(trade -> trade.getProfitLoss() != null ? trade.getProfitLoss() : 0.0)
                        ));
                
                historicalData.put("dailyPnL", dailyPnL);
            } else {
                historicalData.put("message", "No completed trades in the specified period");
            }
            
            historicalData.put("generatedAt", LocalDateTime.now());
            
            log.info("✅ [PortfolioAPI] Historical performance fetched: {} trades over {} days", 
                    historicalTrades.size(), days);
            return ResponseEntity.ok(historicalData);
            
        } catch (Exception e) {
            log.error("🚨 [PortfolioAPI] Error: {}", e.getMessage(), e);
            return ResponseEntity.internalServerError().body(
                Map.of("error", "Failed to fetch historical performance")
            );
        }
    }
    
    /**
     * 🚨 EMERGENCY: Force portfolio rebalancing
     * Manual trigger for immediate portfolio risk adjustment
     */
    @PostMapping("/emergency-rebalance")
    public ResponseEntity<Map<String, Object>> emergencyRebalance() {
        try {
            log.warn("🚨 [PortfolioAPI] EMERGENCY REBALANCE triggered!");
            
            selfHealingService.performCriticalRiskMonitoring();
            selfHealingService.cleanupExpiredSignalsAndRiskViolations();
            selfHealingService.performPortfolioRebalancing();
            
            Map<String, Object> response = new HashMap<>();
            response.put("status", "EMERGENCY_REBALANCE_TRIGGERED");
            response.put("triggeredAt", LocalDateTime.now());
            
            return ResponseEntity.ok(response);
            
        } catch (Exception e) {
            log.error("🚨 [PortfolioAPI] Error: {}", e.getMessage(), e);
            return ResponseEntity.internalServerError().body(
                Map.of("error", "Emergency rebalance failed")
            );
        }
    }
    
    /**
     * Calculate overall risk score
     */
    private int calculateOverallRiskScore(double exposurePercentage, long maxScriptCount, 
                                        int strategyCount, long oldTrades) {
        int score = 0;
        
        // Exposure risk (0-30 points)
        score += Math.min(30, (int)(exposurePercentage * 2));
        
        // Concentration risk (0-25 points)
        score += Math.min(25, (int)(maxScriptCount * 8));
        
        // Strategy diversification risk (0-25 points)
        if (strategyCount == 1) score += 25;
        else if (strategyCount == 2) score += 15;
        else if (strategyCount == 3) score += 5;
        
        // Time risk (0-20 points)
        score += Math.min(20, (int)(oldTrades * 5));
        
        return Math.min(100, score);
    }
} 