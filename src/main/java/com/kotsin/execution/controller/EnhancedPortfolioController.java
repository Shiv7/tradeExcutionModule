package com.kotsin.execution.controller;

import com.kotsin.execution.service.PortfolioManagementService;
import com.kotsin.execution.service.EnhancedRiskManagementService;
import com.kotsin.execution.service.EnhancedRiskManagementService.RiskValidationResult;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;

/**
 * 🎯 ENHANCED PORTFOLIO CONTROLLER
 * 
 * COMPREHENSIVE PORTFOLIO MANAGEMENT ENDPOINTS:
 * 1. 📊 Portfolio Dashboard
 * 2. ⚠️ Risk Assessment  
 * 3. 🧪 Signal Testing
 * 4. 🚨 Emergency Controls
 * 5. 📈 Performance Analytics
 * 6. 🔍 System Status
 */
@RestController
@RequestMapping("/api/enhanced-portfolio")
@Slf4j
@RequiredArgsConstructor
public class EnhancedPortfolioController {
    
    private final PortfolioManagementService portfolioManagementService;
    private final EnhancedRiskManagementService riskManagementService;
    
    /**
     * Get comprehensive portfolio dashboard
     */
    @GetMapping("/dashboard")
    public ResponseEntity<Map<String, Object>> getPortfolioDashboard() {
        try {
            log.info("📊 [Dashboard] Generating comprehensive portfolio dashboard");
            
            Map<String, Object> dashboard = new HashMap<>();
            
            // Portfolio overview
            dashboard.put("portfolioSummary", portfolioManagementService.getPortfolioSummary());
            dashboard.put("portfolioInsights", portfolioManagementService.getPortfolioInsights());
            dashboard.put("riskManagement", riskManagementService.getRiskManagementStats());
            
            // Performance metrics
            dashboard.put("currentValue", portfolioManagementService.getCurrentPortfolioValue());
            dashboard.put("todayPnL", portfolioManagementService.getTodayPnL());
            
            dashboard.put("timestamp", LocalDateTime.now());
            dashboard.put("status", "success");
            
            log.info("✅ [Dashboard] Portfolio dashboard generated successfully");
            return ResponseEntity.ok(dashboard);
            
        } catch (Exception e) {
            log.error("🚨 [Dashboard] Error generating portfolio dashboard: {}", e.getMessage(), e);
            Map<String, Object> errorResponse = new HashMap<>();
            errorResponse.put("error", "Failed to generate portfolio dashboard");
            errorResponse.put("message", e.getMessage());
            errorResponse.put("timestamp", LocalDateTime.now());
            return ResponseEntity.internalServerError().body(errorResponse);
        }
    }
    
    /**
     * Get detailed risk assessment
     */
    @GetMapping("/risk-assessment")
    public ResponseEntity<Map<String, Object>> getRiskAssessment() {
        try {
            log.info("⚠️ [RiskAssessment] Generating detailed risk assessment");
            
            Map<String, Object> assessment = new HashMap<>();
            
            // Portfolio insights with risk analysis
            assessment.put("insights", portfolioManagementService.getPortfolioInsights());
            assessment.put("riskStats", riskManagementService.getRiskManagementStats());
            assessment.put("riskConfiguration", riskManagementService.getRiskConfiguration());
            
            // Current metrics
            double portfolioValue = portfolioManagementService.getCurrentPortfolioValue();
            double maxRiskPerTrade = portfolioManagementService.getMaxRiskPerTrade();
            double todayPnL = portfolioManagementService.getTodayPnL();
            
            assessment.put("portfolioValue", portfolioValue);
            assessment.put("maxRiskPerTrade", maxRiskPerTrade);
            assessment.put("dailyPnL", todayPnL);
            assessment.put("riskUtilization", (maxRiskPerTrade / portfolioValue) * 100);
            
            assessment.put("timestamp", LocalDateTime.now());
            assessment.put("status", "success");
            
            log.info("✅ [RiskAssessment] Risk assessment completed successfully");
            return ResponseEntity.ok(assessment);
            
        } catch (Exception e) {
            log.error("🚨 [RiskAssessment] Error generating risk assessment: {}", e.getMessage(), e);
            Map<String, Object> errorResponse = new HashMap<>();
            errorResponse.put("error", "Failed to generate risk assessment");
            errorResponse.put("message", e.getMessage());
            errorResponse.put("timestamp", LocalDateTime.now());
            return ResponseEntity.internalServerError().body(errorResponse);
        }
    }
    
    /**
     * Test signal validation with current risk parameters
     */
    @PostMapping("/test-signal-validation")
    public ResponseEntity<Map<String, Object>> testSignalValidation(@RequestBody Map<String, Object> signalData) {
        try {
            log.info("🧪 [SignalTest] Testing signal validation: {}", signalData.get("scripCode"));
            
            // Extract test parameters
            double currentPrice = ((Number) signalData.getOrDefault("currentPrice", 100.0)).doubleValue();
            
            // Validate signal using strict risk controls
            RiskValidationResult validationResult = riskManagementService.validateSignalWithStrictRiskControls(signalData, currentPrice);
            
            Map<String, Object> testResult = new HashMap<>();
            testResult.put("signalData", signalData);
            testResult.put("validationResult", validationResult);
            testResult.put("isValid", validationResult.isValid());
            testResult.put("message", validationResult.getMessage());
            
            if (validationResult.isValid()) {
                // Calculate position size and targets
                double entryPrice = currentPrice;
                double stopLoss = ((Number) signalData.getOrDefault("stopLoss", entryPrice * 0.98)).doubleValue();
                boolean isBullish = "BULLISH".equalsIgnoreCase((String) signalData.get("signalType"));
                
                double recommendedPositionSize = validationResult.getRecommendedPositionSize();
                int safePositionSize = (int) Math.floor(recommendedPositionSize);
                
                testResult.put("recommendedPositionSize", safePositionSize);
                testResult.put("riskRewardRatio", validationResult.getRiskRewardRatio());
                testResult.put("calculatedTargets", riskManagementService.calculateSimpleTargets(entryPrice, stopLoss, isBullish));
            }
            
            testResult.put("timestamp", LocalDateTime.now());
            testResult.put("status", "success");
            
            log.info("✅ [SignalTest] Signal validation test completed: Valid={}", validationResult.isValid());
            return ResponseEntity.ok(testResult);
            
        } catch (Exception e) {
            log.error("🚨 [SignalTest] Error testing signal validation: {}", e.getMessage(), e);
            Map<String, Object> errorResponse = new HashMap<>();
            errorResponse.put("error", "Failed to test signal validation");
            errorResponse.put("message", e.getMessage());
            errorResponse.put("timestamp", LocalDateTime.now());
            return ResponseEntity.internalServerError().body(errorResponse);
        }
    }
    
    /**
     * Emergency risk reset
     */
    @PostMapping("/emergency-reset-risk")
    public ResponseEntity<Map<String, Object>> emergencyResetRisk() {
        try {
            log.warn("🚨 [EmergencyReset] Emergency risk parameter reset initiated");
            
            Map<String, Object> resetResult = riskManagementService.resetToSafeRiskParameters();
            resetResult.put("portfolioSnapshot", portfolioManagementService.getPortfolioSummary());
            resetResult.put("updatedRiskStats", riskManagementService.getRiskManagementStats());
            
            resetResult.put("timestamp", LocalDateTime.now());
            resetResult.put("status", "success");
            
            log.warn("✅ [EmergencyReset] Risk parameters reset completed successfully");
            return ResponseEntity.ok(resetResult);
            
        } catch (Exception e) {
            log.error("🚨 [EmergencyReset] Error during emergency reset: {}", e.getMessage(), e);
            Map<String, Object> errorResponse = new HashMap<>();
            errorResponse.put("error", "Failed to reset risk parameters");
            errorResponse.put("message", e.getMessage());
            errorResponse.put("timestamp", LocalDateTime.now());
            return ResponseEntity.internalServerError().body(errorResponse);
        }
    }
    
    /**
     * Get performance insights and analytics
     */
    @GetMapping("/performance-insights")
    public ResponseEntity<Map<String, Object>> getPerformanceInsights() {
        try {
            log.info("📈 [PerformanceInsights] Generating performance analytics");
            
            Map<String, Object> insights = new HashMap<>();
            
            // Portfolio performance
            insights.put("portfolioSummary", portfolioManagementService.getPortfolioSummary());
            insights.put("portfolioInsights", portfolioManagementService.getPortfolioInsights());
            insights.put("riskMetrics", riskManagementService.getRiskManagementStats());
            
            // Additional analytics
            double portfolioValue = portfolioManagementService.getCurrentPortfolioValue();
            double initialCapital = 1000000.0; // Should come from config
            double totalReturn = ((portfolioValue - initialCapital) / initialCapital) * 100;
            
            insights.put("totalReturnPercentage", totalReturn);
            insights.put("portfolioGrowth", portfolioValue - initialCapital);
            
            insights.put("timestamp", LocalDateTime.now());
            insights.put("status", "success");
            
            log.info("✅ [PerformanceInsights] Performance insights generated successfully");
            return ResponseEntity.ok(insights);
            
        } catch (Exception e) {
            log.error("🚨 [PerformanceInsights] Error generating performance insights: {}", e.getMessage(), e);
            Map<String, Object> errorResponse = new HashMap<>();
            errorResponse.put("error", "Failed to generate performance insights");
            errorResponse.put("message", e.getMessage());
            errorResponse.put("timestamp", LocalDateTime.now());
            return ResponseEntity.internalServerError().body(errorResponse);
        }
    }
    
    /**
     * Get comprehensive system status
     */
    @GetMapping("/system-status")
    public ResponseEntity<Map<String, Object>> getSystemStatus() {
        try {
            log.info("🔍 [SystemStatus] Checking comprehensive system status");
            
            Map<String, Object> status = new HashMap<>();
            
            // System health indicators
            status.put("portfolioService", "operational");
            status.put("riskManagement", "operational");
            status.put("timestamp", LocalDateTime.now());
            
            // Quick metrics
            status.put("portfolioValue", portfolioManagementService.getCurrentPortfolioValue());
            status.put("riskConfiguration", riskManagementService.getRiskConfiguration());
            status.put("dailyPnL", portfolioManagementService.getTodayPnL());
            
            status.put("systemHealth", "healthy");
            status.put("status", "success");
            
            log.info("✅ [SystemStatus] System status check completed successfully");
            return ResponseEntity.ok(status);
            
        } catch (Exception e) {
            log.error("🚨 [SystemStatus] Error checking system status: {}", e.getMessage(), e);
            Map<String, Object> errorResponse = new HashMap<>();
            errorResponse.put("error", "Failed to check system status");
            errorResponse.put("message", e.getMessage());
            errorResponse.put("systemHealth", "degraded");
            errorResponse.put("timestamp", LocalDateTime.now());
            return ResponseEntity.internalServerError().body(errorResponse);
        }
    }
} 