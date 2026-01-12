package com.kotsin.execution.service;

import com.kotsin.execution.model.ActiveTrade;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * Portfolio Risk Manager - CRITICAL PRODUCTION COMPONENT
 *
 * Implements portfolio-level risk controls that were MISSING in original system.
 * This prevents catastrophic losses from:
 * 1. Excessive drawdowns (circuit breaker at 15%)
 * 2. Daily loss limits (kill switch at 3% daily loss)
 * 3. Correlation clustering (max 0.7 correlation with open positions)
 * 4. Position concentration (max 5 concurrent positions)
 * 5. Leverage limits (max 2x account value exposure)
 *
 * CRITICAL: Original system had NONE of these checks → could lose 20%+ in single day.
 *
 * @author Kotsin Team
 * @version 2.0 - Production Grade
 */
@Service
@Slf4j
public class PortfolioRiskManager {

    @Autowired
    private SectorMappingService sectorMappingService;

    // Portfolio state
    private final Map<LocalDate, DailyPerformance> dailyPerformance = new ConcurrentHashMap<>();
    private double accountValueAtStart;
    private double currentAccountValue;
    private double peakAccountValue;
    private boolean emergencyStopActivated = false;
    private LocalDateTime emergencyStopTime;

    // FIXED: Risk limits now externalized to application.properties
    @Value("${portfolio.max-drawdown:0.15}")
    private double maxDrawdownPercent;

    @Value("${portfolio.max-daily-loss:0.03}")
    private double maxDailyLossPercent;

    @Value("${portfolio.max-positions:5}")
    private int maxConcurrentPositions;

    @Value("${portfolio.max-correlation:0.70}")
    private double maxCorrelation;

    @Value("${portfolio.max-leverage:2.0}")
    private double maxLeverage;

    @Value("${portfolio.max-sector-concentration:0.40}")
    private double maxSectorConcentration;

    // REMOVED: Hardcoded sector mappings
    // Now using SectorMappingService with 100+ mappings from CSV

    /**
     * Validate configuration on startup
     * CRITICAL FIX: Added validation to prevent invalid config
     */
    @jakarta.annotation.PostConstruct
    public void validateConfiguration() {
        List<String> errors = new ArrayList<>();
        
        if (maxDrawdownPercent <= 0 || maxDrawdownPercent > 1.0) {
            errors.add(String.format("Invalid max-drawdown: %.2f (must be between 0 and 1.0)", maxDrawdownPercent));
        }
        
        if (maxDailyLossPercent <= 0 || maxDailyLossPercent > 1.0) {
            errors.add(String.format("Invalid max-daily-loss: %.2f (must be between 0 and 1.0)", maxDailyLossPercent));
        }
        
        if (maxConcurrentPositions <= 0 || maxConcurrentPositions > 50) {
            errors.add(String.format("Invalid max-positions: %d (must be between 1 and 50)", maxConcurrentPositions));
        }
        
        if (maxCorrelation <= 0 || maxCorrelation > 1.0) {
            errors.add(String.format("Invalid max-correlation: %.2f (must be between 0 and 1.0)", maxCorrelation));
        }
        
        if (maxLeverage <= 0 || maxLeverage > 10.0) {
            errors.add(String.format("Invalid max-leverage: %.2f (must be between 0 and 10.0)", maxLeverage));
        }
        
        if (maxSectorConcentration <= 0 || maxSectorConcentration > 1.0) {
            errors.add(String.format("Invalid max-sector-concentration: %.2f (must be between 0 and 1.0)", maxSectorConcentration));
        }
        
        if (!errors.isEmpty()) {
            String errorMsg = "❌ [PORTFOLIO-RISK] Invalid configuration:\n" + String.join("\n", errors);
            log.error(errorMsg);
            throw new IllegalStateException(errorMsg);
        }
        
        log.info("✅ [PORTFOLIO-RISK] Configuration validated successfully");
    }

    /**
     * Initialize portfolio with starting account value
     */
    public void initialize(double accountValue) {
        this.accountValueAtStart = accountValue;
        this.currentAccountValue = accountValue;
        this.peakAccountValue = accountValue;
        this.emergencyStopActivated = false;

        log.info("[PORTFOLIO-RISK] Initialized with {}", String.format("%.2f", accountValue));
        log.info("   Max Drawdown: {}%", maxDrawdownPercent * 100);
        log.info("   Max Daily Loss: {}%", maxDailyLossPercent * 100);
        log.info("   Max Positions: {}", maxConcurrentPositions);
        log.info("   Max Correlation: {}", maxCorrelation);
    }

    /**
     * MAIN METHOD: Check if a new trade can be taken
     *
     * This is the CRITICAL gate that prevents catastrophic losses.
     * CRITICAL FIX: Added synchronization for thread-safety
     *
     * @return true if trade is allowed, false if risk limits breached
     */
    public synchronized boolean canTakeTrade(
            ActiveTrade proposedTrade,
            List<ActiveTrade> currentPositions
    ) {
        log.debug("🔍 [PORTFOLIO-RISK] Evaluating if trade can be taken: {}", proposedTrade.getScripCode());

        // ========================================
        // CHECK 1: Emergency Stop (Circuit Breaker)
        // ========================================
        if (emergencyStopActivated) {
            log.error("🚨 [PORTFOLIO-RISK] EMERGENCY STOP ACTIVE since {}. Trade BLOCKED.",
                    emergencyStopTime);
            return false;
        }

        // ========================================
        // CHECK 2: Drawdown Limit
        // ========================================
        double currentDrawdown = calculateCurrentDrawdown();
        if (currentDrawdown >= maxDrawdownPercent) {
            log.error("[PORTFOLIO-RISK] MAX DRAWDOWN BREACHED: {}% >= {}%. Trade BLOCKED.",
                    String.format("%.2f", currentDrawdown * 100), String.format("%.2f", maxDrawdownPercent * 100));
            activateEmergencyStop("MAX_DRAWDOWN_BREACHED");
            return false;
        }

        // ========================================
        // CHECK 3: Daily Loss Limit
        // ========================================
        double dailyLoss = calculateDailyLoss();
        if (dailyLoss >= maxDailyLossPercent) {
            log.error("[PORTFOLIO-RISK] DAILY LOSS LIMIT BREACHED: {}% >= {}%. Trade BLOCKED.",
                    String.format("%.2f", dailyLoss * 100), String.format("%.2f", maxDailyLossPercent * 100));
            return false;
        }

        // ========================================
        // CHECK 4: Position Count Limit
        // ========================================
        if (currentPositions.size() >= maxConcurrentPositions) {
            log.warn("⚠️ [PORTFOLIO-RISK] Max positions reached: {} >= {}. Trade BLOCKED.",
                    currentPositions.size(), maxConcurrentPositions);
            return false;
        }

        // ========================================
        // CHECK 5: Correlation Limit
        // ========================================
        double maxCorr = calculateMaxCorrelation(proposedTrade, currentPositions);
        if (maxCorr > this.maxCorrelation) {
            log.warn("[PORTFOLIO-RISK] High correlation: {} > {}. Trade BLOCKED.",
                    String.format("%.2f", maxCorr), String.format("%.2f", this.maxCorrelation));
            return false;
        }

        // ========================================
        // CHECK 6: Sector Concentration
        // ========================================
        String proposedSector = getSector(proposedTrade.getCompanyName());
        double sectorExposure = calculateSectorExposure(proposedSector, currentPositions, proposedTrade);
        if (sectorExposure > maxSectorConcentration) {
            log.warn("[PORTFOLIO-RISK] Sector {} concentration: {}% > {}%. Trade BLOCKED.",
                    proposedSector, String.format("%.2f", sectorExposure * 100), String.format("%.2f", maxSectorConcentration * 100));
            return false;
        }

        // ========================================
        // CHECK 7: Leverage Limit
        // ========================================
        double totalExposure = calculateTotalExposure(currentPositions, proposedTrade);
        double leverage = totalExposure / currentAccountValue;
        if (leverage > maxLeverage) {
            log.warn("[PORTFOLIO-RISK] Leverage too high: {}x > {}x. Trade BLOCKED.",
                    String.format("%.2f", leverage), String.format("%.2f", maxLeverage));
            return false;
        }

        // ========================================
        // ALL CHECKS PASSED
        // ========================================
        log.info("[PORTFOLIO-RISK] Trade APPROVED: {}", proposedTrade.getScripCode());
        log.info("   Drawdown: {}%, Daily Loss: {}%, Positions: {}/{}",
                String.format("%.2f", currentDrawdown * 100), String.format("%.2f", dailyLoss * 100),
                currentPositions.size() + 1, maxConcurrentPositions);
        log.info("   Correlation: {}, Sector: {} ({}%), Leverage: {}x",
                String.format("%.2f", maxCorr), proposedSector, String.format("%.2f", sectorExposure * 100), String.format("%.2f", leverage));

        return true;
    }

    /**
     * Update portfolio value after trade result
     * CRITICAL FIX: Added synchronization for thread-safety
     */
    public synchronized void updatePortfolioValue(double newValue, double pnl) {
        this.currentAccountValue = newValue;

        // Update peak
        if (newValue > peakAccountValue) {
            peakAccountValue = newValue;
        }

        // Update daily performance
        LocalDate today = LocalDate.now();
        DailyPerformance perf = dailyPerformance.computeIfAbsent(today, k -> new DailyPerformance());
        perf.totalPnL += pnl;
        perf.tradeCount++;

        log.info("[PORTFOLIO-RISK] Portfolio updated: {} (PnL: {})", String.format("%.2f", newValue), String.format("%.2f", pnl));
        log.info("   Peak: {}, Drawdown: {}%, Daily PnL: {}",
                String.format("%.2f", peakAccountValue),
                String.format("%.2f", calculateCurrentDrawdown() * 100),
                String.format("%.2f", perf.totalPnL));
    }

    /**
     * Calculate current drawdown from peak
     */
    private double calculateCurrentDrawdown() {
        if (peakAccountValue <= 0) return 0.0;
        return (peakAccountValue - currentAccountValue) / peakAccountValue;
    }

    /**
     * Calculate today's loss percentage
     */
    private double calculateDailyLoss() {
        LocalDate today = LocalDate.now();
        DailyPerformance perf = dailyPerformance.get(today);
        if (perf == null || accountValueAtStart <= 0) return 0.0;

        // Only count losses (not gains)
        double loss = Math.min(0, perf.totalPnL);
        return Math.abs(loss) / accountValueAtStart;
    }

    /**
     * Calculate correlation with existing positions
     *
     * Simplified: Same stock = 1.0, Same sector = 0.7, Different = 0.3
     * TODO: Replace with real correlation matrix from historical data
     */
    private double calculateMaxCorrelation(ActiveTrade proposedTrade, List<ActiveTrade> positions) {
        if (positions.isEmpty()) return 0.0;

        String proposedScrip = proposedTrade.getScripCode();
        String proposedSector = getSector(proposedTrade.getCompanyName());

        double maxCorr = 0.0;
        for (ActiveTrade pos : positions) {
            double corr;
            if (proposedScrip.equals(pos.getScripCode())) {
                corr = 1.0; // Same stock
            } else if (proposedSector.equals(getSector(pos.getCompanyName()))) {
                corr = 0.70; // Same sector
            } else {
                corr = 0.30; // Different sector
            }
            maxCorr = Math.max(maxCorr, corr);
        }

        return maxCorr;
    }

    /**
     * Calculate sector exposure including proposed trade
     */
    private double calculateSectorExposure(
            String sector,
            List<ActiveTrade> positions,
            ActiveTrade proposedTrade
    ) {
        double sectorValue = 0.0;
        double totalValue = 0.0;

        // Existing positions
        for (ActiveTrade pos : positions) {
            double posValue = pos.getEntryPrice() * pos.getPositionSize();
            totalValue += posValue;
            if (getSector(pos.getCompanyName()).equals(sector)) {
                sectorValue += posValue;
            }
        }

        // Proposed trade
        double proposedValue = proposedTrade.getEntryPrice() * proposedTrade.getPositionSize();
        totalValue += proposedValue;
        if (getSector(proposedTrade.getCompanyName()).equals(sector)) {
            sectorValue += proposedValue;
        }

        if (totalValue <= 0) return 0.0;
        return sectorValue / totalValue;
    }

    /**
     * Calculate total portfolio exposure
     */
    private double calculateTotalExposure(List<ActiveTrade> positions, ActiveTrade proposedTrade) {
        double total = 0.0;

        for (ActiveTrade pos : positions) {
            total += pos.getEntryPrice() * pos.getPositionSize();
        }

        total += proposedTrade.getEntryPrice() * proposedTrade.getPositionSize();

        return total;
    }

    /**
     * Get sector for a company (using SectorMappingService)
     */
    private String getSector(String companyName) {
        if (companyName == null) return "UNKNOWN";
        return sectorMappingService.getSector(companyName);
    }

    /**
     * Activate emergency stop (circuit breaker)
     */
    private void activateEmergencyStop(String reason) {
        this.emergencyStopActivated = true;
        this.emergencyStopTime = LocalDateTime.now();

        log.error("[PORTFOLIO-RISK] EMERGENCY STOP ACTIVATED");
        log.error("   Reason: {}", reason);
        log.error("   Time: {}", emergencyStopTime);
        log.error("   Account: {}, Drawdown: {}%",
                String.format("%.2f", currentAccountValue), String.format("%.2f", calculateCurrentDrawdown() * 100));
        log.error("ALL NEW TRADES BLOCKED UNTIL MANUAL RESET");

        // TODO: Send critical alert via Telegram/Email
    }

    /**
     * Manually reset emergency stop (requires operator intervention)
     */
    public void resetEmergencyStop(String operator) {
        log.warn("⚠️ [PORTFOLIO-RISK] Emergency stop RESET by operator: {}", operator);
        this.emergencyStopActivated = false;
        this.emergencyStopTime = null;
    }

    /**
     * Cleanup old daily performance data
     * CRITICAL FIX: Prevents memory leak from unbounded map growth
     * Runs daily at 1 AM IST
     */
    @org.springframework.scheduling.annotation.Scheduled(cron = "0 0 1 * * *", zone = "Asia/Kolkata")
    public void cleanupOldPerformance() {
        LocalDate cutoff = LocalDate.now().minusDays(90);
        int removed = 0;
        
        synchronized (this) {  // Thread-safe cleanup
            Iterator<Map.Entry<LocalDate, DailyPerformance>> iterator = dailyPerformance.entrySet().iterator();
            while (iterator.hasNext()) {
                Map.Entry<LocalDate, DailyPerformance> entry = iterator.next();
                if (entry.getKey().isBefore(cutoff)) {
                    iterator.remove();
                    removed++;
                }
            }
        }
        
        if (removed > 0) {
            log.info("🧹 [PORTFOLIO-RISK] Cleaned up {} old daily performance records (kept last 90 days)", removed);
        }
    }

    /**
     * Get portfolio diagnostics
     */
    public Map<String, Object> getPortfolioDiagnostics(List<ActiveTrade> positions) {
        Map<String, Object> diagnostics = new HashMap<>();

        diagnostics.put("accountValue", currentAccountValue);
        diagnostics.put("peakValue", peakAccountValue);
        diagnostics.put("drawdown", calculateCurrentDrawdown());
        diagnostics.put("dailyLoss", calculateDailyLoss());
        diagnostics.put("emergencyStopActive", emergencyStopActivated);
        diagnostics.put("positionCount", positions.size());
        diagnostics.put("maxPositions", maxConcurrentPositions);

        double totalExposure = positions.stream()
                .mapToDouble(p -> p.getEntryPrice() * p.getPositionSize())
                .sum();
        diagnostics.put("totalExposure", totalExposure);
        diagnostics.put("leverage", totalExposure / currentAccountValue);

        // Sector breakdown
        Map<String, Double> sectorExposure = new HashMap<>();
        for (ActiveTrade pos : positions) {
            String sector = getSector(pos.getCompanyName());
            double value = pos.getEntryPrice() * pos.getPositionSize();
            sectorExposure.merge(sector, value, Double::sum);
        }
        diagnostics.put("sectorExposure", sectorExposure);

        // Today's performance
        LocalDate today = LocalDate.now();
        DailyPerformance perf = dailyPerformance.get(today);
        if (perf != null) {
            diagnostics.put("dailyPnL", perf.totalPnL);
            diagnostics.put("dailyTrades", perf.tradeCount);
        }

        return diagnostics;
    }

    /**
     * Daily performance tracker
     */
    private static class DailyPerformance {
        double totalPnL = 0.0;
        int tradeCount = 0;
    }
}
