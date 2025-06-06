package com.kotsin.execution.service;

import com.kotsin.execution.model.ActiveTrade;
import com.kotsin.execution.model.TradeResult;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;

/**
 * Service to calculate comprehensive profit/loss and create detailed trade results
 */
@Service
@Slf4j
@RequiredArgsConstructor
public class ProfitLossCalculator {
    
    private final IndicatorDataService indicatorDataService;
    
    /**
     * Calculate comprehensive trade result with detailed information
     */
    public TradeResult calculateTradeResult(ActiveTrade trade) {
        try {
            log.info("💰 Calculating trade result for: {}", trade.getTradeId());
            
            // Create base trade result
            TradeResult result = TradeResult.builder()
                    .tradeId(trade.getTradeId())
                    .scripCode(trade.getScripCode())
                    .companyName(trade.getCompanyName())
                    .exchange(trade.getExchange())
                    .exchangeType(trade.getExchangeType())
                    .strategyName(trade.getStrategyName())
                    .signalType(trade.getSignalType())
                    .signalTime(trade.getSignalTime())
                    .originalSignalId(trade.getOriginalSignalId())
                    .entryPrice(trade.getEntryPrice())
                    .entryTime(trade.getEntryTime())
                    .exitPrice(trade.getExitPrice())
                    .exitTime(trade.getExitTime())
                    .exitReason(trade.getExitReason())
                    .positionSize(trade.getPositionSize())
                    .target1Hit(trade.getTarget1Hit())
                    .target2Hit(trade.getTarget2Hit())
                    .initialStopLoss(trade.getStopLoss())
                    .finalStopLoss(trade.getTrailingStopLoss() != null ? trade.getTrailingStopLoss() : trade.getStopLoss())
                    .riskAmount(trade.getRiskAmount())
                    .daysHeld(trade.getDaysHeld())
                    .highSinceEntry(trade.getHighSinceEntry())
                    .lowSinceEntry(trade.getLowSinceEntry())
                    .riskRewardRatios(trade.getRiskRewardRatios())
                    .resultGeneratedTime(LocalDateTime.now())
                    .systemVersion("TradeExecution-v1.0.0")
                    .build();
            
            // Calculate financial metrics
            result.calculateProfitLoss();
            result.calculateDuration();
            
            // Determine if it's a multi-day trade
            result.setIsMultiDayTrade(result.getDurationMinutes() != null && result.getDurationMinutes() > 1440);
            
            // Add comprehensive metadata with indicator information
            addDetailedMetadata(result, trade);
            
            // Add entry and exit indicator snapshots
            addIndicatorSnapshots(result, trade);
            
            // Create detailed summary message
            String detailedSummary = createDetailedSummary(result, trade);
            result.addMetadata("detailedSummary", detailedSummary);
            
            log.info("✅ Trade result calculated: P&L={}, ROI={}%, Duration={}min", 
                    result.getProfitLoss(), result.getRoi(), result.getDurationMinutes());
            
            return result;
            
        } catch (Exception e) {
            log.error("Error calculating trade result for {}: {}", trade.getTradeId(), e.getMessage(), e);
            return null;
        }
    }
    
    /**
     * Add detailed metadata about the trade
     */
    private void addDetailedMetadata(TradeResult result, ActiveTrade trade) {
        // Original signal data
        Map<String, Object> originalSignal = (Map<String, Object>) trade.getMetadata("originalSignal");
        if (originalSignal != null) {
            result.addMetadata("originalSignalData", originalSignal);
        }
        
        // Trade timing information
        result.addMetadata("signalToEntryDelayMinutes", 
            calculateDelayMinutes(trade.getSignalTime(), trade.getEntryTime()));
        result.addMetadata("entryToExitMinutes", result.getDurationMinutes());
        
        // Risk metrics
        result.addMetadata("riskPercentage", calculateRiskPercentage(trade));
        result.addMetadata("rewardRiskRatio", calculateRewardRiskRatio(result));
        
        // Performance classification
        result.addMetadata("performanceCategory", classifyPerformance(result));
        result.addMetadata("exitCategory", classifyExit(result.getExitReason()));
        
        // Market context
        result.addMetadata("priceMovementPercent", calculatePriceMovementPercent(trade));
        result.addMetadata("maxDrawdownPercent", calculateMaxDrawdownPercent(trade));
        result.addMetadata("maxRunupPercent", calculateMaxRunupPercent(trade));
    }
    
    /**
     * Add indicator snapshots at entry and exit times
     */
    private void addIndicatorSnapshots(TradeResult result, ActiveTrade trade) {
        try {
            // Get current (exit time) indicators
            Map<String, Object> exitIndicators = indicatorDataService.getComprehensiveIndicators(trade.getScripCode());
            result.addMetadata("exitIndicators", exitIndicators);
            
            // Create indicator summaries for different timeframes
            Map<String, String> indicatorSummaries = new HashMap<>();
            for (String timeframe : new String[]{"15m", "30m"}) {
                Map<String, Object> indicators = indicatorDataService.getCurrentIndicators(trade.getScripCode(), timeframe);
                if (!indicators.isEmpty()) {
                    String summary = indicatorDataService.createIndicatorSummary(indicators, timeframe);
                    indicatorSummaries.put(timeframe, summary);
                }
            }
            result.addMetadata("indicatorSummaries", indicatorSummaries);
            
            // Entry indicators (from original signal)
            Map<String, Object> originalSignal = (Map<String, Object>) trade.getMetadata("originalSignal");
            if (originalSignal != null) {
                result.addMetadata("entryIndicators", originalSignal);
            }
            
        } catch (Exception e) {
            log.error("Error adding indicator snapshots: {}", e.getMessage());
        }
    }
    
    /**
     * Create a comprehensive detailed summary of the trade
     */
    private String createDetailedSummary(TradeResult result, ActiveTrade trade) {
        StringBuilder summary = new StringBuilder();
        
        // Trade header
        summary.append("=== TRADE EXECUTION SUMMARY ===\n");
        summary.append(String.format("🆔 Trade ID: %s\n", result.getTradeId()));
        summary.append(String.format("📊 Script: %s (%s)\n", result.getCompanyName(), result.getScripCode()));
        summary.append(String.format("🎯 Strategy: %s\n", result.getStrategyName()));
        summary.append(String.format("📈 Direction: %s\n", result.getSignalType()));
        
        // Timing details
        summary.append("\n--- TIMING DETAILS ---\n");
        summary.append(String.format("📡 Signal Time: %s\n", formatDateTime(result.getSignalTime())));
        summary.append(String.format("🚀 Entry Time: %s\n", formatDateTime(result.getEntryTime())));
        summary.append(String.format("🏁 Exit Time: %s\n", formatDateTime(result.getExitTime())));
        summary.append(String.format("⏱️ Duration: %d minutes (%s)\n", 
            result.getDurationMinutes(), formatDuration(result.getDurationMinutes())));
        
        // Entry details with indicators
        summary.append("\n--- ENTRY DETAILS ---\n");
        summary.append(String.format("💰 Entry Price: %.2f\n", result.getEntryPrice()));
        summary.append(String.format("📦 Position Size: %d\n", result.getPositionSize()));
        summary.append(String.format("🛡️ Stop Loss: %.2f\n", result.getInitialStopLoss()));
        summary.append(String.format("🎯 Targets: T1=%.2f, T2=%.2f\n", 
            getTargetFromMetadata(trade, "target1"), getTargetFromMetadata(trade, "target2")));
        
        // Add entry indicator context
        Map<String, String> indicatorSummaries = (Map<String, String>) result.getMetadata("indicatorSummaries");
        if (indicatorSummaries != null) {
            summary.append("\n📊 ENTRY INDICATORS:\n");
            indicatorSummaries.forEach((timeframe, indicatorSummary) -> {
                summary.append(String.format("  %s\n", indicatorSummary));
            });
        }
        
        // Exit details
        summary.append("\n--- EXIT DETAILS ---\n");
        summary.append(String.format("🏁 Exit Price: %.2f\n", result.getExitPrice()));
        summary.append(String.format("❓ Exit Reason: %s\n", result.getExitReason()));
        if (result.getFinalStopLoss() != null && !result.getFinalStopLoss().equals(result.getInitialStopLoss())) {
            summary.append(String.format("🔄 Trailing Stop: %.2f\n", result.getFinalStopLoss()));
        }
        
        // Performance metrics
        summary.append("\n--- PERFORMANCE RESULTS ---\n");
        summary.append(String.format("💰 Profit/Loss: %.2f\n", result.getProfitLoss()));
        summary.append(String.format("📊 ROI: %.2f%%\n", result.getRoi()));
        summary.append(String.format("✅ Success: %s\n", result.isWinner() ? "WIN 🎉" : "LOSS 😔"));
        summary.append(String.format("⚖️ Risk-Adjusted Return: %.2fx\n", result.getRiskAdjustedReturn()));
        
        // Risk analysis
        summary.append("\n--- RISK ANALYSIS ---\n");
        summary.append(String.format("💸 Risk Amount: %.2f\n", result.getRiskAmount()));
        if (result.getMaxFavorableExcursion() != null) {
            summary.append(String.format("📈 Max Favorable: %.2f\n", result.getMaxFavorableExcursion()));
        }
        if (result.getMaxAdverseExcursion() != null) {
            summary.append(String.format("📉 Max Adverse: %.2f\n", result.getMaxAdverseExcursion()));
        }
        
        // Target analysis
        summary.append("\n--- TARGET ANALYSIS ---\n");
        summary.append(String.format("🎯 Target 1 Hit: %s\n", result.getTarget1Hit() ? "YES ✅" : "NO ❌"));
        summary.append(String.format("🎯 Target 2 Hit: %s\n", result.getTarget2Hit() ? "YES ✅" : "NO ❌"));
        
        // Market movement context
        Double priceMovement = (Double) result.getMetadata("priceMovementPercent");
        if (priceMovement != null) {
            summary.append(String.format("📊 Price Movement: %.2f%%\n", priceMovement));
        }
        
        summary.append("\n=== END SUMMARY ===");
        
        return summary.toString();
    }
    
    // Helper methods
    private Long calculateDelayMinutes(LocalDateTime start, LocalDateTime end) {
        if (start == null || end == null) return null;
        return java.time.Duration.between(start, end).toMinutes();
    }
    
    private double calculateRiskPercentage(ActiveTrade trade) {
        if (trade.getEntryPrice() == null || trade.getStopLoss() == null) return 0.0;
        return Math.abs((trade.getEntryPrice() - trade.getStopLoss()) / trade.getEntryPrice()) * 100;
    }
    
    private double calculateRewardRiskRatio(TradeResult result) {
        if (result.getRiskAmount() == null || result.getRiskAmount() == 0) return 0.0;
        return result.getProfitLoss() / result.getRiskAmount();
    }
    
    private String classifyPerformance(TradeResult result) {
        if (result.getRoi() == null) return "Unknown";
        double roi = result.getRoi();
        
        if (roi > 5) return "Excellent";
        else if (roi > 2) return "Good";
        else if (roi > 0) return "Profitable";
        else if (roi > -2) return "Small Loss";
        else if (roi > -5) return "Moderate Loss";
        else return "Large Loss";
    }
    
    private String classifyExit(String exitReason) {
        if (exitReason == null) return "Unknown";
        switch (exitReason) {
            case "TARGET_1": return "First Target Hit";
            case "TARGET_2": return "Second Target Hit";
            case "STOP_LOSS": return "Stop Loss Hit";
            case "TIME_LIMIT": return "Time-based Exit";
            default: return exitReason;
        }
    }
    
    private double calculatePriceMovementPercent(ActiveTrade trade) {
        if (trade.getEntryPrice() == null || trade.getExitPrice() == null) return 0.0;
        return ((trade.getExitPrice() - trade.getEntryPrice()) / trade.getEntryPrice()) * 100;
    }
    
    private double calculateMaxDrawdownPercent(ActiveTrade trade) {
        if (trade.getEntryPrice() == null || trade.getLowSinceEntry() == null) return 0.0;
        if (trade.isBullish()) {
            return ((trade.getLowSinceEntry() - trade.getEntryPrice()) / trade.getEntryPrice()) * 100;
        } else {
            return ((trade.getEntryPrice() - trade.getHighSinceEntry()) / trade.getEntryPrice()) * 100;
        }
    }
    
    private double calculateMaxRunupPercent(ActiveTrade trade) {
        if (trade.getEntryPrice() == null || trade.getHighSinceEntry() == null) return 0.0;
        if (trade.isBullish()) {
            return ((trade.getHighSinceEntry() - trade.getEntryPrice()) / trade.getEntryPrice()) * 100;
        } else {
            return ((trade.getEntryPrice() - trade.getLowSinceEntry()) / trade.getEntryPrice()) * 100;
        }
    }
    
    private Double getTargetFromMetadata(ActiveTrade trade, String targetKey) {
        try {
            if ("target1".equals(targetKey)) return trade.getTarget1();
            if ("target2".equals(targetKey)) return trade.getTarget2();
            return null;
        } catch (Exception e) {
            return null;
        }
    }
    
    private String formatDateTime(LocalDateTime dateTime) {
        if (dateTime == null) return "N/A";
        return dateTime.toString().replace("T", " ");
    }
    
    private String formatDuration(Long minutes) {
        if (minutes == null) return "N/A";
        long hours = minutes / 60;
        long mins = minutes % 60;
        if (hours > 0) {
            return String.format("%dh %dm", hours, mins);
        } else {
            return String.format("%dm", mins);
        }
    }
} 