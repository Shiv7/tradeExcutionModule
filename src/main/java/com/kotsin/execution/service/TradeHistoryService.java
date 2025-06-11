package com.kotsin.execution.service;

import com.kotsin.execution.model.ActiveTrade;
import com.kotsin.execution.model.TradeResult;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * Service for managing trade history, completed trades, and signal tracking
 */
@Service
@Slf4j
@RequiredArgsConstructor
public class TradeHistoryService {
    
    // In-memory storage for completed trades (in production, use database)
    private final Map<String, TradeResult> completedTrades = new ConcurrentHashMap<>();
    private final Map<LocalDate, List<TradeResult>> tradesByDate = new ConcurrentHashMap<>();
    private final Map<LocalDate, List<Map<String, Object>>> signalsByDate = new ConcurrentHashMap<>();
    
    /**
     * Store a completed trade result
     */
    public void storeTradeResult(TradeResult tradeResult) {
        try {
            // Store by trade ID
            completedTrades.put(tradeResult.getTradeId(), tradeResult);
            
            // Store by date
            LocalDate tradeDate = tradeResult.getExitTime().toLocalDate();
            tradesByDate.computeIfAbsent(tradeDate, k -> new ArrayList<>()).add(tradeResult);
            
            log.info("📊 Trade result stored: {} - P&L: {}", tradeResult.getTradeId(), tradeResult.getPnL());
            
        } catch (Exception e) {
            log.error("🚨 Error storing trade result: {}", e.getMessage(), e);
        }
    }
    
    /**
     * Log a signal for tracking (whether it got executed or not)
     */
    public void logSignal(String scripCode, String signal, String strategy, String reason, boolean executed) {
        try {
            LocalDate today = LocalDate.now();
            
            Map<String, Object> signalRecord = new HashMap<>();
            signalRecord.put("timestamp", LocalDateTime.now());
            signalRecord.put("scripCode", scripCode);
            signalRecord.put("signal", signal);
            signalRecord.put("strategy", strategy);
            signalRecord.put("reason", reason);
            signalRecord.put("executed", executed);
            
            signalsByDate.computeIfAbsent(today, k -> new ArrayList<>()).add(signalRecord);
            
            log.debug("📝 Signal logged: {} {} {} - Executed: {}", strategy, signal, scripCode, executed);
            
        } catch (Exception e) {
            log.error("🚨 Error logging signal: {}", e.getMessage(), e);
        }
    }
    
    /**
     * Get trade results for a specific date
     */
    public List<TradeResult> getTradeResultsForDate(LocalDate date) {
        return tradesByDate.getOrDefault(date, new ArrayList<>());
    }
    
    /**
     * Get a specific trade result by ID
     */
    public TradeResult getTradeResultById(String tradeId) {
        return completedTrades.get(tradeId);
    }
    
    /**
     * Get today's signal summary with execution status
     */
    public Map<String, Object> getTodaysSignalSummary(LocalDate date) {
        List<Map<String, Object>> todaysSignals = signalsByDate.getOrDefault(date, new ArrayList<>());
        
        Map<String, Object> summary = new HashMap<>();
        summary.put("date", date);
        summary.put("totalSignals", todaysSignals.size());
        
        // Count executed vs not executed
        long executedCount = todaysSignals.stream()
                .filter(signal -> (Boolean) signal.get("executed"))
                .count();
        
        long rejectedCount = todaysSignals.size() - executedCount;
        
        summary.put("executedSignals", executedCount);
        summary.put("rejectedSignals", rejectedCount);
        
        // Group by strategy
        Map<String, Long> strategyBreakdown = todaysSignals.stream()
                .collect(Collectors.groupingBy(
                        signal -> (String) signal.get("strategy"),
                        Collectors.counting()
                ));
        
        summary.put("strategyBreakdown", strategyBreakdown);
        
        // Group by signal type
        Map<String, Long> signalTypeBreakdown = todaysSignals.stream()
                .collect(Collectors.groupingBy(
                        signal -> (String) signal.get("signal"),
                        Collectors.counting()
                ));
        
        summary.put("signalTypeBreakdown", signalTypeBreakdown);
        
        // Recent signals (last 10)
        List<Map<String, Object>> recentSignals = todaysSignals.stream()
                .sorted((s1, s2) -> ((LocalDateTime) s2.get("timestamp"))
                        .compareTo((LocalDateTime) s1.get("timestamp")))
                .limit(10)
                .collect(Collectors.toList());
        
        summary.put("recentSignals", recentSignals);
        
        // Rejection reasons
        Map<String, Long> rejectionReasons = todaysSignals.stream()
                .filter(signal -> !(Boolean) signal.get("executed"))
                .collect(Collectors.groupingBy(
                        signal -> (String) signal.get("reason"),
                        Collectors.counting()
                ));
        
        summary.put("rejectionReasons", rejectionReasons);
        summary.put("timestamp", LocalDateTime.now());
        
        return summary;
    }
    
    /**
     * Get performance metrics for a date range
     */
    public Map<String, Object> getPerformanceMetrics(LocalDate startDate, LocalDate endDate) {
        List<TradeResult> trades = new ArrayList<>();
        
        // Collect all trades in date range
        for (LocalDate date = startDate; !date.isAfter(endDate); date = date.plusDays(1)) {
            trades.addAll(getTradeResultsForDate(date));
        }
        
        Map<String, Object> metrics = new HashMap<>();
        metrics.put("period", startDate + " to " + endDate);
        metrics.put("totalTrades", trades.size());
        
        if (!trades.isEmpty()) {
            double totalPnL = trades.stream().mapToDouble(TradeResult::getPnL).sum();
            long winners = trades.stream().filter(TradeResult::isWinner).count();
            long losers = trades.stream().filter(TradeResult::isLoser).count();
            
            double winRate = (double) winners / trades.size() * 100.0;
            double avgPnL = totalPnL / trades.size();
            
            double avgWinner = trades.stream()
                    .filter(TradeResult::isWinner)
                    .mapToDouble(TradeResult::getPnL)
                    .average()
                    .orElse(0.0);
            
            double avgLoser = trades.stream()
                    .filter(TradeResult::isLoser)
                    .mapToDouble(TradeResult::getPnL)
                    .average()
                    .orElse(0.0);
            
            metrics.put("totalPnL", totalPnL);
            metrics.put("winRate", winRate);
            metrics.put("avgPnL", avgPnL);
            metrics.put("winners", winners);
            metrics.put("losers", losers);
            metrics.put("avgWinner", avgWinner);
            metrics.put("avgLoser", avgLoser);
            
            if (avgLoser != 0) {
                metrics.put("profitFactor", Math.abs(avgWinner / avgLoser));
            }
        }
        
        metrics.put("timestamp", LocalDateTime.now());
        return metrics;
    }
    
    /**
     * Get all completed trades
     */
    public Collection<TradeResult> getAllCompletedTrades() {
        return completedTrades.values();
    }
    
    /**
     * Get trade count statistics
     */
    public Map<String, Object> getTradeCountStats() {
        Map<String, Object> stats = new HashMap<>();
        stats.put("totalCompletedTrades", completedTrades.size());
        
        LocalDate today = LocalDate.now();
        List<TradeResult> todaysTrades = getTradeResultsForDate(today);
        stats.put("todaysTrades", todaysTrades.size());
        
        List<Map<String, Object>> todaysSignals = signalsByDate.getOrDefault(today, new ArrayList<>());
        stats.put("todaysSignals", todaysSignals.size());
        
        return stats;
    }
    
    /**
     * Clear old data (cleanup method for memory management)
     */
    public void cleanupOldData(int daysToKeep) {
        try {
            LocalDate cutoffDate = LocalDate.now().minusDays(daysToKeep);
            
            // Clean up old trades
            tradesByDate.entrySet().removeIf(entry -> entry.getKey().isBefore(cutoffDate));
            
            // Clean up old signals
            signalsByDate.entrySet().removeIf(entry -> entry.getKey().isBefore(cutoffDate));
            
            log.info("🧹 Cleaned up data older than {} days", daysToKeep);
            
        } catch (Exception e) {
            log.error("🚨 Error during cleanup: {}", e.getMessage(), e);
        }
    }
    
    /**
     * Get all completed trades as a list
     */
    public List<TradeResult> getAllTradeResults() {
        return new ArrayList<>(completedTrades.values());
    }
    
    /**
     * Get trade results by date range
     */
    public List<TradeResult> getTradeResultsByDateRange(LocalDateTime startTime, LocalDateTime endTime) {
        try {
            LocalDate startDate = startTime.toLocalDate();
            LocalDate endDate = endTime.toLocalDate();
            
            List<TradeResult> results = new ArrayList<>();
            for (LocalDate date = startDate; !date.isAfter(endDate); date = date.plusDays(1)) {
                results.addAll(getTradeResultsForDate(date));
            }
            
            // Filter by exact time range
            return results.stream()
                    .filter(trade -> {
                        LocalDateTime exitTime = trade.getExitTime();
                        return exitTime != null && 
                               !exitTime.isBefore(startTime) && 
                               !exitTime.isAfter(endTime);
                    })
                    .collect(Collectors.toList());
                    
        } catch (Exception e) {
            log.error("🚨 Error getting trade results by date range: {}", e.getMessage(), e);
            return new ArrayList<>();
        }
    }
} 