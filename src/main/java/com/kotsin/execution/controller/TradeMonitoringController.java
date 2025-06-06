package com.kotsin.execution.controller;

import com.kotsin.execution.model.ActiveTrade;
import com.kotsin.execution.model.TradeResult;
import com.kotsin.execution.service.TradeStateManager;
import com.kotsin.execution.service.TradeHistoryService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.HashMap;

/**
 * REST controller for monitoring ongoing trades and signal execution history
 */
@RestController
@RequestMapping("/api/v1/trades")
@RequiredArgsConstructor
@Slf4j
@CrossOrigin(origins = "*") // Allow frontend access
public class TradeMonitoringController {
    
    private final TradeStateManager tradeStateManager;
    private final TradeHistoryService tradeHistoryService;
    
    /**
     * Get all currently active trades with real-time P&L
     * GET /api/v1/trades/active
     */
    @GetMapping("/active")
    public ResponseEntity<Map<String, Object>> getActiveTrades() {
        try {
            Map<String, ActiveTrade> activeTrades = tradeStateManager.getAllActiveTrades();
            
            Map<String, Object> response = new HashMap<>();
            response.put("count", activeTrades.size());
            response.put("trades", activeTrades.values());
            response.put("timestamp", LocalDateTime.now());
            
            // Calculate summary metrics
            double totalPnL = activeTrades.values().stream()
                    .mapToDouble(trade -> trade.getCurrentPnL())
                    .sum();
            
            long bullishCount = activeTrades.values().stream()
                    .filter(ActiveTrade::isBullish)
                    .count();
            
            long bearishCount = activeTrades.size() - bullishCount;
            
            Map<String, Object> summary = new HashMap<>();
            summary.put("totalPnL", totalPnL);
            summary.put("bullishTrades", bullishCount);
            summary.put("bearishTrades", bearishCount);
            summary.put("totalRiskAmount", activeTrades.values().stream()
                    .mapToDouble(ActiveTrade::getRiskAmount)
                    .sum());
            
            response.put("summary", summary);
            
            log.info("📊 Active trades API called: {} trades, Total P&L: {}", activeTrades.size(), totalPnL);
            
            return ResponseEntity.ok(response);
            
        } catch (Exception e) {
            log.error("🚨 Error fetching active trades: {}", e.getMessage(), e);
            return ResponseEntity.internalServerError()
                    .body(Map.of("error", "Failed to fetch active trades", "message", e.getMessage()));
        }
    }
    
    /**
     * Get active trades for a specific script
     * GET /api/v1/trades/active/script/{scripCode}
     */
    @GetMapping("/active/script/{scripCode}")
    public ResponseEntity<Map<String, Object>> getActiveTradesForScript(@PathVariable String scripCode) {
        try {
            Map<String, ActiveTrade> trades = tradeStateManager.getActiveTradesForScript(scripCode);
            
            Map<String, Object> response = new HashMap<>();
            response.put("scripCode", scripCode);
            response.put("count", trades.size());
            response.put("trades", trades.values());
            response.put("timestamp", LocalDateTime.now());
            
            return ResponseEntity.ok(response);
            
        } catch (Exception e) {
            log.error("🚨 Error fetching active trades for {}: {}", scripCode, e.getMessage(), e);
            return ResponseEntity.internalServerError()
                    .body(Map.of("error", "Failed to fetch trades for script", "scripCode", scripCode));
        }
    }
    
    /**
     * Get today's completed trades and results
     * GET /api/v1/trades/history/today
     */
    @GetMapping("/history/today")
    public ResponseEntity<Map<String, Object>> getTodaysTradeHistory() {
        try {
            LocalDate today = LocalDate.now();
            List<TradeResult> todaysResults = tradeHistoryService.getTradeResultsForDate(today);
            
            Map<String, Object> response = new HashMap<>();
            response.put("date", today);
            response.put("count", todaysResults.size());
            response.put("trades", todaysResults);
            
            // Calculate today's performance metrics
            double totalPnL = todaysResults.stream()
                    .mapToDouble(TradeResult::getPnL)
                    .sum();
            
            long profitableTrades = todaysResults.stream()
                    .filter(result -> result.getPnL() > 0)
                    .count();
            
            long lossTrades = todaysResults.stream()
                    .filter(result -> result.getPnL() < 0)
                    .count();
            
            double winRate = todaysResults.isEmpty() ? 0.0 : 
                    (double) profitableTrades / todaysResults.size() * 100.0;
            
            Map<String, Object> performance = new HashMap<>();
            performance.put("totalPnL", totalPnL);
            performance.put("profitableTrades", profitableTrades);
            performance.put("lossTrades", lossTrades);
            performance.put("winRate", winRate);
            performance.put("avgPnL", todaysResults.isEmpty() ? 0.0 : totalPnL / todaysResults.size());
            
            response.put("performance", performance);
            response.put("timestamp", LocalDateTime.now());
            
            return ResponseEntity.ok(response);
            
        } catch (Exception e) {
            log.error("🚨 Error fetching today's trade history: {}", e.getMessage(), e);
            return ResponseEntity.internalServerError()
                    .body(Map.of("error", "Failed to fetch today's trade history"));
        }
    }
    
    /**
     * Get today's signals and their execution status
     * GET /api/v1/trades/signals/today
     */
    @GetMapping("/signals/today")
    public ResponseEntity<Map<String, Object>> getTodaysSignals() {
        try {
            LocalDate today = LocalDate.now();
            Map<String, Object> signalSummary = tradeHistoryService.getTodaysSignalSummary(today);
            
            return ResponseEntity.ok(signalSummary);
            
        } catch (Exception e) {
            log.error("🚨 Error fetching today's signals: {}", e.getMessage(), e);
            return ResponseEntity.internalServerError()
                    .body(Map.of("error", "Failed to fetch today's signals"));
        }
    }
    
    /**
     * Get detailed trade information by trade ID
     * GET /api/v1/trades/{tradeId}
     */
    @GetMapping("/{tradeId}")
    public ResponseEntity<Map<String, Object>> getTradeDetails(@PathVariable String tradeId) {
        try {
            ActiveTrade activeTrade = tradeStateManager.getActiveTrade(tradeId);
            
            if (activeTrade != null) {
                Map<String, Object> response = new HashMap<>();
                response.put("trade", activeTrade);
                response.put("status", "ACTIVE");
                response.put("currentPnL", activeTrade.getCurrentPnL());
                response.put("timestamp", LocalDateTime.now());
                
                return ResponseEntity.ok(response);
            }
            
            // Check completed trades
            TradeResult completedTrade = tradeHistoryService.getTradeResultById(tradeId);
            if (completedTrade != null) {
                Map<String, Object> response = new HashMap<>();
                response.put("trade", completedTrade);
                response.put("status", "COMPLETED");
                response.put("finalPnL", completedTrade.getPnL());
                response.put("timestamp", LocalDateTime.now());
                
                return ResponseEntity.ok(response);
            }
            
            return ResponseEntity.notFound().build();
            
        } catch (Exception e) {
            log.error("🚨 Error fetching trade details for {}: {}", tradeId, e.getMessage(), e);
            return ResponseEntity.internalServerError()
                    .body(Map.of("error", "Failed to fetch trade details", "tradeId", tradeId));
        }
    }
    
    /**
     * Get trade execution statistics
     * GET /api/v1/trades/stats
     */
    @GetMapping("/stats")
    public ResponseEntity<Map<String, Object>> getTradeStatistics() {
        try {
            Map<String, Object> stats = new HashMap<>();
            
            // Active trades stats
            Map<String, ActiveTrade> activeTrades = tradeStateManager.getAllActiveTrades();
            stats.put("activeTrades", activeTrades.size());
            
            // Today's performance
            LocalDate today = LocalDate.now();
            List<TradeResult> todaysResults = tradeHistoryService.getTradeResultsForDate(today);
            
            Map<String, Object> todayStats = new HashMap<>();
            todayStats.put("completedTrades", todaysResults.size());
            todayStats.put("totalPnL", todaysResults.stream().mapToDouble(TradeResult::getPnL).sum());
            todayStats.put("winRate", todaysResults.isEmpty() ? 0.0 : 
                    todaysResults.stream().filter(r -> r.getPnL() > 0).count() * 100.0 / todaysResults.size());
            
            stats.put("today", todayStats);
            
            // Strategy breakdown
            Map<String, Long> strategyBreakdown = activeTrades.values().stream()
                    .collect(java.util.stream.Collectors.groupingBy(
                            ActiveTrade::getStrategyName,
                            java.util.stream.Collectors.counting()
                    ));
            
            stats.put("strategyBreakdown", strategyBreakdown);
            stats.put("timestamp", LocalDateTime.now());
            
            return ResponseEntity.ok(stats);
            
        } catch (Exception e) {
            log.error("🚨 Error fetching trade statistics: {}", e.getMessage(), e);
            return ResponseEntity.internalServerError()
                    .body(Map.of("error", "Failed to fetch statistics"));
        }
    }
    
    /**
     * Force close a trade (emergency function)
     * POST /api/v1/trades/{tradeId}/close
     */
    @PostMapping("/{tradeId}/close")
    public ResponseEntity<Map<String, Object>> forceCloseTrade(
            @PathVariable String tradeId,
            @RequestParam(required = false) String reason) {
        try {
            boolean success = tradeStateManager.forceCloseTrade(tradeId, reason != null ? reason : "Manual close");
            
            Map<String, Object> response = new HashMap<>();
            response.put("tradeId", tradeId);
            response.put("success", success);
            response.put("timestamp", LocalDateTime.now());
            
            if (success) {
                response.put("message", "Trade closed successfully");
                log.info("🔴 Trade {} manually closed", tradeId);
            } else {
                response.put("message", "Trade not found or already closed");
            }
            
            return ResponseEntity.ok(response);
            
        } catch (Exception e) {
            log.error("🚨 Error closing trade {}: {}", tradeId, e.getMessage(), e);
            return ResponseEntity.internalServerError()
                    .body(Map.of("error", "Failed to close trade", "tradeId", tradeId));
        }
    }
    
    /**
     * Get system health and trade execution status
     * GET /api/v1/trades/health
     */
    @GetMapping("/health")
    public ResponseEntity<Map<String, Object>> getSystemHealth() {
        try {
            Map<String, Object> health = new HashMap<>();
            health.put("status", "HEALTHY");
            health.put("timestamp", LocalDateTime.now());
            health.put("activeTrades", tradeStateManager.getAllActiveTrades().size());
            health.put("tradeExecutionEnabled", true); // TODO: Make configurable
            
            return ResponseEntity.ok(health);
            
        } catch (Exception e) {
            log.error("🚨 Error checking system health: {}", e.getMessage(), e);
            return ResponseEntity.internalServerError()
                    .body(Map.of("status", "UNHEALTHY", "error", e.getMessage()));
        }
    }
} 