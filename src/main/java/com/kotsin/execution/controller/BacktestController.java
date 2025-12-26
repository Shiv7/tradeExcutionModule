package com.kotsin.execution.controller;

import com.kotsin.execution.model.BacktestTrade;
import com.kotsin.execution.repository.BacktestTradeRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * BacktestController - REST API for backtest results
 * 
 * Endpoints:
 * - GET /api/backtest/results - All completed trades
 * - GET /api/backtest/results/range - By date range
 * - GET /api/backtest/summary - Win rate, P&L stats
 * - GET /api/backtest/results/profitable - Only winning trades
 * - GET /api/backtest/results/xfactor - X-factor signals only
 */
@RestController
@RequestMapping("/api/backtest")
@RequiredArgsConstructor
@Slf4j
public class BacktestController {
    
    private final BacktestTradeRepository repository;
    
    /**
     * Get all completed backtest trades
     */
    @GetMapping("/results")
    public ResponseEntity<List<BacktestTrade>> getAllResults() {
        List<BacktestTrade> trades = repository.findByStatusOrderByExitTimeDesc(
                BacktestTrade.TradeStatus.COMPLETED);
        return ResponseEntity.ok(trades);
    }
    
    /**
     * Get trades by date range
     */
    @GetMapping("/results/range")
    public ResponseEntity<List<BacktestTrade>> getByDateRange(
            @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate from,
            @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate to) {
        
        LocalDateTime fromDateTime = from.atStartOfDay();
        LocalDateTime toDateTime = to.atTime(LocalTime.MAX);
        
        List<BacktestTrade> trades = repository.findBySignalTimeBetweenOrderBySignalTimeDesc(
                fromDateTime, toDateTime);
        return ResponseEntity.ok(trades);
    }
    
    /**
     * Get backtest summary statistics
     */
    @GetMapping("/summary")
    public ResponseEntity<Map<String, Object>> getSummary() {
        List<BacktestTrade> allCompleted = repository.findByStatus(BacktestTrade.TradeStatus.COMPLETED);
        List<BacktestTrade> profitable = repository.findProfitableTrades();
        List<BacktestTrade> losing = repository.findLosingTrades();
        
        Map<String, Object> summary = new HashMap<>();
        
        // Trade counts
        summary.put("totalTrades", allCompleted.size());
        summary.put("profitableTrades", profitable.size());
        summary.put("losingTrades", losing.size());
        
        // Win rate
        double winRate = allCompleted.isEmpty() ? 0 : 
                (double) profitable.size() / allCompleted.size() * 100;
        summary.put("winRatePercent", Math.round(winRate * 100) / 100.0);
        
        // P&L
        double totalProfit = allCompleted.stream()
                .mapToDouble(BacktestTrade::getProfit)
                .sum();
        double avgProfit = allCompleted.isEmpty() ? 0 : totalProfit / allCompleted.size();
        summary.put("totalProfit", Math.round(totalProfit * 100) / 100.0);
        summary.put("averageProfit", Math.round(avgProfit * 100) / 100.0);
        
        // R-Multiple stats
        double avgRMultiple = allCompleted.stream()
                .mapToDouble(BacktestTrade::getRMultiple)
                .average()
                .orElse(0);
        summary.put("averageRMultiple", Math.round(avgRMultiple * 100) / 100.0);
        
        // Best/Worst trades
        double maxProfit = allCompleted.stream()
                .mapToDouble(BacktestTrade::getProfit)
                .max().orElse(0);
        double minProfit = allCompleted.stream()
                .mapToDouble(BacktestTrade::getProfit)
                .min().orElse(0);
        summary.put("bestTrade", maxProfit);
        summary.put("worstTrade", minProfit);
        
        // By exit reason
        Map<String, Long> byExitReason = new HashMap<>();
        allCompleted.forEach(t -> {
            String reason = t.getExitReason() != null ? t.getExitReason() : "UNKNOWN";
            byExitReason.merge(reason, 1L, Long::sum);
        });
        summary.put("byExitReason", byExitReason);
        
        // X-factor trades
        long xfactorCount = allCompleted.stream().filter(BacktestTrade::isXfactorFlag).count();
        summary.put("xfactorTrades", xfactorCount);
        
        return ResponseEntity.ok(summary);
    }
    
    /**
     * Get only profitable trades
     */
    @GetMapping("/results/profitable")
    public ResponseEntity<List<BacktestTrade>> getProfitableTrades() {
        return ResponseEntity.ok(repository.findProfitableTrades());
    }
    
    /**
     * Get only losing trades
     */
    @GetMapping("/results/losing")
    public ResponseEntity<List<BacktestTrade>> getLosingTrades() {
        return ResponseEntity.ok(repository.findLosingTrades());
    }
    
    /**
     * Get X-factor trades (rare strong signals)
     */
    @GetMapping("/results/xfactor")
    public ResponseEntity<List<BacktestTrade>> getXfactorTrades() {
        return ResponseEntity.ok(repository.findByXfactorFlagTrueOrderBySignalTimeDesc());
    }
    
    /**
     * Get trades by signal type
     */
    @GetMapping("/results/signal/{signalType}")
    public ResponseEntity<List<BacktestTrade>> getBySignalType(@PathVariable String signalType) {
        return ResponseEntity.ok(repository.findBySignalTypeOrderBySignalTimeDesc(signalType));
    }
    
    /**
     * Get trades by scripCode
     */
    @GetMapping("/results/scrip/{scripCode}")
    public ResponseEntity<List<BacktestTrade>> getByScripCode(@PathVariable String scripCode) {
        return ResponseEntity.ok(repository.findByScripCodeOrderBySignalTimeDesc(scripCode));
    }
    
    /**
     * Get high confidence trades
     */
    @GetMapping("/results/confidence/{minConfidence}")
    public ResponseEntity<List<BacktestTrade>> getByMinConfidence(@PathVariable double minConfidence) {
        return ResponseEntity.ok(repository.findByMinConfidence(minConfidence));
    }
    
    /**
     * Get single trade by ID
     */
    @GetMapping("/results/{id}")
    public ResponseEntity<BacktestTrade> getById(@PathVariable String id) {
        return repository.findById(id)
                .map(ResponseEntity::ok)
                .orElse(ResponseEntity.notFound().build());
    }
}
