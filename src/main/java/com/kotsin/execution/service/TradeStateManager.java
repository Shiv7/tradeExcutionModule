package com.kotsin.execution.service;

import com.kotsin.execution.model.ActiveTrade;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * Manages active trades in memory for fast access and monitoring
 */
@Service
@Slf4j
@RequiredArgsConstructor
public class TradeStateManager {
    
    // In-memory storage for active trades (can be enhanced with Redis persistence)
    private final Map<String, ActiveTrade> activeTrades = new ConcurrentHashMap<>();
    
    /**
     * Add a new active trade
     */
    public void addActiveTrade(ActiveTrade trade) {
        activeTrades.put(trade.getTradeId(), trade);
        log.info("🏪 Added active trade: {} for {} (Total active: {})", 
                trade.getTradeId(), trade.getScripCode(), activeTrades.size());
    }
    
    /**
     * Update an existing trade
     */
    public void updateTrade(ActiveTrade trade) {
        activeTrades.put(trade.getTradeId(), trade);
        log.debug("🔄 Updated trade: {}", trade.getTradeId());
    }
    
    /**
     * Remove a completed trade
     */
    public void removeActiveTrade(String tradeId) {
        ActiveTrade removed = activeTrades.remove(tradeId);
        if (removed != null) {
            log.info("🗑️ Removed completed trade: {} (Total active: {})", 
                    tradeId, activeTrades.size());
        }
    }
    
    /**
     * Get all active trades for a specific script
     */
    public Map<String, ActiveTrade> getActiveTradesForScript(String scripCode) {
        return activeTrades.values().stream()
                .filter(trade -> scripCode.equals(trade.getScripCode()))
                .collect(Collectors.toMap(
                        ActiveTrade::getTradeId,
                        trade -> trade
                ));
    }
    
    /**
     * Check if there's already an active trade for script + strategy combination
     */
    public boolean hasActiveTrade(String scripCode, String strategyName) {
        return activeTrades.values().stream()
                .anyMatch(trade -> 
                    scripCode.equals(trade.getScripCode()) && 
                    strategyName.equals(trade.getStrategyName()) &&
                    (trade.getStatus() == ActiveTrade.TradeStatus.WAITING_FOR_ENTRY ||
                     trade.getStatus() == ActiveTrade.TradeStatus.ACTIVE ||
                     trade.getStatus() == ActiveTrade.TradeStatus.PARTIAL_EXIT)
                );
    }
    
    /**
     * Get a specific trade by ID
     */
    public ActiveTrade getTrade(String tradeId) {
        return activeTrades.get(tradeId);
    }
    
    /**
     * Get all active trades
     */
    public Map<String, ActiveTrade> getAllActiveTrades() {
        return new HashMap<>(activeTrades);
    }
    
    /**
     * Get count of active trades
     */
    public int getActiveTradeCount() {
        return activeTrades.size();
    }
    
    /**
     * Get trades by status
     */
    public Map<String, ActiveTrade> getTradesByStatus(ActiveTrade.TradeStatus status) {
        return activeTrades.values().stream()
                .filter(trade -> trade.getStatus() == status)
                .collect(Collectors.toMap(
                        ActiveTrade::getTradeId,
                        trade -> trade
                ));
    }
    
    /**
     * Get summary of active trades by script
     */
    public Map<String, Integer> getTradeCountByScript() {
        Map<String, Integer> summary = new HashMap<>();
        activeTrades.values().forEach(trade -> {
            String scripCode = trade.getScripCode();
            summary.put(scripCode, summary.getOrDefault(scripCode, 0) + 1);
        });
        return summary;
    }
    
    /**
     * Get summary of active trades by strategy
     */
    public Map<String, Integer> getTradeCountByStrategy() {
        Map<String, Integer> summary = new HashMap<>();
        activeTrades.values().forEach(trade -> {
            String strategy = trade.getStrategyName();
            summary.put(strategy, summary.getOrDefault(strategy, 0) + 1);
        });
        return summary;
    }
    
    /**
     * Clear all trades (for testing or system reset)
     */
    public void clearAllTrades() {
        int count = activeTrades.size();
        activeTrades.clear();
        log.warn("🧹 Cleared all {} active trades", count);
    }
} 