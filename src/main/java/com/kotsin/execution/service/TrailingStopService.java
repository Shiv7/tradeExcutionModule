package com.kotsin.execution.service;

import com.kotsin.execution.model.ActiveTrade;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Trailing Stop Service - Manages progressive trailing stops for active trades
 * Updates stop loss levels based on price movement to lock in profits
 */
@Service
@Slf4j
@RequiredArgsConstructor
public class TrailingStopService {
    
    // Store trailing stop configurations per trade
    private final Map<String, TrailingStopConfig> trailingStops = new ConcurrentHashMap<>();
    
    // Constants for trailing stop logic
    private static final double DEFAULT_TRAILING_DISTANCE = 0.01; // 1% trailing distance
    private static final double PROFIT_LOCK_THRESHOLD = 0.005; // 0.5% profit before trailing starts
    
    /**
     * Configuration for trailing stop per trade
     */
    public static class TrailingStopConfig {
        public String tradeId;
        public String scripCode;
        public boolean isBullish;
        public double entryPrice;
        public double originalStopLoss;
        public double currentStopLoss;
        public double trailingDistance;
        public double highestPrice; // For bullish trades
        public double lowestPrice;  // For bearish trades
        public boolean trailingActive;
        public LocalDateTime lastUpdate;
        
        public TrailingStopConfig(String tradeId, String scripCode, boolean isBullish, 
                                double entryPrice, double originalStopLoss, double trailingDistance) {
            this.tradeId = tradeId;
            this.scripCode = scripCode;
            this.isBullish = isBullish;
            this.entryPrice = entryPrice;
            this.originalStopLoss = originalStopLoss;
            this.currentStopLoss = originalStopLoss;
            this.trailingDistance = trailingDistance;
            this.highestPrice = entryPrice;
            this.lowestPrice = entryPrice;
            this.trailingActive = false;
            this.lastUpdate = LocalDateTime.now();
        }
    }
    
    /**
     * Enable trailing stop for a trade
     */
    public void enableTrailingStop(ActiveTrade trade) {
        String tradeId = trade.getTradeId();
        boolean isBullish = trade.isBullish();
        double entryPrice = getTradeEntryPrice(trade);
        double stopLoss = trade.getStopLoss();
        
        TrailingStopConfig config = new TrailingStopConfig(
            tradeId, 
            trade.getScripCode(), 
            isBullish, 
            entryPrice, 
            stopLoss, 
            DEFAULT_TRAILING_DISTANCE
        );
        
        trailingStops.put(tradeId, config);
        
        log.info("📈 [TrailingStop] Enabled trailing stop for {} ({}): Entry={}, Original SL={}, Trailing Distance={}%", 
                trade.getScripCode(), isBullish ? "BULLISH" : "BEARISH", 
                entryPrice, stopLoss, DEFAULT_TRAILING_DISTANCE * 100);
    }
    
    /**
     * Update trailing stop based on current market price
     */
    public boolean updateTrailingStop(String tradeId, double currentPrice) {
        TrailingStopConfig config = trailingStops.get(tradeId);
        if (config == null) {
            return false;
        }
        
        boolean stopLossUpdated = false;
        
        if (config.isBullish) {
            stopLossUpdated = updateBullishTrailingStop(config, currentPrice);
        } else {
            stopLossUpdated = updateBearishTrailingStop(config, currentPrice);
        }
        
        if (stopLossUpdated) {
            config.lastUpdate = LocalDateTime.now();
            log.info("🔄 [TrailingStop] Updated trailing stop for {}: Price={}, New SL={}, Profit Locked={}%", 
                    config.scripCode, currentPrice, config.currentStopLoss, 
                    calculateProfitLocked(config) * 100);
        }
        
        return stopLossUpdated;
    }
    
    /**
     * Update trailing stop for bullish trades
     */
    private boolean updateBullishTrailingStop(TrailingStopConfig config, double currentPrice) {
        // Update highest price seen
        if (currentPrice > config.highestPrice) {
            config.highestPrice = currentPrice;
        }
        
        // Check if we have enough profit to start trailing
        double currentProfit = (currentPrice - config.entryPrice) / config.entryPrice;
        if (!config.trailingActive && currentProfit >= PROFIT_LOCK_THRESHOLD) {
            config.trailingActive = true;
            log.info("🎯 [TrailingStop] Trailing activated for {} at {}% profit", 
                    config.scripCode, currentProfit * 100);
        }
        
        if (config.trailingActive) {
            // Calculate new trailing stop level
            double newStopLoss = config.highestPrice * (1 - config.trailingDistance);
            
            // Only update if new stop loss is higher than current
            if (newStopLoss > config.currentStopLoss) {
                config.currentStopLoss = newStopLoss;
                return true;
            }
        }
        
        return false;
    }
    
    /**
     * Update trailing stop for bearish trades
     */
    private boolean updateBearishTrailingStop(TrailingStopConfig config, double currentPrice) {
        // Update lowest price seen
        if (currentPrice < config.lowestPrice) {
            config.lowestPrice = currentPrice;
        }
        
        // Check if we have enough profit to start trailing
        double currentProfit = (config.entryPrice - currentPrice) / config.entryPrice;
        if (!config.trailingActive && currentProfit >= PROFIT_LOCK_THRESHOLD) {
            config.trailingActive = true;
            log.info("🎯 [TrailingStop] Trailing activated for {} at {}% profit", 
                    config.scripCode, currentProfit * 100);
        }
        
        if (config.trailingActive) {
            // Calculate new trailing stop level
            double newStopLoss = config.lowestPrice * (1 + config.trailingDistance);
            
            // Only update if new stop loss is lower than current
            if (newStopLoss < config.currentStopLoss) {
                config.currentStopLoss = newStopLoss;
                return true;
            }
        }
        
        return false;
    }
    
    /**
     * Check if trailing stop is hit
     */
    public boolean isTrailingStopHit(String tradeId, double currentPrice) {
        TrailingStopConfig config = trailingStops.get(tradeId);
        if (config == null) return false;
        
        if (config.isBullish) {
            return currentPrice <= config.currentStopLoss;
        } else {
            return currentPrice >= config.currentStopLoss;
        }
    }
    
    /**
     * Get current trailing stop level
     */
    public double getCurrentStopLoss(String tradeId) {
        TrailingStopConfig config = trailingStops.get(tradeId);
        return config != null ? config.currentStopLoss : 0.0;
    }
    
    /**
     * Remove trailing stop when trade is closed
     */
    public void removeTrailingStop(String tradeId) {
        TrailingStopConfig config = trailingStops.remove(tradeId);
        if (config != null) {
            double profitLocked = calculateProfitLocked(config);
            log.info("🗑️ [TrailingStop] Removed trailing stop for {} - Final profit locked: {}%", 
                    config.scripCode, profitLocked * 100);
        }
    }
    
    /**
     * Get trailing stop statistics
     */
    public Map<String, Object> getTrailingStopStats() {
        int activeTrailingStops = trailingStops.size();
        int activeTrailing = (int) trailingStops.values().stream().filter(c -> c.trailingActive).count();
        
        return Map.of(
            "totalTrailingStops", activeTrailingStops,
            "activelyTrailing", activeTrailing,
            "waitingForProfit", activeTrailingStops - activeTrailing,
            "defaultTrailingDistance", DEFAULT_TRAILING_DISTANCE * 100 + "%",
            "profitThresholdForTrailing", PROFIT_LOCK_THRESHOLD * 100 + "%"
        );
    }
    
    /**
     * Scheduled task to log trailing stop status (every 5 minutes during market hours)
     */
    @Scheduled(fixedRate = 300000) // 5 minutes
    public void logTrailingStopStatus() {
        if (!trailingStops.isEmpty()) {
            log.info("📊 [TrailingStop] Status: {} active trailing stops, {} actively trailing", 
                    trailingStops.size(), 
                    trailingStops.values().stream().filter(c -> c.trailingActive).count());
        }
    }
    
    /**
     * Calculate profit locked by trailing stop
     */
    private double calculateProfitLocked(TrailingStopConfig config) {
        if (config.isBullish) {
            return (config.currentStopLoss - config.entryPrice) / config.entryPrice;
        } else {
            return (config.entryPrice - config.currentStopLoss) / config.entryPrice;
        }
    }
    
    /**
     * Get trade entry price from ActiveTrade
     */
    private double getTradeEntryPrice(ActiveTrade trade) {
        // Try to get from metadata first, then fall back to signal price
        Object entryPrice = trade.getMetadata().get("actualEntryPrice");
        if (entryPrice instanceof Number) {
            return ((Number) entryPrice).doubleValue();
        }
        
        // Fallback to signal price
        Object signalPrice = trade.getMetadata().get("signalPrice");
        if (signalPrice instanceof Number) {
            return ((Number) signalPrice).doubleValue();
        }
        
        // If no entry price available, return 0 (should not happen in normal flow)
        log.warn("⚠️ [TrailingStop] No entry price found for trade {}", trade.getTradeId());
        return 0.0;
    }
} 