package com.kotsin.execution.service;

import com.kotsin.execution.model.ActiveTrade;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.concurrent.ConcurrentHashMap;
import java.util.Map;

/**
 * Capital Management Service - Handles 1 Lakh Initial Capital
 * Ensures only 1 trade at a time with proper capital allocation
 */
@Service
@Slf4j
public class CapitalManagementService {
    
    @Value("${app.portfolio.initial-capital:100000}")
    private double initialCapital; // 1 lakh as configured
    
    private double currentCapital;
    private final Map<String, ActiveTrade> activeTrades = new ConcurrentHashMap<>();
    private double totalProfitLoss = 0.0;
    
    // Initialize capital
    public CapitalManagementService() {
        this.currentCapital = 100000.0; // 1 lakh initial capital
        log.info("💰 [CapitalMgmt] Initialized with capital: ₹{}", String.format("%.2f", initialCapital));
    }
    
    /**
     * Check if we can take a new trade (only 1 trade allowed at a time)
     */
    public boolean canTakeNewTrade() {
        boolean canTrade = activeTrades.isEmpty();
        log.info("🎯 [CapitalMgmt] Can take new trade: {} (Active trades: {})", 
                canTrade, activeTrades.size());
        return canTrade;
    }
    
    /**
     * Check if we should replace current trade with better signal
     * Criteria: New signal R:R > 1.5 AND better than current trade
     */
    public boolean shouldReplaceCurrentTrade(Map<String, Object> newSignalData) {
        if (activeTrades.isEmpty()) {
            return false; // No trade to replace
        }
        
        Double newRiskReward = extractDoubleValue(newSignalData, "riskReward");
        if (newRiskReward == null || newRiskReward <= 1.5) {
            log.info("🔍 [CapitalMgmt] New signal R:R {} not good enough for replacement (need > 1.5)", newRiskReward);
            return false;
        }
        
        // Get current active trade
        ActiveTrade currentTrade = activeTrades.values().iterator().next();
        double currentRR = currentTrade.getRiskRewardRatio();
        
        // Replace if new signal is significantly better
        boolean shouldReplace = newRiskReward > currentRR + 0.3; // At least 0.3 better R:R
        
        log.info("📊 [CapitalMgmt] Trade replacement analysis:");
        log.info("   Current trade: {} R:R = {}", currentTrade.getScripCode(), currentRR);
        log.info("   New signal: {} R:R = {}", extractStringValue(newSignalData, "scripCode"), newRiskReward);
        log.info("   Should replace: {}", shouldReplace);
        
        return shouldReplace;
    }
    
    /**
     * Calculate position size based on available capital and risk management
     */
    public int calculatePositionSize(double entryPrice, double stopLoss) {
        if (entryPrice <= 0 || stopLoss <= 0) {
            return 0;
        }
        
        // Use 2% risk per trade (₹2000 risk from ₹1 lakh capital)
        double riskAmount = currentCapital * 0.02;
        double riskPerShare = Math.abs(entryPrice - stopLoss);
        
        if (riskPerShare <= 0) {
            return 0;
        }
        
        int positionSize = (int) (riskAmount / riskPerShare);
        
        // Ensure minimum position size
        positionSize = Math.max(positionSize, 1);
        
        // Ensure we don't exceed available capital
        double totalTradeValue = positionSize * entryPrice;
        if (totalTradeValue > currentCapital * 0.95) { // Use max 95% of capital
            positionSize = (int) ((currentCapital * 0.95) / entryPrice);
        }
        
        log.info("💰 [CapitalMgmt] Position size calculation:");
        log.info("   Available capital: ₹{}", String.format("%.2f", currentCapital));
        log.info("   Risk amount (2%): ₹{}", String.format("%.2f", riskAmount));
        log.info("   Risk per share: ₹{}", String.format("%.2f", riskPerShare));
        log.info("   Position size: {} shares", positionSize);
        log.info("   Trade value: ₹{}", String.format("%.2f", positionSize * entryPrice));
        
        return positionSize;
    }
    
    /**
     * Add active trade to tracking
     */
    public void addActiveTrade(ActiveTrade trade) {
        activeTrades.put(trade.getScripCode(), trade);
        log.info("📈 [CapitalMgmt] Added active trade: {} (Total active: {})", 
                trade.getScripCode(), activeTrades.size());
    }
    
    /**
     * Remove active trade and update capital
     */
    public void removeActiveTrade(String scripCode, double profitLoss) {
        ActiveTrade removed = activeTrades.remove(scripCode);
        if (removed != null) {
            currentCapital += profitLoss;
            totalProfitLoss += profitLoss;
            
            log.info("💰 [CapitalMgmt] Trade closed: {} P&L: ₹{}", scripCode, String.format("%.2f", profitLoss));
            log.info("💰 [CapitalMgmt] Updated capital: ₹{} (Total P&L: ₹{})", 
                    String.format("%.2f", currentCapital), 
                    String.format("%.2f", totalProfitLoss));
        }
    }
    
    /**
     * Get current active trade (since we only allow 1)
     */
    public ActiveTrade getCurrentActiveTrade() {
        return activeTrades.isEmpty() ? null : activeTrades.values().iterator().next();
    }
    
    /**
     * Force exit current trade (for replacement scenario)
     */
    public ActiveTrade forceExitCurrentTrade(String reason) {
        ActiveTrade currentTrade = getCurrentActiveTrade();
        if (currentTrade != null) {
            log.info("🔄 [CapitalMgmt] Force exiting current trade: {} - Reason: {}", 
                    currentTrade.getScripCode(), reason);
            
            // Mark for forced exit
            currentTrade.addMetadata("forcedExit", true);
            currentTrade.addMetadata("forcedExitReason", reason);
            currentTrade.addMetadata("forcedExitTime", LocalDateTime.now());
        }
        return currentTrade;
    }
    
    /**
     * Get capital statistics
     */
    public Map<String, Object> getCapitalStats() {
        Map<String, Object> stats = new ConcurrentHashMap<>();
        stats.put("initialCapital", initialCapital);
        stats.put("currentCapital", currentCapital);
        stats.put("totalProfitLoss", totalProfitLoss);
        stats.put("activeTradesCount", activeTrades.size());
        stats.put("roi", ((currentCapital - initialCapital) / initialCapital) * 100);
        return stats;
    }
    
    private String extractStringValue(Map<String, Object> data, String key) {
        Object value = data.get(key);
        return value != null ? value.toString() : null;
    }
    
    private Double extractDoubleValue(Map<String, Object> data, String key) {
        Object value = data.get(key);
        if (value == null) return null;
        
        if (value instanceof Number) {
            return ((Number) value).doubleValue();
        }
        
        try {
            return Double.parseDouble(value.toString());
        } catch (NumberFormatException e) {
            return null;
        }
    }
} 