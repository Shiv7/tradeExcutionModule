package com.kotsin.execution.service;

import com.kotsin.execution.model.ActiveTrade;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Trade Selection Service - Ensures only one trade is active at a time
 * Selects the best trade when multiple signals arrive within same time interval
 */
@Service
@Slf4j
@RequiredArgsConstructor
public class TradeSelectionService {
    
    // Store pending signals for selection
    private final Map<String, Map<String, Object>> pendingSignals = new ConcurrentHashMap<>();
    
    // Track active trades
    private final Map<String, ActiveTrade> activeTrades = new ConcurrentHashMap<>();
    
    // Constants for trade selection
    private static final int SELECTION_WINDOW_MINUTES = 2; // 2-minute window to collect signals
    private static final double MIN_RR_THRESHOLD = 1.5; // Minimum R:R to consider
    private static final double HIGH_CONFIDENCE_BONUS = 0.5; // Bonus for HIGH confidence signals
    
    /**
     * Process incoming signal - either queue for selection or reject if trade active
     */
    public boolean processIncomingSignal(Map<String, Object> signalData) {
        String scripCode = extractStringValue(signalData, "scripCode");
        String signalId = generateSignalId(scripCode);
        
        log.info("🎯 [TradeSelection] Processing signal for {}", scripCode);
        
        // Check if we already have an active trade
        if (hasActiveTrade()) {
            log.info("⚠️ [TradeSelection] Active trade exists - evaluating if new signal is better");
            return evaluateSignalAgainstActiveTrade(signalData);
        }
        
        // No active trade - queue signal for selection
        queueSignalForSelection(signalId, signalData);
        
        // Start selection timer if this is the first signal in the window
        if (pendingSignals.size() == 1) {
            scheduleTradeSelection();
        }
        
        return true;
    }
    
    /**
     * Queue signal for selection within the time window
     */
    private void queueSignalForSelection(String signalId, Map<String, Object> signalData) {
        signalData.put("queueTime", LocalDateTime.now());
        signalData.put("signalId", signalId);
        
        pendingSignals.put(signalId, signalData);
        
        log.info("📋 [TradeSelection] Queued signal {} for selection (Total pending: {})", 
                signalId, pendingSignals.size());
    }
    
    /**
     * Schedule trade selection after waiting period
     */
    private void scheduleTradeSelection() {
        // Use a simple timer approach - in production, use @Async or ScheduledExecutorService
        new Thread(() -> {
            try {
                Thread.sleep(SELECTION_WINDOW_MINUTES * 60 * 1000); // Wait for selection window
                selectBestTrade();
            } catch (InterruptedException e) {
                log.warn("⚠️ [TradeSelection] Selection timer interrupted");
                Thread.currentThread().interrupt();
            }
        }).start();
    }
    
    /**
     * Select the best trade from pending signals
     */
    private void selectBestTrade() {
        if (pendingSignals.isEmpty()) {
            log.info("📋 [TradeSelection] No pending signals to select from");
            return;
        }
        
        log.info("🏆 [TradeSelection] Starting trade selection from {} pending signals", pendingSignals.size());
        
        Map<String, Object> bestSignal = null;
        double bestScore = 0.0;
        
        for (Map<String, Object> signal : pendingSignals.values()) {
            double score = calculateSignalScore(signal);
            
            String scripCode = extractStringValue(signal, "scripCode");
            log.info("📊 [TradeSelection] Signal {} score: {:.2f}", scripCode, score);
            
            if (score > bestScore && score >= MIN_RR_THRESHOLD) {
                bestScore = score;
                bestSignal = signal;
            }
        }
        
        if (bestSignal != null) {
            String selectedScripCode = extractStringValue(bestSignal, "scripCode");
            log.info("🎯 [TradeSelection] Selected best signal: {} with score {:.2f}", selectedScripCode, bestScore);
            
            // Execute the selected trade
            executeSelectedTrade(bestSignal);
            
            // Log rejected signals
            logRejectedSignals(bestSignal);
        } else {
            log.warn("❌ [TradeSelection] No signals met minimum criteria (R:R >= {})", MIN_RR_THRESHOLD);
        }
        
        // Clear pending signals
        pendingSignals.clear();
    }
    
    /**
     * Calculate score for signal selection
     * Higher score = better signal
     */
    private double calculateSignalScore(Map<String, Object> signalData) {
        double score = 0.0;
        
        try {
            // Base score from R:R ratio (most important factor)
            Double riskReward = extractDoubleValue(signalData, "riskReward");
            if (riskReward != null && riskReward > 0) {
                score = riskReward * 10; // R:R of 2.0 = score of 20
            }
            
            // Confidence bonus
            String confidence = extractStringValue(signalData, "confidence");
            if ("HIGH".equalsIgnoreCase(confidence)) {
                score += HIGH_CONFIDENCE_BONUS * 10; // +5 points for HIGH confidence
            }
            
            // Signal freshness (newer signals get slight bonus)
            LocalDateTime queueTime = (LocalDateTime) signalData.get("queueTime");
            if (queueTime != null) {
                long minutesOld = java.time.Duration.between(queueTime, LocalDateTime.now()).toMinutes();
                double freshnessBonus = Math.max(0, (5 - minutesOld) * 0.1); // Max 0.5 bonus for very fresh signals
                score += freshnessBonus;
            }
            
            // Target quality bonus (more targets = better)
            Double target2 = extractDoubleValue(signalData, "target2");
            Double target3 = extractDoubleValue(signalData, "target3");
            if (target2 != null && target2 > 0) score += 1.0;
            if (target3 != null && target3 > 0) score += 0.5;
            
        } catch (Exception e) {
            log.warn("⚠️ [TradeSelection] Error calculating score for signal: {}", e.getMessage());
        }
        
        return score;
    }
    
    /**
     * Execute the selected trade
     */
    private void executeSelectedTrade(Map<String, Object> bestSignal) {
        String scripCode = extractStringValue(bestSignal, "scripCode");
        log.info("🚀 [TradeSelection] Executing selected trade for {}", scripCode);
        
        // Create active trade record
        ActiveTrade activeTrade = createActiveTradeFromSignal(bestSignal);
        activeTrades.put(scripCode, activeTrade);
        
        // Here you would integrate with your existing trade execution logic
        // For now, just log the execution
        log.info("✅ [TradeSelection] Trade executed for {} - Entry: {}, SL: {}, T1: {}", 
                scripCode, 
                extractDoubleValue(bestSignal, "entryPrice"),
                extractDoubleValue(bestSignal, "stopLoss"),
                extractDoubleValue(bestSignal, "target1"));
    }
    
    /**
     * Log rejected signals for transparency
     */
    private void logRejectedSignals(Map<String, Object> selectedSignal) {
        String selectedScripCode = extractStringValue(selectedSignal, "scripCode");
        
        pendingSignals.values().stream()
                .filter(signal -> !selectedScripCode.equals(extractStringValue(signal, "scripCode")))
                .forEach(rejectedSignal -> {
                    String scripCode = extractStringValue(rejectedSignal, "scripCode");
                    double score = calculateSignalScore(rejectedSignal);
                    log.info("❌ [TradeSelection] Rejected signal: {} (Score: {:.2f}, R:R: {})", 
                            scripCode, score, extractDoubleValue(rejectedSignal, "riskReward"));
                });
    }
    
    /**
     * Evaluate if new signal is better than current active trade
     */
    private boolean evaluateSignalAgainstActiveTrade(Map<String, Object> newSignalData) {
        // For now, reject all new signals when trade is active
        // In future, you could implement logic to exit current trade for better one
        
        String newScripCode = extractStringValue(newSignalData, "scripCode");
        Double newRR = extractDoubleValue(newSignalData, "riskReward");
        
        log.info("🔄 [TradeSelection] New signal {} (R:R: {}) vs Active trade - REJECTING (One trade at a time policy)", 
                newScripCode, newRR);
        
        return false; // Reject new signal
    }
    
    /**
     * Check if we have an active trade
     */
    private boolean hasActiveTrade() {
        return !activeTrades.isEmpty();
    }
    
    /**
     * Create active trade from signal data
     */
    private ActiveTrade createActiveTradeFromSignal(Map<String, Object> signalData) {
        return ActiveTrade.builder()
                .tradeId(generateTradeId(extractStringValue(signalData, "scripCode")))
                .scripCode(extractStringValue(signalData, "scripCode"))
                .companyName(extractStringValue(signalData, "companyName"))
                .signalType(extractStringValue(signalData, "signal"))
                .strategyName("ENHANCED_30M")
                .signalTime(LocalDateTime.now())
                .stopLoss(extractDoubleValue(signalData, "stopLoss"))
                .target1(extractDoubleValue(signalData, "target1"))
                .target2(extractDoubleValue(signalData, "target2"))
                .status(ActiveTrade.TradeStatus.WAITING_FOR_ENTRY)
                .build();
    }
    
    /**
     * Remove active trade (called when trade is completed)
     */
    public void removeActiveTrade(String scripCode) {
        activeTrades.remove(scripCode);
        log.info("🗑️ [TradeSelection] Removed active trade for {} - Ready for new signals", scripCode);
    }
    
    /**
     * Get current trade selection statistics
     */
    public Map<String, Object> getSelectionStats() {
        Map<String, Object> stats = new HashMap<>();
        stats.put("pendingSignals", pendingSignals.size());
        stats.put("activeTrades", activeTrades.size());
        stats.put("selectionWindowMinutes", SELECTION_WINDOW_MINUTES);
        stats.put("minRRThreshold", MIN_RR_THRESHOLD);
        return stats;
    }
    
    // Utility methods
    private String extractStringValue(Map<String, Object> data, String key) {
        Object value = data.get(key);
        return value != null ? value.toString() : null;
    }
    
    private Double extractDoubleValue(Map<String, Object> data, String key) {
        Object value = data.get(key);
        if (value instanceof Number) {
            return ((Number) value).doubleValue();
        }
        return null;
    }
    
    private String generateSignalId(String scripCode) {
        return scripCode + "_" + System.currentTimeMillis();
    }
    
    private String generateTradeId(String scripCode) {
        return "TRADE_" + scripCode + "_" + System.currentTimeMillis();
    }
} 