package com.kotsin.execution.service;

import com.kotsin.execution.model.ActiveTrade;
import com.kotsin.execution.model.TradeResult;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.Map;
import java.util.HashMap;

/**
 * Enhanced Price Action Service - Implements sophisticated entry/exit logic from kotsinBackTestBE
 * for real-time trading with WebSocket data
 * 
 * Key Features:
 * 1. DELAYED ENTRY with pivot breakout validation
 * 2. PARTIAL EXITS (50% at T1, remaining at T2 or trailing stop)  
 * 3. DYNAMIC TRAILING STOP after T1 hit
 * 4. Real-time tick-by-tick processing (vs candle-by-candle in backtesting)
 */
@Service
@Slf4j
@RequiredArgsConstructor
public class EnhancedPriceActionService {
    
    // Constants from kotsinBackTestBE Enhanced Price Action
    private static final double PIVOT_BREAKOUT_THRESHOLD = 0.1; // 0.1% breakout threshold
    private static final double STOP_LOSS_PERCENTAGE = 0.01; // 1% stop loss
    private static final double PERCENT_DROP_THRESHOLD = 0.01; // 1% drop threshold for exit
    
    /**
     * Check Enhanced Price Action entry conditions with delayed entry logic
     * Based on kotsinBackTestBE's simulateTradeWithImprovedRules method
     */
    public boolean checkEnhancedEntryConditions(ActiveTrade trade, double currentPrice, LocalDateTime timestamp) {
        log.debug("🔍 [EnhancedPA] Entry check for {} - Current: {}, Delayed: {}", 
                 trade.getTradeId(), currentPrice, isEntryDelayed(trade));
        
        // If entry is not delayed, use immediate entry (existing logic)
        if (!isEntryDelayed(trade)) {
            return checkImmediateEntry(trade, currentPrice);
        }
        
        // Enhanced delayed entry logic - check for pivot breakout
        return checkDelayedEntryWithPivotBreakout(trade, currentPrice, timestamp);
    }
    
    /**
     * Check if entry should be delayed based on pivot proximity
     * Replicates kotsinBackTestBE's entry delay logic
     */
    private boolean isEntryDelayed(ActiveTrade trade) {
        // Check if trade metadata indicates entry should be delayed
        Object entryDelayed = trade.getMetadata().get("entryDelayed");
        boolean delayed = entryDelayed != null && (Boolean) entryDelayed;
        
        if (delayed) {
            String delayReason = (String) trade.getMetadata().get("delayReason");
            Double targetProximity = (Double) trade.getMetadata().get("targetProximity");
            Double pivotProximity = (Double) trade.getMetadata().get("pivotProximity");
            
            log.info("⏳ [EnhancedPA] Entry DELAYED for {} - Reason: {} | Target: {:.1f}% | Pivot: {:.1f}%", 
                    trade.getTradeId(), delayReason, 
                    targetProximity != null ? targetProximity : 0.0,
                    pivotProximity != null ? pivotProximity : 0.0);
        }
        
        return delayed;
    }
    
    /**
     * Immediate entry logic (existing simple logic)
     */
    private boolean checkImmediateEntry(ActiveTrade trade, double currentPrice) {
        Double signalPrice = extractSignalPrice(trade);
        String tradeId = trade.getTradeId();
        String scripCode = trade.getScripCode();
        
        // CRITICAL FIX: Don't auto-enter if signal price is missing!
        if (signalPrice == null) {
            log.warn("🚨 [EnhancedPA] ENTRY REJECTED - Missing signal price for trade {} ({})", tradeId, scripCode);
            return false; // Changed from 'true' to 'false' - require valid signal price
        }
        
        boolean isBullish = trade.isBullish();
        boolean shouldEnter = false;
        
        // Enhanced logging for entry decision making
        log.info("🔍 [EnhancedPA] IMMEDIATE ENTRY CHECK - Trade: {} ({}), Signal Price: {}, Current Price: {}, Direction: {}", 
                tradeId, scripCode, signalPrice, currentPrice, isBullish ? "BULLISH" : "BEARISH");
        
        if (isBullish) {
            // For bullish trades, enter if price moves up from signal or is very close
            double upperThreshold = signalPrice * 1.001; // 0.1% buffer
            double priceRange = signalPrice * 0.002; // 0.2% range
            boolean priceCondition1 = currentPrice >= upperThreshold;
            boolean priceCondition2 = Math.abs(currentPrice - signalPrice) <= priceRange;
            
            shouldEnter = priceCondition1 || priceCondition2;
            
            log.info("🔍 [EnhancedPA] BULLISH ENTRY - Current: {}, Signal: {}, Upper Threshold: {}, Range Check: {}, Condition1: {}, Condition2: {}, Result: {}", 
                    currentPrice, signalPrice, upperThreshold, priceRange, priceCondition1, priceCondition2, shouldEnter);
            
        } else {
            // For bearish trades, enter if price moves down from signal or is very close  
            double lowerThreshold = signalPrice * 0.999; // 0.1% buffer
            double priceRange = signalPrice * 0.002; // 0.2% range
            boolean priceCondition1 = currentPrice <= lowerThreshold;
            boolean priceCondition2 = Math.abs(currentPrice - signalPrice) <= priceRange;
            
            shouldEnter = priceCondition1 || priceCondition2;
            
            log.info("🔍 [EnhancedPA] BEARISH ENTRY - Current: {}, Signal: {}, Lower Threshold: {}, Range Check: {}, Condition1: {}, Condition2: {}, Result: {}", 
                    currentPrice, signalPrice, lowerThreshold, priceRange, priceCondition1, priceCondition2, shouldEnter);
        }
        
        if (shouldEnter) {
            log.info("✅ [EnhancedPA] IMMEDIATE ENTRY APPROVED for {} ({}) - Price: {} vs Signal: {}", 
                    tradeId, scripCode, currentPrice, signalPrice);
        } else {
            log.info("❌ [EnhancedPA] IMMEDIATE ENTRY REJECTED for {} ({}) - Price: {} vs Signal: {} (waiting for better price)", 
                    tradeId, scripCode, currentPrice, signalPrice);
        }
        
        return shouldEnter;
    }
    
    /**
     * Delayed entry with pivot breakout - core Enhanced Price Action logic
     * Replicates kotsinBackTestBE's pivot breakout entry conditions
     */
    private boolean checkDelayedEntryWithPivotBreakout(ActiveTrade trade, double currentPrice, LocalDateTime timestamp) {
        Double offendingPivot = extractOffendingPivot(trade);
        String delayReason = (String) trade.getMetadata().get("delayReason");
        
        log.info("🔍 [EnhancedPA] DELAYED ENTRY CHECK for {} - Current: {}, Offending Pivot: {}, Reason: {}", 
                trade.getTradeId(), currentPrice, offendingPivot, delayReason);
        
        if (offendingPivot == null || offendingPivot <= 0) {
            log.warn("🚨 [EnhancedPA] No offending pivot found for delayed entry trade {} - falling back to immediate entry", 
                    trade.getTradeId());
            return checkImmediateEntry(trade, currentPrice); // Fallback to immediate entry
        }
        
        boolean isBullish = trade.isBullish();
        boolean entryTriggered = false;
        
        if (isBullish) {
            // Bullish: Need breakout ABOVE the offending pivot
            double breakoutLevel = offendingPivot * (1 + PIVOT_BREAKOUT_THRESHOLD / 100.0);
            entryTriggered = currentPrice > breakoutLevel;
            
            log.info("🔍 [EnhancedPA] BULLISH delayed entry check - Price: {}, Pivot: {}, Breakout Level: {}, Triggered: {}", 
                    currentPrice, offendingPivot, breakoutLevel, entryTriggered);
                    
            if (entryTriggered) {
                log.info("🚀 [EnhancedPA] BULLISH BREAKOUT CONFIRMED! Price {} > Breakout Level {} (Pivot: {})", 
                        currentPrice, breakoutLevel, offendingPivot);
            } else {
                log.info("⏳ [EnhancedPA] BULLISH waiting for breakout - Need price > {} (currently {})", 
                        breakoutLevel, currentPrice);
            }
        } else {
            // Bearish: Need breakdown BELOW the offending pivot
            double breakdownLevel = offendingPivot * (1 - PIVOT_BREAKOUT_THRESHOLD / 100.0);
            entryTriggered = currentPrice < breakdownLevel;
            
            log.info("🔍 [EnhancedPA] BEARISH delayed entry check - Price: {}, Pivot: {}, Breakdown Level: {}, Triggered: {}", 
                    currentPrice, offendingPivot, breakdownLevel, entryTriggered);
                    
            if (entryTriggered) {
                log.info("🚀 [EnhancedPA] BEARISH BREAKDOWN CONFIRMED! Price {} < Breakdown Level {} (Pivot: {})", 
                        currentPrice, breakdownLevel, offendingPivot);
            } else {
                log.info("⏳ [EnhancedPA] BEARISH waiting for breakdown - Need price < {} (currently {})", 
                        breakdownLevel, currentPrice);
            }
        }
        
        if (entryTriggered) {
            log.info("🚀 [EnhancedPA] DELAYED ENTRY TRIGGERED for {} - Price {} broke {} pivot at {} | Reason was: {}", 
                    trade.getTradeId(), currentPrice, isBullish ? "above" : "below", offendingPivot, delayReason);
            
            // Update trade with actual entry details
            trade.addMetadata("actualEntryTime", timestamp);
            trade.addMetadata("actualEntryPrice", currentPrice);
            trade.addMetadata("pivotBreakoutConfirmed", true);
            trade.addMetadata("breakoutType", isBullish ? "BULLISH_BREAKOUT" : "BEARISH_BREAKDOWN");
        }
        
        return entryTriggered;
    }
    
    /**
     * Enhanced exit conditions with partial exits and dynamic trailing
     * Replicates kotsinBackTestBE's sophisticated exit management
     */
    public String checkEnhancedExitConditions(ActiveTrade trade, double currentPrice, LocalDateTime timestamp) {
        if (!trade.getEntryTriggered() || trade.getStatus() != ActiveTrade.TradeStatus.ACTIVE) {
            return null; // Not in active trade
        }
        
        // 1. Check stop loss BEFORE target 1 is hit
        if (!trade.getTarget1Hit()) {
            if (isStopLossHit(trade, currentPrice)) {
                log.info("🛑 [EnhancedPA] Initial stop loss hit for {} at price {}", trade.getTradeId(), currentPrice);
                return "STOP_LOSS";
            }
        }
        
        // 2. Check for Target 1 hit (partial exit)
        if (!trade.getTarget1Hit() && isTarget1Hit(trade, currentPrice)) {
            handleTarget1Hit(trade, currentPrice, timestamp);
            // Don't return exit reason - continue with remaining 50% position
        }
        
        // 3. After Target 1 hit, check enhanced exit conditions for remaining position
        if (trade.getTarget1Hit()) {
            return checkPostTarget1ExitConditions(trade, currentPrice, timestamp);
        }
        
        return null; // Continue trading
    }
    
    /**
     * Handle Target 1 hit - partial exit and trailing stop update
     * Replicates kotsinBackTestBE's target 1 handling
     */
    private void handleTarget1Hit(ActiveTrade trade, double currentPrice, LocalDateTime timestamp) {
        trade.setTarget1Hit(true);
        
        // Record partial exit details (50% position)
        trade.addMetadata("partialExitPrice", trade.getTarget1());
        trade.addMetadata("partialExitTime", timestamp);
        trade.addMetadata("partialExitPercentage", 50.0);
        
        // Update trailing stop to breakeven (entry price)
        double actualEntryPrice = extractActualEntryPrice(trade);
        updateTrailingStopToBreakeven(trade, actualEntryPrice);
        
        log.info("🎯 [EnhancedPA] Target 1 HIT for {} - Partial exit 50% at {}, trailing stop updated to breakeven {}", 
                trade.getTradeId(), trade.getTarget1(), actualEntryPrice);
    }
    
    /**
     * Check exit conditions after Target 1 is hit (for remaining 50% position)
     * Implements kotsinBackTestBE's post-target-1 exit logic
     */
    private String checkPostTarget1ExitConditions(ActiveTrade trade, double currentPrice, LocalDateTime timestamp) {
        // 1. Check for 1% drop from previous close (enhanced logic)
        if (isPercentDropFromPreviousClose(trade, currentPrice)) {
            log.info("📉 [EnhancedPA] 1% drop from previous close detected for {} at {}", trade.getTradeId(), currentPrice);
            return "PERCENT_DROP";
        }
        
        // 2. Check for Target 2 hit
        if (isTarget2Hit(trade, currentPrice)) {
            log.info("🎯 [EnhancedPA] Target 2 HIT for {} at {}", trade.getTradeId(), currentPrice);
            return "TARGET_2";
        }
        
        // 3. Check trailing stop loss
        if (isTrailingStopHit(trade, currentPrice)) {
            log.info("🛑 [EnhancedPA] Trailing stop hit for {} at {}", trade.getTradeId(), currentPrice);
            return "TRAILING_STOP";
        }
        
        // 4. Update trailing stop dynamically (move with price)
        updateDynamicTrailingStop(trade, currentPrice);
        
        return null; // Continue trading
    }
    
    /**
     * Check if stop loss is hit
     */
    private boolean isStopLossHit(ActiveTrade trade, double currentPrice) {
        if (trade.isBullish()) {
            return currentPrice <= trade.getStopLoss();
        } else {
            return currentPrice >= trade.getStopLoss();
        }
    }
    
    /**
     * Check if Target 1 is hit
     */
    private boolean isTarget1Hit(ActiveTrade trade, double currentPrice) {
        if (trade.isBullish()) {
            return currentPrice >= trade.getTarget1();
        } else {
            return currentPrice <= trade.getTarget1();
        }
    }
    
    /**
     * Check if Target 2 is hit
     */
    private boolean isTarget2Hit(ActiveTrade trade, double currentPrice) {
        if (trade.getTarget2() == null || trade.getTarget2() <= 0) {
            return false;
        }
        
        if (trade.isBullish()) {
            return currentPrice >= trade.getTarget2();
        } else {
            return currentPrice <= trade.getTarget2();
        }
    }
    
    /**
     * Check if trailing stop is hit
     */
    private boolean isTrailingStopHit(ActiveTrade trade, double currentPrice) {
        Double trailingStop = trade.getTrailingStopLoss();
        if (trailingStop == null) return false;
        
        if (trade.isBullish()) {
            return currentPrice <= trailingStop;
        } else {
            return currentPrice >= trailingStop;
        }
    }
    
    /**
     * Check for 1% drop from previous close (Enhanced Price Action logic)
     */
    private boolean isPercentDropFromPreviousClose(ActiveTrade trade, double currentPrice) {
        Double previousClose = extractPreviousClose(trade);
        if (previousClose == null) return false;
        
        if (trade.isBullish()) {
            double onePercentLowerThreshold = previousClose * (1 - PERCENT_DROP_THRESHOLD);
            return currentPrice <= onePercentLowerThreshold;
        } else {
            double onePercentHigherThreshold = previousClose * (1 + PERCENT_DROP_THRESHOLD);
            return currentPrice >= onePercentHigherThreshold;
        }
    }
    
    /**
     * Update trailing stop to breakeven after Target 1 hit
     */
    private void updateTrailingStopToBreakeven(ActiveTrade trade, double entryPrice) {
        if (trade.isBullish()) {
            // For bullish trades, move stop up to entry (or higher if already there)
            double currentTrailing = trade.getTrailingStopLoss() != null ? trade.getTrailingStopLoss() : trade.getStopLoss();
            trade.setTrailingStopLoss(Math.max(currentTrailing, entryPrice));
        } else {
            // For bearish trades, move stop down to entry (or lower if already there)
            double currentTrailing = trade.getTrailingStopLoss() != null ? trade.getTrailingStopLoss() : trade.getStopLoss();
            trade.setTrailingStopLoss(Math.min(currentTrailing, entryPrice));
        }
        
        log.debug("🔄 [EnhancedPA] Updated trailing stop to breakeven: {} for trade {}", 
                trade.getTrailingStopLoss(), trade.getTradeId());
    }
    
    /**
     * Update dynamic trailing stop - moves with favorable price movement
     * Replicates kotsinBackTestBE's improved trailing stop logic
     */
    private void updateDynamicTrailingStop(ActiveTrade trade, double currentPrice) {
        double actualEntryPrice = extractActualEntryPrice(trade);
        double stopLossAmount = actualEntryPrice * STOP_LOSS_PERCENTAGE;
        
        Double currentTrailing = trade.getTrailingStopLoss();
        if (currentTrailing == null) {
            currentTrailing = trade.getStopLoss();
        }
        
        if (trade.isBullish()) {
            // For long trades, move stop up with price movement
            double newTrailingStop = currentPrice - stopLossAmount;
            if (newTrailingStop > currentTrailing) {
                trade.setTrailingStopLoss(newTrailingStop);
                log.debug("📈 [EnhancedPA] Updated trailing stop UP to {} for bullish trade {} (price: {})", 
                        newTrailingStop, trade.getTradeId(), currentPrice);
            }
        } else {
            // For short trades, move stop down with price movement  
            double newTrailingStop = currentPrice + stopLossAmount;
            if (newTrailingStop < currentTrailing) {
                trade.setTrailingStopLoss(newTrailingStop);
                log.debug("📉 [EnhancedPA] Updated trailing stop DOWN to {} for bearish trade {} (price: {})", 
                        newTrailingStop, trade.getTradeId(), currentPrice);
            }
        }
    }
    
    /**
     * Calculate partial exit result for Target 1 hit
     */
    public TradeResult calculatePartialExitResult(ActiveTrade trade, double exitPrice, LocalDateTime exitTime) {
        TradeResult result = new TradeResult();
        
        // Basic trade info
        result.setTradeId(trade.getTradeId());
        result.setScripCode(trade.getScripCode());
        result.setCompanyName(trade.getCompanyName());
        result.setSignalType(trade.getSignalType());
        result.setStrategyName(trade.getStrategyName());
        
        // Entry/Exit details
        result.setEntryPrice(extractActualEntryPrice(trade));
        result.setEntryTime(trade.getEntryTime());
        result.setExitPrice(exitPrice);
        result.setExitTime(exitTime);
        result.setExitReason("PARTIAL_TARGET_1");
        
        // Position details (50% of original position)
        Integer originalSize = trade.getPositionSize();
        Integer partialSize = originalSize != null ? originalSize / 2 : 500; // 50% position
        result.setPositionSize(partialSize);
        
        // Calculate P&L for partial position
        result.calculateProfitLoss();
        result.calculateDuration();
        
        // Mark as successful partial exit
        result.setSuccessful(true);
        
        // Add Enhanced Price Action metadata
        result.addMetadata("enhancedPriceAction", true);
        result.addMetadata("partialExit", true);
        result.addMetadata("partialExitPercentage", 50.0);
        result.addMetadata("remainingPositionActive", true);
        
        return result;
    }
    
    // Helper methods for extracting data from trade metadata
    
    private Double extractSignalPrice(ActiveTrade trade) {
        Object signalPrice = trade.getMetadata().get("signalPrice");
        return signalPrice instanceof Number ? ((Number) signalPrice).doubleValue() : null;
    }
    
    private Double extractOffendingPivot(ActiveTrade trade) {
        Object delayOnPivot = trade.getMetadata().get("delayOnPivot");
        return delayOnPivot instanceof Number ? ((Number) delayOnPivot).doubleValue() : null;
    }
    
    private double extractActualEntryPrice(ActiveTrade trade) {
        Object actualEntryPrice = trade.getMetadata().get("actualEntryPrice");
        if (actualEntryPrice instanceof Number) {
            return ((Number) actualEntryPrice).doubleValue();
        }
        return trade.getEntryPrice() != null ? trade.getEntryPrice() : 0.0;
    }
    
    private Double extractPreviousClose(ActiveTrade trade) {
        Object previousClose = trade.getMetadata().get("previousClose");
        return previousClose instanceof Number ? ((Number) previousClose).doubleValue() : null;
    }
    
    /**
     * Update previous close for percent drop calculation
     */
    public void updatePreviousClose(ActiveTrade trade, double currentPrice) {
        trade.addMetadata("previousClose", currentPrice);
    }
} 