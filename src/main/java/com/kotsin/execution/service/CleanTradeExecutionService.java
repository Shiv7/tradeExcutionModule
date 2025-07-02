package com.kotsin.execution.service;

import com.kotsin.execution.model.ActiveTrade;
import com.kotsin.execution.model.TradeResult;
import com.kotsin.execution.producer.TradeResultProducer;
import com.kotsin.execution.producer.ProfitLossProducer;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.List;
import java.util.ArrayList;

/**
 * Clean Trade Execution Service - Enhanced Price Action Strategy ONLY
 * 
 * Simplified, robust implementation focused on:
 * 1. Enhanced Price Action entry/exit logic
 * 2. Real-time WebSocket price updates
 * 3. Comprehensive logging
 * 4. Single strategy execution
 * 5. 30-minute timeout for delayed entry trades
 */
@Service
@Slf4j
@RequiredArgsConstructor
public class CleanTradeExecutionService {
    
    private final EnhancedPriceActionService enhancedPriceActionService;
    private final TradeResultProducer tradeResultProducer;
    private final ProfitLossProducer profitLossProducer;
    private final CapitalManagementService capitalManagementService;
    private final TradingHoursService tradingHoursService;
    private final TelegramNotificationService telegramNotificationService;
    
    // Active trades storage - thread-safe
    private final Map<String, ActiveTrade> activeTrades = new ConcurrentHashMap<>();
    
    // Constants
    private static final DateTimeFormatter TIME_FORMAT = DateTimeFormatter.ofPattern("HH:mm:ss");
    private static final int DEFAULT_POSITION_SIZE = 1000;
    private static final double DEFAULT_RISK_PERCENTAGE = 1.0; // 1% risk per trade
    private static final int ENTRY_TIMEOUT_MINUTES = 30; // 30-minute timeout for delayed entry
    
    /**
     * Execute Enhanced 30M strategy signal - main entry point
     */
    public void executeEnhanced30MSignal(
            String scripCode,
            String signal,
            Double entryPrice,
            Double stopLoss,
            Double target1,
            String confidence) {
        executeEnhanced30MSignal(scripCode, signal, entryPrice, stopLoss, target1, confidence, LocalDateTime.now());
    }
    
    /**
     * Execute Enhanced 30M strategy signal with specific signal time
     */
    public void executeEnhanced30MSignal(
            String scripCode,
            String signal,
            Double entryPrice,
            Double stopLoss,
            Double target1,
            String confidence,
            LocalDateTime signalTime) {
        executeEnhanced30MSignal(scripCode, signal, entryPrice, stopLoss, target1, confidence, "NSE", signalTime);
    }
    
    /**
     * Execute Enhanced 30M strategy signal with exchange and specific signal time
     */
    public void executeEnhanced30MSignal(
            String scripCode,
            String signal,
            Double entryPrice,
            Double stopLoss,
            Double target1,
            String confidence,
            String exchange,
            LocalDateTime signalTime) {
        
        long startTime = System.currentTimeMillis();
        
        try {
            log.info("🎯 [Enhanced30M] Executing signal: {} {} @ {} (SL: {}, T1: {}, Confidence: {})", 
                    scripCode, signal, entryPrice, stopLoss, target1, confidence);
            
            // Validate trading hours using actual exchange
            LocalDateTime now = tradingHoursService.getCurrentISTTime();
            String exchangeForValidation = exchange != null ? exchange : "NSE"; // Default to NSE if null
            
            if (!tradingHoursService.shouldProcessTrade(exchangeForValidation, now)) {
                log.warn("🚫 [Enhanced30M] Skipping {} - outside trading hours for exchange {}: {}", 
                        scripCode, exchangeForValidation, now.format(TIME_FORMAT));
                return;
            }
            
            log.info("✅ [Enhanced30M] Trading hours validated for exchange {} at {}", 
                    exchangeForValidation, now.format(TIME_FORMAT));
            
            // Check for existing active trade
            if (activeTrades.containsKey(scripCode)) {
                log.warn("⚠️ [Enhanced30M] Already have active trade for {} - skipping", scripCode);
                return;
            }
            
            // Create active trade using capital management
            ActiveTrade trade = createEnhanced30MTrade(scripCode, signal, entryPrice, stopLoss, target1, confidence, signalTime);
            
            // Store trade in local storage AND capital management
            activeTrades.put(scripCode, trade);
            capitalManagementService.addActiveTrade(trade);
            
            long executionTime = System.currentTimeMillis() - startTime;
            log.info("✅ [Enhanced30M] Trade created for {} in {}ms - Waiting for entry conditions", 
                    scripCode, executionTime);
            
            // Send Telegram notification
            sendTradeCreatedNotification(trade);
            
        } catch (Exception e) {
            long executionTime = System.currentTimeMillis() - startTime;
            log.error("🚨 [Enhanced30M] Error executing signal for {} after {}ms: {}", 
                     scripCode, executionTime, e.getMessage(), e);
        }
    }
    
    /**
     * Update trade with real-time price from WebSocket
     */
    public void updateTradeWithPrice(String scripCode, double price, LocalDateTime timestamp) {
        ActiveTrade trade = activeTrades.get(scripCode);
        
        // Enhanced logging for debugging script code matching
        if (trade == null) {
            // Only log every 100th price update for non-matching scripts to avoid spam
            if (System.currentTimeMillis() % 100 == 0) {
                log.debug("💹 [Enhanced30M] Price update for {} @ {} - No active trade found (Total active trades: {})", 
                         scripCode, price, activeTrades.size());
                
                if (!activeTrades.isEmpty()) {
                    log.debug("🔍 [Enhanced30M] Active trade scripts: {}", activeTrades.keySet());
                }
            }
            return; // No active trade for this script
        }
        
        try {
            // Update price and timestamp
            trade.updatePrice(price, timestamp);
            
            // Enhanced logging for trade updates
            log.info("💹 [Enhanced30M] Price update: {} @ {} (Entry: {}, Status: {}, Signal Price: {})", 
                     scripCode, price, 
                     trade.getEntryTriggered() ? String.format("TRIGGERED @ %.2f", trade.getEntryPrice()) : "WAITING", 
                     trade.getStatus(),
                     trade.getMetadata().get("signalPrice"));
            
            // Check entry conditions (if not entered yet)
            if (!trade.getEntryTriggered()) {
                log.info("🎯 [Enhanced30M] CHECKING ENTRY CONDITIONS for {} - Current Price: {}, Trade Status: {}", 
                        scripCode, price, trade.getStatus());
                
                checkEnhancedEntryConditions(trade, price, timestamp);
                
                // Log result of entry check
                if (trade.getEntryTriggered()) {
                    log.info("🚀 [Enhanced30M] ENTRY TRIGGERED! Trade {} now ACTIVE at price {}", 
                            scripCode, price);
                } else {
                    log.info("⏳ [Enhanced30M] Entry conditions NOT met for {} at price {} - still waiting", 
                            scripCode, price);
                }
            }
            
            // Check exit conditions (if trade is active)
            if (trade.getEntryTriggered() && trade.getStatus() == ActiveTrade.TradeStatus.ACTIVE) {
                log.debug("🔍 [Enhanced30M] Checking exit conditions for active trade {} at price {}", 
                         scripCode, price);
                
                checkEnhancedExitConditions(trade, price, timestamp);
            }
            
        } catch (Exception e) {
            log.error("🚨 [Enhanced30M] Error updating trade for {}: {}", scripCode, e.getMessage(), e);
        }
    }
    
    /**
     * Check Enhanced Price Action entry conditions
     */
    private void checkEnhancedEntryConditions(ActiveTrade trade, double currentPrice, LocalDateTime timestamp) {
        boolean shouldEnter = enhancedPriceActionService.checkEnhancedEntryConditions(trade, currentPrice, timestamp);
        
        if (shouldEnter) {
            // Execute entry
            trade.setEntryTriggered(true);
            trade.setEntryPrice(currentPrice);
            trade.setEntryTime(timestamp);
            trade.setStatus(ActiveTrade.TradeStatus.ACTIVE);
            trade.setHighSinceEntry(currentPrice);
            trade.setLowSinceEntry(currentPrice);
            
            log.info("🚀 [Enhanced30M] TRADE ENTERED: {} at {} (Strategy: Enhanced Price Action)", 
                    trade.getScripCode(), currentPrice);
            
            // Publish trade entry to profit-loss topic
            profitLossProducer.publishTradeEntry(trade, currentPrice);
            
            // Send entry notification
            sendTradeEnteredNotification(trade, currentPrice);
            
        } else {
            // Update previous close for Enhanced Price Action calculations
            enhancedPriceActionService.updatePreviousClose(trade, currentPrice);
            
            log.debug("⏳ [Enhanced30M] Entry conditions not met for {} at {}", 
                     trade.getScripCode(), currentPrice);
        }
    }
    
    /**
     * Check Enhanced Price Action exit conditions
     */
    private void checkEnhancedExitConditions(ActiveTrade trade, double currentPrice, LocalDateTime timestamp) {
        String exitReason = enhancedPriceActionService.checkEnhancedExitConditions(trade, currentPrice, timestamp);
        
        if (exitReason != null) {
            // Determine exit price based on reason
            double exitPrice = determineExitPrice(trade, exitReason, currentPrice);
            
            // Close the trade
            closeTrade(trade, exitPrice, timestamp, exitReason);
        } else {
            // Handle partial exit for Target 1 (Enhanced Price Action feature)
            if (trade.getTarget1Hit() && trade.getMetadata().get("partialExitProcessed") == null) {
                handlePartialExit(trade, timestamp);
            }
            
            // Update previous close for Enhanced Price Action calculations
            enhancedPriceActionService.updatePreviousClose(trade, currentPrice);
        }
    }
    
    /**
     * Handle partial exit when Target 1 is hit
     */
    private void handlePartialExit(ActiveTrade trade, LocalDateTime timestamp) {
        // Generate partial exit result (50% position)
        TradeResult partialResult = enhancedPriceActionService.calculatePartialExitResult(
                trade, trade.getTarget1(), timestamp);
        
        // Publish partial result
        tradeResultProducer.publishTradeResult(partialResult);
        
        // Mark partial exit as processed
        trade.addMetadata("partialExitProcessed", true);
        
        log.info("🎯 [Enhanced30M] Target 1 HIT - Partial exit (50%) for {} at {}", 
                trade.getScripCode(), trade.getTarget1());
        
        // Send partial exit notification
        sendPartialExitNotification(trade);
    }
    
    /**
     * Close trade and publish final result
     */
    private void closeTrade(ActiveTrade trade, double exitPrice, LocalDateTime exitTime, String exitReason) {
        try {
            // Update trade status
            trade.setStatus(ActiveTrade.TradeStatus.CLOSED_PROFIT);
            trade.setExitPrice(exitPrice);
            trade.setExitTime(exitTime);
            
            // Calculate final result
            TradeResult result = calculateFinalTradeResult(trade, exitPrice, exitTime, exitReason);
            
            // Calculate P&L for capital management
            double profitLoss = result.getProfitLoss();
            
            // Publish to profit-loss topic FIRST
            profitLossProducer.publishTradeExit(trade, exitPrice, exitReason, profitLoss);
            
            // Publish result to trade-results topic
            tradeResultProducer.publishTradeResult(result);
            
            // Update capital management
            capitalManagementService.removeActiveTrade(trade.getScripCode(), profitLoss);
            
            // Remove from local active trades
            activeTrades.remove(trade.getScripCode());
            
            // Publish portfolio update
            Map<String, Object> capitalStats = capitalManagementService.getCapitalStats();
            profitLossProducer.publishPortfolioUpdate(
                    (Double) capitalStats.get("currentCapital"),
                    (Double) capitalStats.get("totalProfitLoss"),
                    (Double) capitalStats.get("roi")
            );
            
            log.info("🏁 [Enhanced30M] TRADE CLOSED: {} - Entry: {}, Exit: {}, P&L: {}, Reason: {}", 
                    trade.getScripCode(), trade.getEntryPrice(), exitPrice, 
                    result.getProfitLoss(), exitReason);
            
            // Send trade closed notification
            sendTradeClosedNotification(trade, result, exitReason);
            
        } catch (Exception e) {
            log.error("🚨 [Enhanced30M] Error closing trade for {}: {}", trade.getScripCode(), e.getMessage(), e);
        }
    }
    
    /**
     * Create Enhanced 30M active trade
     */
    private ActiveTrade createEnhanced30MTrade(String scripCode, String signal, Double entryPrice, 
                                              Double stopLoss, Double target1, String confidence, LocalDateTime signalTime) {
        
        String tradeId = generateTradeId(scripCode);
        int positionSize = calculatePositionSize(entryPrice, stopLoss);
        boolean isBullish = "BUY".equals(signal);
        
        ActiveTrade trade = ActiveTrade.builder()
                .tradeId(tradeId)
                .scripCode(scripCode)
                .companyName(scripCode) // Simplified - use scripCode as company name
                .signalType(isBullish ? "BULLISH" : "BEARISH")
                .strategyName("ENHANCED_30M")
                .signalTime(signalTime)
                .stopLoss(stopLoss)
                .target1(target1)
                .target2(calculateTarget2(entryPrice, stopLoss, target1, isBullish))
                .positionSize(positionSize)
                .status(ActiveTrade.TradeStatus.WAITING_FOR_ENTRY)
                .entryTriggered(false)
                .target1Hit(false)
                .target2Hit(false)
                .maxHoldingTime(LocalDateTime.now().plusHours(6)) // 6-hour max holding
                .useTrailingStop(true)
                .build();
        
        // Add Enhanced Price Action metadata
        trade.addMetadata("signalPrice", entryPrice);
        trade.addMetadata("confidence", confidence);
        trade.addMetadata("enhancedPriceAction", true);
        trade.addMetadata("strategy", "ENHANCED_30M");
        
        // 🚨 CRITICAL FIX: Implement proper pivot proximity analysis for entry delay
        analyzeEntryDelayRequirement(trade, entryPrice, stopLoss, target1, isBullish);
        
        // Add timeout tracking metadata
        trade.addMetadata("tradeCreatedTime", LocalDateTime.now());
        trade.addMetadata("timeoutMinutes", ENTRY_TIMEOUT_MINUTES);
        
        // Log trade creation with timeout info
        if ((Boolean) trade.getMetadata().get("entryDelayed")) {
            String delayReason = (String) trade.getMetadata().get("delayReason");
            log.info("⏰ [Timeout] Created delayed entry trade for {} - Will timeout in {} minutes if conditions not met (Reason: {})", 
                    scripCode, ENTRY_TIMEOUT_MINUTES, delayReason);
        }
        
        return trade;
    }
    
    /**
     * Analyze if entry should be delayed based on pivot proximity and target distance
     * This implements the missing logic for smart entry timing
     */
    private void analyzeEntryDelayRequirement(ActiveTrade trade, Double entryPrice, Double stopLoss, 
                                            Double target1, boolean isBullish) {
        try {
            log.info("🔍 [EntryDelay] Analyzing pivot proximity for {} at price {}", 
                    trade.getScripCode(), entryPrice);
            
            // Default to immediate entry
            boolean shouldDelayEntry = false;
            Double offendingPivot = null;
            String delayReason = "IMMEDIATE_ENTRY";
            
            // 1. Check target proximity (50% rule)
            double targetProximity = calculateTargetProximity(entryPrice, target1, isBullish);
            log.info("🎯 [EntryDelay] Target proximity: {:.2f}% for {}", targetProximity, trade.getScripCode());
            
            if (targetProximity >= 50.0) {
                shouldDelayEntry = true;
                delayReason = "TARGET_50_PERCENT_CLOSE";
                log.info("⚠️ [EntryDelay] {} is {:.1f}% close to target - DELAYING ENTRY", 
                        trade.getScripCode(), targetProximity);
            }
            
            // 2. Check pivot level proximity (using stop loss as proxy for nearest pivot)
            double pivotProximity = calculatePivotProximity(entryPrice, stopLoss, isBullish);
            log.info("📊 [EntryDelay] Pivot proximity: {:.2f}% for {}", pivotProximity, trade.getScripCode());
            
            if (pivotProximity <= 2.0) { // Within 2% of pivot level
                shouldDelayEntry = true;
                offendingPivot = stopLoss; // Use stop loss level as the pivot to break
                delayReason = "PIVOT_TOO_CLOSE";
                log.info("⚠️ [EntryDelay] {} signal price too close to pivot level - DELAYING ENTRY", 
                        trade.getScripCode());
            }
            
            // 3. Set entry delay metadata
            trade.addMetadata("entryDelayed", shouldDelayEntry);
            trade.addMetadata("delayReason", delayReason);
            trade.addMetadata("targetProximity", targetProximity);
            trade.addMetadata("pivotProximity", pivotProximity);
            
            if (shouldDelayEntry) {
                trade.addMetadata("delayOnPivot", offendingPivot);
                log.info("🛑 [EntryDelay] {} entry DELAYED - Reason: {} | Waiting for breakout/retest", 
                        trade.getScripCode(), delayReason);
            } else {
                log.info("✅ [EntryDelay] {} entry IMMEDIATE - Good proximity to pivots and targets", 
                        trade.getScripCode());
            }
            
        } catch (Exception e) {
            log.error("🚨 [EntryDelay] Error analyzing entry delay for {}: {}", 
                    trade.getScripCode(), e.getMessage(), e);
            // Default to immediate entry on error
            trade.addMetadata("entryDelayed", false);
            trade.addMetadata("delayReason", "ERROR_FALLBACK");
        }
    }
    
    /**
     * Calculate how close the entry price is to the target (as percentage)
     * Returns percentage of distance covered toward target
     */
    private double calculateTargetProximity(Double entryPrice, Double target1, boolean isBullish) {
        if (entryPrice == null || target1 == null) {
            return 0.0;
        }
        
        double totalDistance, currentDistance;
        
        if (isBullish) {
            // For bullish trades: check how close we are to the upside target
            totalDistance = target1 - entryPrice;
            if (totalDistance <= 0) {
                return 100.0; // Already at or past target
            }
            // Assume we want at least 1% gap, so current position relative to that gap
            currentDistance = totalDistance;
            return Math.max(0, (1.0 - (currentDistance / entryPrice)) * 100);
        } else {
            // For bearish trades: check how close we are to the downside target  
            totalDistance = entryPrice - target1;
            if (totalDistance <= 0) {
                return 100.0; // Already at or past target
            }
            currentDistance = totalDistance;
            return Math.max(0, (1.0 - (currentDistance / entryPrice)) * 100);
        }
    }
    
    /**
     * Calculate proximity to pivot level (using stop loss as nearest significant level)
     * Returns percentage distance to pivot
     */
    private double calculatePivotProximity(Double entryPrice, Double stopLoss, boolean isBullish) {
        if (entryPrice == null || stopLoss == null) {
            return 100.0; // Assume far if we can't calculate
        }
        
        double distance = Math.abs(entryPrice - stopLoss);
        return (distance / entryPrice) * 100.0;
    }
    
    /**
     * Calculate position size using Capital Management Service
     */
    private int calculatePositionSize(Double entryPrice, Double stopLoss) {
        if (entryPrice == null || stopLoss == null) {
            return DEFAULT_POSITION_SIZE;
        }
        
        // Use capital management service for proper position sizing
        int positionSize = capitalManagementService.calculatePositionSize(entryPrice, stopLoss);
        
        // Fallback to default if calculation fails
        return positionSize > 0 ? positionSize : DEFAULT_POSITION_SIZE;
    }
    
    /**
     * Calculate Target 2 based on risk-reward ratio
     */
    private Double calculateTarget2(Double entryPrice, Double stopLoss, Double target1, boolean isBullish) {
        if (entryPrice == null || stopLoss == null || target1 == null) {
            return null;
        }
        
        double riskAmount = Math.abs(entryPrice - stopLoss);
        double target2Distance = riskAmount * 2.5; // 2.5:1 risk-reward for Target 2
        
        if (isBullish) {
            return entryPrice + target2Distance;
        } else {
            return entryPrice - target2Distance;
        }
    }
    
    /**
     * Determine exit price based on exit reason
     */
    private double determineExitPrice(ActiveTrade trade, String exitReason, double currentPrice) {
        switch (exitReason) {
            case "STOP_LOSS":
                return trade.getStopLoss();
            case "TARGET_2":
                return trade.getTarget2() != null ? trade.getTarget2() : currentPrice;
            case "TRAILING_STOP":
                return trade.getTrailingStopLoss() != null ? trade.getTrailingStopLoss() : currentPrice;
            case "PERCENT_DROP":
            case "TIME_LIMIT":
            default:
                return currentPrice;
        }
    }
    
    /**
     * Calculate final trade result
     */
    private TradeResult calculateFinalTradeResult(ActiveTrade trade, double exitPrice, 
                                                 LocalDateTime exitTime, String exitReason) {
        
        TradeResult result = TradeResult.builder()
                .tradeId(trade.getTradeId())
                .scripCode(trade.getScripCode())
                .companyName(trade.getCompanyName())
                .strategyName(trade.getStrategyName())
                .signalType(trade.getSignalType())
                .signalTime(trade.getSignalTime())
                .entryPrice(trade.getEntryPrice())
                .entryTime(trade.getEntryTime())
                .exitPrice(exitPrice)
                .exitTime(exitTime)
                .exitReason(exitReason)
                .positionSize(trade.getPositionSize())
                .target1Hit(trade.getTarget1Hit())
                .target2Hit(trade.getTarget2Hit())
                .highSinceEntry(trade.getHighSinceEntry())
                .lowSinceEntry(trade.getLowSinceEntry())
                .initialStopLoss(trade.getStopLoss())
                .finalStopLoss(trade.getTrailingStopLoss())
                .resultGeneratedTime(LocalDateTime.now())
                .build();
        
        // Calculate P&L and duration
        result.calculateProfitLoss();
        result.calculateDuration();
        
        // Add Enhanced Price Action metadata
        result.addMetadata("enhancedPriceAction", true);
        result.addMetadata("confidence", trade.getMetadata().get("confidence"));
        result.addMetadata("strategy", "ENHANCED_30M");
        
        return result;
    }
    
    /**
     * Generate unique trade ID
     */
    private String generateTradeId(String scripCode) {
        return String.format("EPA_%s_%s", scripCode, 
                LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss")));
    }
    
    // Notification methods
    
    private void sendTradeCreatedNotification(ActiveTrade trade) {
        try {
            log.info("📱 [Enhanced30M] Sending trade created notification for {}", trade.getScripCode());
            // For now, just log the notification since TelegramNotificationService expects specific format
        } catch (Exception e) {
            log.warn("Failed to send trade created notification: {}", e.getMessage());
        }
    }
    
    private void sendTradeEnteredNotification(ActiveTrade trade, double entryPrice) {
        try {
            telegramNotificationService.sendTradeNotification(trade);
            log.info("📱 [Enhanced30M] Trade entry notification sent for {}", trade.getScripCode());
        } catch (Exception e) {
            log.warn("Failed to send trade entered notification: {}", e.getMessage());
        }
    }
    
    private void sendPartialExitNotification(ActiveTrade trade) {
        try {
            log.info("📱 [Enhanced30M] Partial exit notification for {} - Target 1 hit at {}", 
                    trade.getScripCode(), trade.getTarget1());
            // For now, just log since TelegramNotificationService doesn't have partial exit method
        } catch (Exception e) {
            log.warn("Failed to send partial exit notification: {}", e.getMessage());
        }
    }
    
    private void sendTradeClosedNotification(ActiveTrade trade, TradeResult result, String exitReason) {
        try {
            telegramNotificationService.sendTradeNotification(trade, result);
            log.info("📱 [Enhanced30M] Trade closed notification sent for {}", trade.getScripCode());
        } catch (Exception e) {
            log.warn("Failed to send trade closed notification: {}", e.getMessage());
        }
    }
    
    /**
     * Get current active trades count
     */
    public int getActiveTradesCount() {
        return activeTrades.size();
    }
    
    /**
     * Get active trades summary
     */
    public String getActiveTradesSummary() {
        if (activeTrades.isEmpty()) {
            return "No active trades";
        }
        
        StringBuilder summary = new StringBuilder();
        summary.append(String.format("Active Trades: %d\n", activeTrades.size()));
        
        activeTrades.values().forEach(trade -> {
            summary.append(String.format("- %s %s (Status: %s)\n", 
                    trade.getScripCode(), trade.getSignalType(), trade.getStatus()));
        });
        
        return summary.toString();
    }
    
    /**
     * Get detailed summary of delayed trades and their timeout status
     */
    public String getDelayedTradesSummary() {
        List<ActiveTrade> delayedTrades = activeTrades.values().stream()
                .filter(trade -> {
                    Object entryDelayed = trade.getMetadata().get("entryDelayed");
                    return entryDelayed != null && (Boolean) entryDelayed && 
                           !trade.getEntryTriggered() && 
                           trade.getStatus() == ActiveTrade.TradeStatus.WAITING_FOR_ENTRY;
                })
                .collect(java.util.stream.Collectors.toList());
        
        if (delayedTrades.isEmpty()) {
            return "No delayed entry trades currently waiting";
        }
        
        StringBuilder summary = new StringBuilder();
        summary.append(String.format("Delayed Entry Trades: %d\n", delayedTrades.size()));
        
        LocalDateTime now = LocalDateTime.now();
        
        for (ActiveTrade trade : delayedTrades) {
            String delayReason = (String) trade.getMetadata().get("delayReason");
            LocalDateTime signalTime = trade.getSignalTime();
            
            long minutesWaiting = signalTime != null ? 
                java.time.Duration.between(signalTime, now).toMinutes() : 0;
            long remainingMinutes = Math.max(0, ENTRY_TIMEOUT_MINUTES - minutesWaiting);
            
            summary.append(String.format("- %s %s: %s (Waiting: %d min, Remaining: %d min)\n", 
                    trade.getScripCode(), trade.getSignalType(), delayReason, 
                    minutesWaiting, remainingMinutes));
        }
        
        return summary.toString();
    }
    
    /**
     * Scheduled task to check for trade entry timeouts every 5 minutes
     * Sends detailed Telegram notifications for timed-out trades
     */
    @Scheduled(fixedRate = 300000) // Check every 5 minutes (300,000 ms)
    public void checkTradeEntryTimeouts() {
        if (activeTrades.isEmpty()) {
            return; // No active trades to check
        }
        
        LocalDateTime now = LocalDateTime.now();
        List<ActiveTrade> timedOutTrades = new ArrayList<>();
        
        // Find trades that have timed out
        for (ActiveTrade trade : activeTrades.values()) {
            if (isTradeTimedOut(trade, now)) {
                timedOutTrades.add(trade);
            }
        }
        
        // Process timed-out trades
        for (ActiveTrade trade : timedOutTrades) {
            handleTradeTimeout(trade, now);
        }
        
        if (!timedOutTrades.isEmpty()) {
            log.info("⏰ [Timeout] Processed {} timed-out delayed entry trades", timedOutTrades.size());
        }
    }
    
    /**
     * Check if a trade has timed out (30 minutes without entry)
     */
    private boolean isTradeTimedOut(ActiveTrade trade, LocalDateTime now) {
        // Only check trades waiting for entry that are delayed
        if (trade.getEntryTriggered() || trade.getStatus() != ActiveTrade.TradeStatus.WAITING_FOR_ENTRY) {
            return false;
        }
        
        // Check if this trade has delayed entry
        Object entryDelayed = trade.getMetadata().get("entryDelayed");
        if (entryDelayed == null || !(Boolean) entryDelayed) {
            return false; // Not a delayed entry trade
        }
        
        // Check timeout based on signal time
        LocalDateTime signalTime = trade.getSignalTime();
        if (signalTime == null) {
            return false;
        }
        
        long minutesWaiting = java.time.Duration.between(signalTime, now).toMinutes();
        return minutesWaiting >= ENTRY_TIMEOUT_MINUTES;
    }
    
    /**
     * Handle a timed-out trade - send notification and clean up
     */
    private void handleTradeTimeout(ActiveTrade trade, LocalDateTime now) {
        try {
            log.info("⏰ [Timeout] Trade {} timed out after {} minutes - cleaning up", 
                    trade.getScripCode(), ENTRY_TIMEOUT_MINUTES);
            
            // Send detailed timeout notification
            sendTimeoutNotification(trade, now);
            
            // Remove from capital management
            capitalManagementService.removeActiveTrade(trade.getScripCode(), 0.0); // No P&L impact
            
            // Remove from local storage
            activeTrades.remove(trade.getScripCode());
            
            log.info("🗑️ [Timeout] Cleaned up timed-out trade for {} - ready for new signals", 
                    trade.getScripCode());
            
        } catch (Exception e) {
            log.error("🚨 [Timeout] Error handling timeout for {}: {}", 
                    trade.getScripCode(), e.getMessage(), e);
        }
    }
    
    /**
     * Send detailed timeout notification via Telegram
     */
    private void sendTimeoutNotification(ActiveTrade trade, LocalDateTime now) {
        try {
            String message = buildTimeoutMessage(trade, now);
            
            // Send to PnL channel (same as exit notifications)
            telegramNotificationService.sendTimeoutNotification(message);
            
            log.info("📱 [Timeout] Sent timeout notification for {}", trade.getScripCode());
            
        } catch (Exception e) {
            log.warn("⚠️ [Timeout] Failed to send timeout notification for {}: {}", 
                    trade.getScripCode(), e.getMessage());
        }
    }
    
    /**
     * Build detailed timeout message for Telegram
     */
    private String buildTimeoutMessage(ActiveTrade trade, LocalDateTime now) {
        String scripCode = trade.getScripCode();
        String companyName = trade.getCompanyName() != null ? trade.getCompanyName() : scripCode;
        String signalType = trade.getSignalType();
        
        // Extract delay information
        String delayReason = (String) trade.getMetadata().get("delayReason");
        Double targetProximity = (Double) trade.getMetadata().get("targetProximity");
        Double pivotProximity = (Double) trade.getMetadata().get("pivotProximity");
        Double offendingPivot = (Double) trade.getMetadata().get("delayOnPivot");
        Double signalPrice = (Double) trade.getMetadata().get("signalPrice");
        
        // Calculate waiting time
        LocalDateTime signalTime = trade.getSignalTime();
        long minutesWaited = signalTime != null ? 
            java.time.Duration.between(signalTime, now).toMinutes() : ENTRY_TIMEOUT_MINUTES;
        
        // Determine next pivot information
        String nextPivotInfo = getNextPivotInfo(trade, offendingPivot);
        
        // Format timing information
        String signalTimeStr = signalTime != null ? 
            signalTime.format(DateTimeFormatter.ofPattern("HH:mm:ss")) : "Unknown";
        String currentTime = now.format(DateTimeFormatter.ofPattern("HH:mm:ss"));
        
        StringBuilder message = new StringBuilder();
        message.append(String.format("⏰ <b>TRADE TIMEOUT - %d MINUTES</b> ⏰\n\n", minutesWaited));
        
        message.append(String.format("%s <b>%s for %s</b>\n", 
                getActionEmoji(signalType), signalType, companyName));
        message.append(String.format("<b>%s (%s)</b>\n\n", companyName, scripCode));
        
        message.append("<b>⏱️ TIMEOUT DETAILS:</b>\n");
        message.append(String.format("🟢 Signal Time: %s\n", signalTimeStr));
        message.append(String.format("🔴 Timeout: %s\n", currentTime));
        message.append(String.format("⏰ Waited: %d minutes\n\n", minutesWaited));
        
        message.append("<b>🚫 ENTRY CONDITIONS NOT MET:</b>\n");
        message.append(String.format("📊 Signal Price: ₹%.2f\n", signalPrice != null ? signalPrice : 0.0));
        
        if ("TARGET_50_PERCENT_CLOSE".equals(delayReason)) {
            message.append(String.format("🎯 Issue: Too close to target (%.1f%%)\n", 
                    targetProximity != null ? targetProximity : 0.0));
            message.append("🔄 Needed: Price to move away from target\n");
        } else if ("PIVOT_TOO_CLOSE".equals(delayReason)) {
            message.append(String.format("📊 Issue: Too close to pivot (%.1f%%)\n", 
                    pivotProximity != null ? pivotProximity : 0.0));
            if (offendingPivot != null) {
                message.append(String.format("🔄 Needed: %s breakout %s ₹%.2f\n", 
                        signalType.equals("BULLISH") ? "Above" : "Below",
                        signalType.equals("BULLISH") ? ">" : "<",
                        offendingPivot));
            }
        }
        
        message.append(String.format("\n<b>📈 NEXT PIVOT ANALYSIS:</b>\n%s", nextPivotInfo));
        
        message.append("\n\n<b>🔄 SWITCHING TO NEXT SIGNAL</b>\n");
        message.append("✅ Trade slot now available for new signals\n");
        message.append("📊 30M strategy continues monitoring\n\n");
        message.append("Enhanced 30M Strategy - Timeout Management");
        
        return message.toString();
    }
    
    /**
     * Get next pivot information for timeout message
     */
    private String getNextPivotInfo(ActiveTrade trade, Double currentPivot) {
        try {
            if (currentPivot == null) {
                return "Next pivot analysis pending for new signal";
            }
            
            boolean isBullish = trade.isBullish();
            double signalPrice = trade.getMetadata().get("signalPrice") != null ? 
                (Double) trade.getMetadata().get("signalPrice") : 0.0;
            
            // Calculate next significant level (simplified)
            double nextPivot;
            String direction;
            
            if (isBullish) {
                nextPivot = currentPivot * 1.02; // 2% above current pivot
                direction = "resistance";
            } else {
                nextPivot = currentPivot * 0.98; // 2% below current pivot  
                direction = "support";
            }
            
            return String.format("🎯 Next %s level: ₹%.2f\n📏 Distance from signal: %.2f%%", 
                    direction, nextPivot, 
                    Math.abs((nextPivot - signalPrice) / signalPrice) * 100);
                    
        } catch (Exception e) {
            log.warn("⚠️ [Timeout] Error calculating next pivot info: {}", e.getMessage());
            return "Next pivot analysis will be done for new signal";
        }
    }
    
    /**
     * Get emoji for signal type
     */
    private String getActionEmoji(String signalType) {
        if ("BULLISH".equalsIgnoreCase(signalType)) return "🟢";
        if ("BEARISH".equalsIgnoreCase(signalType)) return "🔴";
        return "⚪";
    }
    
    /**
     * Force cleanup of a specific delayed trade (manual intervention)
     */
    public boolean forceCleanupDelayedTrade(String scripCode, String reason) {
        ActiveTrade trade = activeTrades.get(scripCode);
        
        if (trade == null) {
            log.warn("⚠️ [ForceCleanup] No active trade found for {}", scripCode);
            return false;
        }
        
        Object entryDelayed = trade.getMetadata().get("entryDelayed");
        if (entryDelayed == null || !(Boolean) entryDelayed) {
            log.warn("⚠️ [ForceCleanup] Trade {} is not a delayed entry trade", scripCode);
            return false;
        }
        
        if (trade.getEntryTriggered()) {
            log.warn("⚠️ [ForceCleanup] Trade {} has already been entered", scripCode);
            return false;
        }
        
        try {
            log.info("🔧 [ForceCleanup] Manually cleaning up delayed trade {} - Reason: {}", 
                    scripCode, reason);
            
            // Send timeout notification with manual reason
            String message = buildManualCleanupMessage(trade, reason);
            telegramNotificationService.sendTimeoutNotification(message);
            
            // Remove from capital management
            capitalManagementService.removeActiveTrade(trade.getScripCode(), 0.0);
            
            // Remove from local storage
            activeTrades.remove(scripCode);
            
            log.info("✅ [ForceCleanup] Successfully cleaned up trade for {}", scripCode);
            return true;
            
        } catch (Exception e) {
            log.error("🚨 [ForceCleanup] Error cleaning up trade {}: {}", scripCode, e.getMessage(), e);
            return false;
        }
    }
    
    /**
     * Build manual cleanup message for Telegram
     */
    private String buildManualCleanupMessage(ActiveTrade trade, String reason) {
        String scripCode = trade.getScripCode();
        String companyName = trade.getCompanyName() != null ? trade.getCompanyName() : scripCode;
        String signalType = trade.getSignalType();
        
        LocalDateTime now = LocalDateTime.now();
        LocalDateTime signalTime = trade.getSignalTime();
        long minutesWaited = signalTime != null ? 
            java.time.Duration.between(signalTime, now).toMinutes() : 0;
        
        StringBuilder message = new StringBuilder();
        message.append("🔧 <b>MANUAL TRADE CLEANUP</b> 🔧\n\n");
        
        message.append(String.format("%s <b>%s for %s</b>\n", 
                getActionEmoji(signalType), signalType, companyName));
        message.append(String.format("<b>%s (%s)</b>\n\n", companyName, scripCode));
        
        message.append(String.format("⏰ Waited: %d minutes\n", minutesWaited));
        message.append(String.format("🔧 Cleanup Reason: %s\n", reason));
        message.append("✅ Trade slot now available for new signals\n\n");
        message.append("Enhanced 30M Strategy - Manual Cleanup");
        
        return message.toString();
    }
} 