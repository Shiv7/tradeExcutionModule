package com.kotsin.execution.service;

import com.kotsin.execution.model.ActiveTrade;
import com.kotsin.execution.model.TradeResult;
import com.kotsin.execution.producer.TradeResultProducer;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Clean Trade Execution Service - Enhanced Price Action Strategy ONLY
 * 
 * Simplified, robust implementation focused on:
 * 1. Enhanced Price Action entry/exit logic
 * 2. Real-time WebSocket price updates
 * 3. Comprehensive logging
 * 4. Single strategy execution
 */
@Service
@Slf4j
@RequiredArgsConstructor
public class CleanTradeExecutionService {
    
    private final EnhancedPriceActionService enhancedPriceActionService;
    private final TradeResultProducer tradeResultProducer;
    private final TradingHoursService tradingHoursService;
    private final TelegramNotificationService telegramNotificationService;
    
    // Active trades storage - thread-safe
    private final Map<String, ActiveTrade> activeTrades = new ConcurrentHashMap<>();
    
    // Constants
    private static final DateTimeFormatter TIME_FORMAT = DateTimeFormatter.ofPattern("HH:mm:ss");
    private static final int DEFAULT_POSITION_SIZE = 1000;
    private static final double DEFAULT_RISK_PERCENTAGE = 1.0; // 1% risk per trade
    
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
        
        long startTime = System.currentTimeMillis();
        
        try {
            log.info("🎯 [Enhanced30M] Executing signal: {} {} @ {} (SL: {}, T1: {}, Confidence: {})", 
                    scripCode, signal, entryPrice, stopLoss, target1, confidence);
            
            // Validate trading hours
            LocalDateTime now = tradingHoursService.getCurrentISTTime();
            if (!tradingHoursService.shouldProcessTrade("NSE", now)) {
                log.warn("🚫 [Enhanced30M] Skipping {} - outside trading hours: {}", 
                        scripCode, now.format(TIME_FORMAT));
                return;
            }
            
            // Check for existing active trade
            if (activeTrades.containsKey(scripCode)) {
                log.warn("⚠️ [Enhanced30M] Already have active trade for {} - skipping", scripCode);
                return;
            }
            
            // Create active trade
            ActiveTrade trade = createEnhanced30MTrade(scripCode, signal, entryPrice, stopLoss, target1, confidence);
            
            // Store trade
            activeTrades.put(scripCode, trade);
            
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
        if (trade == null) {
            return; // No active trade for this script
        }
        
        try {
            // Update price and timestamp
            trade.updatePrice(price, timestamp);
            
            log.debug("💹 [Enhanced30M] Price update: {} @ {} (Entry: {}, Status: {})", 
                     scripCode, price, trade.getEntryTriggered() ? "TRIGGERED" : "WAITING", trade.getStatus());
            
            // Check entry conditions (if not entered yet)
            if (!trade.getEntryTriggered()) {
                checkEnhancedEntryConditions(trade, price, timestamp);
            }
            
            // Check exit conditions (if trade is active)
            if (trade.getEntryTriggered() && trade.getStatus() == ActiveTrade.TradeStatus.ACTIVE) {
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
            
            // Publish result
            tradeResultProducer.publishTradeResult(result);
            
            // Remove from active trades
            activeTrades.remove(trade.getScripCode());
            
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
                                              Double stopLoss, Double target1, String confidence) {
        
        String tradeId = generateTradeId(scripCode);
        int positionSize = calculatePositionSize(entryPrice, stopLoss);
        boolean isBullish = "BUY".equals(signal);
        
        ActiveTrade trade = ActiveTrade.builder()
                .tradeId(tradeId)
                .scripCode(scripCode)
                .companyName(scripCode) // Simplified - use scripCode as company name
                .signalType(isBullish ? "BULLISH" : "BEARISH")
                .strategyName("ENHANCED_30M")
                .signalTime(LocalDateTime.now())
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
        
        // Add pivot-based entry delay if needed (simplified - no pivot analysis for now)
        trade.addMetadata("entryDelayed", false);
        
        return trade;
    }
    
    /**
     * Calculate position size based on risk management
     */
    private int calculatePositionSize(Double entryPrice, Double stopLoss) {
        if (entryPrice == null || stopLoss == null) {
            return DEFAULT_POSITION_SIZE;
        }
        
        double riskPerShare = Math.abs(entryPrice - stopLoss);
        double riskAmount = DEFAULT_POSITION_SIZE * DEFAULT_RISK_PERCENTAGE / 100.0; // 1% of position
        
        if (riskPerShare > 0) {
            int calculatedSize = (int) (riskAmount / riskPerShare);
            return Math.max(100, Math.min(calculatedSize, 5000)); // Between 100 and 5000 shares
        }
        
        return DEFAULT_POSITION_SIZE;
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
} 