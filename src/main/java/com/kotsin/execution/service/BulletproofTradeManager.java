package com.kotsin.execution.service;

import com.kotsin.execution.model.ActiveTrade;
import com.kotsin.execution.model.TradeResult;
import com.kotsin.execution.producer.TradeResultProducer;
import com.kotsin.execution.producer.ProfitLossProducer;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

/**
 * 🛡️ BULLETPROOF TRADE MANAGER 🛡️
 * 
 * Single source of truth for trade execution with:
 * - ONE trade at a time (eliminates dual storage)
 * - Perfect price update pipeline: Entry → Exit → P&L
 * - Smart pivot retest entry logic  
 * - Hierarchical exit: Stop Loss → Target 1 (50%) → Trailing Stop (1%) → Target 2
 * - Fixed ₹1 lakh per trade with correct P&L tracking
 * - Zero zombie trades, zero confusion
 */
@Service
@Slf4j
@RequiredArgsConstructor
public class BulletproofTradeManager {
    
    private final TradeResultProducer tradeResultProducer;
    private final ProfitLossProducer profitLossProducer;
    private final TelegramNotificationService telegramNotificationService;
    
    // 🎯 SINGLE SOURCE OF TRUTH - Only one trade allowed at a time
    private final AtomicReference<ActiveTrade> currentTrade = new AtomicReference<>();
    
    // 💰 Capital Management - Simple and bulletproof
    private static final double INITIAL_CAPITAL = 100000.0; // ₹1 lakh
    private static final double TRADE_AMOUNT = 100000.0;    // ₹1 lakh per trade
    private static final double MAX_STOP_LOSS_PERCENT = 1.0; // Max 1% stop loss
    private static final double TARGET_1_PERCENT = 1.5;     // 1.5% for target 1
    private static final double TRAILING_STOP_PERCENT = 1.0; // 1% trailing stop
    
    // 📊 P&L Tracking - Single source of truth
    private double totalRealizedPnL = 0.0;
    private int totalTrades = 0;
    private int winningTrades = 0;
    
    private static final DateTimeFormatter TIME_FORMAT = DateTimeFormatter.ofPattern("HH:mm:ss");
    
    /**
     * 🚀 CREATE NEW TRADE - Only one at a time
     */
    public boolean createTrade(String scripCode, String signal, double entryPrice, 
                              double stopLoss, double target1, LocalDateTime signalTime) {
        
        // 🛡️ BULLETPROOF: Only one trade at a time
        if (currentTrade.get() != null) {
            log.warn("🚫 [BulletproofTM] Cannot create trade for {} - already have active trade: {}", 
                    scripCode, currentTrade.get().getScripCode());
            return false;
        }
        
        // 🔍 VALIDATE TRADE SETUP
        if (!isValidTradeSetup(entryPrice, stopLoss, target1, signal)) {
            return false;
        }
        
        // 🏗️ CREATE BULLETPROOF TRADE
        ActiveTrade trade = createBulletproofTrade(scripCode, signal, entryPrice, stopLoss, target1, signalTime);
        
        // 🎯 ATOMIC ASSIGNMENT - Thread-safe single trade
        boolean created = currentTrade.compareAndSet(null, trade);
        
        if (created) {
            log.info("🎯 [BulletproofTM] TRADE CREATED: {} {} @ {} (SL: {}, T1: {}) - Amount: ₹{}", 
                    scripCode, signal, entryPrice, stopLoss, target1, 
                    String.format("%.0f", TRADE_AMOUNT));
            
            // 📱 Send notification
            sendTradeCreatedNotification(trade);
            return true;
        } else {
            log.error("🚨 [BulletproofTM] ATOMIC ASSIGNMENT FAILED - Race condition detected!");
            return false;
        }
    }
    
    /**
     * 💹 PERFECT PRICE UPDATE PIPELINE
     * Entry → Exit → P&L Update (No dual processing)
     */
    public void updatePrice(String scripCode, double price, LocalDateTime timestamp) {
        ActiveTrade trade = currentTrade.get();
        
        // 🔍 VALIDATE TRADE EXISTS
        if (trade == null) {
            // Silent - no spam for non-matching scripts
            return;
        }
        
        // 🔍 VALIDATE SCRIPT MATCH
        if (!trade.getScripCode().equals(scripCode)) {
            log.debug("💹 [BulletproofTM] Price update for {} but active trade is {}", 
                     scripCode, trade.getScripCode());
            return;
        }
        
        // 🎯 PERFECT PIPELINE: Check Entry → Check Exit → Update P&L
        if (!trade.getEntryTriggered()) {
            checkEntryConditions(trade, price, timestamp);
        } else {
            checkExitConditions(trade, price, timestamp);
            updateUnrealizedPnL(trade, price);
        }
        
        // 📊 Update price tracking
        trade.updatePrice(price, timestamp);
    }
    
    /**
     * 🎯 SMART ENTRY LOGIC - Pivot retest with target direction movement
     */
    private void checkEntryConditions(ActiveTrade trade, double price, LocalDateTime timestamp) {
        double signalPrice = (Double) trade.getMetadata().get("signalPrice");
        double stopLoss = trade.getStopLoss();
        boolean isBullish = "BUY".equals(trade.getSignalType()) || "BULLISH".equals(trade.getSignalType());
        
        log.debug("🎯 [BulletproofTM] Entry check: {} - Current: {}, Signal: {}, SL: {}, Bullish: {}", 
                 trade.getScripCode(), price, signalPrice, stopLoss, isBullish);
        
        boolean shouldEnter = false;
        String entryReason = "";
        
        if (isBullish) {
            // 🟢 BULLISH ENTRY: Price retests near stop loss (pivot) then moves toward target
            double retestZone = stopLoss + ((signalPrice - stopLoss) * 0.2); // 20% above stop loss
            if (price <= retestZone && price > stopLoss) {
                shouldEnter = true;
                entryReason = String.format("Bullish retest at %.2f (near pivot SL: %.2f)", price, stopLoss);
            }
        } else {
            // 🔴 BEARISH ENTRY: Price retests near stop loss (pivot) then moves toward target  
            double retestZone = stopLoss - ((stopLoss - signalPrice) * 0.2); // 20% below stop loss
            if (price >= retestZone && price < stopLoss) {
                shouldEnter = true;
                entryReason = String.format("Bearish retest at %.2f (near pivot SL: %.2f)", price, stopLoss);
            }
        }
        
        if (shouldEnter) {
            executeEntry(trade, price, timestamp, entryReason);
        }
    }
    
    /**
     * 🚀 EXECUTE ENTRY - Perfect entry execution
     */
    private void executeEntry(ActiveTrade trade, double entryPrice, LocalDateTime timestamp, String entryReason) {
        // 🎯 CALCULATE POSITION SIZE - Fixed ₹1 lakh per trade
        double riskPerShare = Math.abs(entryPrice - trade.getStopLoss());
        int positionSize = (int) (TRADE_AMOUNT / entryPrice);
        
        // 📈 UPDATE TRADE STATE
        trade.setEntryTriggered(true);
        trade.setEntryPrice(entryPrice);
        trade.setEntryTime(timestamp);
        trade.setPositionSize(positionSize);
        trade.setStatus(ActiveTrade.TradeStatus.ACTIVE);
        trade.setHighSinceEntry(entryPrice);
        trade.setLowSinceEntry(entryPrice);
        
        log.info("🚀 [BulletproofTM] ENTRY EXECUTED: {} at {} - Position: {} shares, Amount: ₹{}, Reason: {}", 
                trade.getScripCode(), entryPrice, positionSize, 
                String.format("%.0f", positionSize * entryPrice), entryReason);
        
        // 📊 PUBLISH ENTRY EVENT
        profitLossProducer.publishTradeEntry(trade, entryPrice);
        
        // 📱 SEND NOTIFICATION
        sendTradeEnteredNotification(trade, entryPrice, entryReason);
    }
    
    /**
     * 🚪 COMPREHENSIVE EXIT LOGIC - Hierarchical exit system
     * 1. Stop Loss (nearest pivot or max 1%)
     * 2. Target 1 (1.5% gain, exit 50% position)  
     * 3. Trailing Stop (1% from high) or Target 2
     */
    private void checkExitConditions(ActiveTrade trade, double price, LocalDateTime timestamp) {
        boolean isBullish = "BUY".equals(trade.getSignalType()) || "BULLISH".equals(trade.getSignalType());
        double entryPrice = trade.getEntryPrice();
        double stopLoss = trade.getStopLoss();
        
        // 📊 UPDATE HIGH/LOW TRACKING
        if (price > trade.getHighSinceEntry()) {
            trade.setHighSinceEntry(price);
        }
        if (price < trade.getLowSinceEntry()) {
            trade.setLowSinceEntry(price);
        }
        
        // 🔴 1. STOP LOSS CHECK - Highest priority
        boolean stopLossHit = isBullish ? (price <= stopLoss) : (price >= stopLoss);
        if (stopLossHit) {
            exitTrade(trade, price, timestamp, "STOP_LOSS", "Stop loss hit");
            return;
        }
        
        // 🎯 2. TARGET 1 CHECK - 50% position exit
        if (!trade.isTarget1Hit()) {
            double target1Price = isBullish ? 
                entryPrice * (1 + TARGET_1_PERCENT / 100) : 
                entryPrice * (1 - TARGET_1_PERCENT / 100);
            
            boolean target1Hit = isBullish ? (price >= target1Price) : (price <= target1Price);
            if (target1Hit) {
                executePartialExit(trade, price, timestamp, "TARGET_1");
                return;
            }
        }
        
        // 🏃 3. TRAILING STOP CHECK - After Target 1 hit
        if (trade.isTarget1Hit()) {
            double trailingStopPrice = isBullish ?
                trade.getHighSinceEntry() * (1 - TRAILING_STOP_PERCENT / 100) :
                trade.getLowSinceEntry() * (1 + TRAILING_STOP_PERCENT / 100);
            
            boolean trailingStopHit = isBullish ? (price <= trailingStopPrice) : (price >= trailingStopPrice);
            if (trailingStopHit) {
                exitTrade(trade, price, timestamp, "TRAILING_STOP", 
                         String.format("Trailing stop at %.2f (%.1f%% from high/low)", 
                                     trailingStopPrice, TRAILING_STOP_PERCENT));
                return;
            }
        }
        
        // 🎯 4. TARGET 2 CHECK - Full exit
        if (trade.getTarget2() != null) {
            boolean target2Hit = isBullish ? (price >= trade.getTarget2()) : (price <= trade.getTarget2());
            if (target2Hit) {
                exitTrade(trade, price, timestamp, "TARGET_2", "Target 2 achieved");
                return;
            }
        }
    }
    
    /**
     * 🎯 PARTIAL EXIT - 50% position at Target 1
     */
    private void executePartialExit(ActiveTrade trade, double exitPrice, LocalDateTime timestamp, String reason) {
        // 📊 CALCULATE PARTIAL P&L (50% position)
        double partialPnL = calculatePartialPnL(trade, exitPrice, 0.5);
        
        // 📈 UPDATE TRADE STATE
        trade.setTarget1Hit(true);
        trade.addMetadata("partialExitPrice", exitPrice);
        trade.addMetadata("partialExitTime", timestamp);
        trade.addMetadata("partialPnL", partialPnL);
        
        // 💰 UPDATE REALIZED P&L
        totalRealizedPnL += partialPnL;
        
        log.info("🎯 [BulletproofTM] PARTIAL EXIT (50%): {} at {} - P&L: ₹{}, Reason: {}", 
                trade.getScripCode(), exitPrice, String.format("%.2f", partialPnL), reason);
        
        // 📊 PUBLISH PARTIAL EXIT
        publishPartialExit(trade, exitPrice, partialPnL, reason);
        
        // 📱 SEND NOTIFICATION
        sendPartialExitNotification(trade, exitPrice, partialPnL, reason);
    }
    
    /**
     * 🏁 FULL EXIT - Complete trade closure
     */
    private void exitTrade(ActiveTrade trade, double exitPrice, LocalDateTime timestamp, String exitType, String exitReason) {
        // 📊 CALCULATE FINAL P&L
        double finalPnL = calculateFinalPnL(trade, exitPrice);
        
        // 📈 UPDATE TRADE STATE
        trade.setExitPrice(exitPrice);
        trade.setExitTime(timestamp);
        trade.setExitReason(exitReason);
        trade.setStatus(finalPnL > 0 ? ActiveTrade.TradeStatus.CLOSED_PROFIT : ActiveTrade.TradeStatus.CLOSED_LOSS);
        
        // 💰 UPDATE STATISTICS
        totalRealizedPnL += finalPnL;
        totalTrades++;
        if (finalPnL > 0) winningTrades++;
        
        log.info("🏁 [BulletproofTM] TRADE CLOSED: {} at {} - Final P&L: ₹{}, Type: {}, Reason: {}", 
                trade.getScripCode(), exitPrice, String.format("%.2f", finalPnL), exitType, exitReason);
        
        // 🗑️ BULLETPROOF CLEANUP - Atomic removal
        ActiveTrade removed = currentTrade.getAndSet(null);
        if (removed == null) {
            log.error("🚨 [BulletproofTM] CLEANUP RACE CONDITION - Trade was already removed!");
        }
        
        // 📊 PUBLISH FINAL RESULTS
        publishTradeExit(trade, exitPrice, finalPnL, exitReason);
        publishPortfolioUpdate();
        
        // 📱 SEND FINAL NOTIFICATION
        sendTradeClosedNotification(trade, finalPnL, exitType, exitReason);
        
        log.info("✅ [BulletproofTM] CLEANUP COMPLETE - Ready for next trade (Total P&L: ₹{})", 
                String.format("%.2f", totalRealizedPnL));
    }
    
    /**
     * 💰 PERFECT P&L CALCULATION - No double counting
     */
    private double calculatePartialPnL(ActiveTrade trade, double exitPrice, double positionFraction) {
        double entryPrice = trade.getEntryPrice();
        int positionSize = trade.getPositionSize();
        double partialShares = positionSize * positionFraction;
        
        return (exitPrice - entryPrice) * partialShares;
    }
    
    private double calculateFinalPnL(ActiveTrade trade, double exitPrice) {
        double entryPrice = trade.getEntryPrice();
        int positionSize = trade.getPositionSize();
        
        // If partial exit happened, calculate remaining 50% P&L only
        if (trade.isTarget1Hit()) {
            double remainingShares = positionSize * 0.5;
            return (exitPrice - entryPrice) * remainingShares;
        } else {
            // Full position P&L
            return (exitPrice - entryPrice) * positionSize;
        }
    }
    
    private void updateUnrealizedPnL(ActiveTrade trade, double currentPrice) {
        double entryPrice = trade.getEntryPrice();
        int positionSize = trade.getPositionSize();
        
        // Calculate unrealized P&L for remaining position
        double positionFraction = trade.isTarget1Hit() ? 0.5 : 1.0;
        double unrealizedPnL = (currentPrice - entryPrice) * (positionSize * positionFraction);
        
        trade.addMetadata("unrealizedPnL", unrealizedPnL);
    }
    
    /**
     * 🔍 VALIDATION METHODS
     */
    private boolean isValidTradeSetup(double entryPrice, double stopLoss, double target1, String signal) {
        if (entryPrice <= 0 || stopLoss <= 0 || target1 <= 0) {
            log.warn("🚫 [BulletproofTM] Invalid prices - Entry: {}, SL: {}, T1: {}", entryPrice, stopLoss, target1);
            return false;
        }
        
        boolean isBullish = "BUY".equals(signal) || "BULLISH".equals(signal);
        
        // Validate stop loss placement
        if (isBullish && stopLoss >= entryPrice) {
            log.warn("🚫 [BulletproofTM] Invalid bullish setup - SL {} >= Entry {}", stopLoss, entryPrice);
            return false;
        }
        
        if (!isBullish && stopLoss <= entryPrice) {
            log.warn("🚫 [BulletproofTM] Invalid bearish setup - SL {} <= Entry {}", stopLoss, entryPrice);
            return false;
        }
        
        // Validate stop loss is within 1% limit
        double stopLossPercent = Math.abs((entryPrice - stopLoss) / entryPrice) * 100;
        if (stopLossPercent > MAX_STOP_LOSS_PERCENT) {
            log.warn("🚫 [BulletproofTM] Stop loss {}% exceeds maximum {}%", 
                    String.format("%.2f", stopLossPercent), MAX_STOP_LOSS_PERCENT);
            return false;
        }
        
        return true;
    }
    
    private ActiveTrade createBulletproofTrade(String scripCode, String signal, double entryPrice, 
                                              double stopLoss, double target1, LocalDateTime signalTime) {
        String tradeId = generateTradeId(scripCode);
        boolean isBullish = "BUY".equals(signal) || "BULLISH".equals(signal);
        
        // Calculate Target 2 (3% gain/loss)
        double target2 = isBullish ? 
            entryPrice * (1 + 0.03) : 
            entryPrice * (1 - 0.03);
        
        ActiveTrade trade = ActiveTrade.builder()
                .tradeId(tradeId)
                .scripCode(scripCode)
                .companyName(scripCode)
                .signalType(isBullish ? "BULLISH" : "BEARISH")
                .strategyName("BULLETPROOF_PIVOT_RETEST")
                .signalTime(signalTime)
                .stopLoss(stopLoss)
                .target1(target1)
                .target2(target2)
                .status(ActiveTrade.TradeStatus.WAITING_FOR_ENTRY)
                .entryTriggered(false)
                .target1Hit(false)
                .target2Hit(false)
                .useTrailingStop(true)
                .build();
        
        // Add metadata
        trade.addMetadata("signalPrice", entryPrice);
        trade.addMetadata("strategy", "BULLETPROOF_PIVOT_RETEST");
        trade.addMetadata("tradeAmount", TRADE_AMOUNT);
        trade.addMetadata("createdTime", LocalDateTime.now());
        
        return trade;
    }
    
    private String generateTradeId(String scripCode) {
        return "BT_" + scripCode + "_" + System.currentTimeMillis();
    }
    
    /**
     * 📊 PUBLISHING METHODS
     */
    private void publishPartialExit(ActiveTrade trade, double exitPrice, double partialPnL, String reason) {
        // Create partial trade result
        TradeResult result = TradeResult.builder()
                .tradeId(trade.getTradeId() + "_PARTIAL")
                .scripCode(trade.getScripCode())
                .entryPrice(trade.getEntryPrice())
                .exitPrice(exitPrice)
                .positionSize(trade.getPositionSize() / 2) // 50% position
                .profitLoss(partialPnL)
                .exitReason(reason)
                .exitTime(LocalDateTime.now())
                .strategyName(trade.getStrategyName())
                .build();
        
        tradeResultProducer.publishTradeResult(result);
        profitLossProducer.publishTradeExit(trade, exitPrice, reason, partialPnL);
    }
    
    private void publishTradeExit(ActiveTrade trade, double exitPrice, double finalPnL, String exitReason) {
        // Calculate total P&L (partial + final)
        double totalTradePnL = finalPnL;
        if (trade.isTarget1Hit()) {
            Double partialPnL = (Double) trade.getMetadata().get("partialPnL");
            if (partialPnL != null) {
                totalTradePnL += partialPnL;
            }
        }
        
        TradeResult result = TradeResult.builder()
                .tradeId(trade.getTradeId())
                .scripCode(trade.getScripCode())
                .entryPrice(trade.getEntryPrice())
                .exitPrice(exitPrice)
                .positionSize(trade.getPositionSize())
                .profitLoss(totalTradePnL)
                .exitReason(exitReason)
                .exitTime(trade.getExitTime())
                .strategyName(trade.getStrategyName())
                .build();
        
        tradeResultProducer.publishTradeResult(result);
        profitLossProducer.publishTradeExit(trade, exitPrice, exitReason, finalPnL);
    }
    
    private void publishPortfolioUpdate() {
        double currentCapital = INITIAL_CAPITAL + totalRealizedPnL;
        double roi = (totalRealizedPnL / INITIAL_CAPITAL) * 100;
        
        profitLossProducer.publishPortfolioUpdate(currentCapital, totalRealizedPnL, roi);
    }
    
    /**
     * 📱 NOTIFICATION METHODS
     */
    private void sendTradeCreatedNotification(ActiveTrade trade) {
        String message = String.format(
            "🎯 NEW TRADE SETUP\n" +
            "Script: %s\n" +
            "Signal: %s\n" +
            "Entry Zone: %.2f\n" +
            "Stop Loss: %.2f\n" +
            "Target 1: %.2f\n" +
            "Amount: ₹%.0f\n" +
            "Status: Waiting for pivot retest entry",
            trade.getScripCode(),
            trade.getSignalType(),
            (Double) trade.getMetadata().get("signalPrice"),
            trade.getStopLoss(),
            trade.getTarget1(),
            TRADE_AMOUNT
        );
        
        telegramNotificationService.sendTradeNotificationMessage(message);
    }
    
    private void sendTradeEnteredNotification(ActiveTrade trade, double entryPrice, String entryReason) {
        String message = String.format(
            "🚀 TRADE ENTERED\n" +
            "Script: %s\n" +
            "Entry: %.2f\n" +
            "Position: %d shares\n" +
            "Amount: ₹%.0f\n" +
            "Reason: %s\n" +
            "Time: %s",
            trade.getScripCode(),
            entryPrice,
            trade.getPositionSize(),
            trade.getPositionSize() * entryPrice,
            entryReason,
            LocalDateTime.now().format(TIME_FORMAT)
        );
        
        telegramNotificationService.sendTradeNotificationMessage(message);
    }
    
    private void sendPartialExitNotification(ActiveTrade trade, double exitPrice, double partialPnL, String reason) {
        String companyName = trade.getCompanyName() != null ? trade.getCompanyName() : trade.getScripCode();
        String message = String.format(
            "🎯 TARGET 1 HIT - 50%% EXIT\n" +
            "Company: %s\n" +
            "Script: %s\n" +
            "Exit Price: %.2f\n" +
            "Partial P&L: ₹%.2f\n" +
            "Remaining: 50%% position\n" +
            "Trailing stop now active\n" +
            "Time: %s",
            companyName,
            trade.getScripCode(),
            exitPrice,
            partialPnL,
            LocalDateTime.now().format(TIME_FORMAT)
        );
        
        // 🚨 FIX: Send P&L messages to correct chat ID (-4924122957)
        telegramNotificationService.sendTimeoutNotification(message);
    }
    
    private void sendTradeClosedNotification(ActiveTrade trade, double finalPnL, String exitType, String exitReason) {
        double totalPnL = finalPnL;
        if (trade.isTarget1Hit()) {
            Double partialPnL = (Double) trade.getMetadata().get("partialPnL");
            if (partialPnL != null) {
                totalPnL += partialPnL;
            }
        }
        
        String emoji = totalPnL > 0 ? "💰" : "🔴";
        String status = totalPnL > 0 ? "PROFIT" : "LOSS";
        String companyName = trade.getCompanyName() != null ? trade.getCompanyName() : trade.getScripCode();
        
        String message = String.format(
            "%s TRADE CLOSED - %s\n" +
            "Company: %s\n" +
            "Script: %s\n" +
            "Entry: %.2f → Exit: %.2f\n" +
            "Total P&L: ₹%.2f\n" +
            "Exit Type: %s\n" +
            "Reason: %s\n" +
            "Portfolio P&L: ₹%.2f\n" +
            "Win Rate: %.1f%% (%d/%d)\n" +
            "Time: %s",
            emoji, status,
            companyName,
            trade.getScripCode(),
            trade.getEntryPrice(), trade.getExitPrice(),
            totalPnL,
            exitType,
            exitReason,
            totalRealizedPnL,
            totalTrades > 0 ? (winningTrades * 100.0 / totalTrades) : 0.0,
            winningTrades, totalTrades,
            LocalDateTime.now().format(TIME_FORMAT)
        );
        
        // 🚨 FIX: Send P&L messages to correct chat ID (-4924122957)
        telegramNotificationService.sendTimeoutNotification(message);
    }
    
    /**
     * 📊 STATUS AND MONITORING METHODS
     */
    public boolean hasActiveTrade() {
        return currentTrade.get() != null;
    }
    
    public ActiveTrade getCurrentTrade() {
        return currentTrade.get();
    }
    
    public Map<String, Object> getPortfolioStats() {
        Map<String, Object> stats = new ConcurrentHashMap<>();
        stats.put("currentCapital", INITIAL_CAPITAL + totalRealizedPnL);
        stats.put("totalRealizedPnL", totalRealizedPnL);
        stats.put("totalTrades", totalTrades);
        stats.put("winningTrades", winningTrades);
        stats.put("winRate", totalTrades > 0 ? (winningTrades * 100.0 / totalTrades) : 0.0);
        stats.put("hasActiveTrade", hasActiveTrade());
        
        ActiveTrade active = currentTrade.get();
        if (active != null) {
            stats.put("activeTradeScript", active.getScripCode());
            stats.put("activeTradeStatus", active.getStatus());
            Object unrealizedPnL = active.getMetadata().get("unrealizedPnL");
            stats.put("unrealizedPnL", unrealizedPnL != null ? unrealizedPnL : 0.0);
        }
        
        return stats;
    }
    
    /**
     * 🔧 EMERGENCY METHODS
     */
    public boolean emergencyExit(String reason) {
        ActiveTrade trade = currentTrade.get();
        if (trade == null) {
            log.warn("🚨 [BulletproofTM] Emergency exit requested but no active trade");
            return false;
        }
        
        log.warn("🚨 [BulletproofTM] EMERGENCY EXIT triggered for {} - Reason: {}", 
                trade.getScripCode(), reason);
        
        // Get last known price or use entry price
        double exitPrice = trade.getCurrentPrice() != null ? trade.getCurrentPrice() : trade.getEntryPrice();
        
        exitTrade(trade, exitPrice, LocalDateTime.now(), "EMERGENCY", reason);
        return true;
    }
    
    public void resetPortfolio(String reason) {
        log.warn("🚨 [BulletproofTM] PORTFOLIO RESET - Reason: {}", reason);
        
        // Emergency exit any active trade
        emergencyExit("Portfolio reset: " + reason);
        
        // Reset statistics
        totalRealizedPnL = 0.0;
        totalTrades = 0;
        winningTrades = 0;
        
        log.info("✅ [BulletproofTM] Portfolio reset complete - Fresh start");
    }
} 