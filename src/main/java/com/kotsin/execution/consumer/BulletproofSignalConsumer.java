package com.kotsin.execution.consumer;

import com.kotsin.execution.model.ActiveTrade;
import com.kotsin.execution.model.TradeResult;
import com.kotsin.execution.producer.TradeResultProducer;
import com.kotsin.execution.producer.ProfitLossProducer;
import com.kotsin.execution.service.TelegramNotificationService;
import com.kotsin.execution.service.TradingHoursService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.ZoneOffset;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 🛡️ BULLETPROOF SIGNAL CONSUMER 🛡️
 * 
 * BRUTALLY FIXED ALL CRITICAL ISSUES:
 * ✅ Target direction validation (bullish targets > entry, bearish targets < entry)
 * ✅ Correct exit priority order (Target 2/3 before trailing stop)
 * ✅ Position size overflow protection for cheap stocks
 * ✅ Entry timeout logic to prevent infinite waiting
 * ✅ Precise partial exit share calculation (no lost shares)
 * ✅ Correct trailing stop calculation after partial exit
 * ✅ Proper signal price vs entry price separation
 * ✅ Thread-safe operations with atomic variables
 * ✅ Null-safe metadata access throughout
 */
@Service
@Slf4j
@RequiredArgsConstructor
public class BulletproofSignalConsumer {
    
    private final TradeResultProducer tradeResultProducer;
    private final ProfitLossProducer profitLossProducer;
    private final TelegramNotificationService telegramNotificationService;
    private final TradingHoursService tradingHoursService;
    
    // 🎯 BULLETPROOF: Single trade storage (eliminates dual storage)
    private final AtomicReference<ActiveTrade> currentTrade = new AtomicReference<>();
    
    // 💰 BULLETPROOF CAPITAL MANAGEMENT
    private static final double INITIAL_CAPITAL = 100000.0; // ₹1 lakh
    private static final double TRADE_AMOUNT = 100000.0;    // ₹1 lakh per trade
    private static final double MAX_STOP_LOSS_PERCENT = 1.0; // Max 1% stop loss validation only
    private static final double TRAILING_STOP_PERCENT = 1.0; // 1% trailing stop
    private static final long ENTRY_TIMEOUT_MS = 60 * 60 * 1000; // 1 hour timeout for entry
    
    // 📊 THREAD-SAFE P&L TRACKING - Fixed with atomic operations
    private final AtomicLong totalRealizedPnLCents = new AtomicLong(0); // Store in paisa for precision
    private final AtomicInteger totalTrades = new AtomicInteger(0);
    private final AtomicInteger winningTrades = new AtomicInteger(0);
    
    private static final DateTimeFormatter TIME_FORMAT = DateTimeFormatter.ofPattern("HH:mm:ss");
    
    /**
     * 🚀 PROCESS STRATEGY SIGNALS - Only one trade at a time
     */
    @KafkaListener(topics = "enhanced-30m-signals", groupId = "bulletproof-trade-execution")
    public void processStrategySignal(Map<String, Object> signalData) {
        try {
            String scripCode = extractStringValue(signalData, "scripCode");
            String signal = extractStringValue(signalData, "signal");
            Double entryPrice = extractDoubleValue(signalData, "entryPrice");
            Double stopLoss = extractDoubleValue(signalData, "stopLoss");
            Double target1 = extractDoubleValue(signalData, "target1");
            Double target2 = extractDoubleValue(signalData, "target2");
            Double target3 = extractDoubleValue(signalData, "target3");
            String exchange = extractStringValue(signalData, "exchange");
            Long timestamp = extractLongValue(signalData, "timestamp");
            
            log.info("🎯 [BulletproofSC] Signal received: {} {} @ {} (SL: {}, T1: {}, T2: {}, T3: {})", 
                    scripCode, signal, entryPrice, stopLoss, target1, target2, target3);
            
            // 🛡️ VALIDATE TRADING HOURS
            LocalDateTime signalTime = timestamp != null ? 
                LocalDateTime.ofInstant(java.time.Instant.ofEpochMilli(timestamp), 
                                      java.time.ZoneId.of("Asia/Kolkata")) :
                tradingHoursService.getCurrentISTTime();
            
            String exchangeForValidation = exchange != null ? exchange : "NSE";
            
            if (!tradingHoursService.shouldProcessTrade(exchangeForValidation, signalTime)) {
                log.warn("🚫 [BulletproofSC] Outside trading hours for {} - {}", exchangeForValidation, 
                        signalTime.format(TIME_FORMAT));
                return;
            }
            
            // 🎯 CREATE TRADE (Only one at a time) - Use pivot-based targets
            createTrade(scripCode, signal, entryPrice, stopLoss, target1, target2, target3, signalTime);
            
        } catch (Exception e) {
            log.error("🚨 [BulletproofSC] Error processing signal: {}", e.getMessage(), e);
        }
    }
    
    /**
     * 🚀 CREATE NEW TRADE - Only one at a time with pivot-based targets
     */
    public boolean createTrade(String scripCode, String signal, double entryPrice, 
                              double stopLoss, Double target1, Double target2, Double target3, 
                              LocalDateTime signalTime) {
        
        // 🛡️ BULLETPROOF: Only one trade at a time
        if (currentTrade.get() != null) {
            log.warn("🚫 [BulletproofSC] Cannot create trade for {} - already have active trade: {}", 
                    scripCode, currentTrade.get().getScripCode());
            return false;
        }
        
        // 🔍 VALIDATE TRADE SETUP - FIXED: Now includes target direction validation
        if (!isValidTradeSetup(entryPrice, stopLoss, target1, target2, target3, signal)) {
            return false;
        }
        
        // 🏗️ CREATE BULLETPROOF TRADE with pivot targets
        ActiveTrade trade = createBulletproofTrade(scripCode, signal, entryPrice, stopLoss, 
                                                 target1, target2, target3, signalTime);
        
        // 🎯 ATOMIC ASSIGNMENT - Thread-safe single trade
        boolean created = currentTrade.compareAndSet(null, trade);
        
        if (created) {
            log.info("🎯 [BulletproofSC] TRADE CREATED: {} {} @ {} (SL: {}, T1: {}, T2: {}, T3: {}) - Amount: ₹{}", 
                    scripCode, signal, entryPrice, stopLoss, target1, target2, target3,
                    String.format("%.0f", TRADE_AMOUNT));
            
            // 📱 Send notification
            sendTradeCreatedNotification(trade);
            return true;
        } else {
            log.error("🚨 [BulletproofSC] ATOMIC ASSIGNMENT FAILED - Race condition detected!");
            return false;
        }
    }
    
    /**
     * 💹 PERFECT PRICE UPDATE PIPELINE
     * Entry → Exit → P&L Update (No dual processing)
     */
    public void updatePrice(String scripCode, double price, LocalDateTime timestamp) {
        ActiveTrade trade = currentTrade.get();
        
        // 🔍 VALIDATE TRADE EXISTS AND MATCHES
        if (trade == null || !trade.getScripCode().equals(scripCode)) {
            return; // Silent - no spam for non-matching scripts
        }
        
        // 🧟‍♂️ BULLETPROOF: Zombie trade detection
        if (trade.getStatus() != null && 
            (trade.getStatus() == ActiveTrade.TradeStatus.CLOSED_PROFIT || 
             trade.getStatus() == ActiveTrade.TradeStatus.CLOSED_LOSS || 
             trade.getStatus() == ActiveTrade.TradeStatus.CLOSED_TIME)) {
            
            log.warn("🧟‍♂️ [BulletproofSC] ZOMBIE TRADE DETECTED: {} - FORCE CLEANUP", scripCode);
            currentTrade.compareAndSet(trade, null);
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
     * 🎯 SMART ENTRY LOGIC - Pivot retest with target direction movement + TIMEOUT
     */
    private void checkEntryConditions(ActiveTrade trade, double price, LocalDateTime timestamp) {
        // 🛡️ NULL-SAFE metadata access
        Object signalPriceObj = trade.getMetadata() != null ? trade.getMetadata().get("signalPrice") : null;
        if (signalPriceObj == null) {
            log.error("🚨 [BulletproofSC] Missing signalPrice in metadata for {}", trade.getScripCode());
            return;
        }
        
        double signalPrice = ((Number) signalPriceObj).doubleValue();
        double stopLoss = trade.getStopLoss();
        boolean isBullish = "BUY".equals(trade.getSignalType()) || "BULLISH".equals(trade.getSignalType());
        
        // ⏰ FIXED: Entry timeout check to prevent infinite waiting
        long signalAgeMs = timestamp.toEpochSecond(ZoneOffset.UTC) * 1000 - 
                          trade.getSignalTime().toEpochSecond(ZoneOffset.UTC) * 1000;
        
        if (signalAgeMs > ENTRY_TIMEOUT_MS) {
            log.warn("🕰️ [BulletproofSC] ENTRY TIMEOUT: {} signal is {} minutes old - Using market entry", 
                     trade.getScripCode(), signalAgeMs / 60000);
            executeEntry(trade, price, timestamp, "Market entry (timeout after " + (signalAgeMs/60000) + " minutes)");
            return;
        }
        
        log.debug("🎯 [BulletproofSC] Entry check: {} - Current: {}, Signal: {}, SL: {}, Bullish: {}", 
                 trade.getScripCode(), price, signalPrice, stopLoss, isBullish);
        
        // 🕳️ FIXED: Gap handling - wait for retest if price comes back above stop loss
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
     * 🚀 EXECUTE ENTRY - Perfect entry execution with precise position sizing + overflow protection
     */
    private void executeEntry(ActiveTrade trade, double entryPrice, LocalDateTime timestamp, String entryReason) {
        // 💸 FIXED: Precise position sizing with overflow protection
        long positionSize = Math.round(TRADE_AMOUNT / entryPrice);
        
        // 🚨 FIXED: Position size overflow protection for very cheap stocks
        if (positionSize > Integer.MAX_VALUE) {
            log.error("🚨 [BulletproofSC] POSITION SIZE OVERFLOW: {} > {} for entry price {}", 
                     positionSize, Integer.MAX_VALUE, entryPrice);
            // Use maximum safe position size
            positionSize = Integer.MAX_VALUE;
        }
        
        // 📈 UPDATE TRADE STATE
        trade.setEntryTriggered(true);
        trade.setEntryPrice(entryPrice);
        trade.setEntryTime(timestamp);
        trade.setPositionSize((int) positionSize); // Safe cast after overflow check
        trade.setStatus(ActiveTrade.TradeStatus.ACTIVE);
        trade.setHighSinceEntry(entryPrice);
        trade.setLowSinceEntry(entryPrice);
        
        // FIXED: Store entry price separately from signal price
        if (trade.getMetadata() != null) {
            trade.addMetadata("actualEntryPrice", entryPrice);
        }
        
        // Calculate actual investment amount
        double actualInvestment = positionSize * entryPrice;
        
        log.info("🚀 [BulletproofSC] ENTRY EXECUTED: {} at {} - Position: {} shares, Amount: ₹{}, Reason: {}", 
                trade.getScripCode(), entryPrice, positionSize, 
                String.format("%.2f", actualInvestment), entryReason);
        
        // 📊 PUBLISH ENTRY EVENT
        profitLossProducer.publishTradeEntry(trade, entryPrice);
        
        // 📱 SEND NOTIFICATION
        sendTradeEnteredNotification(trade, entryPrice, entryReason);
    }
    
    /**
     * 🚪 COMPREHENSIVE EXIT LOGIC - FIXED PRIORITY ORDER
     * 1. Stop Loss (emergency exit - highest priority)
     * 2. Target 1 (50% exit)  
     * 3. Target 2 (full exit - HIGHER priority than trailing stop)
     * 4. Target 3 (full exit - HIGHER priority than trailing stop)
     * 5. Trailing Stop (protection - LOWEST priority)
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
        
        // 🎯 2. TARGET 1 CHECK - 50% position exit using PIVOT TARGET
        if (!trade.isTarget1Hit() && trade.getTarget1() != null && trade.getTarget1() > 0) {
            boolean target1Hit = isBullish ? (price >= trade.getTarget1()) : (price <= trade.getTarget1());
            if (target1Hit) {
                executePartialExit(trade, price, timestamp, "TARGET_1");
                return;
            }
        }
        
        // 🎯 3. TARGET 2 CHECK - FIXED: Higher priority than trailing stop
        if (trade.getTarget2() != null && trade.getTarget2() > 0) {
            boolean target2Hit = isBullish ? (price >= trade.getTarget2()) : (price <= trade.getTarget2());
            if (target2Hit) {
                exitTrade(trade, price, timestamp, "TARGET_2", "Target 2 achieved");
                return;
            }
        }
        
        // 🎯 4. TARGET 3 CHECK - FIXED: Higher priority than trailing stop
        if (trade.getTarget3() != null && trade.getTarget3() > 0) {
            boolean target3Hit = isBullish ? (price >= trade.getTarget3()) : (price <= trade.getTarget3());
            if (target3Hit) {
                exitTrade(trade, price, timestamp, "TARGET_3", "Target 3 achieved");
                return;
            }
        }
        
        // 🏃 5. TRAILING STOP CHECK - LOWEST priority (only after targets checked)
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
    }
    
    /**
     * 🎯 PARTIAL EXIT - 50% position at Target 1 with PRECISE share calculation
     */
    private void executePartialExit(ActiveTrade trade, double exitPrice, LocalDateTime timestamp, String reason) {
        // 📊 CALCULATE PARTIAL P&L (50% position)
        double partialPnL = calculatePartialPnL(trade, exitPrice, 0.5);
        
        // 📈 UPDATE TRADE STATE
        trade.setTarget1Hit(true);
        trade.addMetadata("partialExitPrice", exitPrice);
        trade.addMetadata("partialExitTime", timestamp);
        trade.addMetadata("partialPnL", partialPnL);
        
        // 💰 UPDATE REALIZED P&L - Thread-safe
        long partialPnLCents = Math.round(partialPnL * 100);
        totalRealizedPnLCents.addAndGet(partialPnLCents);
        
        log.info("🎯 [BulletproofSC] PARTIAL EXIT (50%): {} at {} - P&L: ₹{}, Reason: {}", 
                trade.getScripCode(), exitPrice, String.format("%.2f", partialPnL), reason);
        
        // 📊 PUBLISH PARTIAL EXIT
        publishPartialExit(trade, exitPrice, partialPnL, reason);
        
        // 📱 SEND NOTIFICATION
        sendPartialExitNotification(trade, exitPrice, partialPnL, reason);
    }
    
    /**
     * 🏁 FULL EXIT - Complete trade closure with FIXED win rate calculation
     */
    private void exitTrade(ActiveTrade trade, double exitPrice, LocalDateTime timestamp, String exitType, String exitReason) {
        // 📊 CALCULATE FINAL P&L
        double finalPnL = calculateFinalPnL(trade, exitPrice);
        
        // 📉 FIXED: Calculate TOTAL P&L for accurate win rate
        double totalTradePnL = finalPnL;
        Object partialPnLObj = trade.getMetadata() != null ? trade.getMetadata().get("partialPnL") : null;
        if (partialPnLObj != null) {
            double partialPnL = ((Number) partialPnLObj).doubleValue();
            totalTradePnL += partialPnL;
        }
        
        // 📈 UPDATE TRADE STATE
        trade.setExitPrice(exitPrice);
        trade.setExitTime(timestamp);
        trade.setExitReason(exitReason);
        trade.setStatus(totalTradePnL > 0 ? ActiveTrade.TradeStatus.CLOSED_PROFIT : ActiveTrade.TradeStatus.CLOSED_LOSS);
        
        // 💰 UPDATE STATISTICS - Thread-safe and ACCURATE win rate
        long finalPnLCents = Math.round(finalPnL * 100);
        totalRealizedPnLCents.addAndGet(finalPnLCents);
        totalTrades.incrementAndGet();
        
        // 📉 FIXED: Win rate based on TOTAL trade P&L, not just final leg
        if (totalTradePnL > 0) {
            winningTrades.incrementAndGet();
        }
        
        log.info("🏁 [BulletproofSC] TRADE CLOSED: {} at {} - Final P&L: ₹{}, Total P&L: ₹{}, Type: {}, Reason: {}", 
                trade.getScripCode(), exitPrice, String.format("%.2f", finalPnL), 
                String.format("%.2f", totalTradePnL), exitType, exitReason);
        
        // 🗑️ BULLETPROOF CLEANUP - Atomic removal
        ActiveTrade removed = currentTrade.getAndSet(null);
        if (removed == null) {
            log.error("🚨 [BulletproofSC] CLEANUP RACE CONDITION - Trade was already removed!");
        }
        
        // 📊 PUBLISH FINAL RESULTS
        publishTradeExit(trade, exitPrice, finalPnL, exitReason);
        publishPortfolioUpdate();
        
        // 📱 SEND FINAL NOTIFICATION
        sendTradeClosedNotification(trade, totalTradePnL, exitType, exitReason);
        
        double currentTotalPnL = totalRealizedPnLCents.get() / 100.0;
        log.info("✅ [BulletproofSC] CLEANUP COMPLETE - Ready for next trade (Total P&L: ₹{})", 
                String.format("%.2f", currentTotalPnL));
    }
    
    // Helper methods
    private boolean isValidTradeSetup(double entryPrice, double stopLoss, Double target1, 
                                     Double target2, Double target3, String signal) {
        if (entryPrice <= 0 || stopLoss <= 0) {
            log.warn("🚫 [BulletproofSC] Invalid prices - Entry: {}, SL: {}", entryPrice, stopLoss);
            return false;
        }
        
        if (target1 == null || target1 <= 0) {
            log.warn("🚫 [BulletproofSC] Missing or invalid Target 1: {}", target1);
            return false;
        }
        
        boolean isBullish = "BUY".equals(signal) || "BULLISH".equals(signal);
        
        // Validate stop loss placement
        if (isBullish && stopLoss >= entryPrice) {
            log.warn("🚫 [BulletproofSC] Invalid bullish setup - SL {} >= Entry {}", stopLoss, entryPrice);
            return false;
        }
        
        if (!isBullish && stopLoss <= entryPrice) {
            log.warn("🚫 [BulletproofSC] Invalid bearish setup - SL {} <= Entry {}", stopLoss, entryPrice);
            return false;
        }
        
        // 🚨 FIXED: TARGET DIRECTION VALIDATION - Critical fix!
        if (isBullish) {
            // BULLISH: All targets must be ABOVE entry price
            if (target1 <= entryPrice) {
                log.error("🚨 [BulletproofSC] BULLISH TARGET 1 BELOW ENTRY: {} <= {} - GUARANTEED LOSS!", 
                         target1, entryPrice);
                return false;
            }
            if (target2 != null && target2 > 0 && target2 <= entryPrice) {
                log.error("🚨 [BulletproofSC] BULLISH TARGET 2 BELOW ENTRY: {} <= {} - GUARANTEED LOSS!", 
                         target2, entryPrice);
                return false;
            }
            if (target3 != null && target3 > 0 && target3 <= entryPrice) {
                log.error("🚨 [BulletproofSC] BULLISH TARGET 3 BELOW ENTRY: {} <= {} - GUARANTEED LOSS!", 
                         target3, entryPrice);
                return false;
            }
        } else {
            // BEARISH: All targets must be BELOW entry price
            if (target1 >= entryPrice) {
                log.error("🚨 [BulletproofSC] BEARISH TARGET 1 ABOVE ENTRY: {} >= {} - GUARANTEED LOSS!", 
                         target1, entryPrice);
                return false;
            }
            if (target2 != null && target2 > 0 && target2 >= entryPrice) {
                log.error("🚨 [BulletproofSC] BEARISH TARGET 2 ABOVE ENTRY: {} >= {} - GUARANTEED LOSS!", 
                         target2, entryPrice);
                return false;
            }
            if (target3 != null && target3 > 0 && target3 >= entryPrice) {
                log.error("🚨 [BulletproofSC] BEARISH TARGET 3 ABOVE ENTRY: {} >= {} - GUARANTEED LOSS!", 
                         target3, entryPrice);
                return false;
            }
        }
        
        // Validate stop loss is within reasonable limit (still check but allow strategy targets)
        double stopLossPercent = Math.abs((entryPrice - stopLoss) / entryPrice) * 100;
        if (stopLossPercent > MAX_STOP_LOSS_PERCENT * 3) { // Allow up to 3% for strategy-based stops
            log.warn("⚠️ [BulletproofSC] Large stop loss {}% for {}, allowing strategy-based decision", 
                    String.format("%.2f", stopLossPercent), signal);
        }
        
        return true;
    }
    
    private ActiveTrade createBulletproofTrade(String scripCode, String signal, double signalPrice, 
                                              double stopLoss, Double target1, Double target2, 
                                              Double target3, LocalDateTime signalTime) {
        String tradeId = generateTradeId(scripCode);
        boolean isBullish = "BUY".equals(signal) || "BULLISH".equals(signal);
        
        // 📈 FIXED: Use ONLY pivot-based targets from strategy module
        ActiveTrade trade = ActiveTrade.builder()
                .tradeId(tradeId)
                .scripCode(scripCode)
                .companyName(scripCode)
                .signalType(isBullish ? "BULLISH" : "BEARISH")
                .strategyName("BULLETPROOF_PIVOT_RETEST")
                .signalTime(signalTime)
                .stopLoss(stopLoss)
                .target1(target1)                    // From strategy pivot analysis
                .target2(target2)                    // From strategy pivot analysis  
                .target3(target3)                    // From strategy pivot analysis
                .status(ActiveTrade.TradeStatus.WAITING_FOR_ENTRY)
                .entryTriggered(false)
                .target1Hit(false)
                .target2Hit(false)
                .useTrailingStop(true)
                .build();
        
        // Add metadata with null safety
        if (trade.getMetadata() == null) {
            trade.setMetadata(new java.util.HashMap<>());
        }
        // 🚨 FIXED: Separate signal price from entry price
        trade.addMetadata("signalPrice", signalPrice);                  // Price when signal was generated
        trade.addMetadata("strategy", "BULLETPROOF_PIVOT_RETEST");
        trade.addMetadata("tradeAmount", TRADE_AMOUNT);
        trade.addMetadata("createdTime", signalTime); // ⏰ FIXED: Consistent timestamp
        
        return trade;
    }
    
    private String generateTradeId(String scripCode) {
        return "BP_" + scripCode + "_" + System.currentTimeMillis();
    }
    
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
    
    // Publishing and notification methods
    private void publishPartialExit(ActiveTrade trade, double exitPrice, double partialPnL, String reason) {
        // 🚨 FIXED: Precise share calculation to avoid losing shares
        int totalShares = trade.getPositionSize();
        int partialShares = totalShares / 2;          // Half the shares
        int remainingShares = totalShares - partialShares; // Ensure no shares lost
        
        log.debug("🔢 [BulletproofSC] Partial exit shares: {} total = {} partial + {} remaining", 
                 totalShares, partialShares, remainingShares);
        
        TradeResult result = TradeResult.builder()
                .tradeId(trade.getTradeId() + "_PARTIAL")
                .scripCode(trade.getScripCode())
                .entryPrice(trade.getEntryPrice())
                .exitPrice(exitPrice)
                .positionSize(partialShares)                    // FIXED: Use calculated partial shares
                .profitLoss(partialPnL)
                .exitReason(reason)
                .exitTime(trade.getExitTime() != null ? trade.getExitTime() : LocalDateTime.now())
                .strategyName(trade.getStrategyName())
                .build();
        
        tradeResultProducer.publishTradeResult(result);
        profitLossProducer.publishTradeExit(trade, exitPrice, reason, partialPnL);
    }
    
    // 🔢 FIXED: Correct total P&L reporting without double counting
    private void publishTradeExit(ActiveTrade trade, double exitPrice, double finalPnL, String exitReason) {
        double totalTradePnL = finalPnL;
        
        // Add partial P&L if exists (null-safe)
        Object partialPnLObj = trade.getMetadata() != null ? trade.getMetadata().get("partialPnL") : null;
        if (partialPnLObj != null) {
            double partialPnL = ((Number) partialPnLObj).doubleValue();
            totalTradePnL += partialPnL;
        }
        
        TradeResult result = TradeResult.builder()
                .tradeId(trade.getTradeId())
                .scripCode(trade.getScripCode())
                .entryPrice(trade.getEntryPrice())
                .exitPrice(exitPrice)
                .positionSize(trade.getPositionSize())
                .profitLoss(totalTradePnL)          // Report TOTAL P&L including partial
                .exitReason(exitReason)
                .exitTime(trade.getExitTime())
                .strategyName(trade.getStrategyName())
                .build();
        
        tradeResultProducer.publishTradeResult(result);
        profitLossProducer.publishTradeExit(trade, exitPrice, exitReason, finalPnL);
    }
    
    private void publishPortfolioUpdate() {
        double currentTotalPnL = totalRealizedPnLCents.get() / 100.0;
        double currentCapital = INITIAL_CAPITAL + currentTotalPnL;
        double roi = (currentTotalPnL / INITIAL_CAPITAL) * 100;
        
        profitLossProducer.publishPortfolioUpdate(currentCapital, currentTotalPnL, roi);
    }
    
    // Notification methods
    private void sendTradeCreatedNotification(ActiveTrade trade) {
        // 🚨 FIXED: Use proper signal price from metadata
        Object signalPriceObj = trade.getMetadata() != null ? trade.getMetadata().get("signalPrice") : null;
        double signalPrice = signalPriceObj != null ? ((Number) signalPriceObj).doubleValue() : trade.getStopLoss();
        
        String message = String.format(
            "🎯 NEW TRADE SETUP\n" +
            "Script: %s\n" +
            "Signal: %s\n" +
            "Signal Price: %.2f\n" +
            "Stop Loss: %.2f\n" +
            "Target 1: %.2f\n" +
            "Target 2: %s\n" +
            "Target 3: %s\n" +
            "Amount: ₹%.0f\n" +
            "Status: Waiting for pivot retest entry",
            trade.getScripCode(),
            trade.getSignalType(),
            signalPrice,
            trade.getStopLoss(),
            trade.getTarget1(),
            trade.getTarget2() != null ? String.format("%.2f", trade.getTarget2()) : "Not set",
            trade.getTarget3() != null ? String.format("%.2f", trade.getTarget3()) : "Not set",
            TRADE_AMOUNT
        );
        
        telegramNotificationService.sendTradeNotificationMessage(message);
    }
    
    private void sendTradeEnteredNotification(ActiveTrade trade, double entryPrice, String entryReason) {
        // 🚨 FIXED: Show both signal price and actual entry price
        Object signalPriceObj = trade.getMetadata() != null ? trade.getMetadata().get("signalPrice") : null;
        double signalPrice = signalPriceObj != null ? ((Number) signalPriceObj).doubleValue() : entryPrice;
        
        String message = String.format(
            "🚀 TRADE ENTERED\n" +
            "Script: %s\n" +
            "Signal Price: %.2f\n" +
            "Entry Price: %.2f\n" +
            "Position: %d shares\n" +
            "Amount: ₹%.2f\n" +
            "Reason: %s\n" +
            "Time: %s",
            trade.getScripCode(),
            signalPrice,
            entryPrice,
            trade.getPositionSize(),
            trade.getPositionSize() * entryPrice,
            entryReason,
            LocalDateTime.now().format(TIME_FORMAT)
        );
        
        telegramNotificationService.sendTradeNotificationMessage(message);
    }
    
    private void sendPartialExitNotification(ActiveTrade trade, double exitPrice, double partialPnL, String reason) {
        String message = String.format(
            "🎯 PARTIAL EXIT (50%%)\n" +
            "Script: %s\n" +
            "Exit: %.2f\n" +
            "Partial P&L: ₹%.2f\n" +
            "Reason: %s\n" +
            "Remaining: 50%% position\n" +
            "Time: %s",
            trade.getScripCode(),
            exitPrice,
            partialPnL,
            reason,
            LocalDateTime.now().format(TIME_FORMAT)
        );
        
        telegramNotificationService.sendTradeNotificationMessage(message);
    }
    
    private void sendTradeClosedNotification(ActiveTrade trade, double totalPnL, String exitType, String exitReason) {
        int currentTotalTrades = totalTrades.get();
        int currentWinningTrades = winningTrades.get();
        double winRate = currentTotalTrades > 0 ? (currentWinningTrades * 100.0 / currentTotalTrades) : 0.0;
        
        String message = String.format(
            "🏁 TRADE CLOSED\n" +
            "Script: %s\n" +
            "Exit: %.2f\n" +
            "Total P&L: ₹%.2f %s\n" +
            "Type: %s\n" +
            "Reason: %s\n" +
            "Win Rate: %.1f%% (%d/%d)\n" +
            "Time: %s",
            trade.getScripCode(),
            trade.getExitPrice(),
            totalPnL,
            totalPnL > 0 ? "✅" : "❌",
            exitType,
            exitReason,
            winRate,
            currentWinningTrades,
            currentTotalTrades,
            LocalDateTime.now().format(TIME_FORMAT)
        );
        
        telegramNotificationService.sendTradeNotificationMessage(message);
    }
    
    // 💔 FIXED: Null-safe extraction methods
    private String extractStringValue(Map<String, Object> data, String key) {
        Object value = data.get(key);
        return value != null ? value.toString() : null;
    }
    
    private Double extractDoubleValue(Map<String, Object> data, String key) {
        Object value = data.get(key);
        if (value == null) return null;
        
        try {
            if (value instanceof Number) {
                return ((Number) value).doubleValue();
            }
            return Double.parseDouble(value.toString());
        } catch (NumberFormatException e) {
            log.warn("⚠️ [BulletproofSC] Invalid double value for {}: {}", key, value);
            return null;
        }
    }
    
    private Long extractLongValue(Map<String, Object> data, String key) {
        Object value = data.get(key);
        if (value == null) return null;
        
        try {
            if (value instanceof Number) {
                return ((Number) value).longValue();
            }
            return Long.parseLong(value.toString());
        } catch (NumberFormatException e) {
            log.warn("⚠️ [BulletproofSC] Invalid long value for {}: {}", key, value);
            return null;
        }
    }
    
    // Public accessors
    public boolean hasActiveTrade() {
        return currentTrade.get() != null;
    }
    
    public ActiveTrade getCurrentTrade() {
        return currentTrade.get();
    }
    
    // Thread-safe statistics accessors
    public double getTotalRealizedPnL() {
        return totalRealizedPnLCents.get() / 100.0;
    }
    
    public int getTotalTrades() {
        return totalTrades.get();
    }
    
    public int getWinningTrades() {
        return winningTrades.get();
    }
    
    public double getWinRate() {
        int total = totalTrades.get();
        return total > 0 ? (winningTrades.get() * 100.0 / total) : 0.0;
    }
} 