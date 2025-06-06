package com.kotsin.execution.service;

import com.kotsin.execution.model.ActiveTrade;
import com.kotsin.execution.model.PendingSignal;
import com.kotsin.execution.model.TradeResult;
import com.kotsin.execution.producer.TradeResultProducer;
import com.kotsin.execution.service.SignalValidationService.SignalValidationResult;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.Map;
import java.util.UUID;
import java.util.HashMap;
import java.util.Collection;

/**
 * Main trade execution service with DYNAMIC VALIDATION architecture
 * 
 * NEW FLOW:
 * 1. Receives signals from Strategy Module → Store as PENDING
 * 2. With each websocket price update → Validate all pending signals 
 * 3. When all kotsinBackTestBE conditions met → Execute trade immediately
 * 4. Continuous validation until signal expires or executed
 */
@Service
@Slf4j
@RequiredArgsConstructor
public class TradeExecutionService {
    
    private final TradeStateManager tradeStateManager;
    private final RiskManager riskManager;
    private final ProfitLossCalculator profitLossCalculator;
    private final TradeResultProducer tradeResultProducer;
    private final IndicatorDataService indicatorDataService;
    private final TradeHistoryService tradeHistoryService;
    private final TradingHoursService tradingHoursService;
    private final CompanyNameService companyNameService;
    private final SignalValidationService signalValidationService;
    private final PendingSignalManager pendingSignalManager; // NEW: Manage pending signals
    
    /**
     * Process a new signal from strategy modules
     * NEW ARCHITECTURE: Store as pending signal for dynamic validation
     */
    public void processNewSignal(
            Map<String, Object> signalData,
            LocalDateTime signalTime,
            String strategyName,
            String signalType,
            String scripCode,
            String companyName,
            String exchange,
            String exchangeType) {
        
        try {
            log.info("🎯 [TradeExecution] Processing NEW signal for {} from strategy: {} - STORING AS PENDING", 
                    scripCode, strategyName);
            
            // Check for duplicate pending signals
            if (pendingSignalManager.hasPendingSignal(scripCode, strategyName)) {
                log.warn("⚠️ [TradeExecution] Already have pending signal for {} with strategy {}, skipping", scripCode, strategyName);
                return;
            }
            
            // Check for active trades (don't add pending if already trading)
            if (tradeStateManager.hasActiveTrade(scripCode, strategyName)) {
                log.warn("⚠️ [TradeExecution] Already have active trade for {} with strategy {}, skipping signal", scripCode, strategyName);
                return;
            }
            
            // Create pending signal with expiry (15 minutes from signal time)
            String signalId = generateSignalId(scripCode, strategyName, signalTime);
            LocalDateTime expiryTime = signalTime.plusMinutes(15); // 15-minute expiry
            
            PendingSignal pendingSignal = PendingSignal.builder()
                    .signalId(signalId)
                    .scripCode(scripCode)
                    .companyName(companyName)
                    .exchange(exchange)
                    .exchangeType(exchangeType)
                    .strategyName(strategyName)
                    .signalType(signalType)
                    .signalTime(signalTime)
                    .expiryTime(expiryTime)
                    .stopLoss(extractDoubleValue(signalData, "stopLoss"))
                    .target1(extractDoubleValue(signalData, "target1"))
                    .target2(extractDoubleValue(signalData, "target2"))
                    .target3(extractDoubleValue(signalData, "target3"))
                    .originalSignalData(signalData)
                    .validationAttempts(0)
                    .build();
            
            // Add metadata
            pendingSignal.addMetadata("originalSignal", signalData);
            pendingSignal.addMetadata("signalSource", strategyName);
            pendingSignal.addMetadata("signalTimeIndicators", indicatorDataService.getComprehensiveIndicators(scripCode));
            
            // Store as pending signal for dynamic validation
            pendingSignalManager.addPendingSignal(pendingSignal);
            
            log.info("📋 [TradeExecution] Signal stored as PENDING: {} - Will validate with each price update until expiry: {}", 
                    pendingSignal.getSummary(), expiryTime.format(java.time.format.DateTimeFormatter.ofPattern("HH:mm:ss")));
            
        } catch (Exception e) {
            log.error("🚨 [TradeExecution] Error processing new signal for {}: {}", scripCode, e.getMessage(), e);
        }
    }
    
    /**
     * DYNAMIC VALIDATION: Update trade with new market price + validate pending signals
     * This is called continuously with each websocket price update
     */
    public void updateTradeWithPrice(String scripCode, double price, LocalDateTime timestamp) {
        try {
            // STEP 1: Validate pending signals with new market price
            validatePendingSignalsWithPrice(scripCode, price, timestamp);
            
            // STEP 2: Update existing active trades
            updateActiveTradesWithPrice(scripCode, price, timestamp);
            
        } catch (Exception e) {
            log.error("🚨 [TradeExecution] Error updating trade with price for {}: {}", scripCode, e.getMessage(), e);
        }
    }
    
    /**
     * DYNAMIC VALIDATION: Check all pending signals for this script with live price
     */
    private void validatePendingSignalsWithPrice(String scripCode, double currentPrice, LocalDateTime timestamp) {
        Collection<PendingSignal> pendingSignals = pendingSignalManager.getPendingSignalsForScript(scripCode);
        
        if (pendingSignals.isEmpty()) {
            return; // No pending signals for this script
        }
        
        log.debug("🔍 [DynamicValidation] Checking {} pending signals for {} at price {}", 
                pendingSignals.size(), scripCode, currentPrice);
        
        for (PendingSignal pendingSignal : pendingSignals) {
            try {
                // Check if signal has expired
                if (pendingSignal.isExpired()) {
                    log.warn("⏰ [DynamicValidation] Signal expired: {} - Removing", pendingSignal.getSummary());
                    pendingSignalManager.removePendingSignal(pendingSignal.getSignalId());
                    continue;
                }
                
                // Perform dynamic validation with current market price
                SignalValidationResult validationResult = signalValidationService
                        .validateSignalWithLivePrice(pendingSignal.getOriginalSignalData(), currentPrice);
                
                if (validationResult.isApproved()) {
                    // ALL CONDITIONS MET! Execute trade immediately
                    log.info("✅ [DynamicValidation] Signal PASSED all validations at price {}: {} - EXECUTING TRADE!", 
                            currentPrice, pendingSignal.getSummary());
                    
                    // Create and execute trade immediately
                    executeValidatedSignal(pendingSignal, currentPrice, timestamp, validationResult);
                    
                    // Remove from pending (trade is now active)
                    pendingSignalManager.removePendingSignal(pendingSignal.getSignalId());
                    
                } else {
                    // Validation failed - record attempt and continue waiting
                    pendingSignal.recordValidationAttempt(validationResult.getReason());
                    
                    // Log every 10th attempt to avoid spam
                    if (pendingSignal.getValidationAttempts() % 10 == 0) {
                        log.debug("⏳ [DynamicValidation] Signal still pending validation #{}: {} - Reason: {}", 
                                pendingSignal.getValidationAttempts(), pendingSignal.getSummary(), validationResult.getReason());
                    }
                }
                
            } catch (Exception e) {
                log.error("🚨 [DynamicValidation] Error validating pending signal {}: {}", 
                        pendingSignal.getSignalId(), e.getMessage(), e);
            }
        }
        
        // Cleanup expired signals periodically (every 100 price updates)
        if (System.currentTimeMillis() % 10000 < 100) { // Roughly every 10 seconds
            pendingSignalManager.cleanupExpiredSignals();
        }
    }
    
    /**
     * Execute a validated signal immediately (all kotsinBackTestBE conditions met)
     */
    private void executeValidatedSignal(PendingSignal pendingSignal, double entryPrice, 
                                      LocalDateTime entryTime, SignalValidationResult validationResult) {
        try {
            log.info("🚀 [TradeExecution] EXECUTING VALIDATED SIGNAL: {} at price {}", 
                    pendingSignal.getSummary(), entryPrice);
            
            // Generate trade ID
            String tradeId = generateTradeId(pendingSignal.getScripCode(), pendingSignal.getStrategyName(), entryTime);
            
            // Extract trade levels from pending signal (Strategy Module's pivot-based calculations)
            Map<String, Double> tradeLevels = extractTradeLevelsFromPendingSignal(pendingSignal, entryPrice);
            
            // Create active trade with immediate entry
            ActiveTrade trade = ActiveTrade.builder()
                    .tradeId(tradeId)
                    .scripCode(pendingSignal.getScripCode())
                    .companyName(pendingSignal.getCompanyName())
                    .exchange(pendingSignal.getExchange())
                    .exchangeType(pendingSignal.getExchangeType())
                    .strategyName(pendingSignal.getStrategyName())
                    .signalType(pendingSignal.getSignalType())
                    .signalTime(pendingSignal.getSignalTime())
                    .originalSignalId(pendingSignal.getSignalId())
                    .status(ActiveTrade.TradeStatus.ACTIVE) // IMMEDIATE ACTIVE STATUS
                    .entryTriggered(true) // IMMEDIATE ENTRY
                    .entryPrice(entryPrice)
                    .entryTime(entryTime)
                    .target1Hit(false)
                    .target2Hit(false)
                    .useTrailingStop(true)
                    .stopLoss(pendingSignal.getStopLoss())
                    .target1(pendingSignal.getTarget1())
                    .target2(pendingSignal.getTarget2())
                    .target3(pendingSignal.getTarget3())
                    .target4(tradeLevels.get("target4"))
                    .riskAmount(tradeLevels.get("riskAmount"))
                    .riskPerShare(tradeLevels.get("riskPerShare"))
                    .positionSize(tradeLevels.get("positionSize").intValue())
                    .maxHoldingTime(entryTime.plusDays(5)) // 5-day max holding
                    .daysHeld(0)
                    .highSinceEntry(entryPrice)
                    .lowSinceEntry(entryPrice)
                    .build();
            
            // Add comprehensive metadata
            trade.addMetadata("pendingSignalData", pendingSignal);
            trade.addMetadata("validationAttempts", pendingSignal.getValidationAttempts());
            trade.addMetadata("kotsinValidation", validationResult);
            trade.addMetadata("validationApproved", true);
            trade.addMetadata("dynamicValidation", true);
            trade.addMetadata("validatedRiskReward", validationResult.getRiskReward());
            trade.addMetadata("entryReason", "Dynamic validation with live price: " + entryPrice);
            trade.addMetadata("entryTimeIndicators", indicatorDataService.getComprehensiveIndicators(pendingSignal.getScripCode()));
            
            // Store active trade
            tradeStateManager.addActiveTrade(trade);
            
            log.info("✅ [TradeExecution] TRADE EXECUTED IMMEDIATELY: {} at entry {} with R:R: {:.2f} after {} validation attempts", 
                    tradeId, entryPrice, validationResult.getRiskReward(), pendingSignal.getValidationAttempts());
            
            // Log detailed execution info
            logDetailedExecutionInfo(trade, pendingSignal, validationResult);
            
        } catch (Exception e) {
            log.error("🚨 [TradeExecution] Error executing validated signal: {}", e.getMessage(), e);
        }
    }
    
    /**
     * Extract trade levels from strategy signal (no recalculation - use Strategy Module's pivot-based targets)
     */
    private Map<String, Double> extractTargetsFromSignal(Map<String, Object> signalData) {
        Map<String, Double> tradeLevels = new HashMap<>();
        
        try {
            // Extract all values calculated by Strategy Module
            Double entryPrice = extractDoubleValue(signalData, "entryPrice");
            Double stopLoss = extractDoubleValue(signalData, "stopLoss");
            Double target1 = extractDoubleValue(signalData, "target1");
            Double target2 = extractDoubleValue(signalData, "target2");
            Double target3 = extractDoubleValue(signalData, "target3");
            
            // Fallbacks for missing fields
            if (entryPrice == null) {
                entryPrice = extractDoubleValue(signalData, "closePrice");
            }
            
            if (entryPrice == null || stopLoss == null || target1 == null) {
                log.error("❌ [TradeExecution] Missing essential price data in signal: entry={}, stop={}, target1={}", 
                         entryPrice, stopLoss, target1);
                return null;
            }
            
            // Calculate derived values
            double riskPerShare = Math.abs(entryPrice - stopLoss);
            int positionSize = calculatePositionSizeFromRisk(entryPrice, riskPerShare);
            double riskAmount = riskPerShare * positionSize;
            
            // Store all levels
            tradeLevels.put("entryPrice", entryPrice);
            tradeLevels.put("stopLoss", stopLoss);
            tradeLevels.put("target1", target1);
            tradeLevels.put("target2", target2 != null ? target2 : calculateFallbackTarget(entryPrice, riskPerShare, 2.5, target1 > entryPrice));
            tradeLevels.put("target3", target3 != null ? target3 : calculateFallbackTarget(entryPrice, riskPerShare, 4.0, target1 > entryPrice));
            tradeLevels.put("target4", calculateFallbackTarget(entryPrice, riskPerShare, 6.0, target1 > entryPrice));
            tradeLevels.put("riskPerShare", riskPerShare);
            tradeLevels.put("riskAmount", riskAmount);
            tradeLevels.put("positionSize", (double) positionSize);
            
            log.debug("📊 [TradeExecution] Extracted trade levels from strategy signal: Entry={}, SL={}, T1={}, T2={}, T3={}", 
                     entryPrice, stopLoss, target1, tradeLevels.get("target2"), tradeLevels.get("target3"));
            
            return tradeLevels;
            
        } catch (Exception e) {
            log.error("🚨 [TradeExecution] Error extracting targets from signal: {}", e.getMessage(), e);
            return null;
        }
    }
    
    /**
     * Calculate fallback target using risk-based multiplier
     */
    private double calculateFallbackTarget(double entryPrice, double riskPerShare, double multiplier, boolean isBullish) {
        double rewardPerShare = riskPerShare * multiplier;
        return isBullish ? entryPrice + rewardPerShare : entryPrice - rewardPerShare;
    }
    
    /**
     * Calculate position size based on risk management (simplified version)
     */
    private int calculatePositionSizeFromRisk(double entryPrice, double riskPerShare) {
        double investment = 100000; // 1 lakh investment
        double maxRiskPerTrade = 1.0; // 1% max risk
        double maxRiskAmount = investment * (maxRiskPerTrade / 100.0);
        
        int calculatedSize = (int) (maxRiskAmount / riskPerShare);
        
        // Apply constraints
        calculatedSize = Math.max(calculatedSize, 1); // Minimum 1 unit
        calculatedSize = Math.min(calculatedSize, 10000); // Maximum 10k units
        
        return calculatedSize;
    }
    
    /**
     * Update existing active trades with new market price (original logic)
     */
    private void updateActiveTradesWithPrice(String scripCode, double price, LocalDateTime timestamp) {
        try {
            Map<String, ActiveTrade> activeTrades = tradeStateManager.getActiveTradesForScript(scripCode);
            
            // Enhanced logging for debugging
            if (!activeTrades.isEmpty()) {
                log.info("💹 [TradeExecution] Price update for {}: {} at {} - Found {} active trades", 
                        scripCode, price, timestamp.format(java.time.format.DateTimeFormatter.ofPattern("HH:mm:ss")), 
                        activeTrades.size());
                
                // Log current trade statuses
                activeTrades.values().forEach(trade -> {
                    double currentPnL = trade.getCurrentPnL();
                    log.info("📊 [TradeExecution] Trade {} - Status: {}, Entry: {}, Current P&L: {}, Position: {}", 
                            trade.getTradeId(), trade.getStatus(), 
                            trade.getEntryTriggered() ? String.format("%.2f", trade.getEntryPrice()) : "Waiting",
                            String.format("%.2f", currentPnL), trade.getPositionSize());
                });
            } else {
                // Enhanced debugging for no trades found
                if (System.currentTimeMillis() % 30000 < 1000) { // Log every 30 seconds
                    log.debug("📉 [TradeExecution] No active trades found for {} at price {} - Total active trades: {}", 
                            scripCode, price, tradeStateManager.getAllActiveTrades().size());
                    
                    // Log what script codes we DO have active trades for
                    Map<String, ActiveTrade> allTrades = tradeStateManager.getAllActiveTrades();
                    if (!allTrades.isEmpty()) {
                        String activeScripts = allTrades.values().stream()
                                .map(ActiveTrade::getScripCode)
                                .distinct()
                                .limit(5)
                                .reduce((a, b) -> a + ", " + b)
                                .orElse("none");
                        log.debug("📋 [TradeExecution] Current active scripts: {}", activeScripts);
                    }
                }
            }
            
            for (ActiveTrade trade : activeTrades.values()) {
                // Store previous values for comparison
                Double previousPrice = trade.getCurrentPrice();
                Double previousPnL = trade.getCurrentPnL();
                
                // Update price in trade object
                trade.updatePrice(price, timestamp);
                
                // Check entry conditions
                if (!trade.getEntryTriggered()) {
                    log.info("🎯 [TradeExecution] Checking entry conditions for trade {} at price {}", trade.getTradeId(), price);
                    checkEntryConditions(trade, price, timestamp);
                    
                    if (trade.getEntryTriggered()) {
                        log.info("🚀 [TradeExecution] TRADE ENTERED: {} at price {} (Previous signal price: {})", 
                                trade.getTradeId(), price, extractPriceFromMetadata(trade));
                    } else {
                        // Enhanced debug logging for why entry wasn't triggered
                        Double signalPrice = extractPriceFromMetadata(trade);
                        String reason = getEntryRejectionReason(trade, price, signalPrice);
                        log.info("⏳ [TradeExecution] Entry conditions NOT met for trade {} - {}", trade.getTradeId(), reason);
                    }
                }
                
                // Check exit conditions for active trades
                if (trade.getEntryTriggered() && trade.getStatus() == ActiveTrade.TradeStatus.ACTIVE) {
                    Double currentPnL = trade.getCurrentPnL();
                    
                    // Log P&L changes for active trades
                    if (previousPnL != null && Math.abs(currentPnL - previousPnL) > 10) {
                        log.info("💰 [TradeExecution] P&L Update - Trade {}: {} → {} (Change: {})", 
                                trade.getTradeId(), 
                                String.format("%.2f", previousPnL),
                                String.format("%.2f", currentPnL),
                                String.format("%.2f", currentPnL - previousPnL));
                    }
                    
                    checkExitConditions(trade, price, timestamp);
                }
                
                // Update trade state
                tradeStateManager.updateTrade(trade);
            }
            
        } catch (Exception e) {
            log.error("🚨 [TradeExecution] Error updating active trades with price for {}: {}", scripCode, e.getMessage(), e);
        }
    }
    
    /**
     * Check if entry conditions are met and capture entry indicator data
     */
    private void checkEntryConditions(ActiveTrade trade, double currentPrice, LocalDateTime timestamp) {
        // Entry logic based on KotsinBackTestBE patterns
        boolean shouldEnter = false;
        String entryReason = "";
        
        Double signalPrice = extractPriceFromMetadata(trade);
        
        log.info("🔍 [EntryCheck] Entry Check for Trade {} - Current: {}, Signal: {}, Type: {}", 
                trade.getTradeId(), currentPrice, signalPrice, trade.getSignalType());
        
        if (trade.isBullish()) {
            log.debug("📈 [EntryCheck] Evaluating BULLISH entry conditions for {}", trade.getTradeId());
            
            // For bullish trades, enter if price moves up from signal or is very close
            if (signalPrice != null) {
                double priceDiff = currentPrice - signalPrice;
                double percentDiff = (priceDiff / signalPrice) * 100;
                
                log.debug("🔍 [EntryCheck] Price analysis: Current={}, Signal={}, Diff={} ({:.3f}%)", 
                        currentPrice, signalPrice, priceDiff, percentDiff);
                
                if (currentPrice >= signalPrice * 1.001) { // 0.1% buffer
                    shouldEnter = true;
                    entryReason = String.format("Price moved up %.3f%% from signal (threshold: 0.1%%)", percentDiff);
                    log.info("✅ [EntryCheck] BULLISH condition met: {}", entryReason);
                } else if (Math.abs(currentPrice - signalPrice) <= signalPrice * 0.002) { // Within 0.2%
                    shouldEnter = true;
                    entryReason = String.format("Price within %.3f%% of signal price (threshold: 0.2%%)", Math.abs(percentDiff));
                    log.info("✅ [EntryCheck] BULLISH condition met: {}", entryReason);
                } else {
                    entryReason = String.format("Price %.3f%% from signal - waiting for 0.1%% move up or 0.2%% proximity", percentDiff);
                    log.debug("⏳ [EntryCheck] BULLISH conditions NOT met: {}", entryReason);
                }
            } else {
                // If no signal price available, enter immediately
                shouldEnter = true;
                entryReason = "No signal price available - immediate entry";
                log.info("✅ [EntryCheck] BULLISH condition met: {}", entryReason);
            }
        } else {
            log.debug("📉 [EntryCheck] Evaluating BEARISH entry conditions for {}", trade.getTradeId());
            
            // For bearish trades, enter if price moves down from signal or is very close
            if (signalPrice != null) {
                double priceDiff = signalPrice - currentPrice; // Positive when price moved down
                double percentDiff = (priceDiff / signalPrice) * 100;
                
                log.debug("🔍 [EntryCheck] Price analysis: Current={}, Signal={}, Diff={} ({:.3f}%)", 
                        currentPrice, signalPrice, priceDiff, percentDiff);
                
                if (currentPrice <= signalPrice * 0.999) { // 0.1% buffer
                    shouldEnter = true;
                    entryReason = String.format("Price moved down %.3f%% from signal (threshold: 0.1%%)", percentDiff);
                    log.info("✅ [EntryCheck] BEARISH condition met: {}", entryReason);
                } else if (Math.abs(currentPrice - signalPrice) <= signalPrice * 0.002) { // Within 0.2%
                    shouldEnter = true;
                    entryReason = String.format("Price within %.3f%% of signal price (threshold: 0.2%%)", Math.abs(percentDiff));
                    log.info("✅ [EntryCheck] BEARISH condition met: {}", entryReason);
                } else {
                    entryReason = String.format("Price %.3f%% from signal - waiting for 0.1%% move down or 0.2%% proximity", -percentDiff);
                    log.debug("⏳ [EntryCheck] BEARISH conditions NOT met: {}", entryReason);
                }
            } else {
                // If no signal price available, enter immediately
                shouldEnter = true;
                entryReason = "No signal price available - immediate entry";
                log.info("✅ [EntryCheck] BEARISH condition met: {}", entryReason);
            }
        }
        
        log.info("🎯 [EntryCheck] Entry Decision for {} - Should Enter: {} ({})", 
                trade.getTradeId(), shouldEnter, entryReason);
        
        if (shouldEnter) {
            // Capture indicator state at entry time
            Map<String, Object> entryTimeIndicators = indicatorDataService.getComprehensiveIndicators(trade.getScripCode());
            
            trade.setEntryTriggered(true);
            trade.setEntryPrice(currentPrice);
            trade.setEntryTime(timestamp);
            trade.setStatus(ActiveTrade.TradeStatus.ACTIVE);
            trade.setHighSinceEntry(currentPrice);
            trade.setLowSinceEntry(currentPrice);
            
            // Add entry time metadata
            trade.addMetadata("entryTimeIndicators", entryTimeIndicators);
            trade.addMetadata("entryConfirmed", true);
            trade.addMetadata("entryReason", entryReason);
            
            log.info("🚀 [EntryCheck] TRADE ENTERED: {} at price {} - {}", trade.getTradeId(), currentPrice, entryReason);
            
            // Log entry details with indicators
            logDetailedEntryInfo(trade, currentPrice, entryTimeIndicators);
        } else {
            // Log why entry was not triggered
            log.debug("❌ [EntryCheck] Entry NOT triggered for {} - {}", trade.getTradeId(), entryReason);
        }
    }
    
    /**
     * Check exit conditions and close trade if needed, capturing exit indicator data
     */
    private void checkExitConditions(ActiveTrade trade, double currentPrice, LocalDateTime timestamp) {
        // Check stop loss first
        if (trade.isStopLossHit()) {
            closeTrade(trade, currentPrice, timestamp, "STOP_LOSS");
            return;
        }
        
        // Check target 1
        if (trade.isTarget1Hit() && !trade.getTarget1Hit()) {
            trade.setTarget1Hit(true);
            
            // Capture target 1 hit indicators
            Map<String, Object> target1Indicators = indicatorDataService.getComprehensiveIndicators(trade.getScripCode());
            trade.addMetadata("target1HitIndicators", target1Indicators);
            trade.addMetadata("target1HitTime", timestamp);
            
            log.info("🎯 Target 1 hit for trade {}: {} - Indicators: {}", 
                    trade.getTradeId(), currentPrice, 
                    indicatorDataService.createIndicatorSummary(
                        indicatorDataService.getCurrentIndicators(trade.getScripCode(), "15m"), "15m"));
            
            // Update trailing stop after target 1
            trade.updateTrailingStop();
        }
        
        // Check target 2
        if (trade.isTarget2Hit() && !trade.getTarget2Hit()) {
            trade.setTarget2Hit(true);
            closeTrade(trade, currentPrice, timestamp, "TARGET_2");
            return;
        }
        
        // Check time-based exit (max holding time)
        if (timestamp.isAfter(trade.getMaxHoldingTime())) {
            closeTrade(trade, currentPrice, timestamp, "TIME_LIMIT");
            return;
        }
        
        // Update trailing stop loss
        trade.updateTrailingStop();
    }
    
    /**
     * Close trade and publish comprehensive result with detailed indicator information
     */
    private void closeTrade(ActiveTrade trade, double exitPrice, LocalDateTime exitTime, String exitReason) {
        try {
            log.info("🏁 Closing trade {}: {} at price: {} due to: {}", 
                    trade.getTradeId(), trade.getScripCode(), exitPrice, exitReason);
            
            // Capture comprehensive exit time indicators
            Map<String, Object> exitTimeIndicators = indicatorDataService.getComprehensiveIndicators(trade.getScripCode());
            
            // Update trade with exit information
            trade.setExitPrice(exitPrice);
            trade.setExitTime(exitTime);
            trade.setExitReason(exitReason);
            
            // Add exit time metadata
            trade.addMetadata("exitTimeIndicators", exitTimeIndicators);
            trade.addMetadata("exitConfirmed", true);
            
            // Set final status
            if ("STOP_LOSS".equals(exitReason)) {
                trade.setStatus(ActiveTrade.TradeStatus.CLOSED_LOSS);
            } else if (exitReason.startsWith("TARGET")) {
                trade.setStatus(ActiveTrade.TradeStatus.CLOSED_PROFIT);
            } else {
                trade.setStatus(ActiveTrade.TradeStatus.CLOSED_TIME);
            }
            
            // Calculate comprehensive profit/loss with all details
            TradeResult result = profitLossCalculator.calculateTradeResult(trade);
            
            if (result != null) {
                // Log detailed exit information before publishing
                logDetailedExitInfo(trade, exitPrice, exitReason, exitTimeIndicators, result);
                
                // Publish comprehensive result to profit/loss topic
                tradeResultProducer.publishTradeResult(result);
                
                log.info("💰 Trade Result Published: {}", result.getSummary());
            }
            
            // Remove from active trades
            tradeStateManager.removeActiveTrade(trade.getTradeId());
            
        } catch (Exception e) {
            log.error("🚨 Error closing trade {}: {}", trade.getTradeId(), e.getMessage(), e);
        }
    }
    
    /**
     * Add strategy-specific context to trade metadata
     */
    private void addStrategyContext(ActiveTrade trade, Map<String, Object> signalData, String strategyName) {
        trade.addMetadata("strategyType", strategyName);
        
        // Add strategy-specific indicator context
        switch (strategyName) {
            case "BB_SUPERTREND":
                trade.addMetadata("bbSuperTrendContext", extractBBSuperTrendContext(signalData));
                break;
            case "BB_BREAKOUT":
                trade.addMetadata("bbBreakoutContext", extractBBBreakoutContext(signalData));
                break;
            case "SUPERTREND_BREAK":
            case "THREE_MINUTE_SUPERTREND":
            case "3M_SUPERTREND":
                trade.addMetadata("superTrendContext", extractSuperTrendContext(signalData));
                break;
            case "FUDKII_STRATEGY":
                trade.addMetadata("fudkiiContext", extractFudkiiContext(signalData));
                break;
            default:
                log.warn("⚠️ Unknown strategy type for context: {} - Adding generic context", strategyName);
                // Add generic context for unknown strategies
                Map<String, Object> genericContext = new HashMap<>();
                genericContext.put("strategyName", strategyName);
                genericContext.put("signalType", signalData.get("signal"));
                genericContext.put("entryPrice", signalData.get("entryPrice"));
                trade.addMetadata("genericContext", genericContext);
        }
    }
    
    /**
     * Log detailed signal information
     */
    private void logDetailedSignalInfo(ActiveTrade trade, Map<String, Object> signalData, Map<String, Object> indicators) {
        String signalSummary = String.format(
            "\n📡 === SIGNAL RECEIVED ===\n" +
            "🆔 Trade ID: %s\n" +
            "📊 Script: %s (%s)\n" +
            "🎯 Strategy: %s\n" +
            "📈 Signal Type: %s\n" +
            "⏰ Signal Time: %s\n" +
            "💰 Signal Price: %.2f\n" +
            "🛡️ Stop Loss: %.2f\n" +
            "🎯 Targets: [%.2f, %.2f, %.2f, %.2f]\n" +
            "📊 Current Indicators: %s\n" +
            "========================",
            trade.getTradeId(),
            trade.getCompanyName(), trade.getScripCode(),
            trade.getStrategyName(),
            trade.getSignalType(),
            trade.getSignalTime(),
            extractDoubleValue(signalData, "closePrice"),
            trade.getStopLoss(),
            trade.getTarget1(), trade.getTarget2(), trade.getTarget3(), trade.getTarget4(),
            indicatorDataService.createIndicatorSummary(
                indicatorDataService.getCurrentIndicators(trade.getScripCode(), "15m"), "15m")
        );
        
        log.info(signalSummary);
    }
    
    /**
     * Log detailed entry information with indicators
     */
    private void logDetailedEntryInfo(ActiveTrade trade, double entryPrice, Map<String, Object> indicators) {
        String entrySummary = String.format(
            "\n🚀 === TRADE ENTRY ===\n" +
            "🆔 Trade ID: %s\n" +
            "📊 Script: %s\n" +
            "💰 Entry Price: %.2f\n" +
            "⏰ Entry Time: %s\n" +
            "📦 Position Size: %d\n" +
            "📊 Entry Indicators (15m): %s\n" +
            "📊 Entry Indicators (30m): %s\n" +
            "====================",
            trade.getTradeId(),
            trade.getScripCode(),
            entryPrice,
            trade.getEntryTime(),
            trade.getPositionSize(),
            indicatorDataService.createIndicatorSummary(
                indicatorDataService.getCurrentIndicators(trade.getScripCode(), "15m"), "15m"),
            indicatorDataService.createIndicatorSummary(
                indicatorDataService.getCurrentIndicators(trade.getScripCode(), "30m"), "30m")
        );
        
        log.info(entrySummary);
    }
    
    /**
     * Log detailed exit information with comprehensive data
     */
    private void logDetailedExitInfo(ActiveTrade trade, double exitPrice, String exitReason, 
                                   Map<String, Object> indicators, TradeResult result) {
        String exitSummary = String.format(
            "\n🏁 === TRADE EXIT ===\n" +
            "🆔 Trade ID: %s\n" +
            "📊 Script: %s\n" +
            "💰 Entry Price: %.2f\n" +
            "🏁 Exit Price: %.2f\n" +
            "❓ Exit Reason: %s\n" +
            "⏱️ Duration: %d minutes\n" +
            "💰 Profit/Loss: %.2f\n" +
            "📊 ROI: %.2f%%\n" +
            "✅ Success: %s\n" +
            "🎯 Targets Hit: T1=%s, T2=%s\n" +
            "📊 Exit Indicators (15m): %s\n" +
            "📊 Exit Indicators (30m): %s\n" +
            "===================",
            trade.getTradeId(),
            trade.getScripCode(),
            trade.getEntryPrice(),
            exitPrice,
            exitReason,
            result.getDurationMinutes(),
            result.getProfitLoss(),
            result.getRoi(),
            result.isWinner() ? "WIN 🎉" : "LOSS 😔",
            trade.getTarget1Hit() ? "YES ✅" : "NO ❌",
            trade.getTarget2Hit() ? "YES ✅" : "NO ❌",
            indicatorDataService.createIndicatorSummary(
                indicatorDataService.getCurrentIndicators(trade.getScripCode(), "15m"), "15m"),
            indicatorDataService.createIndicatorSummary(
                indicatorDataService.getCurrentIndicators(trade.getScripCode(), "30m"), "30m")
        );
        
        log.info(exitSummary);
    }
    
    // Context extraction methods
    private Map<String, Object> extractBBSuperTrendContext(Map<String, Object> signalData) {
        Map<String, Object> context = new HashMap<>();
        context.put("bbUpper", signalData.get("bbUpper"));
        context.put("bbLower", signalData.get("bbLower"));
        context.put("supertrendSignal", signalData.get("supertrendSignal"));
        context.put("closeVsBB", determineBBPosition(signalData));
        return context;
    }
    
    private Map<String, Object> extractSuperTrendContext(Map<String, Object> signalData) {
        Map<String, Object> context = new HashMap<>();
        context.put("supertrendValue", signalData.get("supertrend"));
        context.put("supertrendSignal", signalData.get("supertrendSignal"));
        context.put("supertrendDirection", signalData.get("supertrendIsBullish"));
        return context;
    }
    
    private Map<String, Object> extractFudkiiContext(Map<String, Object> signalData) {
        Map<String, Object> context = new HashMap<>();
        context.put("bullishIndicator", signalData.get("bullishMultiTimeFrameIndicator"));
        context.put("confirmationType", "FUDKII Strategy");
        return context;
    }
    
    private String determineBBPosition(Map<String, Object> signalData) {
        Double closePrice = extractDoubleValue(signalData, "closePrice");
        Double bbUpper = extractDoubleValue(signalData, "bbUpper");
        Double bbLower = extractDoubleValue(signalData, "bbLower");
        
        if (closePrice != null && bbUpper != null && bbLower != null) {
            if (closePrice > bbUpper) return "Above Upper Band";
            else if (closePrice < bbLower) return "Below Lower Band";
            else return "Inside Bands";
        }
        return "Unknown";
    }
    
    /**
     * Extract price from trade metadata
     */
    private Double extractPriceFromMetadata(ActiveTrade trade) {
        try {
            Map<String, Object> signalData = (Map<String, Object>) trade.getMetadata("originalSignal");
            if (signalData != null) {
                Object priceValue = signalData.get("closePrice");
                if (priceValue instanceof Number) {
                    return ((Number) priceValue).doubleValue();
                }
            }
        } catch (Exception e) {
            log.debug("Could not extract price from metadata for trade: {}", trade.getTradeId());
        }
        return null;
    }
    
    /**
     * Extract string value from map
     */
    private String extractStringValue(Map<String, Object> data, String key) {
        Object value = data.get(key);
        return value != null ? value.toString() : null;
    }
    
    /**
     * Extract double value from map
     */
    private Double extractDoubleValue(Map<String, Object> data, String key) {
        Object value = data.get(key);
        if (value instanceof Number) {
            return ((Number) value).doubleValue();
        }
        if (value instanceof String) {
            try {
                return Double.parseDouble((String) value);
            } catch (NumberFormatException e) {
                return null;
            }
        }
        return null;
    }
    
    /**
     * Generate unique trade ID
     */
    private String generateTradeId(String scripCode, String strategyName, LocalDateTime signalTime) {
        return String.format("%s_%s_%s_%s", 
                scripCode, 
                strategyName, 
                signalTime.toString().replaceAll("[^0-9]", "").substring(0, 10),
                UUID.randomUUID().toString().substring(0, 8));
    }
    
    private Map<String, Object> extractBBBreakoutContext(Map<String, Object> signalData) {
        Map<String, Object> context = new HashMap<>();
        context.put("bbBreakoutType", "BB-Only Breakout");
        context.put("breakoutDirection", extractStringValue(signalData, "signal"));
        context.put("bbUpper", extractDoubleValue(signalData, "bbUpper"));
        context.put("bbLower", extractDoubleValue(signalData, "bbLower"));
        context.put("currentPrice", extractDoubleValue(signalData, "lastRate"));
        return context;
    }
    
    /**
     * Main entry point for executing strategy signals
     */
    public void executeStrategySignal(
            String scripCode,
            String signal,
            Double entryPrice,
            Double stopLoss,
            Double target1,
            String strategyType,
            String confidence) {
        
        try {
            // Get current IST time for signal processing
            LocalDateTime signalTime = java.time.LocalDateTime.now(java.time.ZoneId.of("Asia/Kolkata"));
            
            log.info("🎯 Executing strategy signal: {} {} @ {} (Strategy: {}) at IST: {}", 
                    signal, scripCode, entryPrice, strategyType, 
                    signalTime.format(java.time.format.DateTimeFormatter.ofPattern("HH:mm:ss")));
            
            // Build signal data map with proper field mapping
            Map<String, Object> signalData = new HashMap<>();
            signalData.put("entryPrice", entryPrice);
            signalData.put("closePrice", entryPrice); // Add both field names for compatibility
            signalData.put("stopLoss", stopLoss);
            signalData.put("target1", target1);
            signalData.put("signal", signal);
            signalData.put("confidence", confidence);
            signalData.put("scripCode", scripCode);
            signalData.put("signalTime", signalTime.toString());
            
            // Extract additional signal data if available from metadata
            // For now, use reasonable defaults for missing SuperTrend data
            if (stopLoss != null) {
                signalData.put("supertrend", stopLoss); // Use stop loss as SuperTrend reference
                signalData.put("supertrendValue", stopLoss);
            }
            
            // Determine signal type
            String signalType = determineSignalType(signal);
            
            // Log signal for tracking
            tradeHistoryService.logSignal(scripCode, signal, strategyType, "Processing signal at IST: " + 
                    signalTime.format(java.time.format.DateTimeFormatter.ofPattern("HH:mm:ss")), true);
            
            // Process the signal using the main processing method
            processNewSignal(
                    signalData,
                    signalTime, // Use IST time
                    strategyType,
                    signalType,
                    scripCode,
                    extractCompanyName(scripCode), // TODO: Get actual company name
                    "NSE", // Default exchange
                    "EQUITY" // Default exchange type
            );
            
            log.info("✅ Strategy signal executed successfully: {} {} at IST {}", 
                    signal, scripCode, signalTime.format(java.time.format.DateTimeFormatter.ofPattern("HH:mm:ss")));
            
        } catch (Exception e) {
            log.error("🚨 Error executing strategy signal for {}: {}", scripCode, e.getMessage(), e);
            
            // Log failed signal
            tradeHistoryService.logSignal(scripCode, signal, strategyType, 
                    "Execution failed: " + e.getMessage(), false);
        }
    }
    
    /**
     * Extract company name from script code (placeholder implementation)
     */
    private String extractCompanyName(String scripCode) {
        // Use the integrated CompanyNameService for proper company name mapping
        return companyNameService.getCompanyName(scripCode);
    }
    
    /**
     * Determine signal type from signal string
     */
    private String determineSignalType(String signal) {
        if (signal == null) return "UNKNOWN";
        
        String upperSignal = signal.toUpperCase();
        if (upperSignal.contains("BUY") || upperSignal.contains("BULLISH")) {
            return "BULLISH";
        } else if (upperSignal.contains("SELL") || upperSignal.contains("BEARISH")) {
            return "BEARISH";
        }
        
        return "UNKNOWN";
    }
    
    /**
     * Get detailed reason why entry was rejected (for debugging)
     */
    private String getEntryRejectionReason(ActiveTrade trade, double currentPrice, Double signalPrice) {
        if (signalPrice == null) {
            return "No signal price available";
        }
        
        double priceDiff = trade.isBullish() ? (currentPrice - signalPrice) : (signalPrice - currentPrice);
        double percentDiff = (priceDiff / signalPrice) * 100;
        double absPercentDiff = Math.abs((currentPrice - signalPrice) / signalPrice) * 100;
        
        if (trade.isBullish()) {
            if (currentPrice < signalPrice * 1.001 && absPercentDiff > 0.2) {
                return String.format("BULLISH: Price %.2f is %.3f%% below required +0.1%% move (need %.2f+) and outside 0.2%% range", 
                        currentPrice, percentDiff, signalPrice * 1.001);
            } else if (absPercentDiff <= 0.2) {
                return String.format("BULLISH: Price %.2f within 0.2%% range (%.3f%%) - should enter!", 
                        currentPrice, absPercentDiff);
            }
        } else {
            if (currentPrice > signalPrice * 0.999 && absPercentDiff > 0.2) {
                return String.format("BEARISH: Price %.2f is %.3f%% above required -0.1%% move (need %.2f-) and outside 0.2%% range", 
                        currentPrice, -percentDiff, signalPrice * 0.999);
            } else if (absPercentDiff <= 0.2) {
                return String.format("BEARISH: Price %.2f within 0.2%% range (%.3f%%) - should enter!", 
                        currentPrice, absPercentDiff);
            }
        }
        
        return String.format("Unknown condition - Current: %.2f, Signal: %.2f, Diff: %.3f%%", 
                currentPrice, signalPrice, percentDiff);
    }
    
    /**
     * Extract trade levels from pending signal for trade creation
     */
    private Map<String, Double> extractTradeLevelsFromPendingSignal(PendingSignal pendingSignal, double currentPrice) {
        Map<String, Double> tradeLevels = new HashMap<>();
        
        try {
            double entryPrice = currentPrice;
            double stopLoss = pendingSignal.getStopLoss();
            double target1 = pendingSignal.getTarget1();
            
            // Calculate derived values
            double riskPerShare = Math.abs(entryPrice - stopLoss);
            int positionSize = calculatePositionSizeFromRisk(entryPrice, riskPerShare);
            double riskAmount = riskPerShare * positionSize;
            
            // Store all levels
            tradeLevels.put("entryPrice", entryPrice);
            tradeLevels.put("stopLoss", stopLoss);
            tradeLevels.put("target1", target1);
            tradeLevels.put("target2", pendingSignal.getTarget2() != null ? pendingSignal.getTarget2() : 
                          calculateFallbackTarget(entryPrice, riskPerShare, 2.5, target1 > entryPrice));
            tradeLevels.put("target3", pendingSignal.getTarget3() != null ? pendingSignal.getTarget3() : 
                          calculateFallbackTarget(entryPrice, riskPerShare, 4.0, target1 > entryPrice));
            tradeLevels.put("target4", calculateFallbackTarget(entryPrice, riskPerShare, 6.0, target1 > entryPrice));
            tradeLevels.put("riskPerShare", riskPerShare);
            tradeLevels.put("riskAmount", riskAmount);
            tradeLevels.put("positionSize", (double) positionSize);
            
            log.debug("📊 [TradeExecution] Extracted trade levels from pending signal: Entry={}, SL={}, T1={}, T2={}, T3={}", 
                     entryPrice, stopLoss, target1, tradeLevels.get("target2"), tradeLevels.get("target3"));
            
            return tradeLevels;
            
        } catch (Exception e) {
            log.error("🚨 [TradeExecution] Error extracting trade levels from pending signal: {}", e.getMessage(), e);
            return new HashMap<>();
        }
    }
    
    /**
     * Generate unique signal ID
     */
    private String generateSignalId(String scripCode, String strategyName, LocalDateTime signalTime) {
        return String.format("SIG_%s_%s_%s_%s", 
                scripCode, 
                strategyName, 
                signalTime.toString().replaceAll("[^0-9]", "").substring(0, 10),
                UUID.randomUUID().toString().substring(0, 6));
    }
    
    /**
     * Log detailed execution information
     */
    private void logDetailedExecutionInfo(ActiveTrade trade, PendingSignal pendingSignal, 
                                        SignalValidationResult validationResult) {
        String executionSummary = String.format(
            "\n🚀 === DYNAMIC TRADE EXECUTION ===\n" +
            "🆔 Trade ID: %s\n" +
            "📊 Script: %s (%s)\n" +
            "🎯 Strategy: %s\n" +
            "📈 Signal Type: %s\n" +
            "⏰ Signal Time: %s\n" +
            "🚀 Entry Time: %s\n" +
            "💰 Entry Price: %.2f\n" +
            "🛡️ Stop Loss: %.2f\n" +
            "🎯 Targets: [%.2f, %.2f, %.2f, %.2f]\n" +
            "📊 Risk-Reward: %.2f\n" +
            "🔄 Validation Attempts: %d\n" +
            "⏱️ Signal Age: %d minutes\n" +
            "===============================",
            trade.getTradeId(),
            trade.getCompanyName(), trade.getScripCode(),
            trade.getStrategyName(),
            trade.getSignalType(),
            pendingSignal.getSignalTime().format(java.time.format.DateTimeFormatter.ofPattern("HH:mm:ss")),
            trade.getEntryTime().format(java.time.format.DateTimeFormatter.ofPattern("HH:mm:ss")),
            trade.getEntryPrice(),
            trade.getStopLoss(),
            trade.getTarget1(), trade.getTarget2(), trade.getTarget3(), trade.getTarget4(),
            validationResult.getRiskReward(),
            pendingSignal.getValidationAttempts(),
            java.time.Duration.between(pendingSignal.getSignalTime(), trade.getEntryTime()).toMinutes()
        );
        
        log.info(executionSummary);
    }
} 