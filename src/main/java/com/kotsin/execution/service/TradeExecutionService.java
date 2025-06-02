package com.kotsin.execution.service;

import com.kotsin.execution.model.ActiveTrade;
import com.kotsin.execution.model.TradeResult;
import com.kotsin.execution.producer.TradeResultProducer;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.Map;
import java.util.UUID;
import java.util.HashMap;

/**
 * Main trade execution service that processes new signals and manages trade lifecycle.
 * Integrates with Redis for indicators and Kafka for live market data.
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
    
    /**
     * Process a new signal from strategy modules
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
            log.info("🎯 Processing new {} signal for {} from strategy: {}", signalType, scripCode, strategyName);
            
            // Generate unique trade ID
            String tradeId = generateTradeId(scripCode, strategyName, signalTime);
            
            // Check if we already have an active trade for this script+strategy combination
            if (tradeStateManager.hasActiveTrade(scripCode, strategyName)) {
                log.warn("⚠️ Already have active trade for {} with strategy {}, skipping signal", scripCode, strategyName);
                return;
            }
            
            // Calculate risk parameters and trade levels
            Map<String, Double> tradeLevels = riskManager.calculateTradeLevels(signalData, signalType);
            
            if (tradeLevels == null || tradeLevels.isEmpty()) {
                log.warn("⚠️ Could not calculate trade levels for signal: {}", signalData);
                return;
            }
            
            // Capture current indicator state at signal time
            Map<String, Object> signalTimeIndicators = indicatorDataService.getComprehensiveIndicators(scripCode);
            
            // Create active trade object
            ActiveTrade trade = ActiveTrade.builder()
                    .tradeId(tradeId)
                    .scripCode(scripCode)
                    .companyName(companyName)
                    .exchange(exchange)
                    .exchangeType(exchangeType)
                    .strategyName(strategyName)
                    .signalType(signalType)
                    .signalTime(signalTime)
                    .originalSignalId(extractStringValue(signalData, "signalId"))
                    .status(ActiveTrade.TradeStatus.WAITING_FOR_ENTRY)
                    .entryTriggered(false)
                    .target1Hit(false)
                    .target2Hit(false)
                    .useTrailingStop(true)
                    .stopLoss(tradeLevels.get("stopLoss"))
                    .target1(tradeLevels.get("target1"))
                    .target2(tradeLevels.get("target2"))
                    .target3(tradeLevels.get("target3"))
                    .target4(tradeLevels.get("target4"))
                    .riskAmount(tradeLevels.get("riskAmount"))
                    .riskPerShare(tradeLevels.get("riskPerShare"))
                    .positionSize(tradeLevels.get("positionSize").intValue())
                    .maxHoldingTime(signalTime.plusDays(5)) // 5-day max holding
                    .daysHeld(0)
                    .build();
            
            // Add comprehensive metadata from signal
            trade.addMetadata("originalSignal", signalData);
            trade.addMetadata("signalSource", strategyName);
            trade.addMetadata("signalTimeIndicators", signalTimeIndicators);
            trade.addMetadata("tradeLevels", tradeLevels);
            
            // Add strategy-specific context
            addStrategyContext(trade, signalData, strategyName);
            
            // Store the trade for monitoring
            tradeStateManager.addActiveTrade(trade);
            
            log.info("✅ Created new trade: {} for {} at stop: {}, targets: [{}, {}, {}, {}]", 
                    tradeId, scripCode, 
                    trade.getStopLoss(), 
                    trade.getTarget1(), 
                    trade.getTarget2(), 
                    trade.getTarget3(), 
                    trade.getTarget4());
            
            // Log detailed signal information
            logDetailedSignalInfo(trade, signalData, signalTimeIndicators);
            
        } catch (Exception e) {
            log.error("🚨 Error processing new signal for {}: {}", scripCode, e.getMessage(), e);
        }
    }
    
    /**
     * Update trade with new market price (called by LiveMarketDataConsumer)
     */
    public void updateTradeWithPrice(String scripCode, double price, LocalDateTime timestamp) {
        try {
            Map<String, ActiveTrade> activeTrades = tradeStateManager.getActiveTradesForScript(scripCode);
            
            for (ActiveTrade trade : activeTrades.values()) {
                // Update price in trade object
                trade.updatePrice(price, timestamp);
                
                // Check entry conditions
                if (!trade.getEntryTriggered()) {
                    checkEntryConditions(trade, price, timestamp);
                }
                
                // Check exit conditions for active trades
                if (trade.getEntryTriggered() && trade.getStatus() == ActiveTrade.TradeStatus.ACTIVE) {
                    checkExitConditions(trade, price, timestamp);
                }
                
                // Update trade state
                tradeStateManager.updateTrade(trade);
            }
            
        } catch (Exception e) {
            log.error("🚨 Error updating trade with price for {}: {}", scripCode, e.getMessage(), e);
        }
    }
    
    /**
     * Check if entry conditions are met and capture entry indicator data
     */
    private void checkEntryConditions(ActiveTrade trade, double currentPrice, LocalDateTime timestamp) {
        // Entry logic based on KotsinBackTestBE patterns
        boolean shouldEnter = false;
        
        if (trade.isBullish()) {
            // For bullish trades, enter if price moves up from signal
            Double signalPrice = extractPriceFromMetadata(trade);
            if (signalPrice != null && currentPrice >= signalPrice * 1.001) { // 0.1% buffer
                shouldEnter = true;
            }
        } else {
            // For bearish trades, enter if price moves down from signal
            Double signalPrice = extractPriceFromMetadata(trade);
            if (signalPrice != null && currentPrice <= signalPrice * 0.999) { // 0.1% buffer
                shouldEnter = true;
            }
        }
        
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
            
            log.info("🚀 Trade {} entered at price: {} for {}", trade.getTradeId(), currentPrice, trade.getScripCode());
            
            // Log entry details with indicators
            logDetailedEntryInfo(trade, currentPrice, entryTimeIndicators);
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
                trade.addMetadata("superTrendContext", extractSuperTrendContext(signalData));
                break;
            case "FUDKII_STRATEGY":
                trade.addMetadata("fudkiiContext", extractFudkiiContext(signalData));
                break;
            default:
                log.warn("⚠️ Unknown strategy type for context: {}", strategyName);
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
} 