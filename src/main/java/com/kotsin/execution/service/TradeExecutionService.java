package com.kotsin.execution.service;

import com.kotsin.execution.model.ActiveTrade;
import com.kotsin.execution.model.TradeResult;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.Map;
import java.util.UUID;

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
            
            // Add metadata from signal
            trade.addMetadata("originalSignal", signalData);
            trade.addMetadata("signalSource", strategyName);
            
            // Store the trade for monitoring
            tradeStateManager.addActiveTrade(trade);
            
            log.info("✅ Created new trade: {} for {} at stop: {}, targets: [{}, {}, {}, {}]", 
                    tradeId, scripCode, 
                    trade.getStopLoss(), 
                    trade.getTarget1(), 
                    trade.getTarget2(), 
                    trade.getTarget3(), 
                    trade.getTarget4());
            
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
     * Check if entry conditions are met
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
            trade.setEntryTriggered(true);
            trade.setEntryPrice(currentPrice);
            trade.setEntryTime(timestamp);
            trade.setStatus(ActiveTrade.TradeStatus.ACTIVE);
            trade.setHighSinceEntry(currentPrice);
            trade.setLowSinceEntry(currentPrice);
            
            log.info("🚀 Trade {} entered at price: {} for {}", trade.getTradeId(), currentPrice, trade.getScripCode());
        }
    }
    
    /**
     * Check exit conditions and close trade if needed
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
            log.info("🎯 Target 1 hit for trade {}: {}", trade.getTradeId(), currentPrice);
            
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
     * Close trade and publish result
     */
    private void closeTrade(ActiveTrade trade, double exitPrice, LocalDateTime exitTime, String exitReason) {
        try {
            log.info("🏁 Closing trade {}: {} at price: {} due to: {}", 
                    trade.getTradeId(), trade.getScripCode(), exitPrice, exitReason);
            
            // Update trade with exit information
            trade.setExitPrice(exitPrice);
            trade.setExitTime(exitTime);
            trade.setExitReason(exitReason);
            
            // Set final status
            if ("STOP_LOSS".equals(exitReason)) {
                trade.setStatus(ActiveTrade.TradeStatus.CLOSED_LOSS);
            } else if (exitReason.startsWith("TARGET")) {
                trade.setStatus(ActiveTrade.TradeStatus.CLOSED_PROFIT);
            } else {
                trade.setStatus(ActiveTrade.TradeStatus.CLOSED_TIME);
            }
            
            // Calculate profit/loss
            TradeResult result = profitLossCalculator.calculateTradeResult(trade);
            
            if (result != null) {
                // Publish to profit/loss topic
                tradeResultProducer.publishTradeResult(result);
                
                log.info("💰 Trade Result: {}", result.getSummary());
            }
            
            // Remove from active trades
            tradeStateManager.removeActiveTrade(trade.getTradeId());
            
        } catch (Exception e) {
            log.error("🚨 Error closing trade {}: {}", trade.getTradeId(), e.getMessage(), e);
        }
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
     * Generate unique trade ID
     */
    private String generateTradeId(String scripCode, String strategyName, LocalDateTime signalTime) {
        return String.format("%s_%s_%s_%s", 
                scripCode, 
                strategyName, 
                signalTime.toString().replaceAll("[^0-9]", "").substring(0, 10),
                UUID.randomUUID().toString().substring(0, 8));
    }
} 