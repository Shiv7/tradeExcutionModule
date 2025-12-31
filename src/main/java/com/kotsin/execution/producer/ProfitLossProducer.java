package com.kotsin.execution.producer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.kotsin.execution.model.ActiveTrade;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * Profit-Loss Producer - Publishes all entry/exit events to profit-loss topic
 * Real-time P&L tracking for every trade action
 */
@Service
@Slf4j
@RequiredArgsConstructor
public class ProfitLossProducer {
    
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;
    
    private final String profitLossTopic = "profit-loss";
    
    /**
     * Publish trade entry event
     */
    public void publishTradeEntry(ActiveTrade trade, double entryPrice) {
        try {
            Map<String, Object> entryEvent = new HashMap<>();
            entryEvent.put("eventType", "TRADE_ENTRY");
            entryEvent.put("tradeId", trade.getTradeId());
            entryEvent.put("scripCode", trade.getScripCode());
            entryEvent.put("companyName", trade.getCompanyName());
            entryEvent.put("signal", trade.getSignalType());
            entryEvent.put("strategy", trade.getStrategyName());
            entryEvent.put("entryPrice", entryPrice);
            entryEvent.put("positionSize", trade.getPositionSize());
            entryEvent.put("stopLoss", trade.getStopLoss());
            entryEvent.put("target1", trade.getTarget1());
            entryEvent.put("target2", trade.getTarget2());
            entryEvent.put("entryTime", trade.getEntryTime());
            entryEvent.put("riskReward", trade.getRiskRewardRatio());
            entryEvent.put("timestamp", LocalDateTime.now());
            
            String key = "ENTRY_" + trade.getTradeId();
            publishEvent(key, entryEvent);
            
            log.info("📈 [P&L] Published TRADE_ENTRY: {} at ₹{}", trade.getScripCode(), entryPrice);
            
        } catch (Exception e) {
            log.error("🚨 [P&L] Error publishing trade entry: {}", e.getMessage(), e);
        }
    }
    
    /**
     * Publish trade exit event
     */
    public void publishTradeExit(ActiveTrade trade, double exitPrice, String exitReason, double profitLoss) {
        try {
            Map<String, Object> exitEvent = new HashMap<>();
            exitEvent.put("eventType", "TRADE_EXIT");
            exitEvent.put("tradeId", trade.getTradeId());
            exitEvent.put("scripCode", trade.getScripCode());
            exitEvent.put("companyName", trade.getCompanyName());
            exitEvent.put("signal", trade.getSignalType());
            exitEvent.put("strategy", trade.getStrategyName());
            exitEvent.put("entryPrice", trade.getEntryPrice());
            exitEvent.put("exitPrice", exitPrice);
            exitEvent.put("positionSize", trade.getPositionSize());
            exitEvent.put("profitLoss", profitLoss);
            exitEvent.put("roi", trade.getCurrentROI());
            exitEvent.put("exitReason", exitReason);
            exitEvent.put("entryTime", trade.getEntryTime());
            exitEvent.put("exitTime", LocalDateTime.now());
            exitEvent.put("durationMinutes", calculateDuration(trade.getEntryTime(), LocalDateTime.now()));
            exitEvent.put("target1Hit", trade.getTarget1Hit());
            exitEvent.put("target2Hit", trade.getTarget2Hit());
            exitEvent.put("timestamp", LocalDateTime.now());
            
            // Add forced exit details if applicable
            if (trade.getMetadata("forcedExit") != null) {
                exitEvent.put("forcedExit", true);
                exitEvent.put("forcedExitReason", trade.getMetadata("forcedExitReason"));
            }
            
            String key = "EXIT_" + trade.getTradeId();
            publishEvent(key, exitEvent);
            
            String pnlEmoji = profitLoss >= 0 ? "💰" : "💸";
            log.info("{} [P&L] Published TRADE_EXIT: {} at ₹{} | P&L: ₹{} | Reason: {}", 
                    pnlEmoji, trade.getScripCode(), exitPrice, String.format("%.2f", profitLoss), exitReason);
            
        } catch (Exception e) {
            log.error("🚨 [P&L] Error publishing trade exit: {}", e.getMessage(), e);
        }
    }
    
    /**
     * Publish trade replacement event (when better signal forces exit)
     */
    public void publishTradeReplacement(ActiveTrade oldTrade, Map<String, Object> newSignalData, double currentPrice) {
        try {
            Map<String, Object> replacementEvent = new HashMap<>();
            replacementEvent.put("eventType", "TRADE_REPLACEMENT");
            replacementEvent.put("oldTradeId", oldTrade.getTradeId());
            replacementEvent.put("oldScripCode", oldTrade.getScripCode());
            replacementEvent.put("oldEntryPrice", oldTrade.getEntryPrice());
            replacementEvent.put("oldRiskReward", oldTrade.getRiskRewardRatio());
            replacementEvent.put("newScripCode", extractStringValue(newSignalData, "scripCode"));
            replacementEvent.put("newEntryPrice", extractDoubleValue(newSignalData, "entryPrice"));
            replacementEvent.put("newRiskReward", extractDoubleValue(newSignalData, "riskReward"));
            replacementEvent.put("currentPrice", currentPrice);
            replacementEvent.put("timestamp", LocalDateTime.now());
            
            String key = "REPLACEMENT_" + oldTrade.getTradeId();
            publishEvent(key, replacementEvent);
            
            log.info("🔄 [P&L] Published TRADE_REPLACEMENT: {} → {} (R:R: {} → {})", 
                    oldTrade.getScripCode(), 
                    extractStringValue(newSignalData, "scripCode"),
                    oldTrade.getRiskRewardRatio(),
                    extractDoubleValue(newSignalData, "riskReward"));
            
        } catch (Exception e) {
            log.error("🚨 [P&L] Error publishing trade replacement: {}", e.getMessage(), e);
        }
    }
    
    /**
     * Publish portfolio update event
     */
    public void publishPortfolioUpdate(double currentCapital, double totalProfitLoss, double roi) {
        try {
            Map<String, Object> portfolioEvent = new HashMap<>();
            portfolioEvent.put("eventType", "PORTFOLIO_UPDATE");
            portfolioEvent.put("currentCapital", currentCapital);
            portfolioEvent.put("totalProfitLoss", totalProfitLoss);
            portfolioEvent.put("roi", roi);
            portfolioEvent.put("timestamp", LocalDateTime.now());
            
            String key = "PORTFOLIO_" + System.currentTimeMillis();
            publishEvent(key, portfolioEvent);
            
            log.info("📊 [P&L] Published PORTFOLIO_UPDATE: Capital: ₹{}, P&L: ₹{}, ROI: {}%", 
                    String.format("%.2f", currentCapital), 
                    String.format("%.2f", totalProfitLoss), 
                    String.format("%.2f", roi));
            
        } catch (Exception e) {
            log.error("🚨 [P&L] Error publishing portfolio update: {}", e.getMessage(), e);
        }
    }
    
    /**
     * Publish virtual trade entry event (for paper trading)
     */
    public void publishVirtualTradeEntry(String scripCode, String side, int quantity, 
            double entryPrice, Double stopLoss, Double target) {
        try {
            Map<String, Object> entryEvent = new HashMap<>();
            entryEvent.put("eventType", "VIRTUAL_TRADE_ENTRY");
            entryEvent.put("tradeId", "VT_" + scripCode + "_" + System.currentTimeMillis());
            entryEvent.put("scripCode", scripCode);
            entryEvent.put("side", side);
            entryEvent.put("quantity", quantity);
            entryEvent.put("entryPrice", entryPrice);
            entryEvent.put("stopLoss", stopLoss);
            entryEvent.put("target", target);
            entryEvent.put("entryTime", LocalDateTime.now());
            entryEvent.put("timestamp", LocalDateTime.now());
            
            String key = "VIRTUAL_ENTRY_" + scripCode;
            publishEvent(key, entryEvent);
            
            log.info("📈 [V-P&L] Published VIRTUAL_TRADE_ENTRY: {} @ ₹{} qty={}", 
                    scripCode, entryPrice, quantity);
            
        } catch (Exception e) {
            log.error("🚨 [V-P&L] Error publishing virtual entry: {}", e.getMessage(), e);
        }
    }
    
    /**
     * Publish virtual trade exit event (for paper trading)
     */
    public void publishVirtualTradeExit(String scripCode, String side, int quantity,
            double entryPrice, double exitPrice, double pnl, String exitReason) {
        try {
            Map<String, Object> exitEvent = new HashMap<>();
            exitEvent.put("eventType", "VIRTUAL_TRADE_EXIT");
            exitEvent.put("tradeId", "VT_" + scripCode + "_" + System.currentTimeMillis());
            exitEvent.put("scripCode", scripCode);
            exitEvent.put("side", side);
            exitEvent.put("quantity", quantity);
            exitEvent.put("entryPrice", entryPrice);
            exitEvent.put("exitPrice", exitPrice);
            exitEvent.put("profitLoss", pnl);
            exitEvent.put("roi", entryPrice > 0 ? (pnl / (entryPrice * quantity)) * 100 : 0);
            exitEvent.put("exitReason", exitReason);
            exitEvent.put("exitTime", LocalDateTime.now());
            exitEvent.put("timestamp", LocalDateTime.now());
            exitEvent.put("win", pnl > 0);
            
            String key = "VIRTUAL_EXIT_" + scripCode;
            publishEvent(key, exitEvent);
            
            String pnlEmoji = pnl >= 0 ? "💰" : "💸";
            log.info("{} [V-P&L] Published VIRTUAL_TRADE_EXIT: {} @ ₹{} | P&L: ₹{} | Reason: {}", 
                    pnlEmoji, scripCode, exitPrice, String.format("%.2f", pnl), exitReason);
            
        } catch (Exception e) {
            log.error("🚨 [V-P&L] Error publishing virtual exit: {}", e.getMessage(), e);
        }
    }
    
    /**
     * Publish event to Kafka topic
     */
    private void publishEvent(String key, Map<String, Object> event) {
        try {
            String eventJson = objectMapper.writeValueAsString(event);
            
            CompletableFuture<SendResult<String, String>> future = kafkaTemplate.send(profitLossTopic, key, eventJson);
            
            future.whenComplete((result, ex) -> {
                if (ex == null) {
                    log.debug("✅ [P&L] Event published successfully: {} (Offset: {})", 
                            key, result.getRecordMetadata().offset());
                } else {
                    log.error("🚨 [P&L] Failed to publish event: {} - Error: {}", key, ex.getMessage());
                }
            });
            
        } catch (Exception e) {
            log.error("🚨 [P&L] Error publishing event: {}", e.getMessage(), e);
        }
    }
    
    /**
     * Calculate duration between two timestamps in minutes
     */
    private long calculateDuration(LocalDateTime start, LocalDateTime end) {
        if (start == null || end == null) return 0;
        return java.time.Duration.between(start, end).toMinutes();
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