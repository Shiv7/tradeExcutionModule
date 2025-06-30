package com.kotsin.execution.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.kotsin.execution.service.CleanTradeExecutionService;
import com.kotsin.execution.service.TradingHoursService;
import com.kotsin.execution.service.CapitalManagementService;
import com.kotsin.execution.producer.ProfitLossProducer;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Consumer for Enhanced 30M Price Action signals ONLY.
 * Consumes signals from: enhanced-30m-signals topic (Strategy Module output)
 * 
 * SIMPLIFIED ARCHITECTURE - Double Logic Removed:
 * - Strategy Module does smart aggregation and sends ONLY the BEST signal
 * - Trade Execution Module directly executes without complex selection/validation
 * - No more redundant 2-minute selection windows or duplicate scoring
 * 
 * Clean, focused implementation for Enhanced Price Action strategy execution.
 */
@Component
@Slf4j
@RequiredArgsConstructor
public class StrategySignalConsumer {
    
    private final CleanTradeExecutionService cleanTradeExecutionService;
    private final TradingHoursService tradingHoursService;
    private final CapitalManagementService capitalManagementService;
    private final ProfitLossProducer profitLossProducer;
    private final ObjectMapper objectMapper;
    
    // Metrics for Enhanced 30M signals
    private final AtomicLong processedSignals = new AtomicLong(0);
    private final AtomicLong successfulSignals = new AtomicLong(0);
    private final AtomicLong failedSignals = new AtomicLong(0);
    
    /**
     * Enhanced 30M Signals - The ONLY signal type we process
     * Consumes from: enhanced-30m-signals (Strategy Module output)
     */
    @KafkaListener(topics = "enhanced-30m-signals",
               groupId = "kotsin-trade-execution-enhanced-30m-retest-v3",
                   properties = {"auto.offset.reset=earliest"})
    public void consumeEnhanced30MSignals(
            @Payload String message,
            @Header(KafkaHeaders.RECEIVED_TIMESTAMP) long timestamp,
            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
            Acknowledgment acknowledgment) {
        
        long startTime = System.currentTimeMillis();
        String scripCode = "UNKNOWN";
        
        try {
            log.info("🎯 [Enhanced30M] Received signal from: {}", topic);
            log.info("🔍 [Enhanced30M] Raw Kafka message: {}", message);
            log.info("⏰ [Enhanced30M] Kafka timestamp: {}", timestamp);
            
            Map<String, Object> signalData = objectMapper.readValue(message, Map.class);
            scripCode = extractStringValue(signalData, "scripCode");
            
            // LOG ALL EXTRACTED DATA FOR DEBUGGING
            log.info("📊 [Enhanced30M] Extracted signal data for {}:", scripCode);
            log.info("   - scripCode: {}", extractStringValue(signalData, "scripCode"));
            log.info("   - companyName: {}", extractStringValue(signalData, "companyName"));
            log.info("   - signal: {}", extractStringValue(signalData, "signal"));
            log.info("   - strategy: {}", extractStringValue(signalData, "strategy"));
            log.info("   - entryPrice: {}", extractDoubleValue(signalData, "entryPrice"));
            log.info("   - stopLoss: {}", extractDoubleValue(signalData, "stopLoss"));
            log.info("   - target1: {}", extractDoubleValue(signalData, "target1"));
            log.info("   - target2: {}", extractDoubleValue(signalData, "target2"));
            log.info("   - confidence: {}", extractStringValue(signalData, "confidence"));
            // Fix field names - Strategy sends 'timestamp' and 'reason', not 'signalTime' and 'logic'
            Long signalTimestamp = extractLongValue(signalData, "timestamp");
            String signalTimeStr = signalTimestamp != null ? 
                LocalDateTime.ofInstant(java.time.Instant.ofEpochMilli(signalTimestamp), 
                    java.time.ZoneId.of("Asia/Kolkata")).toString() : "null";
            log.info("   - signalTime: {} (from timestamp: {})", signalTimeStr, signalTimestamp);
            log.info("   - logic: {}", extractStringValue(signalData, "reason"));
            log.info("   - riskReward: {}", extractDoubleValue(signalData, "riskReward"));
            
            if (isValidEnhanced30MSignal(signalData)) {
                String signal = extractStringValue(signalData, "signal");
                String confidence = extractStringValue(signalData, "confidence");
                Double entryPrice = extractDoubleValue(signalData, "entryPrice");
                Double stopLoss = extractDoubleValue(signalData, "stopLoss");
                Double target1 = extractDoubleValue(signalData, "target1");
                
                log.info("🚀 [Enhanced30M] Processing {} signal: {} -> {} @ {} (SL: {}, T1: {}, Confidence: {})", 
                         scripCode, signal, scripCode, entryPrice, stopLoss, target1, confidence);
                
                // TRADE REPLACEMENT LOGIC - Handle 1 trade at a time with better signal replacement
                boolean shouldProcess = handleTradeReplacementLogic(signalData, timestamp);
                
                if (shouldProcess) {
                    processEnhanced30MSignal(signalData, timestamp);
                } else {
                    log.info("📋 [Enhanced30M] Signal {} not processed due to trade replacement logic", scripCode);
                }
                
                successfulSignals.incrementAndGet();
                
                long processingTime = System.currentTimeMillis() - startTime;
                log.info("✅ [Enhanced30M] Successfully processed {} signal in {}ms (Confidence: {})", 
                        scripCode, processingTime, confidence);
                
                // Log high confidence signals prominently
                if ("HIGH".equalsIgnoreCase(confidence)) {
                    log.info("⭐ [Enhanced30M] HIGH CONFIDENCE signal executed for {}", scripCode);
                }
                
            } else {
                failedSignals.incrementAndGet();
                log.warn("❌ [Enhanced30M] Invalid signal received for {}: signal={}, strategy={}, entryPrice={}, stopLoss={}, target1={}", 
                        scripCode, 
                        extractStringValue(signalData, "signal"),
                        extractStringValue(signalData, "strategy"),
                        extractDoubleValue(signalData, "entryPrice"),
                        extractDoubleValue(signalData, "stopLoss"),
                        extractDoubleValue(signalData, "target1"));
            }
            
            acknowledgment.acknowledge();
            
        } catch (Exception e) {
            failedSignals.incrementAndGet();
            long processingTime = System.currentTimeMillis() - startTime;
            
            log.error("🚨 [Enhanced30M] Error processing signal for {} after {}ms: {}", 
                     scripCode, processingTime, e.getMessage(), e);
            acknowledgment.acknowledge(); // Acknowledge to avoid infinite retries
        } finally {
            processedSignals.incrementAndGet();
            
            // Log stats every 10 signals
            if (processedSignals.get() % 10 == 0) {
                logProcessingStats();
            }
        }
    }
    
    /**
     * Minimal validation - Strategy Module already validates, this is just safety check
     */
    private boolean isValidEnhanced30MSignal(Map<String, Object> signalData) {
        try {
            String scripCode = extractStringValue(signalData, "scripCode");
            String signal = extractStringValue(signalData, "signal");
            String strategy = extractStringValue(signalData, "strategy");
            
            // Minimal safety checks since Strategy Module already validated
            if (scripCode == null || scripCode.isEmpty()) {
                log.warn("⚠️ [Enhanced30M] Invalid signal: missing scripCode");
                return false;
            }
            
            if (!"ENHANCED_30M".equals(strategy)) {
                log.warn("⚠️ [Enhanced30M] Invalid signal: wrong strategy '{}' for {}", strategy, scripCode);
                return false;
            }
            
            if (signal == null || (!signal.equals("BUY") && !signal.equals("SELL") && 
                                   !signal.equals("BULLISH") && !signal.equals("BEARISH"))) {
                log.warn("⚠️ [Enhanced30M] Invalid signal: invalid signal type '{}' for {}", signal, scripCode);
                return false;
            }
            
            log.info("✅ [Enhanced30M] Signal passed minimal safety validation for {}", scripCode);
            return true;
            
        } catch (Exception e) {
            log.error("🚨 [Enhanced30M] Error validating signal: {}", e.getMessage());
            return false;
        }
    }
    
    /**
     * Handle trade replacement logic - Core of the 1-trade-at-a-time system
     * Returns true if signal should be processed, false if rejected
     */
    private boolean handleTradeReplacementLogic(Map<String, Object> signalData, long kafkaTimestamp) {
        String scripCode = extractStringValue(signalData, "scripCode");
        Double newRiskReward = extractDoubleValue(signalData, "riskReward");
        
        // Check if we can take a new trade (no active trades)
        if (capitalManagementService.canTakeNewTrade()) {
            log.info("✅ [TradeReplacement] No active trades - can take new signal: {}", scripCode);
            return true;
        }
        
        // We have an active trade - check if new signal is worth replacing
        if (capitalManagementService.shouldReplaceCurrentTrade(signalData)) {
            // Force exit current trade for better signal
            var currentTrade = capitalManagementService.getCurrentActiveTrade();
            
            // Safely handle null risk reward ratio to prevent NPE
            Double currentRR = currentTrade.getRiskRewardRatio();
            String currentRRDisplay = currentRR != null ? String.format("%.2f", currentRR) : "null";
            String newRRDisplay = newRiskReward != null ? String.format("%.2f", newRiskReward) : "null";
            
            log.info("🔄 [TradeReplacement] Replacing current trade {} (R:R: {}) with {} (R:R: {})", 
                    currentTrade.getScripCode(), currentRRDisplay,
                    scripCode, newRRDisplay);
            
            // Get current market price for forced exit
            double currentPrice = currentTrade.getCurrentPrice() != null ? 
                    currentTrade.getCurrentPrice() : currentTrade.getEntryPrice();
            
            // Publish replacement event
            profitLossProducer.publishTradeReplacement(currentTrade, signalData, currentPrice);
            
            // Force exit current trade
            var exitedTrade = capitalManagementService.forceExitCurrentTrade(
                    "Better signal available - R:R: " + newRiskReward);
            
            if (exitedTrade != null) {
                // Calculate P&L for forced exit
                double profitLoss = exitedTrade.getCurrentPnL();
                
                // Publish forced exit event
                profitLossProducer.publishTradeExit(exitedTrade, currentPrice, 
                        "FORCED_EXIT_BETTER_SIGNAL", profitLoss);
                
                // Update capital management
                capitalManagementService.removeActiveTrade(exitedTrade.getScripCode(), profitLoss);
                
                log.info("🔄 [TradeReplacement] Forced exit completed: {} | P&L: ₹{}", 
                        exitedTrade.getScripCode(), String.format("%.2f", profitLoss));
            }
            
            return true; // Process new signal
        }
        
        // Current trade is better or new signal doesn't meet criteria
        log.info("❌ [TradeReplacement] Rejecting signal {} - current trade is better or new R:R {} not sufficient", 
                scripCode, newRiskReward);
        return false;
    }
    
    /**
     * Process Enhanced 30M signal - clean and focused
     */
    private void processEnhanced30MSignal(Map<String, Object> signalData, long kafkaTimestamp) {
        String scripCode = extractStringValue(signalData, "scripCode");
        String signal = extractStringValue(signalData, "signal");
        Double entryPrice = extractDoubleValue(signalData, "entryPrice");
        Double stopLoss = extractDoubleValue(signalData, "stopLoss");
        Double target1 = extractDoubleValue(signalData, "target1");
        String confidence = extractStringValue(signalData, "confidence");
        String logic = extractStringValue(signalData, "reason");
        
        // Convert Kafka timestamp to LocalDateTime
        LocalDateTime signalTime = LocalDateTime.ofInstant(
                java.time.Instant.ofEpochMilli(kafkaTimestamp), 
                java.time.ZoneId.of("Asia/Kolkata"));
        
        log.info("📊 [Enhanced30M] Executing Enhanced Price Action signal:");
        log.info("   - Script: {} {}", scripCode, signal);
        log.info("   - Entry: {}", entryPrice);
        log.info("   - Stop Loss: {}", stopLoss);
        log.info("   - Target 1: {}", target1);
        log.info("   - Confidence: {}", confidence);
        log.info("   - Logic: {} (from reason field)", logic);
        log.info("   - Signal Time (from Kafka): {}", signalTime);
        log.info("   - R:R Ratio: {}", calculateRiskReward(entryPrice, stopLoss, target1, signal));
        
        // Forward to clean trade execution service with Kafka timestamp
        cleanTradeExecutionService.executeEnhanced30MSignal(
                scripCode, signal, entryPrice, stopLoss, target1, confidence, signalTime);
    }
    
    /**
     * Calculate risk-reward ratio for logging
     */
    private double calculateRiskReward(Double entryPrice, Double stopLoss, Double target1, String signal) {
        if (entryPrice == null || stopLoss == null || target1 == null) return 0.0;
        
        double risk, reward;
        if ("BUY".equals(signal)) {
            risk = Math.abs(entryPrice - stopLoss);
            reward = Math.abs(target1 - entryPrice);
        } else {
            risk = Math.abs(stopLoss - entryPrice);
            reward = Math.abs(entryPrice - target1);
        }
        
        return risk > 0 ? Math.round((reward / risk) * 100.0) / 100.0 : 0.0;
    }
    
    /**
     * Log processing statistics
     */
    private void logProcessingStats() {
        long total = processedSignals.get();
        long successful = successfulSignals.get();
        long failed = failedSignals.get();
        double successRate = total > 0 ? (double) successful / total * 100.0 : 0.0;
        
        log.info("📈 [Enhanced30M] Stats - Total: {}, Successful: {}, Failed: {}, Success Rate: {:.1f}%", 
                total, successful, failed, successRate);
    }
    
    /**
     * Extract string value safely
     */
    private String extractStringValue(Map<String, Object> data, String key) {
        Object value = data.get(key);
        return value != null ? value.toString().trim() : null;
    }
    
    /**
     * Extract double value safely
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
     * Extract long value safely  
     */
    private Long extractLongValue(Map<String, Object> data, String key) {
        Object value = data.get(key);
        if (value instanceof Number) {
            return ((Number) value).longValue();
        }
        if (value instanceof String) {
            try {
                return Long.parseLong((String) value);
            } catch (NumberFormatException e) {
                return null;
            }
        }
        return null;
    }
    
    /**
     * Get current processing statistics
     */
    public String getStats() {
        return String.format("Enhanced 30M Signals - Total: %d, Successful: %d, Failed: %d", 
                processedSignals.get(), successfulSignals.get(), failedSignals.get());
    }
} 