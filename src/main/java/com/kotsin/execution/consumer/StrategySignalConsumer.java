package com.kotsin.execution.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.kotsin.execution.service.CleanTradeExecutionService;
import com.kotsin.execution.service.TradingHoursService;
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
 * Clean, focused implementation for Enhanced Price Action strategy execution.
 */
@Component
@Slf4j
@RequiredArgsConstructor
public class StrategySignalConsumer {
    
    private final CleanTradeExecutionService cleanTradeExecutionService;
    private final TradingHoursService tradingHoursService;
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
                   groupId = "kotsin-trade-execution-enhanced-30m-today-v3",
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
            
            Map<String, Object> signalData = objectMapper.readValue(message, Map.class);
            scripCode = extractStringValue(signalData, "scripCode");
            
            if (isValidEnhanced30MSignal(signalData) && isTodaySignal(signalData)) {
                String signal = extractStringValue(signalData, "signal");
                String confidence = extractStringValue(signalData, "confidence");
                Double entryPrice = extractDoubleValue(signalData, "entryPrice");
                
                log.info("🚀 [Enhanced30M] Processing {} signal: {} -> {} @ {} (Confidence: {})", 
                         scripCode, signal, scripCode, entryPrice, confidence);
                
                // Process the Enhanced 30M signal
                processEnhanced30MSignal(signalData);
                
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
                log.warn("❌ [Enhanced30M] Invalid signal received for {}", scripCode);
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
     * Validate Enhanced 30M signal data
     */
    private boolean isValidEnhanced30MSignal(Map<String, Object> signalData) {
        try {
            String scripCode = extractStringValue(signalData, "scripCode");
            String companyName = extractStringValue(signalData, "companyName");
            String signal = extractStringValue(signalData, "signal");
            String strategy = extractStringValue(signalData, "strategy");
            Double entryPrice = extractDoubleValue(signalData, "entryPrice");
            Double stopLoss = extractDoubleValue(signalData, "stopLoss");
            Double target1 = extractDoubleValue(signalData, "target1");
            
            // Basic validation
            if (scripCode == null || scripCode.isEmpty()) {
                log.warn("⚠️ [Enhanced30M] Invalid signal: missing scripCode");
                return false;
            }
            
            if (signal == null || (!signal.equals("BUY") && !signal.equals("SELL"))) {
                log.warn("⚠️ [Enhanced30M] Invalid signal: invalid signal type '{}' for {}", signal, scripCode);
                return false;
            }
            
            if (!"ENHANCED_30M".equals(strategy)) {
                log.warn("⚠️ [Enhanced30M] Invalid signal: wrong strategy '{}' for {}", strategy, scripCode);
                return false;
            }
            
            if (entryPrice == null || entryPrice <= 0) {
                log.warn("⚠️ [Enhanced30M] Invalid signal: invalid entry price {} for {}", entryPrice, scripCode);
                return false;
            }
            
            if (stopLoss == null || stopLoss <= 0) {
                log.warn("⚠️ [Enhanced30M] Invalid signal: invalid stop loss {} for {}", stopLoss, scripCode);
                return false;
            }
            
            if (target1 == null || target1 <= 0) {
                log.warn("⚠️ [Enhanced30M] Invalid signal: invalid target1 {} for {}", target1, scripCode);
                return false;
            }
            
            // Validate trading hours
            LocalDateTime now = tradingHoursService.getCurrentISTTime();
            if (!tradingHoursService.shouldProcessTrade("NSE", now)) {
                log.warn("🚫 [Enhanced30M] Skipping signal for {} - outside trading hours", scripCode);
                return false;
            }
            
            return true;
            
        } catch (Exception e) {
            log.error("🚨 [Enhanced30M] Error validating signal: {}", e.getMessage());
            return false;
        }
    }
    
    /**
     * Process Enhanced 30M signal - clean and focused
     */
    private void processEnhanced30MSignal(Map<String, Object> signalData) {
        String scripCode = extractStringValue(signalData, "scripCode");
        String signal = extractStringValue(signalData, "signal");
        Double entryPrice = extractDoubleValue(signalData, "entryPrice");
        Double stopLoss = extractDoubleValue(signalData, "stopLoss");
        Double target1 = extractDoubleValue(signalData, "target1");
        String confidence = extractStringValue(signalData, "confidence");
        
        log.info("📊 [Enhanced30M] Executing Enhanced Price Action signal: {} {} @ {} (SL: {}, T1: {}, R:R: {})", 
                 scripCode, signal, entryPrice, stopLoss, target1, 
                 calculateRiskReward(entryPrice, stopLoss, target1, signal));
        
        // Forward to clean trade execution service
        cleanTradeExecutionService.executeEnhanced30MSignal(
                scripCode, signal, entryPrice, stopLoss, target1, confidence);
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
     * Get current processing statistics
     */
    public String getStats() {
        return String.format("Enhanced 30M Signals - Total: %d, Successful: %d, Failed: %d", 
                processedSignals.get(), successfulSignals.get(), failedSignals.get());
    }
    
    /**
     * Check if the signal is from today
     */
    private boolean isTodaySignal(Map<String, Object> signalData) {
        try {
            String signalTime = extractStringValue(signalData, "signalTime");
            if (signalTime == null || signalTime.isEmpty()) {
                // Try alternative timestamp fields
                signalTime = extractStringValue(signalData, "timestamp");
                if (signalTime == null) {
                    log.warn("⚠️ [Enhanced30M] No timestamp found in signal data for {}", 
                            extractStringValue(signalData, "scripCode"));
                    return false;
                }
            }
            
            // Extract date from timestamp (format: 2025-06-23T13:45+05:30[Asia/Kolkata] or similar)
            String dateStr = signalTime.split("T")[0]; // Get date part before 'T'
            String todayStr = java.time.LocalDate.now().toString(); // Format: 2025-06-23
            
            boolean isToday = dateStr.equals(todayStr);
            
            if (!isToday) {
                log.debug("📅 [Enhanced30M] Signal from {}, today is {} - skipping", dateStr, todayStr);
            } else {
                log.debug("✅ [Enhanced30M] Processing today's signal from {}", dateStr);
            }
            
            return isToday;
            
        } catch (Exception e) {
            String scripCode = extractStringValue(signalData, "scripCode");
            log.error("🚨 [Enhanced30M] Error parsing signal timestamp for {}: {}", scripCode, e.getMessage());
            // If we can't parse the timestamp, process it anyway (conservative approach)
            return true;
        }
    }
} 