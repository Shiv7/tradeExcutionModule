package com.kotsin.execution.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.kotsin.execution.service.TradeExecutionService;
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
import java.time.format.DateTimeFormatter;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Consumer for ENHANCED 30M Price Action signals ONLY.
 * Consumes signals from: 30m-bb, 30m-supertrend, 30m-fudkii signals.
 * 
 * ALL 3M and 15M strategies have been REMOVED as requested.
 */
@Component
@Slf4j
@RequiredArgsConstructor
public class StrategySignalConsumer {
    
    private final TradeExecutionService tradeExecutionService;
    private final TradingHoursService tradingHoursService;
    private final ObjectMapper objectMapper;
    
    private static final DateTimeFormatter TIMESTAMP_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    
    // Metrics - ONLY 30M now
    private final AtomicLong processed30m = new AtomicLong(0);
    
    /**
     * 30-Minute BB Signals - ENHANCED Bollinger Band breakouts with pivot validation
     */
    @KafkaListener(topics = "30m-bb-signals", 
                   groupId = "trade-execution-30m-group-v1")
    public void consume30mBBSignals(
            @Payload String message,
            @Header(KafkaHeaders.RECEIVED_TIMESTAMP) long timestamp,
            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
            Acknowledgment acknowledgment) {
        
        try {
            log.info("📊 [30M-BB] ENHANCED - Received BB signal from: {}", topic);
            
            Map<String, Object> signalData = objectMapper.readValue(message, Map.class);
            
            if (isValidSignal(signalData, "30M-BB")) {
                String confidence = extractStringValue(signalData, "confidence");
                log.info("📊 [30M-BB] Processing ENHANCED signal with confidence: {}", confidence);
                
                processEnhancedStrategySignal(signalData, "30M_BB_ENHANCED");
                processed30m.incrementAndGet();
                
                log.info("✅ [30M-BB] ENHANCED BB signal processed: {} (Confidence: {})", 
                        extractStringValue(signalData, "scripCode"), confidence);
            }
            
            acknowledgment.acknowledge();
            
        } catch (Exception e) {
            log.error("🚨 [30M-BB] Error processing ENHANCED BB signal: {}", e.getMessage(), e);
            acknowledgment.acknowledge();
        }
    }
    
    /**
     * 30-Minute SuperTrend Signals - ENHANCED SuperTrend direction changes with pivot validation
     */
    @KafkaListener(topics = "30m-supertrend-signals", 
                   groupId = "trade-execution-30m-group-v1")
    public void consume30mSuperTrendSignals(
            @Payload String message,
            @Header(KafkaHeaders.RECEIVED_TIMESTAMP) long timestamp,
            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
            Acknowledgment acknowledgment) {
        
        try {
            log.info("📈 [30M-ST] ENHANCED - Received SuperTrend signal from: {}", topic);
            
            Map<String, Object> signalData = objectMapper.readValue(message, Map.class);
            
            if (isValidSignal(signalData, "30M-ST")) {
                String confidence = extractStringValue(signalData, "confidence");
                log.info("📈 [30M-ST] Processing ENHANCED signal with confidence: {}", confidence);
                
                processEnhancedStrategySignal(signalData, "30M_SUPERTREND_ENHANCED");
                processed30m.incrementAndGet();
                
                log.info("✅ [30M-ST] ENHANCED SuperTrend signal processed: {} (Confidence: {})", 
                        extractStringValue(signalData, "scripCode"), confidence);
            }
            
            acknowledgment.acknowledge();
            
        } catch (Exception e) {
            log.error("🚨 [30M-ST] Error processing ENHANCED SuperTrend signal: {}", e.getMessage(), e);
            acknowledgment.acknowledge();
        }
    }
    
    /**
     * 30-Minute FUDKII Signals - ENHANCED High confidence combined signals with pivot validation
     */
    @KafkaListener(topics = "30m-fudkii-signals", 
                   groupId = "trade-execution-30m-group-v1")
    public void consume30mFudkiiSignals(
            @Payload String message,
            @Header(KafkaHeaders.RECEIVED_TIMESTAMP) long timestamp,
            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
            Acknowledgment acknowledgment) {
        
        try {
            log.info("🎯 [30M-FUDKII] ENHANCED - Received HIGH confidence signal from: {}", topic);
            
            Map<String, Object> signalData = objectMapper.readValue(message, Map.class);
            
            if (isValidSignal(signalData, "30M-FUDKII")) {
                String confidence = extractStringValue(signalData, "confidence");
                log.info("🎯 [30M-FUDKII] Processing ENHANCED HIGH confidence signal: {}", confidence);
                
                // FUDKII signals get priority processing due to higher confidence
                processEnhancedStrategySignal(signalData, "30M_FUDKII_ENHANCED");
                processed30m.incrementAndGet();
                
                log.info("✅ [30M-FUDKII] ENHANCED HIGH confidence signal processed: {} (Confidence: {})", 
                        extractStringValue(signalData, "scripCode"), confidence);
            }
            
            acknowledgment.acknowledge();
            
        } catch (Exception e) {
            log.error("🚨 [30M-FUDKII] Error processing ENHANCED FUDKII signal: {}", e.getMessage(), e);
            acknowledgment.acknowledge();
        }
    }
    
    /**
     * Validate signal data and trading hours
     */
    private boolean isValidSignal(Map<String, Object> signalData, String timeframe) {
        try {
            String scripCode = extractStringValue(signalData, "scripCode");
            String exchange = extractStringValue(signalData, "exchange");
            String signal = extractStringValue(signalData, "signal");
            
            if (scripCode == null || scripCode.isEmpty()) {
                log.warn("⚠️ [{}] Invalid signal: missing scripCode", timeframe);
                return false;
            }
            
            if (signal == null || signal.isEmpty()) {
                log.warn("⚠️ [{}] Invalid signal: missing signal for {}", timeframe, scripCode);
                return false;
            }
            
            // Validate trading hours
            LocalDateTime now = tradingHoursService.getCurrentISTTime();
            if (!tradingHoursService.shouldProcessTrade(exchange, now)) {
                log.warn("🚫 [{}] Skipping signal for {} - outside trading hours", timeframe, scripCode);
                return false;
            }
            
            return true;
            
        } catch (Exception e) {
            log.error("🚨 [{}] Error validating signal: {}", timeframe, e.getMessage());
            return false;
        }
    }
    
    /**
     * Process enhanced strategy signal with additional logging and confidence handling
     */
    private void processEnhancedStrategySignal(Map<String, Object> signalData, String strategyType) {
        try {
            String scripCode = extractStringValue(signalData, "scripCode");
            String signal = extractStringValue(signalData, "signal");
            Double entryPrice = extractDoubleValue(signalData, "entryPrice");
            Double stopLoss = extractDoubleValue(signalData, "stopLoss");
            Double target1 = extractDoubleValue(signalData, "target1");
            String confidence = extractStringValue(signalData, "confidence");
            
            log.info("🎯 Processing ENHANCED {} signal: {} -> {} @ {} (SL: {}, T1: {}, Confidence: {})", 
                     strategyType, scripCode, signal, entryPrice, stopLoss, target1, confidence);
            
            // Enhanced logging for high confidence signals
            if ("HIGH".equalsIgnoreCase(confidence)) {
                log.info("⭐ HIGH CONFIDENCE Enhanced 30M Signal detected for {}: {} -> {}", 
                        scripCode, signal, strategyType);
            }
            
            // Forward to existing trade execution service method
            tradeExecutionService.executeStrategySignal(
                    scripCode, signal, entryPrice, stopLoss, target1, 
                    strategyType, confidence);
            
        } catch (Exception e) {
            log.error("🚨 Error processing enhanced strategy signal: {}", e.getMessage());
        }
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
     * Get processing statistics - ONLY 30M now
     */
    public String getStats() {
        return String.format("Enhanced 30M Signal Processing: %d", processed30m.get());
    }
} 