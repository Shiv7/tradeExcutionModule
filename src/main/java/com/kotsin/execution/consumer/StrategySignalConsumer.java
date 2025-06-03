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
 * Consumer for all 7 strategy signal topics from the new signal routing system.
 * Consumes signals from: 3m-supertrend, 15m-bb, 15m-supertrend, 15m-fudkii, 
 * 30m-bb, 30m-supertrend, 30m-fudkii signals.
 */
@Component
@Slf4j
@RequiredArgsConstructor
public class StrategySignalConsumer {
    
    private final TradeExecutionService tradeExecutionService;
    private final TradingHoursService tradingHoursService;
    private final ObjectMapper objectMapper;
    
    private static final DateTimeFormatter TIMESTAMP_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    
    // Metrics
    private final AtomicLong processed3m = new AtomicLong(0);
    private final AtomicLong processed15m = new AtomicLong(0);
    private final AtomicLong processed30m = new AtomicLong(0);
    
    /**
     * 3-Minute SuperTrend Signals - Fast scalping signals
     */
    @KafkaListener(topics = "3m-supertrend-signals", 
                   groupId = "${spring.kafka.consumer.group-id}")
    public void consume3mSuperTrendSignals(
            @Payload String message,
            @Header(KafkaHeaders.RECEIVED_TIMESTAMP) long timestamp,
            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
            Acknowledgment acknowledgment) {
        
        try {
            log.info("🔥 [3M] Received SuperTrend signal from: {}", topic);
            
            Map<String, Object> signalData = objectMapper.readValue(message, Map.class);
            
            if (isValidSignal(signalData, "3M")) {
                processStrategySignal(signalData, "3M_SUPERTREND");
                processed3m.incrementAndGet();
                
                log.info("✅ [3M] SuperTrend signal processed: {}", extractStringValue(signalData, "scripCode"));
            }
            
            acknowledgment.acknowledge();
            
        } catch (Exception e) {
            log.error("🚨 [3M] Error processing SuperTrend signal: {}", e.getMessage(), e);
            acknowledgment.acknowledge();
        }
    }
    
    /**
     * 15-Minute BB Signals - Bollinger Band breakouts
     */
    @KafkaListener(topics = "15m-bb-signals", 
                   groupId = "${spring.kafka.consumer.group-id}")
    public void consume15mBBSignals(
            @Payload String message,
            @Header(KafkaHeaders.RECEIVED_TIMESTAMP) long timestamp,
            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
            Acknowledgment acknowledgment) {
        
        try {
            log.info("📊 [15M-BB] Received BB signal from: {}", topic);
            
            Map<String, Object> signalData = objectMapper.readValue(message, Map.class);
            
            if (isValidSignal(signalData, "15M-BB")) {
                processStrategySignal(signalData, "15M_BB");
                processed15m.incrementAndGet();
                
                log.info("✅ [15M-BB] BB signal processed: {}", extractStringValue(signalData, "scripCode"));
            }
            
            acknowledgment.acknowledge();
            
        } catch (Exception e) {
            log.error("🚨 [15M-BB] Error processing BB signal: {}", e.getMessage(), e);
            acknowledgment.acknowledge();
        }
    }
    
    /**
     * 15-Minute SuperTrend Signals - SuperTrend direction changes
     */
    @KafkaListener(topics = "15m-supertrend-signals", 
                   groupId = "${spring.kafka.consumer.group-id}")
    public void consume15mSuperTrendSignals(
            @Payload String message,
            @Header(KafkaHeaders.RECEIVED_TIMESTAMP) long timestamp,
            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
            Acknowledgment acknowledgment) {
        
        try {
            log.info("📈 [15M-ST] Received SuperTrend signal from: {}", topic);
            
            Map<String, Object> signalData = objectMapper.readValue(message, Map.class);
            
            if (isValidSignal(signalData, "15M-ST")) {
                processStrategySignal(signalData, "15M_SUPERTREND");
                processed15m.incrementAndGet();
                
                log.info("✅ [15M-ST] SuperTrend signal processed: {}", extractStringValue(signalData, "scripCode"));
            }
            
            acknowledgment.acknowledge();
            
        } catch (Exception e) {
            log.error("🚨 [15M-ST] Error processing SuperTrend signal: {}", e.getMessage(), e);
            acknowledgment.acknowledge();
        }
    }
    
    /**
     * 15-Minute FUDKII Signals - High confidence combined signals
     */
    @KafkaListener(topics = "15m-fudkii-signals", 
                   groupId = "${spring.kafka.consumer.group-id}")
    public void consume15mFudkiiSignals(
            @Payload String message,
            @Header(KafkaHeaders.RECEIVED_TIMESTAMP) long timestamp,
            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
            Acknowledgment acknowledgment) {
        
        try {
            log.info("🎯 [15M-FUDKII] Received HIGH confidence signal from: {}", topic);
            
            Map<String, Object> signalData = objectMapper.readValue(message, Map.class);
            
            if (isValidSignal(signalData, "15M-FUDKII")) {
                processStrategySignal(signalData, "15M_FUDKII");
                processed15m.incrementAndGet();
                
                log.info("✅ [15M-FUDKII] HIGH confidence signal processed: {}", extractStringValue(signalData, "scripCode"));
            }
            
            acknowledgment.acknowledge();
            
        } catch (Exception e) {
            log.error("🚨 [15M-FUDKII] Error processing FUDKII signal: {}", e.getMessage(), e);
            acknowledgment.acknowledge();
        }
    }
    
    /**
     * 30-Minute BB Signals - Bollinger Band breakouts
     */
    @KafkaListener(topics = "30m-bb-signals", 
                   groupId = "${spring.kafka.consumer.group-id}")
    public void consume30mBBSignals(
            @Payload String message,
            @Header(KafkaHeaders.RECEIVED_TIMESTAMP) long timestamp,
            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
            Acknowledgment acknowledgment) {
        
        try {
            log.info("📊 [30M-BB] Received BB signal from: {}", topic);
            
            Map<String, Object> signalData = objectMapper.readValue(message, Map.class);
            
            if (isValidSignal(signalData, "30M-BB")) {
                processStrategySignal(signalData, "30M_BB");
                processed30m.incrementAndGet();
                
                log.info("✅ [30M-BB] BB signal processed: {}", extractStringValue(signalData, "scripCode"));
            }
            
            acknowledgment.acknowledge();
            
        } catch (Exception e) {
            log.error("🚨 [30M-BB] Error processing BB signal: {}", e.getMessage(), e);
            acknowledgment.acknowledge();
        }
    }
    
    /**
     * 30-Minute SuperTrend Signals - SuperTrend direction changes
     */
    @KafkaListener(topics = "30m-supertrend-signals", 
                   groupId = "${spring.kafka.consumer.group-id}")
    public void consume30mSuperTrendSignals(
            @Payload String message,
            @Header(KafkaHeaders.RECEIVED_TIMESTAMP) long timestamp,
            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
            Acknowledgment acknowledgment) {
        
        try {
            log.info("📈 [30M-ST] Received SuperTrend signal from: {}", topic);
            
            Map<String, Object> signalData = objectMapper.readValue(message, Map.class);
            
            if (isValidSignal(signalData, "30M-ST")) {
                processStrategySignal(signalData, "30M_SUPERTREND");
                processed30m.incrementAndGet();
                
                log.info("✅ [30M-ST] SuperTrend signal processed: {}", extractStringValue(signalData, "scripCode"));
            }
            
            acknowledgment.acknowledge();
            
        } catch (Exception e) {
            log.error("🚨 [30M-ST] Error processing SuperTrend signal: {}", e.getMessage(), e);
            acknowledgment.acknowledge();
        }
    }
    
    /**
     * 30-Minute FUDKII Signals - High confidence combined signals
     */
    @KafkaListener(topics = "30m-fudkii-signals", 
                   groupId = "${spring.kafka.consumer.group-id}")
    public void consume30mFudkiiSignals(
            @Payload String message,
            @Header(KafkaHeaders.RECEIVED_TIMESTAMP) long timestamp,
            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
            Acknowledgment acknowledgment) {
        
        try {
            log.info("🎯 [30M-FUDKII] Received HIGH confidence signal from: {}", topic);
            
            Map<String, Object> signalData = objectMapper.readValue(message, Map.class);
            
            if (isValidSignal(signalData, "30M-FUDKII")) {
                processStrategySignal(signalData, "30M_FUDKII");
                processed30m.incrementAndGet();
                
                log.info("✅ [30M-FUDKII] HIGH confidence signal processed: {}", extractStringValue(signalData, "scripCode"));
            }
            
            acknowledgment.acknowledge();
            
        } catch (Exception e) {
            log.error("🚨 [30M-FUDKII] Error processing FUDKII signal: {}", e.getMessage(), e);
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
     * Process validated strategy signal
     */
    private void processStrategySignal(Map<String, Object> signalData, String strategyType) {
        try {
            String scripCode = extractStringValue(signalData, "scripCode");
            String signal = extractStringValue(signalData, "signal");
            Double entryPrice = extractDoubleValue(signalData, "entryPrice");
            Double stopLoss = extractDoubleValue(signalData, "stopLoss");
            Double target1 = extractDoubleValue(signalData, "target1");
            String confidence = extractStringValue(signalData, "confidence");
            
            log.info("🎯 Processing {} signal: {} -> {} @ {} (SL: {}, T1: {}, Confidence: {})", 
                     strategyType, scripCode, signal, entryPrice, stopLoss, target1, confidence);
            
            // Forward to trade execution service
            tradeExecutionService.executeStrategySignal(
                    scripCode, signal, entryPrice, stopLoss, target1, 
                    strategyType, confidence);
            
        } catch (Exception e) {
            log.error("🚨 Error processing strategy signal: {}", e.getMessage());
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
     * Get processing statistics
     */
    public String getStats() {
        return String.format("Signal Processing: 3m=%d, 15m=%d, 30m=%d", 
                           processed3m.get(), processed15m.get(), processed30m.get());
    }
} 