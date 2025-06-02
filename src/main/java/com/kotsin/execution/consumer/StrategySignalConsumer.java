package com.kotsin.execution.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.kotsin.execution.service.TradeExecutionService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Map;

/**
 * Kafka consumer that listens to all strategy module topics and 
 * initiates trade execution for received signals.
 * 
 * Based on KotsinBackTestBE signal processing architecture.
 */
@Component
@Slf4j
@RequiredArgsConstructor
public class StrategySignalConsumer {
    
    private final TradeExecutionService tradeExecutionService;
    private final ObjectMapper objectMapper;
    
    /**
     * Listen to BB SuperTrend strategy signals
     */
    @KafkaListener(topics = "bb-supertrend-signals", groupId = "trade-execution-group")
    public void consumeBBSuperTrendSignal(
            @Payload String signalJson,
            @Header(KafkaHeaders.RECEIVED_TIMESTAMP) Long timestamp,
            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic) {
        
        processSignal(signalJson, timestamp, topic, "BB_SUPERTREND");
    }
    
    /**
     * Listen to SuperTrend Break strategy signals
     */
    @KafkaListener(topics = "supertrend-break-signals", groupId = "trade-execution-group")
    public void consumeSuperTrendBreakSignal(
            @Payload String signalJson,
            @Header(KafkaHeaders.RECEIVED_TIMESTAMP) Long timestamp,
            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic) {
        
        processSignal(signalJson, timestamp, topic, "SUPERTREND_BREAK");
    }
    
    /**
     * Listen to Three Minute SuperTrend strategy signals
     */
    @KafkaListener(topics = "three-minute-supertrend-signals", groupId = "trade-execution-group")
    public void consumeThreeMinuteSuperTrendSignal(
            @Payload String signalJson,
            @Header(KafkaHeaders.RECEIVED_TIMESTAMP) Long timestamp,
            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic) {
        
        processSignal(signalJson, timestamp, topic, "THREE_MINUTE_SUPERTREND");
    }
    
    /**
     * Listen to any custom strategy signals
     */
    @KafkaListener(topics = "fudkii_Signal", groupId = "trade-execution-group")
    public void consumeFudkiiSignal(
            @Payload String signalJson,
            @Header(KafkaHeaders.RECEIVED_TIMESTAMP) Long timestamp,
            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic) {
        
        processSignal(signalJson, timestamp, topic, "FUDKII_STRATEGY");
    }
    
    /**
     * Generic signal processor for all strategy signals
     */
    private void processSignal(String signalJson, Long timestamp, String topic, String strategyName) {
        try {
            log.info("📨 Received signal from strategy: {} on topic: {}", strategyName, topic);
            log.debug("📨 Signal payload: {}", signalJson);
            
            // Parse the signal JSON
            Map<String, Object> signalData = objectMapper.readValue(signalJson, Map.class);
            
            // Extract basic signal information
            String scripCode = extractStringValue(signalData, "scripCode");
            String companyName = extractStringValue(signalData, "companyName");
            String exchange = extractStringValue(signalData, "exch");
            String exchangeType = extractStringValue(signalData, "exchType");
            
            if (scripCode == null || scripCode.isEmpty()) {
                log.warn("⚠️ Received signal with missing scripCode, skipping: {}", signalJson);
                return;
            }
            
            // Convert timestamp to LocalDateTime
            LocalDateTime signalTime = timestamp != null ? 
                LocalDateTime.ofInstant(Instant.ofEpochMilli(timestamp), ZoneId.of("Asia/Kolkata")) :
                LocalDateTime.now(ZoneId.of("Asia/Kolkata"));
            
            // Determine signal type based on strategy-specific fields
            String signalType = determineSignalType(signalData, strategyName);
            
            if (signalType == null) {
                log.warn("⚠️ Could not determine signal type for strategy: {}, data: {}", strategyName, signalData);
                return;
            }
            
            log.info("🎯 Processing {} signal for {} ({}): {} at {}", 
                    signalType, companyName, scripCode, strategyName, signalTime);
            
            // Hand over to trade execution service
            tradeExecutionService.processNewSignal(
                    signalData, 
                    signalTime, 
                    strategyName, 
                    signalType,
                    scripCode,
                    companyName,
                    exchange,
                    exchangeType
            );
            
        } catch (Exception e) {
            log.error("🚨 Error processing signal from topic {}: {}", topic, e.getMessage(), e);
        }
    }
    
    /**
     * Determine signal type (BULLISH/BEARISH) based on strategy-specific data
     */
    private String determineSignalType(Map<String, Object> signalData, String strategyName) {
        try {
            switch (strategyName) {
                case "BB_SUPERTREND":
                    return determineBBSuperTrendSignalType(signalData);
                    
                case "SUPERTREND_BREAK":
                case "THREE_MINUTE_SUPERTREND":
                    return determineSuperTrendSignalType(signalData);
                    
                case "FUDKII_STRATEGY":
                    return determineFudkiiSignalType(signalData);
                    
                default:
                    return determineGenericSignalType(signalData);
            }
        } catch (Exception e) {
            log.error("Error determining signal type for strategy {}: {}", strategyName, e.getMessage());
            return null;
        }
    }
    
    /**
     * Determine BB SuperTrend signal type
     */
    private String determineBBSuperTrendSignalType(Map<String, Object> signalData) {
        // For BB SuperTrend, check SuperTrend signal and BB position
        String supertrendSignal = extractStringValue(signalData, "supertrendSignal");
        Double closePrice = extractDoubleValue(signalData, "closePrice");
        Double bbUpper = extractDoubleValue(signalData, "bbUpper");
        Double bbLower = extractDoubleValue(signalData, "bbLower");
        
        if ("Buy".equals(supertrendSignal) && closePrice != null && bbUpper != null && closePrice > bbUpper) {
            return "BULLISH";
        } else if ("Sell".equals(supertrendSignal) && closePrice != null && bbLower != null && closePrice < bbLower) {
            return "BEARISH";
        }
        
        return null; // No valid signal
    }
    
    /**
     * Determine SuperTrend signal type
     */
    private String determineSuperTrendSignalType(Map<String, Object> signalData) {
        String supertrendSignal = extractStringValue(signalData, "supertrendSignal");
        
        if ("Buy".equals(supertrendSignal)) {
            return "BULLISH";
        } else if ("Sell".equals(supertrendSignal)) {
            return "BEARISH";
        }
        
        return null;
    }
    
    /**
     * Determine Fudkii strategy signal type
     */
    private String determineFudkiiSignalType(Map<String, Object> signalData) {
        // Check for bullish multi-timeframe indicator
        Map<String, Object> bullishIndicator = (Map<String, Object>) signalData.get("bullishMultiTimeFrameIndicator");
        
        if (bullishIndicator != null) {
            Object isBullishValue = bullishIndicator.get("isBullish");
            if (isBullishValue instanceof Number) {
                int bullishFlag = ((Number) isBullishValue).intValue();
                if (bullishFlag == 1) {
                    return "BULLISH";
                } else if (bullishFlag == -1) {
                    return "BEARISH";
                }
            }
        }
        
        return null;
    }
    
    /**
     * Generic signal type determination
     */
    private String determineGenericSignalType(Map<String, Object> signalData) {
        // Try common signal type fields
        String signalType = extractStringValue(signalData, "signalType");
        if (signalType != null) {
            return signalType.toUpperCase();
        }
        
        // Try SuperTrend signal as fallback
        return determineSuperTrendSignalType(signalData);
    }
    
    /**
     * Extract string value from signal data
     */
    private String extractStringValue(Map<String, Object> data, String key) {
        Object value = data.get(key);
        return value != null ? value.toString() : null;
    }
    
    /**
     * Extract double value from signal data
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
} 