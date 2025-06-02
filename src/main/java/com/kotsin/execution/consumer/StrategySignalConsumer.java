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

/**
 * Consumer for strategy signals with trading hours validation.
 * Uses latest offset to avoid processing old messages outside trading hours.
 */
@Component
@Slf4j
@RequiredArgsConstructor
public class StrategySignalConsumer {
    
    private final TradeExecutionService tradeExecutionService;
    private final TradingHoursService tradingHoursService;
    private final ObjectMapper objectMapper;
    
    private static final DateTimeFormatter TIMESTAMP_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    
    /**
     * BB SuperTrend Strategy Signals - 30m timeframe
     */
    @KafkaListener(topics = "${kafka.topics.signals.bb-supertrend:bb-supertrend-signals}", 
                   groupId = "${spring.kafka.consumer.group-id}")
    public void consumeBBSuperTrendSignal(
            @Payload String message,
            @Header(KafkaHeaders.RECEIVED_TIMESTAMP) long timestamp,
            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
            Acknowledgment acknowledgment) {
        
        try {
            log.info("📡 Received BB SuperTrend signal from topic: {}", topic);
            
            // Parse signal data
            Map<String, Object> signalData = objectMapper.readValue(message, Map.class);
            
            // Extract timing and exchange information
            LocalDateTime messageTime = extractMessageTime(signalData, timestamp);
            String exchange = extractStringValue(signalData, "exchange");
            String scripCode = extractStringValue(signalData, "scripCode");
            
            // Validate trading hours and message timing
            if (!tradingHoursService.shouldProcessTrade(exchange, messageTime)) {
                log.warn("🚫 Skipping BB SuperTrend signal for {} - outside trading hours or too old", scripCode);
                acknowledgment.acknowledge();
                return;
            }
            
            // Process the signal
            processStrategySignal(signalData, messageTime, "BB_SUPERTREND");
            
            log.info("✅ BB SuperTrend signal processed successfully for: {}", scripCode);
            acknowledgment.acknowledge();
            
        } catch (Exception e) {
            log.error("🚨 Error processing BB SuperTrend signal: {}", e.getMessage(), e);
            acknowledgment.acknowledge(); // Acknowledge to avoid reprocessing
        }
    }
    
    /**
     * SuperTrend Break Strategy Signals - Multi-timeframe
     */
    @KafkaListener(topics = "${kafka.topics.signals.supertrend-break:supertrend-break-signals}", 
                   groupId = "${spring.kafka.consumer.group-id}")
    public void consumeSuperTrendBreakSignal(
            @Payload String message,
            @Header(KafkaHeaders.RECEIVED_TIMESTAMP) long timestamp,
            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
            Acknowledgment acknowledgment) {
        
        try {
            log.info("📡 Received SuperTrend Break signal from topic: {}", topic);
            
            Map<String, Object> signalData = objectMapper.readValue(message, Map.class);
            
            LocalDateTime messageTime = extractMessageTime(signalData, timestamp);
            String exchange = extractStringValue(signalData, "exchange");
            String scripCode = extractStringValue(signalData, "scripCode");
            
            if (!tradingHoursService.shouldProcessTrade(exchange, messageTime)) {
                log.warn("🚫 Skipping SuperTrend Break signal for {} - outside trading hours or too old", scripCode);
                acknowledgment.acknowledge();
                return;
            }
            
            processStrategySignal(signalData, messageTime, "SUPERTREND_BREAK");
            
            log.info("✅ SuperTrend Break signal processed successfully for: {}", scripCode);
            acknowledgment.acknowledge();
            
        } catch (Exception e) {
            log.error("🚨 Error processing SuperTrend Break signal: {}", e.getMessage(), e);
            acknowledgment.acknowledge();
        }
    }
    
    /**
     * Three Minute SuperTrend Strategy Signals - 3m timeframe
     */
    @KafkaListener(topics = "${kafka.topics.signals.three-minute-supertrend:three-minute-supertrend-signals}", 
                   groupId = "${spring.kafka.consumer.group-id}")
    public void consumeThreeMinuteSuperTrendSignal(
            @Payload String message,
            @Header(KafkaHeaders.RECEIVED_TIMESTAMP) long timestamp,
            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
            Acknowledgment acknowledgment) {
        
        try {
            log.info("📡 Received 3-Minute SuperTrend signal from topic: {}", topic);
            
            Map<String, Object> signalData = objectMapper.readValue(message, Map.class);
            
            LocalDateTime messageTime = extractMessageTime(signalData, timestamp);
            String exchange = extractStringValue(signalData, "exchange");
            String scripCode = extractStringValue(signalData, "scripCode");
            
            if (!tradingHoursService.shouldProcessTrade(exchange, messageTime)) {
                log.warn("🚫 Skipping 3-Minute SuperTrend signal for {} - outside trading hours or too old", scripCode);
                acknowledgment.acknowledge();
                return;
            }
            
            processStrategySignal(signalData, messageTime, "THREE_MINUTE_SUPERTREND");
            
            log.info("✅ 3-Minute SuperTrend signal processed successfully for: {}", scripCode);
            acknowledgment.acknowledge();
            
        } catch (Exception e) {
            log.error("🚨 Error processing 3-Minute SuperTrend signal: {}", e.getMessage(), e);
            acknowledgment.acknowledge();
        }
    }
    
    /**
     * BB Breakout Strategy Signals - BB-only breakouts
     */
    @KafkaListener(topics = "bb-breakout-signals", 
                   groupId = "${spring.kafka.consumer.group-id}")
    public void consumeBBBreakoutSignal(
            @Payload String message,
            @Header(KafkaHeaders.RECEIVED_TIMESTAMP) long timestamp,
            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
            Acknowledgment acknowledgment) {
        
        try {
            log.info("📡 Received BB Breakout signal from topic: {}", topic);
            
            Map<String, Object> signalData = objectMapper.readValue(message, Map.class);
            
            // Validate trading hours
            if (!tradingHoursService.isTradingHours()) {
                log.warn("⏰ BB Breakout signal received outside trading hours - rejected");
                acknowledgment.acknowledge();
                return;
            }
            
            // Validate message age
            if (!isRecentMessage(timestamp)) {
                log.warn("⏰ BB Breakout signal too old ({} minutes) - rejected", 
                        (System.currentTimeMillis() - timestamp) / 60000);
                acknowledgment.acknowledge();
                return;
            }
            
            // Process BB breakout signal
            String signalType = determineSignalType(signalData, "BB_BREAKOUT");
            if (signalType != null) {
                tradeExecutionService.processSignal(signalData, "BB_BREAKOUT", signalType);
                log.info("✅ BB Breakout signal processed successfully: {}", signalType);
            } else {
                log.warn("⚠️ Invalid BB Breakout signal received - no valid signal type found");
            }
            
            acknowledgment.acknowledge();
            
        } catch (Exception e) {
            log.error("❌ Error processing BB Breakout signal: {}", e.getMessage(), e);
            acknowledgment.acknowledge(); // Acknowledge to prevent reprocessing
        }
    }
    
    /**
     * Fudkii Strategy Signals - Custom strategy
     */
    @KafkaListener(topics = "${kafka.topics.signals.fudkii:fudkii_Signal}", 
                   groupId = "${spring.kafka.consumer.group-id}")
    public void consumeFudkiiSignal(
            @Payload String message,
            @Header(KafkaHeaders.RECEIVED_TIMESTAMP) long timestamp,
            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
            Acknowledgment acknowledgment) {
        
        try {
            log.info("📡 Received Fudkii signal from topic: {}", topic);
            
            Map<String, Object> signalData = objectMapper.readValue(message, Map.class);
            
            LocalDateTime messageTime = extractMessageTime(signalData, timestamp);
            String exchange = extractStringValue(signalData, "exchange");
            String scripCode = extractStringValue(signalData, "scripCode");
            
            if (!tradingHoursService.shouldProcessTrade(exchange, messageTime)) {
                log.warn("🚫 Skipping Fudkii signal for {} - outside trading hours or too old", scripCode);
                acknowledgment.acknowledge();
                return;
            }
            
            processStrategySignal(signalData, messageTime, "FUDKII_STRATEGY");
            
            log.info("✅ Fudkii signal processed successfully for: {}", scripCode);
            acknowledgment.acknowledge();
            
        } catch (Exception e) {
            log.error("🚨 Error processing Fudkii signal: {}", e.getMessage(), e);
            acknowledgment.acknowledge();
        }
    }
    
    /**
     * Process strategy signal with comprehensive validation
     */
    private void processStrategySignal(Map<String, Object> signalData, LocalDateTime messageTime, String strategyName) {
        try {
            // Extract required fields
            String scripCode = extractStringValue(signalData, "scripCode");
            String companyName = extractStringValue(signalData, "companyName");
            String exchange = extractStringValue(signalData, "exchange");
            String exchangeType = extractStringValue(signalData, "exchangeType");
            
            // Determine signal type based on signal data
            String signalType = determineSignalType(signalData, strategyName);
            
            // Validate required fields
            if (scripCode == null || signalType == null) {
                log.warn("⚠️ Invalid signal data - missing scripCode or signal type: {}", signalData);
                return;
            }
            
            // Log signal processing with trading hours context
            log.info("🎯 Processing {} signal for {} ({}) - Signal Type: {}, Exchange: {}, Time: {}", 
                    strategyName, companyName, scripCode, signalType, exchange, messageTime);
            
            // Forward to trade execution service
            tradeExecutionService.processNewSignal(
                    signalData,
                    messageTime,
                    strategyName,
                    signalType,
                    scripCode,
                    companyName != null ? companyName : scripCode,
                    exchange != null ? exchange : "N", // Default to NSE
                    exchangeType != null ? exchangeType : "EQUITY"
            );
            
        } catch (Exception e) {
            log.error("🚨 Error in strategy signal processing: {}", e.getMessage(), e);
            throw e; // Re-throw to trigger acknowledgment
        }
    }
    
    /**
     * Determine signal type (BULLISH/BEARISH) from signal data
     */
    private String determineSignalType(Map<String, Object> signalData, String strategyName) {
        try {
            // Check common signal fields
            String supertrendSignal = extractStringValue(signalData, "supertrendSignal");
            Boolean isBullish = extractBooleanValue(signalData, "supertrendIsBullish");
            String signal = extractStringValue(signalData, "signal");
            
            // Strategy-specific signal detection
            switch (strategyName) {
                case "BB_SUPERTREND":
                    return determineBBSuperTrendSignal(signalData);
                case "BB_BREAKOUT":
                    return determineBBBreakoutSignal(signalData);
                case "SUPERTREND_BREAK":
                case "THREE_MINUTE_SUPERTREND":
                    return determineSuperTrendSignal(supertrendSignal, isBullish, signal);
                case "FUDKII_STRATEGY":
                    return determineFudkiiSignal(signalData);
                default:
                    log.warn("⚠️ Unknown strategy type: {}", strategyName);
                    return null;
            }
        } catch (Exception e) {
            log.error("❌ Error determining signal type for {}: {}", strategyName, e.getMessage());
            return null;
        }
    }
    
    /**
     * Determine BB SuperTrend signal type based on simultaneous conditions
     */
    private String determineBBSuperTrendSignal(Map<String, Object> signalData) {
        try {
            String supertrendSignal = extractStringValue(signalData, "supertrendSignal");
            Double closePrice = extractDoubleValue(signalData, "closePrice");
            Double bbUpper = extractDoubleValue(signalData, "bbUpper");
            Double bbLower = extractDoubleValue(signalData, "bbLower");
            
            // BB SuperTrend requires both SuperTrend signal AND BB breakout
            if ("Buy".equalsIgnoreCase(supertrendSignal) && closePrice != null && bbUpper != null) {
                if (closePrice > bbUpper) {
                    return "BULLISH"; // SuperTrend Buy + Price above BB Upper
                }
            }
            
            if ("Sell".equalsIgnoreCase(supertrendSignal) && closePrice != null && bbLower != null) {
                if (closePrice < bbLower) {
                    return "BEARISH"; // SuperTrend Sell + Price below BB Lower
                }
            }
            
            return null; // No valid BB SuperTrend signal
            
        } catch (Exception e) {
            log.error("Error determining BB SuperTrend signal: {}", e.getMessage());
            return null;
        }
    }
    
    /**
     * Extract message timestamp, preferring signal timestamp over Kafka timestamp
     */
    private LocalDateTime extractMessageTime(Map<String, Object> signalData, long kafkaTimestamp) {
        try {
            // Try to extract timestamp from signal data first
            String timestampStr = extractStringValue(signalData, "timestamp");
            if (timestampStr != null) {
                return LocalDateTime.parse(timestampStr, TIMESTAMP_FORMATTER);
            }
            
            // Fallback to Kafka timestamp converted to IST
            return tradingHoursService.getCurrentISTTime();
            
        } catch (Exception e) {
            log.debug("Could not extract message timestamp, using current time: {}", e.getMessage());
            return tradingHoursService.getCurrentISTTime();
        }
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
    
    /**
     * Extract boolean value from signal data
     */
    private Boolean extractBooleanValue(Map<String, Object> data, String key) {
        Object value = data.get(key);
        if (value instanceof Boolean) {
            return (Boolean) value;
        }
        if (value instanceof String) {
            return Boolean.parseBoolean((String) value);
        }
        return null;
    }
    
    /**
     * Helper method to determine BB Breakout signal type
     */
    private String determineBBBreakoutSignal(Map<String, Object> signalData) {
        // BB breakout signals use BB band breakout direction
        String signal = extractStringValue(signalData, "signal");
        Boolean isBullish = extractBooleanValue(signalData, "supertrendIsBullish");
        
        if ("Buy".equalsIgnoreCase(signal)) return "BULLISH";
        if ("Sell".equalsIgnoreCase(signal)) return "BEARISH";
        if (isBullish != null) return isBullish ? "BULLISH" : "BEARISH";
        
        return null;
    }
    
    /**
     * Helper method to determine SuperTrend signal type
     */
    private String determineSuperTrendSignal(String supertrendSignal, Boolean isBullish, String signal) {
        if ("Buy".equalsIgnoreCase(supertrendSignal)) return "BULLISH";
        if ("Sell".equalsIgnoreCase(supertrendSignal)) return "BEARISH";
        if (isBullish != null) return isBullish ? "BULLISH" : "BEARISH";
        if ("Buy".equalsIgnoreCase(signal) || "BULLISH".equalsIgnoreCase(signal)) return "BULLISH";
        if ("Sell".equalsIgnoreCase(signal) || "BEARISH".equalsIgnoreCase(signal)) return "BEARISH";
        
        return null;
    }
    
    /**
     * Helper method to determine FUDKII signal type
     */
    private String determineFudkiiSignal(Map<String, Object> signalData) {
        Boolean fudkiiBullish = extractBooleanValue(signalData, "bullishMultiTimeFrameIndicator");
        if (fudkiiBullish != null) return fudkiiBullish ? "BULLISH" : "BEARISH";
        
        // Fallback to standard signal fields
        String signal = extractStringValue(signalData, "signal");
        Boolean isBullish = extractBooleanValue(signalData, "supertrendIsBullish");
        
        if ("Buy".equalsIgnoreCase(signal)) return "BULLISH";
        if ("Sell".equalsIgnoreCase(signal)) return "BEARISH";
        if (isBullish != null) return isBullish ? "BULLISH" : "BEARISH";
        
        return null;
    }
} 