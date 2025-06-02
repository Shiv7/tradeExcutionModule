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
 * Kafka consumer that listens to real-time market data (forwardtesting-data topic)
 * and updates active trades with current prices for exit detection.
 */
@Component
@Slf4j
@RequiredArgsConstructor
public class LiveMarketDataConsumer {
    
    private final TradeExecutionService tradeExecutionService;
    private final ObjectMapper objectMapper;
    
    /**
     * Listen to real-time market data for price updates
     */
    @KafkaListener(topics = "forwardtesting-data", groupId = "trade-execution-market-data")
    public void consumeMarketData(
            @Payload String tickJson,
            @Header(KafkaHeaders.RECEIVED_TIMESTAMP) Long timestamp) {
        
        try {
            // Parse tick data
            Map<String, Object> tickData = objectMapper.readValue(tickJson, Map.class);
            
            // Extract relevant fields
            String scripCode = extractStringValue(tickData, "scripCode");
            String companyName = extractStringValue(tickData, "companyName");
            Double lastRate = extractDoubleValue(tickData, "lastRate");
            Integer totalQuantity = extractIntegerValue(tickData, "totalQuantity");
            
            if (scripCode == null || lastRate == null) {
                return; // Skip invalid ticks
            }
            
            // Convert timestamp to LocalDateTime
            LocalDateTime tickTime = timestamp != null ? 
                LocalDateTime.ofInstant(Instant.ofEpochMilli(timestamp), ZoneId.of("Asia/Kolkata")) :
                LocalDateTime.now(ZoneId.of("Asia/Kolkata"));
            
            // Log tick data (debug level to avoid spam)
            log.debug("📊 Market tick: {} @ {} at {}", companyName, lastRate, tickTime);
            
            // Update trades with new price
            tradeExecutionService.updateTradeWithPrice(scripCode, lastRate, tickTime);
            
        } catch (Exception e) {
            // Don't log every parsing error as it will spam logs
            log.debug("Error processing market tick: {}", e.getMessage());
        }
    }
    
    /**
     * Extract string value from tick data
     */
    private String extractStringValue(Map<String, Object> data, String key) {
        Object value = data.get(key);
        return value != null ? value.toString() : null;
    }
    
    /**
     * Extract double value from tick data
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
     * Extract integer value from tick data
     */
    private Integer extractIntegerValue(Map<String, Object> data, String key) {
        Object value = data.get(key);
        if (value instanceof Number) {
            return ((Number) value).intValue();
        }
        if (value instanceof String) {
            try {
                return Integer.parseInt((String) value);
            } catch (NumberFormatException e) {
                return null;
            }
        }
        return null;
    }
} 