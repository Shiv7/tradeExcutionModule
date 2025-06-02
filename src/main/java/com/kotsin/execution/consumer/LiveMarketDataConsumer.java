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
 * Consumer for live market data from forwardtesting-data topic.
 * Uses latest offset and validates trading hours to avoid processing old ticks.
 */
@Component
@Slf4j
@RequiredArgsConstructor
public class LiveMarketDataConsumer {
    
    private final TradeExecutionService tradeExecutionService;
    private final TradingHoursService tradingHoursService;
    private final ObjectMapper objectMapper;
    
    private static final DateTimeFormatter TIMESTAMP_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    
    // Metrics for monitoring
    private final AtomicLong processedTicks = new AtomicLong(0);
    private final AtomicLong rejectedTicks = new AtomicLong(0);
    
    /**
     * Consume live market data with trading hours validation
     */
    @KafkaListener(topics = "${kafka.topics.market-data:forwardtesting-data}", 
                   groupId = "${spring.kafka.consumer.group-id}")
    public void consumeMarketData(
            @Payload String message,
            @Header(KafkaHeaders.RECEIVED_TIMESTAMP) long timestamp,
            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
            Acknowledgment acknowledgment) {
        
        try {
            // Parse tick data
            Map<String, Object> tickData = objectMapper.readValue(message, Map.class);
            
            // Extract basic information
            String scripCode = extractStringValue(tickData, "companyName");
            String exchange = extractStringValue(tickData, "exchange");
            Double lastRate = extractDoubleValue(tickData, "lastRate");
            
            // Extract or generate timestamp
            LocalDateTime tickTime = extractTickTime(tickData, timestamp);
            
            // Quick validation
            if (scripCode == null || lastRate == null) {
                log.debug("Skipping tick with missing data: scripCode={}, lastRate={}", scripCode, lastRate);
                acknowledgment.acknowledge();
                return;
            }
            
            // Validate trading hours and tick timing
            if (!tradingHoursService.shouldProcessTrade(exchange, tickTime)) {
                rejectedTicks.incrementAndGet();
                // Log rejection only occasionally to avoid spam
                if (rejectedTicks.get() % 100 == 0) {
                    log.debug("🚫 Rejected {} ticks outside trading hours (Exchange: {}, Latest: {})", 
                            rejectedTicks.get(), exchange, tickTime);
                }
                acknowledgment.acknowledge();
                return;
            }
            
            // Process valid tick
            processValidTick(scripCode, lastRate, tickTime);
            
            // Update metrics
            long processed = processedTicks.incrementAndGet();
            
            // Log progress periodically
            if (processed % 1000 == 0) {
                log.info("📊 Processed {} market ticks, rejected {} (Trading hours validation)", 
                        processed, rejectedTicks.get());
            }
            
            acknowledgment.acknowledge();
            
        } catch (Exception e) {
            log.error("🚨 Error processing market data: {}", e.getMessage());
            acknowledgment.acknowledge(); // Acknowledge to avoid reprocessing
        }
    }
    
    /**
     * Process valid tick data by forwarding to trade execution service
     */
    private void processValidTick(String scripCode, double price, LocalDateTime tickTime) {
        try {
            // Forward tick to trade execution service for active trade updates
            tradeExecutionService.updateTradeWithPrice(scripCode, price, tickTime);
            
            log.debug("📈 Updated trades for {} at price: {} (Time: {})", scripCode, price, tickTime);
            
        } catch (Exception e) {
            log.error("🚨 Error processing tick for {}: {}", scripCode, e.getMessage());
        }
    }
    
    /**
     * Extract tick timestamp, preferring tick data timestamp over Kafka timestamp
     */
    private LocalDateTime extractTickTime(Map<String, Object> tickData, long kafkaTimestamp) {
        try {
            // Try to extract from tick data first
            String timestampStr = extractStringValue(tickData, "tickDt");
            if (timestampStr != null) {
                // Try different timestamp formats
                try {
                    return LocalDateTime.parse(timestampStr, TIMESTAMP_FORMATTER);
                } catch (Exception e) {
                    // Try alternative formats
                    return parseAlternativeTimestamp(timestampStr);
                }
            }
            
            // Try numeric timestamp
            Object timeObj = tickData.get("time");
            if (timeObj instanceof Number) {
                long timeMillis = ((Number) timeObj).longValue();
                return LocalDateTime.ofEpochSecond(timeMillis / 1000, 0, 
                        java.time.ZoneOffset.of("+05:30")); // IST offset
            }
            
            // Fallback to current IST time
            return tradingHoursService.getCurrentISTTime();
            
        } catch (Exception e) {
            log.debug("Could not extract tick timestamp, using current time: {}", e.getMessage());
            return tradingHoursService.getCurrentISTTime();
        }
    }
    
    /**
     * Try alternative timestamp formats
     */
    private LocalDateTime parseAlternativeTimestamp(String timestampStr) {
        try {
            // Try ISO format
            if (timestampStr.contains("T")) {
                return LocalDateTime.parse(timestampStr.replace("Z", ""));
            }
            
            // Try date only format
            if (timestampStr.length() == 10) {
                return LocalDateTime.parse(timestampStr + " 00:00:00", 
                        DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
            }
            
            // Default fallback
            return tradingHoursService.getCurrentISTTime();
            
        } catch (Exception e) {
            return tradingHoursService.getCurrentISTTime();
        }
    }
    
    /**
     * Extract string value from tick data
     */
    private String extractStringValue(Map<String, Object> data, String key) {
        Object value = data.get(key);
        return value != null ? value.toString().trim() : null;
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
     * Get processing statistics
     */
    public String getProcessingStats() {
        return String.format("Processed: %d ticks, Rejected: %d ticks, Acceptance Rate: %.2f%%",
                processedTicks.get(),
                rejectedTicks.get(),
                processedTicks.get() > 0 ? 
                    (double) processedTicks.get() / (processedTicks.get() + rejectedTicks.get()) * 100.0 : 0.0);
    }
    
    /**
     * Reset processing statistics
     */
    public void resetStats() {
        processedTicks.set(0);
        rejectedTicks.set(0);
        log.info("📊 Reset market data processing statistics");
    }
} 