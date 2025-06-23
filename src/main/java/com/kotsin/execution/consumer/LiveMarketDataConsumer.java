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
    
    private final CleanTradeExecutionService cleanTradeExecutionService;
    private final TradingHoursService tradingHoursService;
    private final ObjectMapper objectMapper;
    
    private static final DateTimeFormatter TIMESTAMP_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    
    // Metrics for monitoring
    private final AtomicLong processedTicks = new AtomicLong(0);
    private final AtomicLong rejectedTicks = new AtomicLong(0);
    private final AtomicLong matchedTicks = new AtomicLong(0);
    
    /**
     * Consume live market data with trading hours validation
     * Configured to consume from earliest offset to process all today's market data
     */
    @KafkaListener(topics = "forwardtesting-data",
                   groupId = "kotsin-trade-execution-market-data-today-v2",
                   properties = {"auto.offset.reset=earliest"})
    public void consumeMarketData(
            @Payload String message,
            @Header(KafkaHeaders.RECEIVED_TIMESTAMP) long timestamp,
            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
            Acknowledgment acknowledgment) {
        
        try {
            // Parse tick data
            Map<String, Object> tickData = objectMapper.readValue(message, Map.class);
            
            // Extract basic information with multiple fallback fields
            String scripCode = extractScripCode(tickData);
            String exchange = extractStringValue(tickData, "Exch");
            Double lastRate = extractDoubleValue(tickData, "LastRate");
            
            // Extract or generate timestamp
            LocalDateTime tickTime = extractTickTime(tickData, timestamp);
            
            // Enhanced debugging every 500 ticks to see what script codes we're getting
            if (processedTicks.get() % 500 == 0) {
                log.info("🔍 [MarketData-Debug] Sample tick data fields: {}", tickData.keySet());
                log.info("🔍 [MarketData-Debug] Extracted: scripCode={}, exchange={}, price={}, companyName={}, Token={}", 
                        scripCode, exchange, lastRate, 
                        tickData.get("companyName"), tickData.get("Token"));
                
                // Log all possible script code fields
                log.info("🔍 [MarketData-Debug] All potential script fields: companyName={}, scripCode={}, Token={}, token={}, instrument_token={}, symbol={}", 
                        tickData.get("companyName"), tickData.get("scripCode"), 
                        tickData.get("Token"), tickData.get("token"), tickData.get("instrument_token"), tickData.get("symbol"));
            }
            
            // Enhanced logging for debugging
            if (processedTicks.get() % 100 == 0) {
                log.debug("🔍 Market Data Sample - ScripCode: {}, Exchange: {}, Price: {}, Time: {}, Available fields: {}", 
                        scripCode, exchange, lastRate, tickTime, tickData.keySet());
            }
            
            // Quick validation
            if (scripCode == null || lastRate == null) {
                log.debug("Skipping tick with missing data: scripCode={}, lastRate={}, fields={}", 
                        scripCode, lastRate, tickData.keySet());
                acknowledgment.acknowledge();
                return;
            }
            
            // Process only today's market data
            if (!isTodayMarketData(tickTime)) {
                rejectedTicks.incrementAndGet();
                if (rejectedTicks.get() % 1000 == 0) {
                    log.debug("📅 Rejected {} non-today ticks - Tick time: {}, Today: {}", 
                            rejectedTicks.get(), tickTime.toLocalDate(), 
                            java.time.LocalDate.now());
                }
                acknowledgment.acknowledge();
                return;
            }
            
            // Enhanced trading hours validation with debugging
            if (!isValidTradingTime(exchange, tickTime)) {
                rejectedTicks.incrementAndGet();
                // Log rejection with more details
                if (rejectedTicks.get() % 500 == 0) {
                    log.warn("🚫 Rejected {} ticks outside trading hours - Exchange: {}, Current IST: {}, Tick Time: {}", 
                            rejectedTicks.get(), exchange, 
                            tradingHoursService.getCurrentISTTime(), tickTime);
                }
                acknowledgment.acknowledge();
                return;
            }
            
            // Process valid tick
            boolean tickMatched = processValidTick(scripCode, lastRate, tickTime);
            if (tickMatched) {
                matchedTicks.incrementAndGet();
            }
            
            // Update metrics
            long processed = processedTicks.incrementAndGet();
            
            // Enhanced progress logging
            if (processed % 1000 == 0) {
                log.info("📊 Market Data Stats - Processed: {}, Rejected: {}, Matched to Trades: {}, Current IST: {}", 
                        processed, rejectedTicks.get(), matchedTicks.get(), 
                        tradingHoursService.getCurrentISTTime().format(DateTimeFormatter.ofPattern("HH:mm:ss")));
            }
            
            acknowledgment.acknowledge();
            
        } catch (Exception e) {
            log.error("🚨 Error processing market data: {}", e.getMessage());
            acknowledgment.acknowledge(); // Acknowledge to avoid reprocessing
        }
    }
    
    /**
     * Enhanced script code extraction with multiple fallback fields
     */
    private String extractScripCode(Map<String, Object> tickData) {
        // Primary: Try Token field first (this contains the actual script code)
        Object tokenObj = tickData.get("Token");
        if (tokenObj != null) {
            String scripCode = tokenObj.toString().trim();
            if (!scripCode.isEmpty() && !scripCode.equals("null")) {
                log.debug("🔍 [ScriptCode] Extracted from Token: {} (type: {})", scripCode, tokenObj.getClass().getSimpleName());
                return scripCode;
            }
        }
        
        // Try other possible fields as fallbacks
        String scripCode = extractStringValue(tickData, "companyName");
        if (scripCode != null && !scripCode.isEmpty()) {
            log.debug("🔍 [ScriptCode] Fallback to companyName: {}", scripCode);
            return scripCode.trim();
        }
        
        scripCode = extractStringValue(tickData, "scripCode");
        if (scripCode != null && !scripCode.isEmpty()) {
            log.debug("🔍 [ScriptCode] Fallback to scripCode: {}", scripCode);
            return scripCode.trim();
        }
        
        scripCode = extractStringValue(tickData, "token");
        if (scripCode != null && !scripCode.isEmpty()) {
            log.debug("🔍 [ScriptCode] Fallback to token: {}", scripCode);
            return scripCode.trim();
        }
        
        scripCode = extractStringValue(tickData, "instrument_token");
        if (scripCode != null && !scripCode.isEmpty()) {
            log.debug("🔍 [ScriptCode] Fallback to instrument_token: {}", scripCode);
            return scripCode.trim();
        }
        
        log.warn("🚨 [ScriptCode] Could not extract script code from tick data fields: {}", tickData.keySet());
        return null;
    }
    
    /**
     * Enhanced trading hours validation with detailed logging
     */
    private boolean isValidTradingTime(String exchange, LocalDateTime tickTime) {
        LocalDateTime currentIST = tradingHoursService.getCurrentISTTime();
        
        // Check if it's weekend
        if (tradingHoursService.isWeekend()) {
            if (rejectedTicks.get() % 1000 == 0) {
                log.debug("🚫 Weekend - rejecting tick for {}", exchange);
            }
            return false;
        }
        
        // For testing/demo purposes, allow some flexibility outside trading hours
        // but log it clearly
        boolean withinStrictHours = tradingHoursService.shouldProcessTrade(exchange, tickTime);
        
        if (!withinStrictHours) {
            // For demo/testing, allow processing within 2 hours of market hours
            boolean isFlexibleTime = isWithinFlexibleHours(exchange, currentIST);
            
            if (isFlexibleTime) {
                if (rejectedTicks.get() % 500 == 0) {
                    log.info("⚠️ Processing tick in FLEXIBLE hours (outside strict trading hours) - Exchange: {}, IST: {}", 
                            exchange, currentIST.format(DateTimeFormatter.ofPattern("HH:mm:ss")));
                }
                return true;
            }
            
            return false;
        }
        
        return true;
    }
    
    /**
     * Check if current time is within flexible hours (for testing/demo)
     */
    private boolean isWithinFlexibleHours(String exchange, LocalDateTime currentTime) {
        java.time.LocalTime currentTimeOfDay = currentTime.toLocalTime();
        
        if ("N".equalsIgnoreCase(exchange) || "NSE".equalsIgnoreCase(exchange)) {
            // NSE flexible: 7:00 AM to 6:00 PM (extended for testing)
            return !currentTimeOfDay.isBefore(java.time.LocalTime.of(7, 0)) && 
                   !currentTimeOfDay.isAfter(java.time.LocalTime.of(18, 0));
        } else if ("M".equalsIgnoreCase(exchange) || "MCX".equalsIgnoreCase(exchange)) {
            // MCX flexible: 7:00 AM to 1:00 AM next day
            return !currentTimeOfDay.isBefore(java.time.LocalTime.of(7, 0)) && 
                   !currentTimeOfDay.isAfter(java.time.LocalTime.of(1, 0));
        }
        
        // Default to allow for unknown exchanges
        return true;
    }
    
    /**
     * Process valid tick data by forwarding to trade execution service
     */
    private boolean processValidTick(String scripCode, double price, LocalDateTime tickTime) {
        try {
            log.debug("📈 [LiveMarketData] Processing tick for {} at price {} (Time: {})", 
                    scripCode, price, tickTime.format(DateTimeFormatter.ofPattern("HH:mm:ss")));
            
            // Forward tick to trade execution service for active trade updates
            cleanTradeExecutionService.updateTradeWithPrice(scripCode, price, tickTime);
            
            return true;
            
        } catch (Exception e) {
            log.error("🚨 [LiveMarketData] Error processing tick for {}: {}", scripCode, e.getMessage());
            return false;
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
                    // Parse as UTC and convert to IST
                    LocalDateTime utcTime = LocalDateTime.parse(timestampStr, TIMESTAMP_FORMATTER);
                    return convertUTCtoIST(utcTime);
                } catch (Exception e) {
                    // Try alternative formats
                    return parseAlternativeTimestamp(timestampStr);
                }
            }
            
            // Try numeric timestamp
            Object timeObj = tickData.get("time");
            if (timeObj instanceof Number) {
                long timeMillis = ((Number) timeObj).longValue();
                // Convert epoch millis (UTC) to IST
                java.time.Instant instant = java.time.Instant.ofEpochMilli(timeMillis);
                return LocalDateTime.ofInstant(instant, java.time.ZoneId.of("Asia/Kolkata"));
            }
            
            // Use Kafka timestamp (UTC) and convert to IST
            if (kafkaTimestamp > 0) {
                java.time.Instant instant = java.time.Instant.ofEpochMilli(kafkaTimestamp);
                LocalDateTime istTime = LocalDateTime.ofInstant(instant, java.time.ZoneId.of("Asia/Kolkata"));
                log.debug("🕐 Converted Kafka timestamp {} UTC to {} IST", 
                        java.time.Instant.ofEpochMilli(kafkaTimestamp), istTime);
                return istTime;
            }
            
            // Fallback to current IST time
            return tradingHoursService.getCurrentISTTime();
            
        } catch (Exception e) {
            log.debug("Could not extract tick timestamp, using current time: {}", e.getMessage());
            return tradingHoursService.getCurrentISTTime();
        }
    }
    
    /**
     * Convert UTC LocalDateTime to IST LocalDateTime
     */
    private LocalDateTime convertUTCtoIST(LocalDateTime utcTime) {
        try {
            // Create a ZonedDateTime in UTC
            java.time.ZonedDateTime utcZoned = utcTime.atZone(java.time.ZoneId.of("UTC"));
            
            // Convert to IST
            java.time.ZonedDateTime istZoned = utcZoned.withZoneSameInstant(java.time.ZoneId.of("Asia/Kolkata"));
            
            LocalDateTime istTime = istZoned.toLocalDateTime();
            
            log.debug("🕐 Timezone conversion: {} UTC → {} IST", utcTime, istTime);
            
            return istTime;
            
        } catch (Exception e) {
            log.warn("Error converting UTC to IST: {}", e.getMessage());
            return utcTime; // Return as-is if conversion fails
        }
    }
    
    /**
     * Try alternative timestamp formats with proper UTC to IST conversion
     */
    private LocalDateTime parseAlternativeTimestamp(String timestampStr) {
        try {
            LocalDateTime parsedTime = null;
            
            // Try ISO format (assume UTC)
            if (timestampStr.contains("T")) {
                String cleanStr = timestampStr.replace("Z", "").replace("+00:00", "");
                parsedTime = LocalDateTime.parse(cleanStr);
            }
            // Try date only format
            else if (timestampStr.length() == 10) {
                parsedTime = LocalDateTime.parse(timestampStr + " 00:00:00", 
                        DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
            }
            // Try with different separators
            else if (timestampStr.contains("/")) {
                // Handle MM/dd/yyyy format
                try {
                    parsedTime = LocalDateTime.parse(timestampStr + " 00:00:00", 
                            DateTimeFormatter.ofPattern("MM/dd/yyyy HH:mm:ss"));
                } catch (Exception ignored) {}
            }
            
            // If we parsed something, convert from UTC to IST
            if (parsedTime != null) {
                return convertUTCtoIST(parsedTime);
            }
            
            // Default fallback
            return tradingHoursService.getCurrentISTTime();
            
        } catch (Exception e) {
            log.debug("Failed to parse alternative timestamp format: {}", timestampStr);
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
    
    /**
     * Check if the market data tick is from today
     */
    private boolean isTodayMarketData(LocalDateTime tickTime) {
        try {
            java.time.LocalDate tickDate = tickTime.toLocalDate();
            java.time.LocalDate today = java.time.LocalDate.now();
            
            boolean isToday = tickDate.equals(today);
            
            if (!isToday) {
                log.debug("📅 [MarketData] Tick from {}, today is {} - skipping", tickDate, today);
            }
            
            return isToday;
            
        } catch (Exception e) {
            log.error("🚨 [MarketData] Error checking tick date for time {}: {}", tickTime, e.getMessage());
            // If we can't parse the date, process it anyway (conservative approach)
            return true;
        }
    }
} 