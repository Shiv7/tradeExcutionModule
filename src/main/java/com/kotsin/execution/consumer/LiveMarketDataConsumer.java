package com.kotsin.execution.consumer;

import com.kotsin.execution.model.MarketData;
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
import java.util.concurrent.atomic.AtomicLong;

/**
 * 🛡️ BULLETPROOF Consumer for live market data from forwardtesting-data topic.
 * 🔧 FIXED: Now uses MarketData POJO for proper type safety and Token-scripCode linking.
 */
import com.kotsin.execution.logic.TradeManager;

@Component
@Slf4j
@RequiredArgsConstructor
public class LiveMarketDataConsumer {
    
    private final TradingHoursService tradingHoursService;
    private final TradeManager tradeManager;
    
    private static final DateTimeFormatter TIMESTAMP_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    
    // Metrics for monitoring
    private final AtomicLong processedTicks = new AtomicLong(0);
    private final AtomicLong rejectedTicks = new AtomicLong(0);
    private final AtomicLong matchedTicks = new AtomicLong(0);
    
    /**
     * 🔧 FIXED: Consume MarketData POJO directly with proper JSON deserialization
     * Uses marketDataKafkaListenerContainerFactory for type-safe POJO conversion
     * Token (market data) = scripCode (strategy signals) for company linking
     * 🎯 Group ID: Configured in application.properties via containerFactory
     */
    @KafkaListener(topics = "forwardtesting-data",
                   properties = {"auto.offset.reset=earliest"},
                   containerFactory = "marketDataKafkaListenerContainerFactory")
    public void consumeMarketData(
            @Payload MarketData marketData,
            @Header(KafkaHeaders.RECEIVED_TIMESTAMP) long timestamp,
            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
            Acknowledgment acknowledgment) {
        
        try {
            // 🔗 CRITICAL: Extract unique identifier (Token as String for linking)
            String scripCode = marketData.getUniqueIdentifier(); // Token as String
            String exchange = marketData.getExchange();
            double lastRate = marketData.getLastRate();
            
            // Extract or generate timestamp
            LocalDateTime tickTime = extractTickTime(marketData, timestamp);
            
            // 🔇 SMART LOGGING: Only log detailed info if there's an active trade for this scripCode
            boolean hasActiveTradeForScript = tradeManager.getCurrentTrade() != null &&
                scripCode.equals(tradeManager.getCurrentTrade().getScripCode());
            
            // 🎯 DEBUGGING: Log market data for active trade token
            if (hasActiveTradeForScript) {
                log.info("📈 [LiveMarketData-ACTIVE] Received data for ACTIVE TRADE token: {} at price: {} (Exchange: {}, Time: {})", 
                        scripCode, lastRate, exchange, tickTime.format(DateTimeFormatter.ofPattern("HH:mm:ss")));
            }
            
            // Quick validation
            if (scripCode == null || lastRate <= 0) {
                if (hasActiveTradeForScript) {
                    log.warn("⚠️ [ACTIVE-TRADE] Invalid data for ACTIVE trade: Token={}, LastRate={}", 
                            marketData.getToken(), lastRate);
                }
                acknowledgment.acknowledge();
                return;
            }
            
            // Process only today's market data
            if (!isTodayMarketData(tickTime)) {
                rejectedTicks.incrementAndGet();
                acknowledgment.acknowledge();
                return;
            }
            
            // Enhanced trading hours validation - only log rejections for active trades
            if (!isValidTradingTime(exchange, tickTime)) {
                rejectedTicks.incrementAndGet();
                
                // Only log rejection details for active trades
                if (hasActiveTradeForScript) {
                    log.warn("🚫 [ACTIVE-TRADE] REJECTED outside trading hours - Exchange: {}, Current IST: {}, Tick Time: {}", 
                            exchange, tradingHoursService.getCurrentISTTime(), tickTime);
                }
                acknowledgment.acknowledge();
                return;
            }
            
            // Process valid tick with proper linking
            boolean tickMatched = processValidTick(scripCode, lastRate, tickTime, marketData);
            if (tickMatched) {
                matchedTicks.incrementAndGet();
            }
            
            // Update metrics
            long processed = processedTicks.incrementAndGet();
            
            // 📊 REDUCED STATS LOGGING: Only every 5000 ticks instead of 1000
            if (processed % 5000 == 0) {
                log.info("📊 Market Data Stats - Processed: {}, Rejected: {}, Matched to Trades: {}, Current IST: {}", 
                        processed, rejectedTicks.get(), matchedTicks.get(), 
                        tradingHoursService.getCurrentISTTime().format(DateTimeFormatter.ofPattern("HH:mm:ss")));
            }
            
            acknowledgment.acknowledge();
            
        } catch (Exception e) {
            log.error("🚨 Error processing market data POJO: {}", e.getMessage());
            acknowledgment.acknowledge(); // Acknowledge to avoid reprocessing
        }
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
            // 🕐 NSE STRICT: Only during trading hours 9:15 AM - 3:30 PM
            return !currentTimeOfDay.isBefore(java.time.LocalTime.of(9, 15)) && 
                   !currentTimeOfDay.isAfter(java.time.LocalTime.of(15, 30));
        } else if ("M".equalsIgnoreCase(exchange) || "MCX".equalsIgnoreCase(exchange)) {
            // MCX flexible: 9:00 AM to 11:30 PM (commodities extended hours)
            return !currentTimeOfDay.isBefore(java.time.LocalTime.of(9, 0)) && 
                   !currentTimeOfDay.isAfter(java.time.LocalTime.of(23, 30));
        }
        
        // Default to allow for unknown exchanges
        return true;
    }
    
    /**
     * 🛡️ BULLETPROOF: Process valid tick data using bulletproof trade execution system
     * 🔗 FIXED: Enhanced with MarketData POJO and proper Token-scripCode linking
     */
    private boolean processValidTick(String scripCode, double price, LocalDateTime tickTime, MarketData marketData) {
        try {
            log.debug("📈 [LiveMarketData-POJO] Processing Token {} (scripCode: {}) at price {} (Time: {})", 
                    marketData.getToken(), scripCode, price, tickTime.format(DateTimeFormatter.ofPattern("HH:mm:ss")));
            
            // 🛡️ BULLETPROOF: Forward to bulletproof signal consumer for single trade management
            // 🔗 CRITICAL: Use scripCode (Token as String) for linking with strategy signals
            // DEPRECATED: This logic is now handled by the 5-minute candle consumer.
            // bulletproofSignalConsumer.updatePrice(scripCode, price, tickTime);
            
            return true;
            
        } catch (Exception e) {
            log.error("🚨 [LiveMarketData-POJO] Error processing tick for Token {} (scripCode: {}): {}", 
                     marketData.getToken(), scripCode, e.getMessage());
            return false;
        }
    }
    
    /**
     * Extract tick timestamp from MarketData POJO, preferring tick data timestamp over Kafka timestamp
     */
    private LocalDateTime extractTickTime(MarketData marketData, long kafkaTimestamp) {
        try {
            // Try to extract from tick data first
            String timestampStr = marketData.getTickDt();
            if (timestampStr != null && !timestampStr.trim().isEmpty()) {
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
            
            // Try numeric timestamp from MarketData
            long timeMillis = marketData.getTime();
            if (timeMillis > 0) {
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
        matchedTicks.set(0);
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
                log.debug("📅 [MarketData-POJO] Tick from {}, today is {} - skipping", tickDate, today);
            }
            
            return isToday;
            
        } catch (Exception e) {
            log.error("🚨 [MarketData-POJO] Error checking tick date for time {}: {}", tickTime, e.getMessage());
            // If we can't parse the date, process it anyway (conservative approach)
            return true;
        }
    }
}
