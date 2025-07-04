package com.kotsin.execution.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

/**
 * Service to validate trading hours and prevent processing old messages
 */
@Service
@Slf4j
public class TradingHoursService {
    
    @Value("${app.trading.hours.nse.start:09:15}")
    private String nseStartTime;
    
    @Value("${app.trading.hours.nse.end:15:30}")
    private String nseEndTime;
    
    @Value("${app.trading.hours.mcx.start:09:00}")
    private String mcxStartTime;
    
    @Value("${app.trading.hours.mcx.end:23:30}")
    private String mcxEndTime;
    
    @Value("${app.trading.hours.timezone:Asia/Kolkata}")
    private String timezone;
    
    private static final DateTimeFormatter TIME_FORMATTER = DateTimeFormatter.ofPattern("HH:mm");
    
    /**
     * Check if current time is within trading hours for given exchange
     */
    public boolean isWithinTradingHours(String exchange) {
        return isWithinTradingHours(exchange, getCurrentISTTime());
    }
    
    /**
     * Check if given time is within trading hours for given exchange
     */
    public boolean isWithinTradingHours(String exchange, LocalDateTime dateTime) {
        try {
            LocalTime currentTime = dateTime.toLocalTime();
            
            if ("N".equalsIgnoreCase(exchange) || "NSE".equalsIgnoreCase(exchange)) {
                return isWithinNSEHours(currentTime);
            } else if ("M".equalsIgnoreCase(exchange) || "MCX".equalsIgnoreCase(exchange)) {
                return isWithinMCXHours(currentTime);
            } else {
                // Default to NSE hours for unknown exchanges
                log.warn("Unknown exchange: {}, defaulting to NSE hours", exchange);
                return isWithinNSEHours(currentTime);
            }
        } catch (Exception e) {
            log.error("Error checking trading hours for exchange {}: {}", exchange, e.getMessage());
            return false;
        }
    }
    
    /**
     * Check if message timestamp is recent (within last 5 minutes) and within trading hours
     */
    public boolean isValidForProcessing(String exchange, LocalDateTime messageTime) {
        try {
            LocalDateTime currentTime = getCurrentISTTime();
            
            // Check if message is too old (more than 5 minutes)
            if (messageTime.isBefore(currentTime.minusMinutes(5))) {
                log.debug("🕐 Message too old - Message time: {}, Current time: {}, Age: {} minutes", 
                        messageTime, currentTime, 
                        java.time.Duration.between(messageTime, currentTime).toMinutes());
                return false;
            }
            
            // Check if within trading hours - NO SPAM LOGGING
            if (!isWithinTradingHours(exchange, messageTime)) {
                log.debug("🚫 Message outside trading hours - Exchange: {}, Message time: {}", 
                        exchange, messageTime);
                return false;
            }
            
            return true;
            
        } catch (Exception e) {
            log.error("Error validating message for processing: {}", e.getMessage());
            return false;
        }
    }
    
    /**
     * Check if within NSE trading hours (9:15 AM - 3:30 PM)
     */
    private boolean isWithinNSEHours(LocalTime currentTime) {
        LocalTime start = LocalTime.parse(nseStartTime, TIME_FORMATTER);
        LocalTime end = LocalTime.parse(nseEndTime, TIME_FORMATTER);
        
        boolean withinHours = !currentTime.isBefore(start) && !currentTime.isAfter(end);
        
        if (!withinHours) {
            log.debug("Outside NSE trading hours: {} (Trading: {} - {})", 
                    currentTime, start, end);
        }
        
        return withinHours;
    }
    
    /**
     * Check if within MCX trading hours (9:00 AM - 11:30 PM)
     */
    private boolean isWithinMCXHours(LocalTime currentTime) {
        LocalTime start = LocalTime.parse(mcxStartTime, TIME_FORMATTER);
        LocalTime end = LocalTime.parse(mcxEndTime, TIME_FORMATTER);
        
        boolean withinHours = !currentTime.isBefore(start) && !currentTime.isAfter(end);
        
        if (!withinHours) {
            log.debug("Outside MCX trading hours: {} (Trading: {} - {})", 
                    currentTime, start, end);
        }
        
        return withinHours;
    }
    
    /**
     * Get current IST time
     */
    public LocalDateTime getCurrentISTTime() {
        return LocalDateTime.now(ZoneId.of(timezone));
    }
    
    /**
     * Get time until market opens for given exchange
     */
    public String getTimeUntilMarketOpen(String exchange) {
        try {
            LocalDateTime currentTime = getCurrentISTTime();
            LocalTime currentTimeOfDay = currentTime.toLocalTime();
            
            LocalTime marketStart;
            if ("N".equalsIgnoreCase(exchange) || "NSE".equalsIgnoreCase(exchange)) {
                marketStart = LocalTime.parse(nseStartTime, TIME_FORMATTER);
            } else {
                marketStart = LocalTime.parse(mcxStartTime, TIME_FORMATTER);
            }
            
            if (currentTimeOfDay.isBefore(marketStart)) {
                long minutesUntilOpen = java.time.Duration.between(currentTimeOfDay, marketStart).toMinutes();
                return String.format("%d minutes", minutesUntilOpen);
            } else {
                return "Market is open or closed for the day";
            }
            
        } catch (Exception e) {
            log.error("Error calculating time until market open: {}", e.getMessage());
            return "Unknown";
        }
    }
    
    /**
     * Check if it's a weekend (Saturday/Sunday)
     */
    public boolean isWeekend() {
        LocalDateTime currentTime = getCurrentISTTime();
        int dayOfWeek = currentTime.getDayOfWeek().getValue();
        return dayOfWeek == 6 || dayOfWeek == 7; // Saturday or Sunday
    }
    
    /**
     * Comprehensive validation for trade processing
     */
    public boolean shouldProcessTrade(String exchange, LocalDateTime messageTime) {
        // Don't process on weekends
        if (isWeekend()) {
            log.warn("🚫 Weekend detected - not processing trades");
            return false;
        }
        
        // Validate message timing and trading hours
        return isValidForProcessing(exchange, messageTime);
    }
    
    /**
     * Log trading hours status
     */
    public void logTradingHoursStatus() {
        LocalDateTime currentTime = getCurrentISTTime();
        
        log.info("🕐 Trading Hours Status at {}:", currentTime);
        log.info("📈 NSE Trading Hours: {} - {} (Status: {})", 
                nseStartTime, nseEndTime, 
                isWithinTradingHours("NSE") ? "OPEN 🟢" : "CLOSED 🔴");
        log.info("🏷️ MCX Trading Hours: {} - {} (Status: {})", 
                mcxStartTime, mcxEndTime, 
                isWithinTradingHours("MCX") ? "OPEN 🟢" : "CLOSED 🔴");
        
        if (isWeekend()) {
            log.info("📅 Weekend Status: WEEKEND 🚫");
        }
    }
} 