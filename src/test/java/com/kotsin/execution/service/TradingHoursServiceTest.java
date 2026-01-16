package com.kotsin.execution.service;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.time.LocalDateTime;
import java.time.DayOfWeek;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for TradingHoursService
 * Tests exchange-specific trading hours for NSE, BSE, and MCX
 */
class TradingHoursServiceTest {

    private final TradingHoursService service = new TradingHoursService();

    // ======================== MCX TESTS ========================

    @Test
    @DisplayName("MCX should accept trades at 8:56 PM on weekday")
    void testMCXEveningHours() {
        // 8:56 PM IST on Thursday (within MCX hours)
        LocalDateTime mcxEvening = LocalDateTime.of(2026, 1, 16, 20, 56);
        
        assertTrue(service.shouldProcessTrade("M", mcxEvening), 
                   "MCX should accept trades at 8:56 PM");
        assertTrue(service.shouldProcessTrade("MCX", mcxEvening),
                   "MCX (full name) should accept trades at 8:56 PM");
    }

    @Test
    @DisplayName("MCX should accept trades at 11:00 PM")
    void testMCXLateEvening() {
        LocalDateTime lateEvening = LocalDateTime.of(2026, 1, 16, 23, 0);
        assertTrue(service.shouldProcessTrade("M", lateEvening),
                   "MCX should accept trades at 11:00 PM");
    }

    @Test
    @DisplayName("MCX should reject trades after 11:30 PM")
    void testMCXClosedAfterMidnight() {
        LocalDateTime afterClose = LocalDateTime.of(2026, 1, 16, 23, 45);
        assertFalse(service.shouldProcessTrade("M", afterClose),
                    "MCX should reject trades at 11:45 PM");
    }

    @Test
    @DisplayName("MCX should accept trades at 9:00 AM morning open")
    void testMCXMorningOpen() {
        LocalDateTime morningOpen = LocalDateTime.of(2026, 1, 16, 9, 0);
        assertTrue(service.shouldProcessTrade("M", morningOpen),
                   "MCX should accept trades at 9:00 AM");
    }

    @Test
    @DisplayName("MCX should reject trades before 9:00 AM")
    void testMCXBeforeOpen() {
        LocalDateTime beforeOpen = LocalDateTime.of(2026, 1, 16, 8, 45);
        assertFalse(service.shouldProcessTrade("M", beforeOpen),
                    "MCX should reject trades at 8:45 AM");
    }

    // ======================== NSE TESTS ========================

    @Test
    @DisplayName("NSE should reject trades at 8:56 PM")
    void testNSEEveningRejected() {
        LocalDateTime evening = LocalDateTime.of(2026, 1, 16, 20, 56);
        
        assertFalse(service.shouldProcessTrade("N", evening),
                    "NSE should reject trades at 8:56 PM");
        assertFalse(service.shouldProcessTrade("NSE", evening),
                    "NSE (full name) should reject trades at 8:56 PM");
    }

    @Test
    @DisplayName("NSE should accept trades during market hours")
    void testNSEMarketHours() {
        LocalDateTime duringHours = LocalDateTime.of(2026, 1, 16, 12, 0);
        assertTrue(service.shouldProcessTrade("N", duringHours),
                   "NSE should accept trades at 12:00 PM");
    }

    @Test
    @DisplayName("NSE should reject trades after 3:30 PM")
    void testNSEAfterClose() {
        LocalDateTime afterClose = LocalDateTime.of(2026, 1, 16, 15, 45);
        assertFalse(service.shouldProcessTrade("N", afterClose),
                    "NSE should reject trades at 3:45 PM");
    }

    // ======================== WEEKEND TESTS ========================

    @Test
    @DisplayName("All exchanges should reject trades on weekends")
    void testWeekendRejected() {
        LocalDateTime saturday = LocalDateTime.of(2026, 1, 17, 12, 0);  // Saturday
        LocalDateTime sunday = LocalDateTime.of(2026, 1, 18, 12, 0);    // Sunday
        
        assertFalse(service.shouldProcessTrade("N", saturday), "NSE should reject Saturday");
        assertFalse(service.shouldProcessTrade("M", saturday), "MCX should reject Saturday");
        assertFalse(service.shouldProcessTrade("N", sunday), "NSE should reject Sunday");
        assertFalse(service.shouldProcessTrade("M", sunday), "MCX should reject Sunday");
    }

    // ======================== PARAMETERIZED TESTS ========================

    @ParameterizedTest
    @CsvSource({
        // MCX evening hours should pass
        "M, 20, 30, true",   // 8:30 PM
        "M, 21, 0, true",    // 9:00 PM
        "M, 22, 30, true",   // 10:30 PM
        "M, 23, 30, true",   // 11:30 PM (edge)
        "M, 23, 31, false",  // 11:31 PM (after close)
        
        // NSE should never pass evening hours
        "N, 15, 30, true",   // 3:30 PM (edge)
        "N, 15, 31, false",  // 3:31 PM (after close)
        "N, 20, 30, false",  // 8:30 PM
    })
    @DisplayName("Exchange-specific hours boundary tests")
    void testExchangeHoursBoundaries(String exchange, int hour, int minute, boolean expected) {
        LocalDateTime time = LocalDateTime.of(2026, 1, 16, hour, minute);
        assertEquals(expected, service.shouldProcessTrade(exchange, time),
                String.format("Exchange %s at %02d:%02d should be %s", 
                              exchange, hour, minute, expected ? "allowed" : "rejected"));
    }
}
