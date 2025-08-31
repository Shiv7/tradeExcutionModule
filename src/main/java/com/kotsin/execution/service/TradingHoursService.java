package com.kotsin.execution.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.time.*;
import java.util.Set;

/**
 * Basic market-hours guard for NSE/BSE: 09:15–15:30 IST, Mon–Fri.
 * Extend with a holiday calendar if needed.
 */
@Service
@Slf4j
public class TradingHoursService {

    private static final ZoneId IST = ZoneId.of("Asia/Kolkata");
    private static final LocalTime OPEN = LocalTime.of(9, 15);
    private static final LocalTime CLOSE = LocalTime.of(15, 30);

    public boolean shouldProcessTrade(String exchange, LocalDateTime receivedIst) {
        DayOfWeek dow = receivedIst.getDayOfWeek();
        if (dow == DayOfWeek.SATURDAY || dow == DayOfWeek.SUNDAY) return false;
        LocalTime t = receivedIst.toLocalTime();
        return !t.isBefore(OPEN) && !t.isAfter(CLOSE);
    }

    public boolean isMarketOpenNow() {
        ZonedDateTime now = ZonedDateTime.now(IST);
        return shouldProcessTrade("N", now.toLocalDateTime());
    }
}
