package com.kotsin.execution.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.time.*;

/**
 * Market-hours guard for NSE/BSE/MCX/Currency exchanges.
 *
 * Trading Hours:
 * - NSE/BSE: 09:15–15:30 IST, Mon–Fri
 * - MCX: 09:00–23:30 IST, Mon–Fri (includes evening session)
 * - Currency (CDS): 09:00–17:00 IST, Mon–Fri
 */
@Service
@Slf4j
public class TradingHoursService {

    private static final ZoneId IST = ZoneId.of("Asia/Kolkata");

    // NSE/BSE trading hours
    private static final LocalTime NSE_OPEN = LocalTime.of(9, 15);
    private static final LocalTime NSE_CLOSE = LocalTime.of(15, 30);

    // MCX trading hours (includes evening session until 11:30 PM)
    private static final LocalTime MCX_OPEN = LocalTime.of(9, 0);
    private static final LocalTime MCX_CLOSE = LocalTime.of(23, 30);

    // Currency (CDS) trading hours
    private static final LocalTime CDS_OPEN = LocalTime.of(9, 0);
    private static final LocalTime CDS_CLOSE = LocalTime.of(17, 0);

    /**
     * Check if trading is allowed for the given exchange at the specified time.
     *
     * @param exchange Exchange code: "N"/"NSE", "B"/"BSE", "M"/"MCX", "C"/"Currency"
     * @param receivedIst The time to check in IST
     * @return true if trading is allowed
     */
    public boolean shouldProcessTrade(String exchange, LocalDateTime receivedIst) {
        DayOfWeek dow = receivedIst.getDayOfWeek();
        if (dow == DayOfWeek.SATURDAY || dow == DayOfWeek.SUNDAY) {
            log.debug("trade_rejected_weekend exchange={} day={}", exchange, dow);
            return false;
        }

        LocalTime t = receivedIst.toLocalTime();

        // MCX has extended hours (9:00 AM - 11:30 PM)
        if (isMCX(exchange)) {
            boolean allowed = !t.isBefore(MCX_OPEN) && !t.isAfter(MCX_CLOSE);
            if (!allowed) {
                log.debug("mcx_outside_hours time={} open={} close={}", t, MCX_OPEN, MCX_CLOSE);
            }
            return allowed;
        }

        // Currency (CDS) hours (9:00 AM - 5:00 PM)
        if (isCurrency(exchange)) {
            boolean allowed = !t.isBefore(CDS_OPEN) && !t.isAfter(CDS_CLOSE);
            if (!allowed) {
                log.debug("cds_outside_hours time={} open={} close={}", t, CDS_OPEN, CDS_CLOSE);
            }
            return allowed;
        }

        // NSE/BSE default hours (9:15 AM - 3:30 PM)
        boolean allowed = !t.isBefore(NSE_OPEN) && !t.isAfter(NSE_CLOSE);
        if (!allowed) {
            log.debug("nse_outside_hours exchange={} time={} open={} close={}", exchange, t, NSE_OPEN, NSE_CLOSE);
        }
        return allowed;
    }

    private boolean isMCX(String exchange) {
        if (exchange == null) return false;
        String upper = exchange.toUpperCase();
        return "M".equals(upper) || "MCX".equals(upper);
    }

    private boolean isCurrency(String exchange) {
        if (exchange == null) return false;
        String upper = exchange.toUpperCase();
        return "C".equals(upper);
    }

    public boolean isMarketOpenNow() {
        ZonedDateTime now = ZonedDateTime.now(IST);
        return shouldProcessTrade("N", now.toLocalDateTime());
    }

    public boolean isMCXOpenNow() {
        ZonedDateTime now = ZonedDateTime.now(IST);
        return shouldProcessTrade("M", now.toLocalDateTime());
    }
}
