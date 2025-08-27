package com.kotsin.execution.service;

import com.kotsin.execution.model.Candlestick;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.time.LocalTime;
import java.time.ZoneId;
import java.util.List;
import lombok.extern.slf4j.Slf4j;

@Service
@Slf4j
public class TradeAnalysisService {

     private static final double VOLUME_SPIKE_FACTOR = 1.5;

    // Checks for a bullish engulfing pattern
    public boolean isBullishEngulfing(Candlestick previous, Candlestick current) {
        if (previous == null || current == null) return false;
        boolean isEngulfing = current.getOpen() < previous.getClose() && current.getClose() > previous.getOpen();
        log.info("Bullish Engulfing Check for {}: {}", current.getCompanyName(), isEngulfing);
        return isEngulfing;
    }

    // Checks for a bearish engulfing pattern
    public boolean isBearishEngulfing(Candlestick previous, Candlestick current) {
        if (previous == null || current == null) return false;
        boolean isEngulfing = current.getOpen() > previous.getClose() && current.getClose() < previous.getOpen();
        log.info("Bearish Engulfing Check for {}: {}", current.getCompanyName(), isEngulfing);
        return isEngulfing;
    }

    // Confirms if the volume profile supports the trade
    public boolean confirmVolumeProfile(Candlestick current, List<Candlestick> recentCandles) {
        if (recentCandles == null || recentCandles.isEmpty()) return true; // Not enough data, default to true
        double averageVolume = recentCandles.stream().mapToLong(Candlestick::getVolume).average().orElse(0.0);
        boolean isSpike = current.getVolume() > averageVolume * VOLUME_SPIKE_FACTOR;
        log.info("Volume Spike Check for {}: Current={}, Avg={}, Spike={}", current.getCompanyName(), current.getVolume(), averageVolume, isSpike);
        return isSpike;
    }

    // Overloaded method for simulation, using the candle's timestamp
    public boolean isWithinGoldenWindows(long timestampMillis) {
        LocalTime candleTime = Instant.ofEpochMilli(timestampMillis)
                                      .atZone(ZoneId.of("Asia/Kolkata"))
                                      .toLocalTime();
        return isWithinGoldenWindows(candleTime);
    }

    // Private helper method with the core logic
    private boolean isWithinGoldenWindows(LocalTime timeToCheck) {
        LocalTime morningStart = LocalTime.of(9, 30);
        LocalTime morningEnd = LocalTime.of(11, 30);
        LocalTime afternoonStart = LocalTime.of(13, 30);
        LocalTime afternoonEnd = LocalTime.of(15, 30);
        boolean inMorningSession = !timeToCheck.isBefore(morningStart) && timeToCheck.isBefore(morningEnd);
        boolean inAfternoonSession = !timeToCheck.isBefore(afternoonStart) && timeToCheck.isBefore(afternoonEnd);
        return inMorningSession || inAfternoonSession;
    }
}
