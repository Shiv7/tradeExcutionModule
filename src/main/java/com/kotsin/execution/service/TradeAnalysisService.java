package com.kotsin.execution.service;

import com.kotsin.execution.model.Candlestick;
import org.springframework.stereotype.Service;
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
        if (isEngulfing) log.info("Bullish Engulfing pattern detected for {}.", current.getCompanyName());
        return isEngulfing;
    }

    // Checks for a bearish engulfing pattern
    public boolean isBearishEngulfing(Candlestick previous, Candlestick current) {
        if (previous == null || current == null) return false;
        boolean isEngulfing = current.getOpen() > previous.getClose() && current.getClose() < previous.getOpen();
        if (isEngulfing) log.info("Bearish Engulfing pattern detected for {}.", current.getCompanyName());
        return isEngulfing;
    }

    // Confirms if the volume profile supports the trade
    public boolean confirmVolumeProfile(Candlestick current, List<Candlestick> recentCandles) {
        if (recentCandles == null || recentCandles.isEmpty()) return true; // Not enough data, default to true
        double averageVolume = recentCandles.stream().mapToLong(Candlestick::getVolume).average().orElse(0.0);
        boolean isSpike = current.getVolume() > averageVolume * VOLUME_SPIKE_FACTOR;
        if (isSpike) log.info("Volume spike confirmed for {}. Current: {}, Average: {}", current.getCompanyName(), current.getVolume(), averageVolume);
        return isSpike;
    }

    // Checks if the trade is within the golden windows
    public boolean isWithinGoldenWindows() {
        java.time.LocalTime now = java.time.LocalTime.now(java.time.ZoneId.of("Asia/Kolkata"));
        java.time.LocalTime morningStart = java.time.LocalTime.of(9, 30);
        java.time.LocalTime morningEnd = java.time.LocalTime.of(11, 30);
        java.time.LocalTime afternoonStart = java.time.LocalTime.of(13, 30);
        java.time.LocalTime afternoonEnd = java.time.LocalTime.of(15, 30);

        boolean inMorningSession = !now.isBefore(morningStart) && now.isBefore(morningEnd);
        boolean inAfternoonSession = !now.isBefore(afternoonStart) && now.isBefore(afternoonEnd);
        
        return inMorningSession || inAfternoonSession;
    }
}
