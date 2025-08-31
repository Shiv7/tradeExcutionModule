package com.kotsin.execution.service;

import com.kotsin.execution.model.Candlestick;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * Basic TA helpers used by TradeManager.
 * Keep logic simple & deterministic.
 */
@Service
@Slf4j
public class TradeAnalysisService {

    /** Golden windows: e.g., first hour and power hour. */
    public boolean isWithinGoldenWindows(long windowStartMillis) {
        // Hook for time-based filtering; keep always true for now.
        return true;
    }

    /** Simple volume confirmation: current volume > avg of last N (e.g., 5). */
    public boolean confirmVolumeProfile(Candlestick curr, List<Candlestick> history) {
        if (curr == null) return false;
        if (history == null || history.isEmpty()) return true; // allow if no history
        int n = Math.min(5, history.size());
        long sum = 0;
        for (int i = history.size() - n; i < history.size(); i++) {
            sum += Math.max(0, history.get(i).getVolume());
        }
        double avg = sum / (double)n;
        return curr.getVolume() >= avg;
    }

    /** Classic bullish engulfing: current body engulfs previous body and closes up. */
    public boolean isBullishEngulfing(Candlestick prev, Candlestick curr) {
        if (prev == null || curr == null) return false;
        double prevOpen = prev.getOpen(), prevClose = prev.getClose();
        double currOpen = curr.getOpen(), currClose = curr.getClose();
        boolean prevRed = prevClose < prevOpen;
        boolean currGreen = currClose > currOpen;
        boolean bodyEngulfs = Math.min(currOpen, currClose) <= Math.min(prevOpen, prevClose)
                && Math.max(currOpen, currClose) >= Math.max(prevOpen, prevClose);
        return prevRed && currGreen && bodyEngulfs;
    }

    /** Classic bearish engulfing. */
    public boolean isBearishEngulfing(Candlestick prev, Candlestick curr) {
        if (prev == null || curr == null) return false;
        double prevOpen = prev.getOpen(), prevClose = prev.getClose();
        double currOpen = curr.getOpen(), currClose = curr.getClose();
        boolean prevGreen = prevClose > prevOpen;
        boolean currRed = currClose < currOpen;
        boolean bodyEngulfs = Math.min(currOpen, currClose) <= Math.min(prevOpen, prevClose)
                && Math.max(currOpen, currClose) >= Math.max(prevOpen, prevClose);
        return prevGreen && currRed && bodyEngulfs;
    }
}
