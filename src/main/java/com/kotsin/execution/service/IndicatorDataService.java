package com.kotsin.execution.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Service;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.HashMap;
import java.util.Map;

/**
 * Service to fetch current indicator data from Redis for trade decisions
 */
@Service
@Slf4j
@RequiredArgsConstructor
public class IndicatorDataService {
    
    private final RedisTemplate<String, String> redisTemplate;
    private final ObjectMapper objectMapper;
    
    /**
     * Fetch current indicators for a script code and timeframe
     */
    public Map<String, Object> getCurrentIndicators(String scripCode, String timeframe) {
        try {
            String redisKey = String.format("indicator:%s:%s", scripCode, timeframe);
            String indicatorJson = redisTemplate.opsForValue().get(redisKey);
            
            if (indicatorJson != null) {
                return objectMapper.readValue(indicatorJson, Map.class);
            } else {
                log.warn("No indicator data found in Redis for key: {}", redisKey);
                return new HashMap<>();
            }
        } catch (Exception e) {
            log.error("Error fetching indicators for {} ({}): {}", scripCode, timeframe, e.getMessage());
            return new HashMap<>();
        }
    }
    
    /**
     * Fetch comprehensive indicator state for all timeframes
     */
    public Map<String, Object> getComprehensiveIndicators(String scripCode) {
        Map<String, Object> allIndicators = new HashMap<>();
        
        String[] timeframes = {"1m", "2m", "3m", "5m", "15m", "30m"};
        
        for (String timeframe : timeframes) {
            Map<String, Object> indicators = getCurrentIndicators(scripCode, timeframe);
            if (!indicators.isEmpty()) {
                allIndicators.put(timeframe, indicators);
            }
        }
        
        return allIndicators;
    }
    
    /**
     * Extract specific indicator values for summary
     */
    public Map<String, Object> extractKeyIndicators(Map<String, Object> indicators) {
        Map<String, Object> keyIndicators = new HashMap<>();
        
        try {
            // SuperTrend data
            if (indicators.containsKey("supertrend")) {
                keyIndicators.put("supertrend", indicators.get("supertrend"));
            }
            if (indicators.containsKey("supertrendSignal")) {
                keyIndicators.put("supertrendSignal", indicators.get("supertrendSignal"));
            }
            if (indicators.containsKey("supertrendIsBullish")) {
                keyIndicators.put("supertrendIsBullish", indicators.get("supertrendIsBullish"));
            }
            
            // Bollinger Bands
            if (indicators.containsKey("bbUpper")) {
                keyIndicators.put("bbUpper", indicators.get("bbUpper"));
            }
            if (indicators.containsKey("bbMiddle")) {
                keyIndicators.put("bbMiddle", indicators.get("bbMiddle"));
            }
            if (indicators.containsKey("bbLower")) {
                keyIndicators.put("bbLower", indicators.get("bbLower"));
            }
            
            // Moving Averages
            if (indicators.containsKey("ema20")) {
                keyIndicators.put("ema20", indicators.get("ema20"));
            }
            if (indicators.containsKey("ema60")) {
                keyIndicators.put("ema60", indicators.get("ema60"));
            }
            if (indicators.containsKey("ema240")) {
                keyIndicators.put("ema240", indicators.get("ema240"));
            }
            
            // Volume indicators
            if (indicators.containsKey("vwap")) {
                keyIndicators.put("vwap", indicators.get("vwap"));
            }
            if (indicators.containsKey("lastCandleVolume")) {
                keyIndicators.put("lastCandleVolume", indicators.get("lastCandleVolume"));
            }
            if (indicators.containsKey("avgVolume")) {
                keyIndicators.put("avgVolume", indicators.get("avgVolume"));
            }
            
            // RSI
            if (indicators.containsKey("rsi")) {
                keyIndicators.put("rsi", indicators.get("rsi"));
            }
            
            // Current price context
            if (indicators.containsKey("closePrice")) {
                keyIndicators.put("closePrice", indicators.get("closePrice"));
            }
            if (indicators.containsKey("timestamp")) {
                keyIndicators.put("timestamp", indicators.get("timestamp"));
            }
            
        } catch (Exception e) {
            log.error("Error extracting key indicators: {}", e.getMessage());
        }
        
        return keyIndicators;
    }
    
    /**
     * Create a human-readable summary of indicators
     */
    public String createIndicatorSummary(Map<String, Object> indicators, String timeframe) {
        if (indicators.isEmpty()) {
            return String.format("No indicator data available for %s timeframe", timeframe);
        }
        
        StringBuilder summary = new StringBuilder();
        summary.append(String.format("[%s] ", timeframe));
        
        // SuperTrend
        Object supertrend = indicators.get("supertrend");
        Object supertrendSignal = indicators.get("supertrendSignal");
        Object isBullish = indicators.get("supertrendIsBullish");
        
        if (supertrend != null) {
            summary.append(String.format("ST: %.2f", ((Number) supertrend).doubleValue()));
            if (supertrendSignal != null && !supertrendSignal.toString().isEmpty()) {
                summary.append(String.format(" (%s)", supertrendSignal));
            }
            if (isBullish != null) {
                summary.append(String.format(" %s", ((Boolean) isBullish) ? "🟢" : "🔴"));
            }
            summary.append(" | ");
        }
        
        // Bollinger Bands
        Object bbUpper = indicators.get("bbUpper");
        Object bbLower = indicators.get("bbLower");
        Object closePrice = indicators.get("closePrice");
        
        if (bbUpper != null && bbLower != null && closePrice != null) {
            double upper = ((Number) bbUpper).doubleValue();
            double lower = ((Number) bbLower).doubleValue();
            double close = ((Number) closePrice).doubleValue();
            
            String bbPosition = "";
            if (close > upper) bbPosition = "Above BB 🔥";
            else if (close < lower) bbPosition = "Below BB ❄️";
            else bbPosition = "Inside BB";
            
            summary.append(String.format("BB: %.2f-%.2f (%s) | ", lower, upper, bbPosition));
        }
        
        // RSI
        Object rsi = indicators.get("rsi");
        if (rsi != null) {
            double rsiValue = ((Number) rsi).doubleValue();
            String rsiLevel = "";
            if (rsiValue > 70) rsiLevel = "Overbought 📈";
            else if (rsiValue < 30) rsiLevel = "Oversold 📉";
            else rsiLevel = "Neutral";
            
            summary.append(String.format("RSI: %.1f (%s) | ", rsiValue, rsiLevel));
        }
        
        // VWAP
        Object vwap = indicators.get("vwap");
        if (vwap != null && closePrice != null) {
            double vwapValue = ((Number) vwap).doubleValue();
            double close = ((Number) closePrice).doubleValue();
            String vwapPosition = close > vwapValue ? "Above VWAP ⬆️" : "Below VWAP ⬇️";
            
            summary.append(String.format("VWAP: %.2f (%s)", vwapValue, vwapPosition));
        }
        
        return summary.toString();
    }
} 