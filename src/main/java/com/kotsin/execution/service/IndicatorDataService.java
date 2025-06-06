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
 * Redis Key Format: scripCode (e.g., "53213") contains IndicatorState with all timeframes
 */
@Service
@Slf4j
@RequiredArgsConstructor
public class IndicatorDataService {
    
    private final RedisTemplate<String, String> redisTemplate;
    private final ObjectMapper objectMapper;
    
    /**
     * Fetch current indicators for a script code and timeframe from IndicatorState
     */
    public Map<String, Object> getCurrentIndicators(String scripCode, String timeframe) {
        try {
            log.debug("🔍 [IndicatorService] Fetching indicators for scripCode: {}, timeframe: {}", scripCode, timeframe);
            
            // Use scripCode as Redis key (same as indicatorAgg module)
            String redisKey = scripCode;
            String indicatorStateJson = redisTemplate.opsForValue().get(redisKey);
            
            if (indicatorStateJson != null) {
                log.debug("📊 [IndicatorService] Found Redis data for {}, parsing IndicatorState...", scripCode);
                
                // Parse the IndicatorState JSON
                Map<String, Object> indicatorState = objectMapper.readValue(indicatorStateJson, Map.class);
                
                // Extract the specific timeframe data
                Map<String, Object> timeframeData = extractTimeframeData(indicatorState, timeframe);
                
                if (!timeframeData.isEmpty()) {
                    log.info("✅ [IndicatorService] Found {} indicators for {}:{} - ST={}, BB=[{}-{}], Close={}", 
                            timeframeData.size(), scripCode, timeframe,
                            timeframeData.get("supertrend"),
                            timeframeData.get("bbLower"), timeframeData.get("bbUpper"),
                            timeframeData.get("closePrice"));
                } else {
                    log.warn("⚠️ [IndicatorService] No {} timeframe data found in IndicatorState for {}", timeframe, scripCode);
                    log.debug("🔍 [IndicatorService] Available timeframes in state: {}", getAvailableTimeframes(indicatorState));
                }
                
                return timeframeData;
            } else {
                log.warn("❌ [IndicatorService] No Redis data found for scripCode: {} (Key: {})", scripCode, redisKey);
                log.debug("🔍 [IndicatorService] Checking if key exists in Redis...");
                
                boolean keyExists = redisTemplate.hasKey(redisKey);
                log.debug("🔍 [IndicatorService] Redis key '{}' exists: {}", redisKey, keyExists);
                
                // Try to list some keys for debugging
                var keys = redisTemplate.keys("*");
                if (keys != null && !keys.isEmpty()) {
                    log.debug("🔍 [IndicatorService] Sample Redis keys available: {}", 
                            keys.stream().limit(5).toList());
                } else {
                    log.warn("⚠️ [IndicatorService] No Redis keys found at all - Redis connection issue?");
                }
                
                return new HashMap<>();
            }
        } catch (Exception e) {
            log.error("🚨 [IndicatorService] Error fetching indicators for {} ({}): {}", 
                    scripCode, timeframe, e.getMessage(), e);
            return new HashMap<>();
        }
    }
    
    /**
     * Extract specific timeframe data from IndicatorState
     */
    private Map<String, Object> extractTimeframeData(Map<String, Object> indicatorState, String timeframe) {
        try {
            // Map timeframe to IndicatorState field names
            String fieldName = mapTimeframeToField(timeframe);
            
            if (fieldName != null && indicatorState.containsKey(fieldName)) {
                Object timeframeObj = indicatorState.get(fieldName);
                if (timeframeObj instanceof Map) {
                    return (Map<String, Object>) timeframeObj;
                } else {
                    log.warn("⚠️ [IndicatorService] Timeframe data for {} is not a Map: {}", 
                            fieldName, timeframeObj != null ? timeframeObj.getClass() : "null");
                }
            } else {
                log.debug("🔍 [IndicatorService] Field '{}' not found in IndicatorState for timeframe '{}'", 
                        fieldName, timeframe);
            }
            
            return new HashMap<>();
        } catch (Exception e) {
            log.error("🚨 [IndicatorService] Error extracting timeframe data for {}: {}", timeframe, e.getMessage());
            return new HashMap<>();
        }
    }
    
    /**
     * Map timeframe string to IndicatorState field names
     */
    private String mapTimeframeToField(String timeframe) {
        return switch (timeframe.toLowerCase()) {
            case "1m" -> "oneMinute";
            case "2m" -> "twoMinute";
            case "3m" -> "threeMinute";
            case "5m" -> "fiveMinute";
            case "15m" -> "fifteenMinute";
            case "30m" -> "thirtyMinute";
            default -> {
                log.warn("⚠️ [IndicatorService] Unknown timeframe: {}", timeframe);
                yield null;
            }
        };
    }
    
    /**
     * Get available timeframes from IndicatorState for debugging
     */
    private String getAvailableTimeframes(Map<String, Object> indicatorState) {
        StringBuilder available = new StringBuilder();
        String[] possibleFields = {"oneMinute", "twoMinute", "threeMinute", "fiveMinute", "fifteenMinute", "thirtyMinute"};
        
        for (String field : possibleFields) {
            if (indicatorState.containsKey(field) && indicatorState.get(field) != null) {
                if (available.length() > 0) available.append(", ");
                available.append(field);
            }
        }
        
        return available.toString();
    }
    
    /**
     * Fetch comprehensive indicator state for all timeframes
     */
    public Map<String, Object> getComprehensiveIndicators(String scripCode) {
        Map<String, Object> allIndicators = new HashMap<>();
        
        log.debug("🔍 [IndicatorService] Fetching comprehensive indicators for {}", scripCode);
        
        String[] timeframes = {"1m", "2m", "3m", "5m", "15m", "30m"};
        int foundTimeframes = 0;
        
        for (String timeframe : timeframes) {
            Map<String, Object> indicators = getCurrentIndicators(scripCode, timeframe);
            if (!indicators.isEmpty()) {
                allIndicators.put(timeframe, indicators);
                foundTimeframes++;
            }
        }
        
        log.info("📊 [IndicatorService] Comprehensive indicators for {}: Found {}/{} timeframes", 
                scripCode, foundTimeframes, timeframes.length);
        
        if (foundTimeframes == 0) {
            log.warn("❌ [IndicatorService] No indicator data found for ANY timeframe for scripCode: {}", scripCode);
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
            log.error("🚨 [IndicatorService] Error extracting key indicators: {}", e.getMessage());
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