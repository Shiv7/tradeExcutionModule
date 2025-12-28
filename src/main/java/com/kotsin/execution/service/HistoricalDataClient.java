package com.kotsin.execution.service;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kotsin.execution.model.Candlestick;
import lombok.extern.slf4j.Slf4j;
import okhttp3.HttpUrl;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.time.Duration;
import java.time.LocalDate;
import java.util.Collections;
import java.util.List;

@Service
@Slf4j
public class HistoricalDataClient {

    private final OkHttpClient http = new OkHttpClient();
    private final ObjectMapper mapper = new ObjectMapper();

    @Autowired(required = false)
    private StringRedisTemplate redisTemplate;

    @Value("${history.base-url:http://localhost:8002}")
    private String baseUrl;
    
    private static final Duration CACHE_TTL = Duration.ofHours(24);

    /**
     * Fetch historical 1-min candles for a date range
     * Uses Redis cache to avoid repeated API calls
     */
    public List<Candlestick> getHistoricalCandles(String scripCode, LocalDate startDate, LocalDate endDate,
                                                   String exchange, String exchangeType) {
        // Cap end date to tomorrow
        LocalDate tomorrow = LocalDate.now().plusDays(1);
        if (endDate.isAfter(tomorrow)) {
            endDate = tomorrow;
        }
        
        // Ensure start is not after end
        if (startDate.isAfter(endDate)) {
            log.warn("Start date {} is after end date {} for {}", startDate, endDate, scripCode);
            return Collections.emptyList();
        }
        
        // Try cache first
        String cacheKey = buildCacheKey(scripCode, startDate, endDate);
        List<Candlestick> cached = getFromCache(cacheKey);
        if (cached != null) {
            log.debug("Cache HIT for {} ({} candles)", cacheKey, cached.size());
            return cached;
        }
        
        // For the API, end_date needs to be +1 day to include the last day's data
        LocalDate apiEndDate = endDate.plusDays(1);
        
        try {
            HttpUrl url = HttpUrl.parse(baseUrl)
                    .newBuilder()
                    .addPathSegments("getHisDataFromFivePaisa")
                    .addQueryParameter("scrip_code", scripCode)
                    .addQueryParameter("start_date", startDate.toString())
                    .addQueryParameter("end_date", apiEndDate.toString())
                    .addQueryParameter("exch", exchange.toLowerCase())
                    .addQueryParameter("exch_type", exchangeType.toLowerCase())
                    .addQueryParameter("interval", "1m")
                    .build();
            
            log.debug("Fetching historical candles: {}", url);
            Request req = new Request.Builder().url(url).get().build();
            
            try (Response resp = http.newCall(req).execute()) {
                if (!resp.isSuccessful() || resp.body() == null) {
                    log.warn("HistoricalDataClient non-200/empty for {} {} to {}: status={}",
                            scripCode, startDate, endDate, resp.code());
                    return Collections.emptyList();
                }
                List<Candlestick> candles = mapper.readValue(resp.body().byteStream(), 
                        new TypeReference<List<Candlestick>>(){});
                log.info("Fetched {} candles for {} from {} to {}", 
                        candles.size(), scripCode, startDate, endDate);
                
                // Cache the result
                if (!candles.isEmpty()) {
                    saveToCache(cacheKey, candles);
                }
                
                return candles;
            }
        } catch (IOException e) {
            log.error("HistoricalDataClient error for {} {} to {}: {}", 
                    scripCode, startDate, endDate, e.toString());
            return Collections.emptyList();
        }
    }

    /**
     * Legacy single-date method (for backward compatibility)
     */
    public List<Candlestick> getHistorical1MinCandles(String scripCode, String isoDate, 
                                                       String exchange, String exchangeType) {
        LocalDate date = LocalDate.parse(isoDate);
        return getHistoricalCandles(scripCode, date, date, exchange, exchangeType);
    }
    
    // ========== Redis Cache Methods ==========
    
    private String buildCacheKey(String scripCode, LocalDate start, LocalDate end) {
        return "candles:" + scripCode + ":" + start + ":" + end;
    }
    
    private List<Candlestick> getFromCache(String key) {
        if (redisTemplate == null) return null;
        
        try {
            String json = redisTemplate.opsForValue().get(key);
            if (json != null && !json.isEmpty()) {
                return mapper.readValue(json, new TypeReference<List<Candlestick>>(){});
            }
        } catch (Exception e) {
            log.debug("Cache read error for {}: {}", key, e.getMessage());
        }
        return null;
    }
    
    private void saveToCache(String key, List<Candlestick> candles) {
        if (redisTemplate == null) return;
        
        try {
            String json = mapper.writeValueAsString(candles);
            redisTemplate.opsForValue().set(key, json, CACHE_TTL);
            log.debug("Cached {} candles for key {}", candles.size(), key);
        } catch (Exception e) {
            log.debug("Cache write error for {}: {}", key, e.getMessage());
        }
    }
}

