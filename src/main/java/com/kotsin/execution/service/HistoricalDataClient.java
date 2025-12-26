package com.kotsin.execution.service;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kotsin.execution.model.Candlestick;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import okhttp3.HttpUrl;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.time.LocalDate;
import java.util.Collections;
import java.util.List;

@Service
@Slf4j
@RequiredArgsConstructor
public class HistoricalDataClient {

    private final OkHttpClient http = new OkHttpClient();
    private final ObjectMapper mapper = new ObjectMapper();

    @Value("${history.base-url:http://localhost:8002}")
    private String baseUrl;

    /**
     * Fetch historical 1-min candles for a date range
     * API: GET /getHisDataFromFivePaisa?scripCode=X&startDate=YYYY-MM-DD&endDate=YYYY-MM-DD&exch=N&exchType=D
     */
    public List<Candlestick> getHistoricalCandles(String scripCode, LocalDate startDate, LocalDate endDate,
                                                   String exchange, String exchangeType) {
        // Cap end date to today (can't fetch future data)
        LocalDate today = LocalDate.now();
        if (endDate.isAfter(today)) {
            endDate = today;
        }
        
        // Ensure start is not after end
        if (startDate.isAfter(endDate)) {
            log.warn("Start date {} is after end date {} for {}", startDate, endDate, scripCode);
            return Collections.emptyList();
        }
        
        try {
            HttpUrl url = HttpUrl.parse(baseUrl)
                    .newBuilder()
                    .addPathSegments("getHisDataFromFivePaisa")
                    .addQueryParameter("scripCode", scripCode)
                    .addQueryParameter("startDate", startDate.toString())
                    .addQueryParameter("endDate", endDate.toString())
                    .addQueryParameter("exch", exchange)
                    .addQueryParameter("exchType", exchangeType)
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
}
