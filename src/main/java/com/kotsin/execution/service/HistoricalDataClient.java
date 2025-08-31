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

    public List<Candlestick> getHistorical1MinCandles(String scripCode, String isoDate, String exchange, String exchangeType) {
        try {
            HttpUrl url = HttpUrl.parse(baseUrl)
                    .newBuilder()
                    .addPathSegments("getHisDataFromFivePaisa")
                    .addQueryParameter("scripCode", scripCode)
                    .addQueryParameter("date", isoDate)
                    .addQueryParameter("exch", exchange)
                    .addQueryParameter("exchType", exchangeType)
                    .build();
            Request req = new Request.Builder().url(url).get().build();
            try (Response resp = http.newCall(req).execute()) {
                if (!resp.isSuccessful() || resp.body() == null) {
                    log.warn("HistoricalDataClient non-200/empty for {} {}", scripCode, isoDate);
                    return Collections.emptyList();
                }
                return mapper.readValue(resp.body().byteStream(), new TypeReference<List<Candlestick>>(){});
            }
        } catch (IOException e) {
            log.error("HistoricalDataClient error for {} {}: {}", scripCode, isoDate, e.toString());
            return Collections.emptyList();
        }
    }
}
