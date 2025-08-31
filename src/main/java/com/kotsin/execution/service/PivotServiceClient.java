package com.kotsin.execution.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kotsin.execution.model.PivotData;
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

@Service
@Slf4j
@RequiredArgsConstructor
public class PivotServiceClient {

    private final OkHttpClient httpClient = new OkHttpClient();
    private final ObjectMapper mapper = new ObjectMapper();

    @Value("${pivot.base-url:http://localhost:8103}")
    private String baseUrl;

    public PivotData getDailyPivots(String scripCode, LocalDate date) {
        try {
            HttpUrl url = HttpUrl.parse(baseUrl)
                    .newBuilder()
                    .addPathSegments("pivotGetter/getDailyPivotForGivenDateAndTicker")
                    .addQueryParameter("date", date.toString())
                    .addQueryParameter("ticker", scripCode)
                    .build();
            Request req = new Request.Builder().url(url).get().build();
            try (Response resp = httpClient.newCall(req).execute()) {
                if (!resp.isSuccessful() || resp.body() == null) {
                    log.warn("PivotClient: non-200 or empty body for {} {}", scripCode, date);
                    return null;
                }
                JsonNode n = mapper.readTree(resp.body().byteStream());
                PivotData pd = new PivotData();
                pd.setScripCode(scripCode);
                pd.setDate(date);
                pd.setPivot(n.path("pivot").asDouble(0));
                pd.setR1(n.path("r1").asDouble(0));
                pd.setR2(n.path("r2").asDouble(0));
                pd.setR3(n.path("r3").asDouble(0));
                pd.setR4(n.path("r4").asDouble(0));
                pd.setS1(n.path("s1").asDouble(0));
                pd.setS2(n.path("s2").asDouble(0));
                pd.setS3(n.path("s3").asDouble(0));
                pd.setS4(n.path("s4").asDouble(0));
                return pd;
            }
        } catch (IOException e) {
            log.error("PivotClient error for {} {}: {}", scripCode, date, e.toString());
            return null;
        }
    }
}
