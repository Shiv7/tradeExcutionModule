package com.kotsin.execution.virtual;

import com.fasterxml.jackson.databind.JsonNode;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
@Slf4j
public class PriceProvider {
    private final OkHttpClient http = new OkHttpClient();

    @Value("${execution.virtual.price-api-base:http://localhost:8208}")
    private String priceApiBase;

    public Double getLtp(String scripCode){
        try {
            Request req = new Request.Builder().url(priceApiBase+"/api/price/"+scripCode).build();
            try (Response r = http.newCall(req).execute()){
                if (!r.isSuccessful() || r.body()==null) return null;
                String raw = r.body().string();
                JsonNode node = new com.fasterxml.jackson.databind.ObjectMapper().readTree(raw);
                if (node.has("lastRate")) return node.get("lastRate").asDouble();
                return null;
            }
        } catch (Exception e){
            log.debug("Price fetch failed for {}: {}", scripCode, e.getMessage());
            return null;
        }
    }
}

