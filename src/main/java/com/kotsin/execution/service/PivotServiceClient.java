package com.kotsin.execution.service;

import com.kotsin.execution.model.PivotData;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;
import java.util.Map;
import org.springframework.cache.annotation.Cacheable;
import lombok.extern.slf4j.Slf4j;

@Service
@Slf4j
public class PivotServiceClient {

    private final RestTemplate restTemplate = new RestTemplate();
    private static final String PIVOT_API_URL = "http://localhost:8103/pivotGetter/getDailyPivotForGivenDateAndTicker?date={date}&exch=N&exch_type=C&scrip_code={scrip_code}";

    @Cacheable(value = "dailyPivotsFull", key = "#scripCode")
    public PivotData getDailyPivots(String scripCode) {
        try {
            String today = java.time.LocalDate.now().format(java.time.format.DateTimeFormatter.ofPattern("yyyy-MM-dd"));
            Map<String, Object> response = restTemplate.getForObject(PIVOT_API_URL, Map.class, today, scripCode);

            if (response != null && response.get("response") != null) {
                Map<String, Object> responseData = (Map<String, Object>) response.get("response");
                Map<String, Object> pivotDataMap = (Map<String, Object>) responseData.get("pivotIndicatorData");
                if (pivotDataMap != null) {
                    log.info("Fetched full daily pivot data for {}", scripCode);
                    return PivotData.fromMap(pivotDataMap);
                }
            }
        } catch (Exception e) {
            log.error("Failed to fetch full daily pivot data for {}: {}", scripCode, e.getMessage());
        }
        return null;
    }

    public Double getDailyPivot(String scripCode) {
        PivotData pivotData = getDailyPivots(scripCode);
        return pivotData != null ? pivotData.getPivot() : null;
    }
}
