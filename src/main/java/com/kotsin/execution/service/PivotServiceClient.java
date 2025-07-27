package com.kotsin.execution.service;

import com.kotsin.execution.model.PivotData;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;

@Service
@Slf4j
public class PivotServiceClient {

    private final RestTemplate restTemplate = new RestTemplate();
    private static final String PIVOT_API_URL = "http://localhost:8103/pivotGetter/getDailyPivotForGivenDateAndTicker?date={date}&exch=N&exch_type=C&scrip_code={scrip_code}";

    public PivotData getDailyPivots(String scripCode, LocalDate date) {
        try {
            String formattedDate = date.format(DateTimeFormatter.ofPattern("yyyy-MM-dd"));
            Map<String, Object> response = restTemplate.getForObject(PIVOT_API_URL, Map.class, formattedDate, scripCode);

            if (response != null && response.get("response") != null) {
                Map<String, Object> responseData = (Map<String, Object>) response.get("response");
                Map<String, Object> pivotDataMap = (Map<String, Object>) responseData.get("pivotIndicatorData");
                if (pivotDataMap != null) {
                    log.info("Fetched full daily pivot data for {} on {}: {}", scripCode, formattedDate, pivotDataMap.keySet());
                    return PivotData.fromMap(pivotDataMap);
                }
            }
        } catch (Exception e) {
            log.error("Failed to fetch full daily pivot data for {} on {}: {}", scripCode, date, e.getMessage());
        }
        return null;
    }

    public Double getDailyPivot(String scripCode, LocalDate date) {
        PivotData pivotData = getDailyPivots(scripCode, date);
        return pivotData != null ? pivotData.getPivot() : null;
    }
}
