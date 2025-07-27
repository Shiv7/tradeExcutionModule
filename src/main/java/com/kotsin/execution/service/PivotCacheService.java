package com.kotsin.execution.service;

import com.kotsin.execution.model.PivotData;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

import java.time.LocalDate;
import java.util.concurrent.ConcurrentHashMap;
import java.util.Map;

@Service
@RequiredArgsConstructor
public class PivotCacheService {

    private final PivotServiceClient pivotServiceClient;
    private final Map<String, PivotData> dailyPivotsCache = new ConcurrentHashMap<>();

    public PivotData getDailyPivots(String scripCode, LocalDate date) {
        String cacheKey = scripCode + "_" + date.toString();
        return dailyPivotsCache.computeIfAbsent(cacheKey, k -> pivotServiceClient.getDailyPivots(scripCode, date));
    }
}
