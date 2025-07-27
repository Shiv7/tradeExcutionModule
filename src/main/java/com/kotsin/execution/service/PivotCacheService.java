package com.kotsin.execution.service;

import com.kotsin.execution.model.PivotData;
import lombok.RequiredArgsConstructor;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class PivotCacheService {

    private final PivotServiceClient pivotServiceClient;

    @Cacheable(value = "dailyPivots_v2", key = "#scripCode")
    public PivotData getDailyPivots(String scripCode) {
        return pivotServiceClient.getDailyPivots(scripCode);
    }
}
