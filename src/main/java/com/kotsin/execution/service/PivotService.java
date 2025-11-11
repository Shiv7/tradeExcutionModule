package com.kotsin.execution.service;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.kotsin.execution.model.PivotData;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.time.LocalDate;
import java.util.Objects;

@Service
@Slf4j
@RequiredArgsConstructor
public class PivotService {

    private final PivotServiceClient pivotClient;
    private final Cache<String, PivotData> cache = Caffeine.newBuilder()
            .maximumSize(20_000)
            .build();

    public PivotData getDailyPivots(String scripCode, LocalDate date) {
        Objects.requireNonNull(scripCode, "scripCode");
        Objects.requireNonNull(date, "date");
        final String key = scripCode + "|" + date;
        PivotData val = cache.getIfPresent(key);
        if (val != null) return val;
        PivotData fetched = pivotClient.getDailyPivots(scripCode, date);
        if (fetched != null) cache.put(key, fetched);
        return fetched;
    }
}
