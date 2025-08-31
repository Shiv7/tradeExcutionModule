package com.kotsin.execution.service;

import com.kotsin.execution.model.Candlestick;
import com.kotsin.execution.model.PivotData;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

import java.time.LocalDate;
import java.util.List;
import java.util.Objects;

@Service
@RequiredArgsConstructor
public class MarketDataService {

    private final PivotService pivotService;
    private final HistoricalDataClient historicalDataClient;

    public PivotData getDailyPivots(String scripCode, LocalDate date) {
        return pivotService.getDailyPivots(scripCode, date);
    }

    public List<Candlestick> getHistorical1MinCandles(String scripCode, String isoDate, String exchange, String exchangeType) {
        Objects.requireNonNull(scripCode, "scripCode");
        Objects.requireNonNull(isoDate, "isoDate");
        return historicalDataClient.getHistorical1MinCandles(scripCode, isoDate, exchange, exchangeType);
    }
}
