package com.kotsin.execution.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.kotsin.execution.model.Candlestick;
import com.kotsin.execution.model.HistoricalDataResponse;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

@Service
@Slf4j
public class HistoricalDataClient {

    private final RestTemplate restTemplate = new RestTemplate();
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Value("${ltp.api.url:http://localhost:8002/getHisDataFromFivePaisa}")
    private String ltpApiUrl;

    public List<Candlestick> getHistorical5MinCandles(String scripCode, String date) {
        try {
            LocalDate localDate = LocalDate.parse(date, DateTimeFormatter.ofPattern("yyyy-MM-dd"));
            String startDate = localDate.format(DateTimeFormatter.ofPattern("yyyy-MM-dd"));
            String endDate = localDate.plusDays(1).format(DateTimeFormatter.ofPattern("yyyy-MM-dd"));

            String url = String.format("%s?exch=N&exch_type=C&scrip_code=%s&start_date=%s&end_date=%s&interval=5m",
                    ltpApiUrl, scripCode, startDate, endDate);

            log.info("Fetching historical 5-min candles from URL: {}", url);
            String rawResponse = restTemplate.getForObject(url, String.class);

            HistoricalDataResponse[] response = objectMapper.readValue(rawResponse, HistoricalDataResponse[].class);

            if (response != null) {
                return Arrays.stream(response)
                        .map(this::transformToCandlestick)
                        .collect(Collectors.toList());
            }
        } catch (Exception e) {
            log.error("Failed to fetch historical 5-min candles for scripCode {} on date {}: {}", scripCode, date, e.getMessage());
        }
        return Collections.emptyList();
    }

    private Candlestick transformToCandlestick(HistoricalDataResponse res) {
        Candlestick candle = new Candlestick();
        candle.setOpen(res.getOpen());
        candle.setHigh(res.getHigh());
        candle.setLow(res.getLow());
        candle.setClose(res.getClose());
        candle.setVolume(res.getVolume());
        // Note: CompanyName and timestamps would need to be set if required by analysis services.
        return candle;
    }
}
