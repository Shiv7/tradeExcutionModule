package com.kotsin.execution.service;

import com.kotsin.execution.consumer.BulletproofSignalConsumer;
import com.kotsin.execution.model.Candlestick;
import com.kotsin.execution.model.StrategySignal;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.List;

@Service
@Slf4j
@RequiredArgsConstructor
public class SimulationService {

    private final HistoricalDataClient historicalDataClient;
    private final BulletproofSignalConsumer bulletproofSignalConsumer;

    public void runSimulation(String scripCode, String date, StrategySignal signal) {
        log.info("--- Starting Simulation for {} on {} ---", scripCode, date);

        // 1. Add the signal to the watchlist
        bulletproofSignalConsumer.addSignalToWatchlist(signal, LocalDateTime.now());

        // 2. Fetch historical data
        List<Candlestick> candles = historicalDataClient.getHistorical5MinCandles(scripCode, date);
        if (candles.isEmpty()) {
            log.error("Cannot run simulation. No historical data found for {} on {}", scripCode, date);
            return;
        }
        log.info("Found {} historical 5-min candles for simulation.", candles.size());

        // 3. Replay the market data
        for (Candlestick candle : candles) {
            // Set company name for history tracking
            candle.setCompanyName(scripCode);
            
            log.debug("[SIMULATION] Processing candle: {}", candle);
            bulletproofSignalConsumer.process5MinCandle(candle);

            // If a trade was executed, we can stop the simulation for this signal
            if (bulletproofSignalConsumer.hasActiveTrade()) {
                log.info("Simulation ended because a trade was executed.");
                break;
            }
        }

        log.info("--- Simulation for {} on {} Finished ---", scripCode, date);
    }
}
