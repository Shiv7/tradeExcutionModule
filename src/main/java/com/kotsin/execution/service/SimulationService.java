package com.kotsin.execution.service;

import com.kotsin.execution.model.Candlestick;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Service
@Slf4j
@RequiredArgsConstructor
public class SimulationService {

    private final ApplicationEventPublisher eventPublisher;
    private final ExecutorService executor = Executors.newSingleThreadExecutor();

    public void runSimulation(List<Candlestick> candles) {
        if (candles == null || candles.isEmpty()) {
            log.warn("No candles provided for simulation.");
            return;
        }

        log.info("Starting simulation with {} candles.", candles.size());
        executor.submit(() -> {
            for (Candlestick candle : candles) {
                try {
                    log.debug("Publishing candle: {}", candle);
                    eventPublisher.publishEvent(candle);
                    // Simulate real-time market data with a small delay
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    log.error("Simulation was interrupted.", e);
                    break;
                }
            }
            log.info("Simulation finished.");
        });
    }
}
