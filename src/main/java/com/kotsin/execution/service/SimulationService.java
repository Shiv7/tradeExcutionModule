package com.kotsin.execution.service;

import com.kotsin.execution.model.Candlestick;
import com.kotsin.execution.model.SimulationEndEvent;
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
            for (int i = 0; i < candles.size(); i++) {
                Candlestick candle = candles.get(i);
                try {
                    log.info("Publishing candle {}/{}: {}", i + 1, candles.size(), candle);
                    eventPublisher.publishEvent(candle);
                    // Simulate real-time market data with a small delay
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    log.error("Simulation was interrupted.", e);
                    break;
                }
            }
            log.info("Simulation finished publishing candles.");
            // Publish an event to signal the end of the simulation
            if (!candles.isEmpty()) {
                eventPublisher.publishEvent(new SimulationEndEvent(candles.get(candles.size() - 1)));
            }
        });
    }
}
