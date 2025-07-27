package com.kotsin.execution.controller;

import com.kotsin.execution.model.StrategySignal;
import com.kotsin.execution.model.Candlestick;
import com.kotsin.execution.service.HistoricalDataClient;
import com.kotsin.execution.service.SimulationService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

@RestController
@RequestMapping("/api/simulation")
@Slf4j
@RequiredArgsConstructor
public class SimulationController {

    private final SimulationService simulationService;
    private final HistoricalDataClient historicalDataClient;

    @PostMapping("/run")
    public ResponseEntity<Map<String, String>> runSimulation(@RequestBody SimulationRequest request) {
        try {
            log.info("Received simulation request: {}", request);
            // Basic validation
            if (request.getScripCode() == null || request.getDate() == null || request.getSignal() == null) {
                return ResponseEntity.badRequest().body(Map.of("status", "error", "message", "Missing required fields: scripCode, date, signal"));
            }

            // Fetch historical data
            java.util.List<Candlestick> candles = historicalDataClient.getHistorical1MinCandles(request.getScripCode(), request.getDate());
            if (candles.isEmpty()) {
                return ResponseEntity.badRequest().body(Map.of("status", "error", "message", "No historical data found for the given scrip and date."));
            }

            // Run simulation asynchronously to not block the HTTP thread
            new Thread(() -> simulationService.runSimulation(candles)).start();

            return ResponseEntity.ok(Map.of("status", "success", "message", "Simulation started for " + request.getScripCode() + " on " + request.getDate()));
        } catch (Exception e) {
            log.error("Error starting simulation: {}", e.getMessage(), e);
            return ResponseEntity.internalServerError().body(Map.of("status", "error", "message", e.getMessage()));
        }
    }

    // Inner class for the request body
    @lombok.Data
    static class SimulationRequest {
        private String scripCode;
        private String date; // yyyy-MM-dd
        private StrategySignal signal;
    }
}
