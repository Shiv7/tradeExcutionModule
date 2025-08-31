package com.kotsin.execution.controller;

import com.kotsin.execution.producer.TradeResultProducer;
import com.kotsin.execution.service.ErrorMonitoringService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

@RestController
@RequestMapping("/api/monitor")
@Slf4j
@RequiredArgsConstructor
public class ErrorMonitoringController {

    private final ErrorMonitoringService errorMonitoringService;
    private final TradeResultProducer tradeResultProducer;

    @PostMapping("/error")
    public ResponseEntity<Map<String, Object>> recordError(@RequestParam String area, @RequestParam String message) {
        errorMonitoringService.recordError(area, message, null);
        return ResponseEntity.ok(Map.of("status", "logged"));
    }

    @GetMapping("/kafka")
    public ResponseEntity<Map<String, Object>> kafkaStatus() {
        String status = tradeResultProducer.getProducerMetrics();
        return ResponseEntity.ok(Map.of("producer", status));
    }
}
