package com.kotsin.execution.risk;

import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

/**
 * REST API for risk monitoring and circuit breaker control.
 */
@RestController
@RequestMapping("/api/risk")
@RequiredArgsConstructor
public class RiskController {

    private final RiskMonitorService riskMonitorService;

    /**
     * Get current risk status
     */
    @GetMapping("/status")
    public ResponseEntity<RiskMonitorService.RiskStatus> getRiskStatus() {
        return ResponseEntity.ok(riskMonitorService.getRiskStatus());
    }

    /**
     * Manually trip circuit breaker
     */
    @PostMapping("/circuit-breaker/trip")
    public ResponseEntity<Map<String, Object>> tripCircuitBreaker(
            @RequestParam(defaultValue = "Manual intervention") String reason) {
        riskMonitorService.tripCircuitBreaker(reason);
        return ResponseEntity.ok(Map.of(
                "success", true,
                "message", "Circuit breaker tripped: " + reason
        ));
    }

    /**
     * Reset circuit breaker
     */
    @PostMapping("/circuit-breaker/reset")
    public ResponseEntity<Map<String, Object>> resetCircuitBreaker() {
        riskMonitorService.resetCircuitBreaker();
        return ResponseEntity.ok(Map.of(
                "success", true,
                "message", "Circuit breaker reset - trading resumed"
        ));
    }

    /**
     * Health check for risk monitoring service
     */
    @GetMapping("/health")
    public ResponseEntity<Map<String, Object>> healthCheck() {
        RiskMonitorService.RiskStatus status = riskMonitorService.getRiskStatus();
        return ResponseEntity.ok(Map.of(
                "service", "risk-monitor",
                "status", status.isHealthy() ? "UP" : "DEGRADED",
                "riskStatus", status.getStatus(),
                "circuitBreakerTripped", status.isCircuitBreakerTripped()
        ));
    }
}
