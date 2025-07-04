package com.kotsin.execution.controller;

import com.kotsin.execution.service.ErrorMonitoringService;
import com.kotsin.execution.config.BulletproofErrorHandler;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.HashMap;
import java.util.Map;

/**
 * 🛡️ BULLETPROOF Error Monitoring Controller
 * Provides REST endpoints for monitoring error statistics and getting suggestions
 */
@RestController
@RequestMapping("/api/error-monitoring")
@RequiredArgsConstructor
@Slf4j
public class ErrorMonitoringController {

    private final ErrorMonitoringService errorMonitoringService;
    private final BulletproofErrorHandler bulletproofErrorHandler;

    /**
     * Get comprehensive error statistics
     */
    @GetMapping("/statistics")
    public ResponseEntity<Map<String, Object>> getErrorStatistics() {
        try {
            Map<String, Object> response = new HashMap<>();
            
            // Get comprehensive statistics from monitoring service
            String globalStats = errorMonitoringService.getErrorStatistics();
            response.put("globalStatistics", globalStats);
            
            // Get local handler statistics
            String localStats = bulletproofErrorHandler.getErrorStats();
            response.put("localHandlerStats", localStats);
            
            // Get recent error samples
            String errorSamples = errorMonitoringService.getRecentErrorSamples();
            response.put("recentErrorSamples", errorSamples);
            
            response.put("status", "success");
            response.put("timestamp", java.time.LocalDateTime.now().toString());
            
            log.info("📊 [ERROR-MONITOR-API] Error statistics requested");
            
            return ResponseEntity.ok(response);
            
        } catch (Exception e) {
            log.error("🚨 [ERROR-MONITOR-API] Error getting statistics: {}", e.getMessage(), e);
            
            Map<String, Object> errorResponse = new HashMap<>();
            errorResponse.put("status", "error");
            errorResponse.put("message", "Failed to retrieve error statistics: " + e.getMessage());
            errorResponse.put("timestamp", java.time.LocalDateTime.now().toString());
            
            return ResponseEntity.internalServerError().body(errorResponse);
        }
    }

    /**
     * Get error resolution suggestions
     */
    @GetMapping("/suggestions")
    public ResponseEntity<Map<String, Object>> getErrorSuggestions() {
        try {
            Map<String, Object> response = new HashMap<>();
            
            // Get suggestions from monitoring service
            String suggestions = errorMonitoringService.generateErrorSuggestions();
            response.put("suggestions", suggestions);
            
            response.put("status", "success");
            response.put("timestamp", java.time.LocalDateTime.now().toString());
            
            log.info("💡 [ERROR-MONITOR-API] Error suggestions requested");
            
            return ResponseEntity.ok(response);
            
        } catch (Exception e) {
            log.error("🚨 [ERROR-MONITOR-API] Error getting suggestions: {}", e.getMessage(), e);
            
            Map<String, Object> errorResponse = new HashMap<>();
            errorResponse.put("status", "error");
            errorResponse.put("message", "Failed to retrieve error suggestions: " + e.getMessage());
            errorResponse.put("timestamp", java.time.LocalDateTime.now().toString());
            
            return ResponseEntity.internalServerError().body(errorResponse);
        }
    }

    /**
     * Reset error statistics (useful for testing or periodic cleanup)
     */
    @PostMapping("/reset")
    public ResponseEntity<Map<String, Object>> resetErrorStatistics() {
        try {
            // Reset monitoring service statistics
            errorMonitoringService.resetStatistics();
            
            // Reset local handler statistics
            bulletproofErrorHandler.resetStats();
            
            Map<String, Object> response = new HashMap<>();
            response.put("status", "success");
            response.put("message", "All error statistics have been reset");
            response.put("timestamp", java.time.LocalDateTime.now().toString());
            
            log.info("🔄 [ERROR-MONITOR-API] Error statistics reset requested");
            
            return ResponseEntity.ok(response);
            
        } catch (Exception e) {
            log.error("🚨 [ERROR-MONITOR-API] Error resetting statistics: {}", e.getMessage(), e);
            
            Map<String, Object> errorResponse = new HashMap<>();
            errorResponse.put("status", "error");
            errorResponse.put("message", "Failed to reset error statistics: " + e.getMessage());
            errorResponse.put("timestamp", java.time.LocalDateTime.now().toString());
            
            return ResponseEntity.internalServerError().body(errorResponse);
        }
    }

    /**
     * Get system health based on error rates
     */
    @GetMapping("/health")
    public ResponseEntity<Map<String, Object>> getSystemHealth() {
        try {
            Map<String, Object> response = new HashMap<>();
            
            // Assume some baseline for processed messages (this could be tracked separately)
            long estimatedProcessedMessages = 1000; // This should come from actual metrics
            
            boolean isErrorRateHigh = errorMonitoringService.isErrorRateHigh(estimatedProcessedMessages);
            
            if (isErrorRateHigh) {
                response.put("health", "WARNING");
                response.put("message", "Error rate is higher than 5% threshold - investigate error patterns");
                response.put("severity", "HIGH");
            } else {
                response.put("health", "HEALTHY");
                response.put("message", "Error rate is within acceptable limits");
                response.put("severity", "LOW");
            }
            
            response.put("status", "success");
            response.put("timestamp", java.time.LocalDateTime.now().toString());
            
            log.debug("🏥 [ERROR-MONITOR-API] System health check requested");
            
            return ResponseEntity.ok(response);
            
        } catch (Exception e) {
            log.error("🚨 [ERROR-MONITOR-API] Error checking system health: {}", e.getMessage(), e);
            
            Map<String, Object> errorResponse = new HashMap<>();
            errorResponse.put("status", "error");
            errorResponse.put("health", "UNKNOWN");
            errorResponse.put("message", "Failed to check system health: " + e.getMessage());
            errorResponse.put("timestamp", java.time.LocalDateTime.now().toString());
            
            return ResponseEntity.internalServerError().body(errorResponse);
        }
    }

    /**
     * Force a comprehensive error summary to be logged
     */
    @PostMapping("/log-summary")
    public ResponseEntity<Map<String, Object>> logComprehensiveErrorSummary() {
        try {
            // Trigger comprehensive error summary logging
            bulletproofErrorHandler.logComprehensiveErrorSummary();
            
            Map<String, Object> response = new HashMap<>();
            response.put("status", "success");
            response.put("message", "Comprehensive error summary has been logged - check application logs");
            response.put("timestamp", java.time.LocalDateTime.now().toString());
            
            log.info("📝 [ERROR-MONITOR-API] Comprehensive error summary logging requested");
            
            return ResponseEntity.ok(response);
            
        } catch (Exception e) {
            log.error("🚨 [ERROR-MONITOR-API] Error logging summary: {}", e.getMessage(), e);
            
            Map<String, Object> errorResponse = new HashMap<>();
            errorResponse.put("status", "error");
            errorResponse.put("message", "Failed to log error summary: " + e.getMessage());
            errorResponse.put("timestamp", java.time.LocalDateTime.now().toString());
            
            return ResponseEntity.internalServerError().body(errorResponse);
        }
    }
} 