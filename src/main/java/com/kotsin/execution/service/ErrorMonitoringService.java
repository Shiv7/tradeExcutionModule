package com.kotsin.execution.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 🛡️ BULLETPROOF Error Monitoring Service
 * Tracks and reports on discarded messages and error patterns
 * Provides insights into message quality and system health
 */
@Service
@Slf4j
public class ErrorMonitoringService {

    private static final DateTimeFormatter TIME_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    
    // Global error tracking
    private final AtomicLong totalDiscardedMessages = new AtomicLong(0);
    private final AtomicLong deserializationErrors = new AtomicLong(0);
    private final AtomicLong validationErrors = new AtomicLong(0);
    private final AtomicLong processingErrors = new AtomicLong(0);
    
    // Topic-specific error tracking
    private final ConcurrentHashMap<String, AtomicLong> topicErrorCounts = new ConcurrentHashMap<>();
    
    // Error pattern tracking
    private final ConcurrentHashMap<String, AtomicLong> errorPatterns = new ConcurrentHashMap<>();
    
    // Recent error samples for debugging
    private final ConcurrentHashMap<String, String> recentErrorSamples = new ConcurrentHashMap<>();
    
    /**
     * Record a deserialization error (malformed JSON)
     */
    public void recordDeserializationError(String topic, String errorMessage, String sampleData) {
        totalDiscardedMessages.incrementAndGet();
        deserializationErrors.incrementAndGet();
        
        // Track by topic
        topicErrorCounts.computeIfAbsent(topic + "_deserialization", k -> new AtomicLong(0)).incrementAndGet();
        
        // Store recent sample for debugging
        String key = topic + "_deserialization_sample";
        String sample = sampleData != null && sampleData.length() > 200 ? 
                       sampleData.substring(0, 200) + "..." : sampleData;
        recentErrorSamples.put(key, sample);
        
        // Track error patterns
        String pattern = extractErrorPattern(errorMessage);
        if (pattern != null) {
            errorPatterns.computeIfAbsent(pattern, k -> new AtomicLong(0)).incrementAndGet();
        }
        
        log.warn("🚨 [ERROR-MONITOR] Deserialization error in topic '{}' - Total discarded: {}", 
                topic, totalDiscardedMessages.get());
    }
    
    /**
     * Record a validation error (invalid business data)
     */
    public void recordValidationError(String topic, String scripCode, String errorMessage) {
        totalDiscardedMessages.incrementAndGet();
        validationErrors.incrementAndGet();
        
        // Track by topic
        topicErrorCounts.computeIfAbsent(topic + "_validation", k -> new AtomicLong(0)).incrementAndGet();
        
        // Store recent sample for debugging
        String key = topic + "_validation_sample";
        recentErrorSamples.put(key, String.format("ScripCode: %s, Error: %s", scripCode, errorMessage));
        
        log.warn("🚨 [ERROR-MONITOR] Validation error for {} in topic '{}' - Total discarded: {}", 
                scripCode, topic, totalDiscardedMessages.get());
    }
    
    /**
     * Record a processing error (business logic failure)
     */
    public void recordProcessingError(String topic, String scripCode, String errorMessage) {
        totalDiscardedMessages.incrementAndGet();
        processingErrors.incrementAndGet();
        
        // Track by topic
        topicErrorCounts.computeIfAbsent(topic + "_processing", k -> new AtomicLong(0)).incrementAndGet();
        
        // Store recent sample for debugging
        String key = topic + "_processing_sample";
        recentErrorSamples.put(key, String.format("ScripCode: %s, Error: %s", scripCode, errorMessage));
        
        log.error("🚨 [ERROR-MONITOR] Processing error for {} in topic '{}' - Total discarded: {}", 
                scripCode, topic, totalDiscardedMessages.get());
    }
    
    /**
     * Extract common error patterns for analysis
     */
    private String extractErrorPattern(String errorMessage) {
        if (errorMessage == null) return null;
        
        String lowerMessage = errorMessage.toLowerCase();
        
        // Common JSON patterns
        if (lowerMessage.contains("unexpected character") || lowerMessage.contains("malformed json")) {
            return "MALFORMED_JSON";
        } else if (lowerMessage.contains("unexpected end")) {
            return "TRUNCATED_JSON";
        } else if (lowerMessage.contains("unrecognized field")) {
            return "UNKNOWN_FIELD";
        } else if (lowerMessage.contains("cannot deserialize")) {
            return "TYPE_MISMATCH";
        } else if (lowerMessage.contains("missing property") || lowerMessage.contains("required property")) {
            return "MISSING_REQUIRED_FIELD";
        } else if (lowerMessage.contains("unicode") || lowerMessage.contains("\\u")) {
            return "UNICODE_ISSUE";
        }
        
        return "OTHER";
    }
    
    /**
     * Get comprehensive error statistics
     */
    public String getErrorStatistics() {
        StringBuilder stats = new StringBuilder();
        stats.append("🛡️ BULLETPROOF Error Monitoring Statistics\n");
        stats.append("========================================\n");
        stats.append(String.format("📊 Total Discarded Messages: %d\n", totalDiscardedMessages.get()));
        stats.append(String.format("🔧 Deserialization Errors: %d\n", deserializationErrors.get()));
        stats.append(String.format("✅ Validation Errors: %d\n", validationErrors.get()));
        stats.append(String.format("⚙️ Processing Errors: %d\n", processingErrors.get()));
        stats.append(String.format("📅 Last Updated: %s\n\n", LocalDateTime.now().format(TIME_FORMAT)));
        
        // Topic breakdown
        if (!topicErrorCounts.isEmpty()) {
            stats.append("📋 Topic Error Breakdown:\n");
            topicErrorCounts.forEach((topic, count) -> 
                stats.append(String.format("  • %s: %d errors\n", topic, count.get()))
            );
            stats.append("\n");
        }
        
        // Error patterns
        if (!errorPatterns.isEmpty()) {
            stats.append("🔍 Error Patterns:\n");
            errorPatterns.entrySet().stream()
                .sorted((e1, e2) -> Long.compare(e2.getValue().get(), e1.getValue().get()))
                .forEach(entry -> 
                    stats.append(String.format("  • %s: %d occurrences\n", entry.getKey(), entry.getValue().get()))
                );
            stats.append("\n");
        }
        
        return stats.toString();
    }
    
    /**
     * Get recent error samples for debugging
     */
    public String getRecentErrorSamples() {
        if (recentErrorSamples.isEmpty()) {
            return "No recent error samples available.";
        }
        
        StringBuilder samples = new StringBuilder();
        samples.append("🔍 Recent Error Samples for Debugging:\n");
        samples.append("=====================================\n");
        
        recentErrorSamples.forEach((key, sample) -> {
            samples.append(String.format("📋 %s:\n", key));
            samples.append(String.format("   %s\n\n", sample));
        });
        
        return samples.toString();
    }
    
    /**
     * Check if error rate is concerning (above 5%)
     */
    public boolean isErrorRateHigh(long totalMessagesProcessed) {
        if (totalMessagesProcessed == 0) return false;
        
        double errorRate = (double) totalDiscardedMessages.get() / (totalMessagesProcessed + totalDiscardedMessages.get());
        return errorRate > 0.05; // 5% threshold
    }
    
    /**
     * Reset all error statistics (useful for testing or periodic resets)
     */
    public void resetStatistics() {
        totalDiscardedMessages.set(0);
        deserializationErrors.set(0);
        validationErrors.set(0);
        processingErrors.set(0);
        topicErrorCounts.clear();
        errorPatterns.clear();
        recentErrorSamples.clear();
        
        log.info("📊 [ERROR-MONITOR] All error statistics have been reset");
    }
    
    /**
     * Scheduled task to log periodic error summaries
     */
    @Scheduled(fixedRate = 300000) // Every 5 minutes
    public void logPeriodicSummary() {
        long totalErrors = totalDiscardedMessages.get();
        
        if (totalErrors > 0) {
            log.info("📊 [ERROR-MONITOR] Periodic Summary - Total Discarded: {} (Deserialization: {}, Validation: {}, Processing: {})",
                    totalErrors, deserializationErrors.get(), validationErrors.get(), processingErrors.get());
                    
            // Log top error patterns
            if (!errorPatterns.isEmpty()) {
                String topPattern = errorPatterns.entrySet().stream()
                    .max((e1, e2) -> Long.compare(e1.getValue().get(), e2.getValue().get()))
                    .map(entry -> entry.getKey() + "(" + entry.getValue().get() + ")")
                    .orElse("None");
                log.info("🔍 [ERROR-MONITOR] Top Error Pattern: {}", topPattern);
            }
        }
    }
    
    /**
     * Generate suggestions based on error patterns
     */
    public String generateErrorSuggestions() {
        if (totalDiscardedMessages.get() == 0) {
            return "✅ No errors detected - system running smoothly!";
        }
        
        StringBuilder suggestions = new StringBuilder();
        suggestions.append("💡 Error Resolution Suggestions:\n");
        suggestions.append("================================\n");
        
        // Deserialization suggestions
        if (deserializationErrors.get() > 0) {
            suggestions.append("🔧 Deserialization Issues:\n");
            suggestions.append("  • Check message producers for JSON format compliance\n");
            suggestions.append("  • Verify strategy module is sending valid StrategySignal JSON\n");
            suggestions.append("  • Look for unicode escape character issues (\\u0009)\n");
            suggestions.append("  • Ensure messages are not truncated during transmission\n\n");
        }
        
        // Validation suggestions
        if (validationErrors.get() > 0) {
            suggestions.append("✅ Validation Issues:\n");
            suggestions.append("  • Review strategy module signal generation logic\n");
            suggestions.append("  • Check price level calculations (entry, stop loss, targets)\n");
            suggestions.append("  • Verify signal direction matches price levels\n");
            suggestions.append("  • Ensure all required fields are populated\n\n");
        }
        
        // Processing suggestions
        if (processingErrors.get() > 0) {
            suggestions.append("⚙️ Processing Issues:\n");
            suggestions.append("  • Review business logic for edge cases\n");
            suggestions.append("  • Check for null pointer exceptions\n");
            suggestions.append("  • Verify database connectivity and operations\n");
            suggestions.append("  • Monitor system resources and performance\n\n");
        }
        
        // Pattern-specific suggestions
        errorPatterns.forEach((pattern, count) -> {
            switch (pattern) {
                case "MALFORMED_JSON":
                    suggestions.append("🚨 MALFORMED_JSON (").append(count.get()).append(" occurrences):\n");
                    suggestions.append("  • Producer may be sending invalid JSON syntax\n");
                    suggestions.append("  • Check for unescaped quotes or special characters\n\n");
                    break;
                case "UNICODE_ISSUE":
                    suggestions.append("🚨 UNICODE_ISSUE (").append(count.get()).append(" occurrences):\n");
                    suggestions.append("  • Configure producer to handle Unicode properly\n");
                    suggestions.append("  • May need UTF-8 encoding fixes\n\n");
                    break;
                case "MISSING_REQUIRED_FIELD":
                    suggestions.append("🚨 MISSING_REQUIRED_FIELD (").append(count.get()).append(" occurrences):\n");
                    suggestions.append("  • Ensure strategy module populates all required fields\n");
                    suggestions.append("  • Check StrategySignal model completeness\n\n");
                    break;
            }
        });
        
        return suggestions.toString();
    }
} 