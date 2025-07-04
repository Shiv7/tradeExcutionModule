package com.kotsin.execution.config;

import com.kotsin.execution.service.ErrorMonitoringService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.springframework.kafka.listener.ConsumerAwareListenerErrorHandler;
import org.springframework.kafka.listener.ListenerExecutionFailedException;
import org.springframework.kafka.support.serializer.DeserializationException;
import org.springframework.messaging.Message;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 🛡️ BULLETPROOF Error Handler for Kafka Consumer
 * Handles deserialization failures and other consumer errors gracefully
 * Integrates with ErrorMonitoringService for comprehensive tracking
 */
@Component
@Slf4j
@RequiredArgsConstructor
public class BulletproofErrorHandler implements ConsumerAwareListenerErrorHandler {

    private final ErrorMonitoringService errorMonitoringService;
    
    private static final DateTimeFormatter TIME_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    
    // Local error tracking metrics for immediate feedback
    private final AtomicLong deserializationErrors = new AtomicLong(0);
    private final AtomicLong processingErrors = new AtomicLong(0);
    private final AtomicLong totalErrorsHandled = new AtomicLong(0);

    @Override
    public Object handleError(Message<?> message, ListenerExecutionFailedException exception, 
                            Consumer<?, ?> consumer) {
        
        totalErrorsHandled.incrementAndGet();
        
        // Extract topic and partition information
        String topic = "unknown";
        int partition = -1;
        long offset = -1;
        
        if (message.getHeaders().containsKey("kafka_receivedTopic")) {
            topic = (String) message.getHeaders().get("kafka_receivedTopic");
        }
        if (message.getHeaders().containsKey("kafka_receivedPartition")) {
            partition = (Integer) message.getHeaders().get("kafka_receivedPartition");
        }
        if (message.getHeaders().containsKey("kafka_offset")) {
            offset = (Long) message.getHeaders().get("kafka_offset");
        }

        // Handle different types of exceptions
        Throwable rootCause = getRootCause(exception);
        
        if (rootCause instanceof DeserializationException) {
            handleDeserializationError((DeserializationException) rootCause, topic, partition, offset);
        } else if (rootCause instanceof org.apache.kafka.common.errors.SerializationException) {
            handleSerializationError((org.apache.kafka.common.errors.SerializationException) rootCause, topic, partition, offset);
        } else {
            handleProcessingError(exception, topic, partition, offset);
        }

        // 🛡️ BULLETPROOF: Always acknowledge and discard malformed messages
        // This allows the consumer to continue processing good messages
        log.info("⏭️ [BULLETPROOF-RECOVERY] Message discarded, continuing with next message");
        
        // Return null to indicate the message should be acknowledged and skipped
        return null;
    }

    /**
     * Handle JSON deserialization errors (malformed JSON)
     */
    private void handleDeserializationError(DeserializationException ex, String topic, int partition, long offset) {
        deserializationErrors.incrementAndGet();
        
        log.error("🚨 [DESERIALIZATION-ERROR] Malformed JSON message in topic '{}' at offset {} (partition {})", 
                topic, offset, partition);
        
        // Extract the malformed data if available
        String malformedData = null;
        if (ex.getData() != null) {
            malformedData = new String(ex.getData());
            log.error("🔍 [MALFORMED-DATA] Raw message content (first 500 chars): {}", 
                    malformedData.length() > 500 ? malformedData.substring(0, 500) + "..." : malformedData);
            
            // Try to identify the issue
            identifyJsonIssue(malformedData, topic);
        }
        
        log.error("🔧 [ERROR-DETAILS] Deserialization error: {}", ex.getMessage());
        
        // 📊 Record error in monitoring service
        errorMonitoringService.recordDeserializationError(topic, ex.getMessage(), malformedData);
        
        log.warn("⏭️ [RECOVERY] Skipping malformed message and continuing with next message");
    }

    /**
     * Handle Kafka serialization errors
     */
    private void handleSerializationError(org.apache.kafka.common.errors.SerializationException ex, 
                                        String topic, int partition, long offset) {
        deserializationErrors.incrementAndGet();
        
        log.error("🚨 [SERIALIZATION-ERROR] Kafka serialization failure in topic '{}' at offset {} (partition {})", 
                topic, offset, partition);
        log.error("🔧 [ERROR-DETAILS] Serialization error: {}", ex.getMessage());
        
        // 📊 Record error in monitoring service
        errorMonitoringService.recordDeserializationError(topic, ex.getMessage(), null);
        
        log.warn("⏭️ [RECOVERY] Skipping corrupted message and continuing with next message");
    }

    /**
     * Handle general processing errors (business logic failures)
     */
    private void handleProcessingError(ListenerExecutionFailedException ex, String topic, int partition, long offset) {
        processingErrors.incrementAndGet();
        
        log.error("🚨 [PROCESSING-ERROR] Business logic failure in topic '{}' at offset {} (partition {})", 
                topic, offset, partition);
        log.error("🔧 [ERROR-DETAILS] Processing error: {}", ex.getMessage(), ex);
        
        // 📊 Record error in monitoring service
        errorMonitoringService.recordProcessingError(topic, "unknown", ex.getMessage());
        
        log.warn("⏭️ [RECOVERY] Skipping failed message and continuing with next message");
    }

    /**
     * Try to identify common JSON issues in malformed data
     */
    private void identifyJsonIssue(String malformedData, String topic) {
        try {
            if (malformedData == null || malformedData.trim().isEmpty()) {
                log.warn("🔍 [JSON-ISSUE] Empty or null message content");
                return;
            }

            String trimmed = malformedData.trim();
            
            // Check for common JSON issues
            if (!trimmed.startsWith("{") && !trimmed.startsWith("[")) {
                log.warn("🔍 [JSON-ISSUE] Message doesn't start with JSON delimiter ({{ or [)");
            } else if (!trimmed.endsWith("}") && !trimmed.endsWith("]")) {
                log.warn("🔍 [JSON-ISSUE] Message appears to be truncated (missing closing delimiter)");
            } else if (trimmed.contains("\\u0009") || trimmed.contains("\\u")) {
                log.warn("🔍 [JSON-ISSUE] Message contains escaped unicode characters that may be causing issues");
            } else if (countChar(trimmed, '{') != countChar(trimmed, '}')) {
                log.warn("🔍 [JSON-ISSUE] Unbalanced curly braces - {{ count: {}, }} count: {}", 
                        countChar(trimmed, '{'), countChar(trimmed, '}'));
            } else if (countChar(trimmed, '[') != countChar(trimmed, ']')) {
                log.warn("🔍 [JSON-ISSUE] Unbalanced square brackets - [ count: {}, ] count: {}", 
                        countChar(trimmed, '['), countChar(trimmed, ']'));
            } else {
                log.warn("🔍 [JSON-ISSUE] JSON structure appears valid, likely a data type or field issue");
            }

            // Log suggestions based on topic
            if ("enhanced-30m-signals".equals(topic)) {
                log.info("💡 [SUGGESTION] For enhanced-30m-signals topic, ensure strategy module sends valid StrategySignal JSON");
                log.info("💡 [SUGGESTION] Check that all required fields are present: scripCode, signal, entryPrice, stopLoss, target1");
            } else if ("forwardtesting-data".equals(topic)) {
                log.info("💡 [SUGGESTION] For forwardtesting-data topic, ensure market data producer sends valid MarketData JSON");
                log.info("💡 [SUGGESTION] Check market data feed and timestamp formatting");
            }

        } catch (Exception e) {
            log.debug("Could not analyze malformed JSON: {}", e.getMessage());
        }
    }

    /**
     * Count occurrences of a character in a string
     */
    private int countChar(String str, char ch) {
        return (int) str.chars().filter(c -> c == ch).count();
    }

    /**
     * Get the root cause of an exception
     */
    private Throwable getRootCause(Throwable throwable) {
        Throwable rootCause = throwable;
        while (rootCause.getCause() != null && rootCause != rootCause.getCause()) {
            rootCause = rootCause.getCause();
        }
        return rootCause;
    }

    /**
     * Get error handling statistics for monitoring
     */
    public String getErrorStats() {
        return String.format(
            "Local Error Handler Stats [%s]: Total: %d, Deserialization: %d, Processing: %d",
            LocalDateTime.now().format(TIME_FORMAT),
            totalErrorsHandled.get(),
            deserializationErrors.get(),
            processingErrors.get()
        );
    }

    /**
     * Reset local error statistics
     */
    public void resetStats() {
        totalErrorsHandled.set(0);
        deserializationErrors.set(0);
        processingErrors.set(0);
        log.info("📊 Reset local error handler statistics");
    }

    /**
     * Check if local error rate is concerning
     */
    public boolean isErrorRateHigh(long totalMessagesProcessed) {
        if (totalMessagesProcessed == 0) return false;
        
        double errorRate = (double) totalErrorsHandled.get() / totalMessagesProcessed;
        return errorRate > 0.05; // 5% error rate threshold
    }

    /**
     * Get comprehensive error summary including monitoring service data
     */
    public void logComprehensiveErrorSummary() {
        log.info("🛡️ [COMPREHENSIVE-ERROR-SUMMARY] Bulletproof Error Handler & Monitoring:");
        log.info("   📊 Local Handler Stats: {}", getErrorStats());
        log.info("   🔍 Global Monitoring Stats:");
        
        // Get stats from monitoring service
        String globalStats = errorMonitoringService.getErrorStatistics();
        for (String line : globalStats.split("\n")) {
            if (!line.trim().isEmpty()) {
                log.info("      {}", line);
            }
        }
        
        if (totalErrorsHandled.get() > 0) {
            log.info("   💡 All errors were handled gracefully - consumer continued processing");
            log.info("   🎯 Check logs above for specific error details and suggestions");
            
            // Get suggestions from monitoring service
            String suggestions = errorMonitoringService.generateErrorSuggestions();
            log.info("   💡 Error Resolution Suggestions:");
            for (String line : suggestions.split("\n")) {
                if (!line.trim().isEmpty()) {
                    log.info("      {}", line);
                }
            }
        }
    }
} 