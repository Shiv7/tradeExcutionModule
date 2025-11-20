package com.kotsin.execution.config;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.ExponentialBackOffWithMaxRetries;
import org.springframework.util.backoff.FixedBackOff;

/**
 * 🚨 Dead Letter Queue (DLQ) Configuration
 * 
 * Handles Kafka consumer failures with:
 * - Automatic retries with exponential backoff
 * - Failed messages sent to DLQ for manual inspection
 * - Prevents message loss
 * - Allows replay after fixing issues
 * 
 * DLQ Topic Pattern: {original-topic}.DLQ
 * Example: advanced-signal → advanced-signal.DLQ
 */
@Configuration
@Slf4j
public class KafkaDLQConfig {
    
    /**
     * Configure DLQ error handler for all consumers
     */
    @Bean
    public CommonErrorHandler errorHandler(KafkaTemplate<String, Object> kafkaTemplate) {
        
        // Dead Letter Publishing Recoverer
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(
                kafkaTemplate,
                (record, ex) -> {
                    String dlqTopic = record.topic() + ".DLQ";
                    
                    log.error("🚨 [DLQ] Message failed after retries | " +
                            "topic={} partition={} offset={} key={} | " +
                            "Sending to DLQ: {} | error: {}",
                            record.topic(),
                            record.partition(),
                            record.offset(),
                            record.key(),
                            dlqTopic,
                            ex.getMessage());
                    
                    // Return DLQ topic partition (same partition as source)
                    return new org.apache.kafka.common.TopicPartition(dlqTopic, record.partition());
                }
        );
        
        // Exponential backoff: start=1s, multiplier=2, max=30s, maxAttempts=5
        // Retry sequence: 1s, 2s, 4s, 8s, 16s, 30s (capped)
        ExponentialBackOffWithMaxRetries backOff = new ExponentialBackOffWithMaxRetries(5);
        backOff.setInitialInterval(1000);  // 1 second
        backOff.setMultiplier(2.0);
        backOff.setMaxInterval(30000);  // 30 seconds max
        
        // Error handler with retries + DLQ
        DefaultErrorHandler errorHandler = new DefaultErrorHandler(recoverer, backOff);
        
        // Add custom retry logic
        errorHandler.setRetryListeners((record, ex, deliveryAttempt) -> {
            log.warn("🔄 [DLQ] Retry attempt #{} | topic={} offset={} error: {}",
                    deliveryAttempt,
                    record.topic(),
                    record.offset(),
                    ex.getMessage());
        });
        
        log.info("🚨 DLQ Error Handler configured | retries=5 backoff=exponential(1s→30s)");
        
        return errorHandler;
    }
    
    /**
     * Optional: Separate listener for DLQ monitoring/alerting
     */
    // Uncomment if you want to process DLQ messages
    /*
    @KafkaListener(topics = "#{__listener.DLQ_PATTERN}", groupId = "dlq-monitor")
    public void processDLQ(ConsumerRecord<String, Object> record) {
        log.error("🚨 [DLQ_MONITOR] Message in DLQ | " +
                "topic={} partition={} offset={} key={} value={}",
                record.topic(),
                record.partition(),
                record.offset(),
                record.key(),
                record.value());
        
        // Send alert (Telegram, email, PagerDuty, etc.)
        telegramService.sendCriticalAlert(
                String.format("DLQ: %s - %s", record.topic(), record.key())
        );
    }
    
    public static final String DLQ_PATTERN = ".*\\.DLQ";
    */
}
