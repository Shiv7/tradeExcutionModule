package com.kotsin.execution.producer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.kotsin.execution.model.TradeResult;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

import java.util.concurrent.CompletableFuture;

/**
 * Producer service that publishes comprehensive trade results to Kafka topics.
 * Publishes final profit/loss details when trades close.
 */
@Service
@Slf4j
@RequiredArgsConstructor
public class TradeResultProducer {
    
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;
    
    private String tradeResultsTopic = "trade-results";
    
    private String dailySummaryTopic = "daily-trade-summary";
    
    /**
     * Publish comprehensive trade result to trade-results topic
     */
    public void publishTradeResult(TradeResult tradeResult) {
        try {
            // Convert trade result to JSON
            String tradeResultJson = objectMapper.writeValueAsString(tradeResult);
            
            // Use trade ID as Kafka key for partitioning
            String key = tradeResult.getTradeId();
            
            log.info("💰 Publishing trade result to topic '{}' for trade: {}", tradeResultsTopic, key);
            
            // Send to Kafka with callback for monitoring
            CompletableFuture<SendResult<String, String>> future = kafkaTemplate.send(tradeResultsTopic, key, tradeResultJson);
            
            future.whenComplete((result, ex) -> {
                if (ex == null) {
                    log.info("✅ Trade result published successfully: {} (Offset: {}, Partition: {})", 
                            key, 
                            result.getRecordMetadata().offset(),
                            result.getRecordMetadata().partition());
                    
                    // Log key details for monitoring
                    logTradeResultSummary(tradeResult);
                    
                } else {
                    log.error("🚨 Failed to publish trade result: {} - Error: {}", key, ex.getMessage(), ex);
                }
            });
            
        } catch (Exception e) {
            log.error("🚨 Error publishing trade result for {}: {}", tradeResult.getTradeId(), e.getMessage(), e);
        }
    }
    
    /**
     * Publish daily trading summary
     */
    public void publishDailySummary(Object dailySummary) {
        try {
            String summaryJson = objectMapper.writeValueAsString(dailySummary);
            String key = "daily-summary-" + java.time.LocalDate.now();
            
            log.info("📊 Publishing daily summary to topic '{}'", dailySummaryTopic);
            
            CompletableFuture<SendResult<String, String>> future = kafkaTemplate.send(dailySummaryTopic, key, summaryJson);
            
            future.whenComplete((result, ex) -> {
                if (ex == null) {
                    log.info("✅ Daily summary published successfully");
                } else {
                    log.error("🚨 Failed to publish daily summary: {}", ex.getMessage(), ex);
                }
            });
            
        } catch (Exception e) {
            log.error("🚨 Error publishing daily summary: {}", e.getMessage(), e);
        }
    }
    
    /**
     * Log trade result summary for monitoring
     */
    private void logTradeResultSummary(TradeResult tradeResult) {
        try {
            String summary = String.format(
                "💰 TRADE RESULT PUBLISHED 💰\n" +
                "🆔 Trade ID: %s\n" +
                "📊 Script: %s (%s)\n" +
                "🎯 Strategy: %s\n" +
                "📈 Direction: %s\n" +
                "💰 Entry: %.2f → Exit: %.2f\n" +
                "💸 P&L: %.2f (%.2f%% ROI)\n" +
                "✅ Result: %s\n" +
                "⏱️ Duration: %d minutes\n" +
                "❓ Exit Reason: %s\n" +
                "🎯 Targets Hit: T1=%s, T2=%s\n" +
                "📊 Published to: %s",
                
                tradeResult.getTradeId(),
                tradeResult.getCompanyName(), tradeResult.getScripCode(),
                tradeResult.getStrategyName(),
                tradeResult.getSignalType(),
                tradeResult.getEntryPrice(), tradeResult.getExitPrice(),
                tradeResult.getProfitLoss(), tradeResult.getRoi(),
                tradeResult.isWinner() ? "WIN 🎉" : "LOSS 😔",
                tradeResult.getDurationMinutes() != null ? tradeResult.getDurationMinutes() : 0,
                tradeResult.getExitReason(),
                tradeResult.getTarget1Hit() ? "YES ✅" : "NO ❌",
                tradeResult.getTarget2Hit() ? "YES ✅" : "NO ❌",
                tradeResultsTopic
            );
            
            log.info(summary);
            
        } catch (Exception e) {
            log.debug("Error logging trade result summary: {}", e.getMessage());
        }
    }
    
    /**
     * Test connectivity to Kafka topics
     */
    public boolean testKafkaConnectivity() {
        try {
            // Send a test message
            String testMessage = "{\"test\": \"connectivity\", \"timestamp\": \"" + java.time.LocalDateTime.now() + "\"}";
            
            CompletableFuture<SendResult<String, String>> future = kafkaTemplate.send(tradeResultsTopic, "test-key", testMessage);
            
            // Wait for result with timeout
            SendResult<String, String> result = future.get(5, java.util.concurrent.TimeUnit.SECONDS);
            
            log.info("✅ Kafka connectivity test successful - Offset: {}, Partition: {}", 
                    result.getRecordMetadata().offset(),
                    result.getRecordMetadata().partition());
            
            return true;
            
        } catch (Exception e) {
            log.error("🚨 Kafka connectivity test failed: {}", e.getMessage());
            return false;
        }
    }
    
    /**
     * Get producer metrics for monitoring
     */
    public String getProducerMetrics() {
        try {
            return String.format("Kafka Producer Status: Connected to %s, Topics: [%s, %s]",
                    kafkaTemplate.getProducerFactory().getConfigurationProperties().get("bootstrap.servers"),
                    tradeResultsTopic,
                    dailySummaryTopic);
        } catch (Exception e) {
            return "Kafka Producer Status: Error getting metrics - " + e.getMessage();
        }
    }
} 