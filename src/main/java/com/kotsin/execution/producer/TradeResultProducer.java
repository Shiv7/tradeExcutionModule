package com.kotsin.execution.producer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.kotsin.execution.model.TradeResult;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.concurrent.CompletableFuture;

/**
 * Kafka producer service that publishes final trade results with profit/loss
 * to the trade-results topic when trades are closed.
 */
@Service
@Slf4j
@RequiredArgsConstructor
public class TradeResultProducer {
    
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;
    
    // Topic for publishing trade results with profit/loss
    private static final String TRADE_RESULTS_TOPIC = "trade-results";
    
    /**
     * Publish trade result to Kafka topic
     */
    public void publishTradeResult(TradeResult tradeResult) {
        try {
            // Set result generation timestamp
            tradeResult.setResultGeneratedTime(LocalDateTime.now());
            tradeResult.setSystemVersion("1.0.0");
            
            // Calculate final metrics
            tradeResult.calculateProfitLoss();
            tradeResult.calculateDuration();
            
            // Convert to JSON
            String resultJson = objectMapper.writeValueAsString(tradeResult);
            
            // Use scripCode as partition key for ordered processing
            String partitionKey = tradeResult.getScripCode();
            
            log.info("📤 Publishing trade result for {}: P&L = {}, ROI = {}%", 
                    tradeResult.getScripCode(), 
                    tradeResult.getProfitLoss(), 
                    tradeResult.getRoi());
            
            // Send to Kafka
            CompletableFuture<SendResult<String, String>> future = kafkaTemplate.send(
                    TRADE_RESULTS_TOPIC, 
                    partitionKey, 
                    resultJson
            );
            
            // Handle success/failure asynchronously
            future.whenComplete((result, exception) -> {
                if (exception == null) {
                    log.info("✅ Successfully published trade result for trade: {} to topic: {} at offset: {}", 
                            tradeResult.getTradeId(), 
                            TRADE_RESULTS_TOPIC,
                            result.getRecordMetadata().offset());
                } else {
                    log.error("🚨 Failed to publish trade result for trade: {} to topic: {}: {}", 
                            tradeResult.getTradeId(), 
                            TRADE_RESULTS_TOPIC, 
                            exception.getMessage(), 
                            exception);
                }
            });
            
        } catch (Exception e) {
            log.error("🚨 Error serializing trade result for trade {}: {}", tradeResult.getTradeId(), e.getMessage(), e);
        }
    }
    
    /**
     * Publish daily summary of all trades
     */
    public void publishDailySummary(String date, int totalTrades, int winners, int losers, 
                                   double totalPnL, double totalROI) {
        try {
            // Create daily summary object
            DailySummary summary = DailySummary.builder()
                    .date(date)
                    .totalTrades(totalTrades)
                    .winners(winners)
                    .losers(losers)
                    .winRate(totalTrades > 0 ? (double) winners / totalTrades * 100 : 0.0)
                    .totalPnL(totalPnL)
                    .totalROI(totalROI)
                    .averagePnL(totalTrades > 0 ? totalPnL / totalTrades : 0.0)
                    .generatedTime(LocalDateTime.now())
                    .build();
            
            String summaryJson = objectMapper.writeValueAsString(summary);
            
            log.info("📊 Publishing daily summary for {}: {} trades, {}% win rate, P&L: {}", 
                    date, totalTrades, summary.getWinRate(), totalPnL);
            
            kafkaTemplate.send("daily-trade-summary", date, summaryJson);
            
        } catch (Exception e) {
            log.error("🚨 Error publishing daily summary: {}", e.getMessage(), e);
        }
    }
    
    /**
     * Inner class for daily summary data
     */
    @lombok.Data
    @lombok.Builder
    @lombok.AllArgsConstructor
    @lombok.NoArgsConstructor
    public static class DailySummary {
        private String date;
        private int totalTrades;
        private int winners;
        private int losers;
        private double winRate;
        private double totalPnL;
        private double totalROI;
        private double averagePnL;
        private LocalDateTime generatedTime;
    }
} 