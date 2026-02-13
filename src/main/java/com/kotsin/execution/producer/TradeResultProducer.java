package com.kotsin.execution.producer;

import com.kotsin.execution.model.TradeResult;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.Map;

@Service
@Slf4j
@RequiredArgsConstructor
public class TradeResultProducer {

    private final KafkaTemplate<String, Object> kafkaTemplate;

    @Value("${kafka.topics.results.trade-results:trade-results}")
    private String tradeResultsTopic;

    @Value("${kafka.topics.entries.trade-entries:trade-entries}")
    private String tradeEntriesTopic;

    public boolean publishTradeResult(TradeResult result) {
        try {
            String key = result.getTradeId() != null ? result.getTradeId() : result.getScripCode();
            kafkaTemplate.send(tradeResultsTopic, key, result);
            log.info("trade_result_published topic={} key={} scrip={} reason={} entry={} exit={}",
                    tradeResultsTopic, key, result.getScripCode(), result.getExitReason(), result.getEntryPrice(), result.getExitPrice());
            return true;
        } catch (Exception e) {
            log.error("Failed to publish TradeResult: {}", e.toString(), e);
            return false;
        }
    }

    /**
     * Publish a trade entry event for the orchestrator to transition READY → POSITIONED.
     */
    public void publishTradeEntry(String scripCode, String direction, double entryPrice,
            double stopLoss, double takeProfit, int quantity, String orderId, String strategy, String signalId) {
        try {
            Map<String, Object> event = new HashMap<>();
            event.put("scripCode", scripCode);
            event.put("direction", direction);
            event.put("entryPrice", entryPrice);
            event.put("stopLoss", stopLoss);
            event.put("takeProfit", takeProfit);
            event.put("quantity", quantity);
            event.put("entryTime", System.currentTimeMillis());
            event.put("orderId", orderId);
            event.put("strategyId", strategy);
            event.put("signalId", signalId);
            kafkaTemplate.send(tradeEntriesTopic, scripCode, event);
            log.info("trade_entry_published topic={} scrip={} dir={} entry={} sl={} tp={}",
                    tradeEntriesTopic, scripCode, direction, entryPrice, stopLoss, takeProfit);
        } catch (Exception e) {
            log.error("Failed to publish trade entry for {}: {}", scripCode, e.toString(), e);
        }
    }

    /** Simple producer metrics string for health endpoints. */
    public String getProducerMetrics() {
        try {
            Object servers = kafkaTemplate.getProducerFactory().getConfigurationProperties().get("bootstrap.servers");
            return "Kafka Producer OK, bootstrap.servers=" + servers + ", topic=" + tradeResultsTopic;
        } catch (Exception e) {
            return "Kafka Producer ERROR: " + e.getMessage();
        }
    }
}
