package com.kotsin.execution.producer;

import com.kotsin.execution.model.TradeResult;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
@Slf4j
@RequiredArgsConstructor
public class TradeResultProducer {

    private final KafkaTemplate<String, Object> kafkaTemplate;

    @Value("${kafka.topics.results.trade-results:trade-results}")
    private String tradeResultsTopic;

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
