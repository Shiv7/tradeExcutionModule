package com.kotsin.execution.paper;

import com.kotsin.execution.paper.model.PaperTradeOutcome;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

/**
 * PaperTradeOutcomeProducer - Sends trade outcomes back to StreamingCandle
 * 
 * StreamingCandle will use these to:
 * 1. Update SignalStats for learning
 * 2. Link outcomes to SignalHistory
 */
@Service
@Slf4j
public class PaperTradeOutcomeProducer {

    private static final String TOPIC = "trade-outcomes";

    @Autowired
    private KafkaTemplate<String, PaperTradeOutcome> outcomeKafkaTemplate;

    /**
     * Send outcome to StreamingCandle
     */
    public void send(PaperTradeOutcome outcome) {
        if (outcome == null || outcome.getSignalId() == null) {
            log.warn("Cannot send outcome - missing signalId");
            return;
        }

        try {
            outcomeKafkaTemplate.send(TOPIC, outcome.getScripCode(), outcome);
            log.info("OUTCOME SENT | {} | signalId={} | win={} | R={:.2f} | exitReason={}",
                    outcome.getScripCode(),
                    outcome.getSignalId(),
                    outcome.isWin(),
                    outcome.getRMultiple(),
                    outcome.getExitReason());
        } catch (Exception e) {
            log.error("Failed to send outcome: {}", e.getMessage(), e);
        }
    }
}

