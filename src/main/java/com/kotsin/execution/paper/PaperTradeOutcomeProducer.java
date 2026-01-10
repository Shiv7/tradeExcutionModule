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
 * 3. Update Pattern HistoricalStats (Phase 4 SMTIS)
 */
@Service
@Slf4j
public class PaperTradeOutcomeProducer {

    private static final String TOPIC = "trade-outcomes";
    private static final String PATTERN_OUTCOMES_TOPIC = "pattern-outcomes";

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
            // Send to main trade-outcomes topic
            outcomeKafkaTemplate.send(TOPIC, outcome.getScripCode(), outcome);
            log.info("OUTCOME SENT | {} | signalId={} | win={} | R={:.2f} | exitReason={}",
                    outcome.getScripCode(),
                    outcome.getSignalId(),
                    outcome.isWin(),
                    outcome.getRMultiple(),
                    outcome.getExitReason());

            // If this is a pattern-based trade, also send to pattern-outcomes topic
            if (isPatternTrade(outcome)) {
                sendPatternOutcome(outcome);
            }
        } catch (Exception e) {
            log.error("Failed to send outcome: {}", e.getMessage(), e);
        }
    }

    /**
     * Check if this outcome is from a pattern-based trade
     */
    private boolean isPatternTrade(PaperTradeOutcome outcome) {
        return outcome.getPatternId() != null && !outcome.getPatternId().isEmpty();
    }

    /**
     * Send pattern outcome for SMTIS learning
     */
    private void sendPatternOutcome(PaperTradeOutcome outcome) {
        try {
            String key = outcome.getFamilyId() != null
                    ? outcome.getFamilyId() + ":" + outcome.getPatternId()
                    : outcome.getPatternId();

            outcomeKafkaTemplate.send(PATTERN_OUTCOMES_TOPIC, key, outcome);
            log.info("PATTERN OUTCOME SENT | {} | pattern={} | win={} | pnl={:.2f}%",
                    outcome.getScripCode(),
                    outcome.getPatternId(),
                    outcome.isWin(),
                    outcome.getPnlPct());
        } catch (Exception e) {
            log.error("Failed to send pattern outcome: {}", e.getMessage(), e);
        }
    }
}

