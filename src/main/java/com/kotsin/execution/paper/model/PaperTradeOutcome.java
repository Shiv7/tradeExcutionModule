package com.kotsin.execution.paper.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;

import java.time.LocalDateTime;

/**
 * PaperTradeOutcome - Result of a paper trade
 * 
 * Sent back to StreamingCandle for stats update and learning.
 * Compatible with TradeOutcome in StreamingCandle.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class PaperTradeOutcome {

    private String id;
    private String signalId;      // Links to SignalHistory
    private String scripCode;
    private String signalType;
    private String direction;     // BULLISH, BEARISH
    
    private double entryPrice;
    private double exitPrice;
    private double stopLoss;
    private double target;
    private int quantity;
    
    private String exitReason;    // TARGET_HIT, STOP_LOSS, TIME_EXIT, TRAILING_STOP
    private double pnl;           // Absolute P&L
    private double rMultiple;     // Risk-adjusted return
    private boolean win;
    
    private LocalDateTime entryTime;
    private LocalDateTime exitTime;
    private long holdingPeriodMinutes;
    
    private double positionValue;
    private double positionSizeMultiplier;

    // Fields needed by Dashboard TradeOutcomeConsumer for StrategyWalletsService
    private String signalSource;   // Strategy source: FUDKII, FUKAA, PIVOT, etc.
    private String companyName;    // Display name: e.g. "GOLD MINI 995", "NATURALGAS 270 CE"
    private String exchange;       // N=NSE, M=MCX, C=Currency
    private String side;           // BUY, SELL

    // ========== Pattern Metadata (Phase 4 SMTIS) ==========
    private String patternId;           // Pattern template ID for stats update
    private String sequenceId;          // Sequence ID for correlation
    private String familyId;            // Family ID for family-specific stats
    private String patternCategory;     // REVERSAL, CONTINUATION, etc.
    private boolean target1Hit;         // Did trade hit target 1?
    private boolean target2Hit;         // Did trade hit target 2?
    private boolean target3Hit;         // Did trade hit target 3 (extended)?
    private boolean stopHit;            // Did trade hit stop loss?
    private long timeToOutcomeMs;       // Time from entry to outcome
    private long timeToTarget1Ms;       // Time to hit target 1 (if hit)
    private double pnlPct;              // P&L as percentage of entry

    // ========== Kafka Serde ==========
    
    private static final ObjectMapper MAPPER = new ObjectMapper()
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
            .findAndRegisterModules();
    
    public static Serde<PaperTradeOutcome> serde() {
        return Serdes.serdeFrom(new PaperTradeOutcomeSerializer(), new PaperTradeOutcomeDeserializer());
    }
    
    public static class PaperTradeOutcomeSerializer implements Serializer<PaperTradeOutcome> {
        @Override
        public byte[] serialize(String topic, PaperTradeOutcome data) {
            try {
                return data != null ? MAPPER.writeValueAsBytes(data) : null;
            } catch (Exception e) {
                throw new RuntimeException("Serialization failed", e);
            }
        }
    }
    
    public static class PaperTradeOutcomeDeserializer implements Deserializer<PaperTradeOutcome> {
        @Override
        public PaperTradeOutcome deserialize(String topic, byte[] bytes) {
            try {
                return bytes != null ? MAPPER.readValue(bytes, PaperTradeOutcome.class) : null;
            } catch (Exception e) {
                throw new RuntimeException("Deserialization failed", e);
            }
        }
    }
}

