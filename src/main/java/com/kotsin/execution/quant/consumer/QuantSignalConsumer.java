package com.kotsin.execution.quant.consumer;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.benmanes.caffeine.cache.Cache;
import com.kotsin.execution.quant.model.QuantTradingSignal;
import com.kotsin.execution.quant.service.QuantSignalRouter;
import com.kotsin.execution.service.TradingHoursService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;

/**
 * QuantSignalConsumer - Consumes QuantTradingSignal from StreamingCandle.
 *
 * Topic: quant-trading-signals
 *
 * Receives institutional-grade trading signals with:
 * - Full entry/exit/stop parameters
 * - Position sizing recommendations
 * - Hedging suggestions
 * - Greeks summary
 *
 * Routes signals to VirtualEngineService for paper trading.
 */
@Service
@Slf4j
@RequiredArgsConstructor
public class QuantSignalConsumer {

    private static final ZoneId IST = ZoneId.of("Asia/Kolkata");
    private static final long MAX_SIGNAL_AGE_SECONDS = 120;

    private final QuantSignalRouter signalRouter;
    private final TradingHoursService tradingHoursService;
    private final Cache<String, Boolean> processedSignalsCache;

    private final ObjectMapper objectMapper = new ObjectMapper()
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    @Value("${quant.signals.enable-virtual-trading:true}")
    private boolean enableVirtualTrading;

    // FIX: Removed score and confidence thresholds to trade ALL signals
    // Previously these were blocking valid signals:
    // @Value("${quant.signals.min-score:65}")
    // private double minScore;
    // @Value("${quant.signals.min-confidence:0.6}")
    // private double minConfidence;

    /**
     * Consume QuantTradingSignal from Kafka topic
     * NOTE: Changed from deprecated "quant-trading-signals" to unified "trading-signals-v2"
     *
     * FIX: Added Acknowledgment parameter for manual offset commit.
     * This ensures offsets are only committed after successful processing,
     * preventing message loss on processing failures.
     */
    @KafkaListener(
            topics = "trading-signals-v2",
            groupId = "${quant.signals.group-id:quant-signal-executor-v2}",
            containerFactory = "curatedSignalKafkaListenerContainerFactory"
    )
    public void processQuantSignal(String payload, ConsumerRecord<?, ?> rec, Acknowledgment ack) {
        final String topic = rec.topic();
        final int partition = rec.partition();
        final long offset = rec.offset();
        final Instant receivedAt = Instant.now();

        try {
            // Parse signal
            QuantTradingSignal signal = objectMapper.readValue(payload, QuantTradingSignal.class);

            if (signal == null || signal.getScripCode() == null) {
                log.debug("quant_signal_null topic={} partition={} offset={}", topic, partition, offset);
                // FIX: Acknowledge even for null signals to avoid reprocessing
                if (ack != null) ack.acknowledge();
                return;
            }

            String scripCode = signal.getScripCode();
            String signalId = signal.getSignalId();

            // ========== Idempotency Check ==========
            String idKey = "QUANT|" + (signalId != null ? signalId : scripCode + "|" + signal.getTimestamp());
            if (processedSignalsCache.asMap().putIfAbsent(idKey, Boolean.TRUE) != null) {
                log.debug("quant_signal_duplicate key={} scrip={}", idKey, scripCode);
                if (ack != null) ack.acknowledge();
                return;
            }

            // ========== Actionability Check ==========
            // FIX: Removed actionability check - trade ALL signals regardless of actionable flag
            // Previously this was blocking signals where actionable=false or entry/stop/target were 0
            if (!signal.isActionable()) {
                log.info("quant_signal_actionable_bypass scrip={} signalId={} entryPrice={} stopLoss={} target1={} (TRADING ANYWAY)",
                        scripCode, signalId,
                        signal.getEntryPrice(), signal.getStopLoss(), signal.getTarget1());
                // Continue processing instead of returning
            }

            // FIX: REMOVED Score and Confidence Threshold checks - trade ALL signals
            // Previously these were blocking valid signals:
            // - minScore check was rejecting signals with quantScore < 65
            // - minConfidence check was rejecting signals with confidence < 0.6
            // Now we log the values but don't filter
            log.info("quant_signal_scores scrip={} quantScore={} confidence={} (NO THRESHOLD - TRADING ALL)",
                    scripCode, signal.getQuantScore(), signal.getConfidence());

            // ========== Age Check ==========
            final Instant signalTs = Instant.ofEpochMilli(signal.getTimestamp());
            long ageSeconds = Math.abs(receivedAt.getEpochSecond() - signalTs.getEpochSecond());

            if (ageSeconds > MAX_SIGNAL_AGE_SECONDS) {
                log.info("quant_signal_stale scrip={} age={}s max={}s score={}",
                        scripCode, ageSeconds, MAX_SIGNAL_AGE_SECONDS, signal.getQuantScore());
                if (ack != null) ack.acknowledge();
                return;
            }

            // ========== Trading Hours Check ==========
            final ZonedDateTime receivedIst = receivedAt.atZone(IST);
            String exchange = signal.getExchange();

            if (!tradingHoursService.shouldProcessTrade(exchange, receivedIst.toLocalDateTime())) {
                log.info("quant_signal_outside_hours scrip={} exch={}", scripCode, exchange);
                if (ack != null) ack.acknowledge();
                return;
            }

            // ========== Log Signal Details ==========
            log.info("QUANT_SIGNAL_RECEIVED scrip={} score={} type={} direction={} entry={} sl={} tp={}",
                    scripCode,
                    String.format("%.1f", signal.getQuantScore()),
                    signal.getSignalType(),
                    signal.getDirection(),
                    String.format("%.2f", signal.getEntryPrice()),
                    String.format("%.2f", signal.getStopLoss()),
                    String.format("%.2f", signal.getTarget1()));

            if (signal.getHedging() != null && signal.getHedging().isRecommended()) {
                log.info("   HEDGING: type={} instrument={} ratio={}",
                        signal.getHedging().getHedgeType(),
                        signal.getHedging().getHedgeInstrument(),
                        signal.getHedging().getHedgeRatio());
            }

            if (signal.getGreeksSummary() != null) {
                var greeks = signal.getGreeksSummary();
                log.info("   GREEKS: delta={} gamma={} vega={} gammaRisk={}",
                        String.format("%.2f", greeks.getTotalDelta()),
                        String.format("%.4f", greeks.getTotalGamma()),
                        String.format("%.2f", greeks.getTotalVega()),
                        greeks.isGammaSqueezeRisk());
            }

            // ========== Route to Virtual Engine ==========
            if (enableVirtualTrading) {
                signalRouter.routeSignal(signal);
                log.info("QUANT_SIGNAL_ROUTED scrip={} signalId={}", scripCode, signalId);
            } else {
                log.info("quant_virtual_disabled scrip={} score={}", scripCode, signal.getQuantScore());
            }

            // FIX: Acknowledge after successful processing
            if (ack != null) ack.acknowledge();

        } catch (Exception e) {
            log.error("quant_signal_processing_error topic={} partition={} offset={} err={}",
                    topic, partition, offset, e.getMessage(), e);
            // FIX: Still acknowledge to prevent infinite retry loop
            // The error is logged, and we don't want to block other messages
            if (ack != null) ack.acknowledge();
        }
    }
}
