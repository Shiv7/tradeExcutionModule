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

    @Value("${quant.signals.min-score:65}")
    private double minScore;

    @Value("${quant.signals.min-confidence:0.6}")
    private double minConfidence;

    /**
     * Consume QuantTradingSignal from Kafka topic
     * NOTE: Changed from deprecated "quant-trading-signals" to unified "trading-signals-v2"
     */
    @KafkaListener(
            topics = "trading-signals-v2",
            groupId = "${quant.signals.group-id:quant-signal-executor-v2}",
            containerFactory = "curatedSignalKafkaListenerContainerFactory"
    )
    public void processQuantSignal(String payload, ConsumerRecord<?, ?> rec) {
        final String topic = rec.topic();
        final int partition = rec.partition();
        final long offset = rec.offset();
        final Instant receivedAt = Instant.now();

        try {
            // Parse signal
            QuantTradingSignal signal = objectMapper.readValue(payload, QuantTradingSignal.class);

            if (signal == null || signal.getScripCode() == null) {
                log.debug("quant_signal_null topic={} partition={} offset={}", topic, partition, offset);
                return;
            }

            String scripCode = signal.getScripCode();
            String signalId = signal.getSignalId();

            // ========== Idempotency Check ==========
            String idKey = "QUANT|" + (signalId != null ? signalId : scripCode + "|" + signal.getTimestamp());
            if (processedSignalsCache.asMap().putIfAbsent(idKey, Boolean.TRUE) != null) {
                log.debug("quant_signal_duplicate key={} scrip={}", idKey, scripCode);
                return;
            }

            // ========== Actionability Check ==========
            if (!signal.isActionable()) {
                log.debug("quant_signal_not_actionable scrip={} reason={}",
                        scripCode, signal.getActionableReason());
                return;
            }

            // ========== Score and Confidence Threshold ==========
            if (signal.getQuantScore() < minScore) {
                log.debug("quant_signal_below_score scrip={} score={} min={}",
                        scripCode, signal.getQuantScore(), minScore);
                return;
            }

            if (signal.getConfidence() < minConfidence) {
                log.debug("quant_signal_below_confidence scrip={} conf={} min={}",
                        scripCode, signal.getConfidence(), minConfidence);
                return;
            }

            // ========== Age Check ==========
            final Instant signalTs = Instant.ofEpochMilli(signal.getTimestamp());
            long ageSeconds = Math.abs(receivedAt.getEpochSecond() - signalTs.getEpochSecond());

            if (ageSeconds > MAX_SIGNAL_AGE_SECONDS) {
                log.info("quant_signal_stale scrip={} age={}s max={}s score={}",
                        scripCode, ageSeconds, MAX_SIGNAL_AGE_SECONDS, signal.getQuantScore());
                return;
            }

            // ========== Trading Hours Check ==========
            final ZonedDateTime receivedIst = receivedAt.atZone(IST);
            String exchange = signal.getExchange();

            if (!tradingHoursService.shouldProcessTrade(exchange, receivedIst.toLocalDateTime())) {
                log.info("quant_signal_outside_hours scrip={} exch={}", scripCode, exchange);
                return;
            }

            // ========== Log Signal Details ==========
            log.info("QUANT_SIGNAL_RECEIVED scrip={} score={:.1f} type={} direction={} entry={:.2f} sl={:.2f} tp={:.2f}",
                    scripCode,
                    signal.getQuantScore(),
                    signal.getSignalType(),
                    signal.getDirection(),
                    signal.getEntryPrice(),
                    signal.getStopLoss(),
                    signal.getTarget1());

            if (signal.getHedging() != null && signal.getHedging().isRecommended()) {
                log.info("   HEDGING: type={} instrument={} ratio={}",
                        signal.getHedging().getHedgeType(),
                        signal.getHedging().getHedgeInstrument(),
                        signal.getHedging().getHedgeRatio());
            }

            if (signal.getGreeksSummary() != null) {
                var greeks = signal.getGreeksSummary();
                log.info("   GREEKS: delta={:.2f} gamma={:.4f} vega={:.2f} gammaRisk={}",
                        greeks.getTotalDelta(),
                        greeks.getTotalGamma(),
                        greeks.getTotalVega(),
                        greeks.isGammaSqueezeRisk());
            }

            // ========== Route to Virtual Engine ==========
            if (enableVirtualTrading) {
                signalRouter.routeSignal(signal);
            } else {
                log.info("quant_virtual_disabled scrip={} score={}", scripCode, signal.getQuantScore());
            }

        } catch (Exception e) {
            log.error("quant_signal_processing_error topic={} partition={} offset={} err={}",
                    topic, partition, offset, e.getMessage(), e);
        }
    }
}
