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
 * Topic: trading-signals-v2
 *
 * Implements 3-layer quality gate:
 * 1. Quality Gate: quantScore, confidence, exhaustion thresholds
 * 2. Hard Gate: risk-reward ratio validation
 * 3. MTF Gate: family alignment and trend confirmation
 *
 * Routes validated signals to VirtualEngineService for paper trading.
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

    // ========== Quality Gate Configuration ==========
    @Value("${quant.gates.enabled:true}")
    private boolean gatesEnabled;

    @Value("${quant.gates.quality.enabled:true}")
    private boolean qualityGateEnabled;

    @Value("${quant.gates.quality.min-quant-score:50}")
    private double minQuantScore;

    @Value("${quant.gates.quality.min-confidence:0.55}")
    private double minConfidence;

    @Value("${quant.gates.quality.max-exhaustion:0.75}")
    private double maxExhaustion;

    // ========== Hard Gate Configuration ==========
    @Value("${quant.gates.hard.enabled:true}")
    private boolean hardGateEnabled;

    @Value("${quant.gates.hard.min-risk-reward:1.5}")
    private double minRiskReward;

    // ========== MTF Gate Configuration ==========
    @Value("${quant.gates.mtf.enabled:true}")
    private boolean mtfGateEnabled;

    @Value("${quant.gates.mtf.min-family-alignment:0.60}")
    private double minFamilyAlignment;

    @Value("${quant.gates.mtf.require-trend-alignment:true}")
    private boolean requireTrendAlignment;

    /**
     * Consume QuantTradingSignal from Kafka topic
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
            if (!signal.isActionable()) {
                log.info("quant_signal_skipped_not_actionable scrip={} signalId={} entry={} sl={} tp={} actionable={} (WATCH signals have no prices yet)",
                        scripCode, signalId,
                        signal.getEntryPrice(), signal.getStopLoss(), signal.getTarget1(), signal.isActionable());
                if (ack != null) ack.acknowledge();
                return;
            }

            // ========== 3-LAYER QUALITY GATES ==========
            if (gatesEnabled) {
                String rejection = applyQualityGates(signal);
                if (rejection != null) {
                    log.info("QUANT_SIGNAL_REJECTED scrip={} score={} confidence={} reason={}",
                            scripCode,
                            String.format("%.1f", signal.getQuantScore()),
                            String.format("%.2f", signal.getConfidence()),
                            rejection);
                    if (ack != null) ack.acknowledge();
                    return;
                }
            }

            // ========== Age Check ==========
            long rawTs = signal.getTimestamp();
            if (rawTs > 0) {
                long tsMillis = rawTs < 1_000_000_000_000L ? rawTs * 1000 : rawTs;
                final Instant signalTs = Instant.ofEpochMilli(tsMillis);
                long ageSeconds = Math.abs(receivedAt.getEpochSecond() - signalTs.getEpochSecond());

                if (ageSeconds > MAX_SIGNAL_AGE_SECONDS) {
                    log.info("quant_signal_stale scrip={} age={}s max={}s score={}",
                            scripCode, ageSeconds, MAX_SIGNAL_AGE_SECONDS, signal.getQuantScore());
                    if (ack != null) ack.acknowledge();
                    return;
                }
            } else {
                log.debug("quant_signal_no_timestamp scrip={} - skipping age check", scripCode);
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
            log.info("QUANT_SIGNAL_ACCEPTED scrip={} score={} confidence={} type={} direction={} entry={} sl={} tp={} rr={}",
                    scripCode,
                    String.format("%.1f", signal.getQuantScore()),
                    String.format("%.2f", signal.getConfidence()),
                    signal.getSignalType(),
                    signal.getDirection(),
                    String.format("%.2f", signal.getEntryPrice()),
                    String.format("%.2f", signal.getStopLoss()),
                    String.format("%.2f", signal.getTarget1()),
                    String.format("%.2f", signal.getRiskRewardRatio()));

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

            // Acknowledge after successful processing
            if (ack != null) ack.acknowledge();

        } catch (Exception e) {
            log.error("quant_signal_processing_error topic={} partition={} offset={} err={}",
                    topic, partition, offset, e.getMessage(), e);
            if (ack != null) ack.acknowledge();
        }
    }

    /**
     * Apply 3-layer quality gates to the signal.
     *
     * @return null if signal passes all gates, or rejection reason string
     */
    private String applyQualityGates(QuantTradingSignal signal) {

        // ====== GATE 1: Quality Gate ======
        if (qualityGateEnabled) {
            double quantScore = signal.getQuantScore();
            double confidence = signal.getConfidence();

            if (quantScore < minQuantScore) {
                return String.format("QUALITY_GATE: quantScore=%.1f < min=%.1f", quantScore, minQuantScore);
            }

            if (confidence < minConfidence) {
                return String.format("QUALITY_GATE: confidence=%.2f < min=%.2f", confidence, minConfidence);
            }

            // Check quality score (0-100, complementary to quantScore)
            int qualityScore = signal.getQualityScore();
            if (qualityScore > 0 && qualityScore < 40) {
                return String.format("QUALITY_GATE: qualityScore=%d < min=40", qualityScore);
            }
        }

        // ====== GATE 2: Hard Gate - Risk/Reward Validation ======
        if (hardGateEnabled) {
            double riskReward = signal.getRiskRewardRatio();
            if (riskReward > 0 && riskReward < minRiskReward) {
                return String.format("HARD_GATE: riskReward=%.2f < min=%.2f", riskReward, minRiskReward);
            }

            // Validate price levels make sense
            double entry = signal.getEntryPrice();
            double sl = signal.getStopLoss();
            double tp = signal.getTarget1();

            if (entry <= 0 || sl <= 0 || tp <= 0) {
                return "HARD_GATE: invalid price levels (zero or negative)";
            }

            String direction = signal.getDirection();
            if ("LONG".equalsIgnoreCase(direction)) {
                if (sl >= entry) {
                    return String.format("HARD_GATE: LONG but SL=%.2f >= entry=%.2f", sl, entry);
                }
                if (tp <= entry) {
                    return String.format("HARD_GATE: LONG but TP=%.2f <= entry=%.2f", tp, entry);
                }
            } else if ("SHORT".equalsIgnoreCase(direction)) {
                if (sl <= entry) {
                    return String.format("HARD_GATE: SHORT but SL=%.2f <= entry=%.2f", sl, entry);
                }
                if (tp >= entry) {
                    return String.format("HARD_GATE: SHORT but TP=%.2f >= entry=%.2f", tp, entry);
                }
            }
        }

        // ====== GATE 3: MTF Gate - Multi-Timeframe Alignment ======
        if (mtfGateEnabled) {
            // Check family alignment (equity + futures + options should agree)
            Double familyAlignment = signal.getFamilyAlignment();
            if (familyAlignment != null && familyAlignment > 0) {
                double alignmentRatio = familyAlignment / 100.0; // Convert 0-100 to 0-1
                if (alignmentRatio < minFamilyAlignment) {
                    return String.format("MTF_GATE: familyAlignment=%.0f%% < min=%.0f%%",
                            familyAlignment, minFamilyAlignment * 100);
                }
            }

            // Check trend alignment with higher timeframe
            if (requireTrendAlignment) {
                String direction = signal.getDirection();
                String superTrend = signal.getSuperTrendDirection();
                String familyBias = signal.getFamilyBias();

                // If SuperTrend data is available, signal direction must align
                if (superTrend != null && !superTrend.isEmpty() && direction != null) {
                    boolean trendAligned = false;
                    if ("LONG".equalsIgnoreCase(direction) &&
                            ("BULLISH".equalsIgnoreCase(superTrend) || "UP".equalsIgnoreCase(superTrend))) {
                        trendAligned = true;
                    } else if ("SHORT".equalsIgnoreCase(direction) &&
                            ("BEARISH".equalsIgnoreCase(superTrend) || "DOWN".equalsIgnoreCase(superTrend))) {
                        trendAligned = true;
                    }

                    if (!trendAligned) {
                        return String.format("MTF_GATE: direction=%s vs superTrend=%s (misaligned)",
                                direction, superTrend);
                    }
                }

                // If family bias is available, signal direction must align
                if (familyBias != null && !familyBias.isEmpty() &&
                        !"NEUTRAL".equalsIgnoreCase(familyBias) && direction != null) {
                    boolean familyAligned = false;
                    if ("LONG".equalsIgnoreCase(direction) &&
                            (familyBias.toUpperCase().contains("BULLISH"))) {
                        familyAligned = true;
                    } else if ("SHORT".equalsIgnoreCase(direction) &&
                            (familyBias.toUpperCase().contains("BEARISH"))) {
                        familyAligned = true;
                    }

                    if (!familyAligned) {
                        return String.format("MTF_GATE: direction=%s vs familyBias=%s (misaligned)",
                                direction, familyBias);
                    }
                }
            }
        }

        // All gates passed
        return null;
    }
}
