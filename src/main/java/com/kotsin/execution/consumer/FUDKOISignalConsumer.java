package com.kotsin.execution.consumer;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.benmanes.caffeine.cache.Cache;
import com.kotsin.execution.model.BacktestTrade;
import com.kotsin.execution.model.StrategySignal;
import com.kotsin.execution.repository.BacktestTradeRepository;
import com.kotsin.execution.service.BacktestEngine;
import com.kotsin.execution.service.SignalBufferService;
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
 * FUDKOISignalConsumer - Consumes OI-filtered FUDKII signals.
 *
 * Topic: kotsin_FUDKOI
 *
 * FUDKOI signals are FUDKII signals that passed OI Change% thresholds:
 *   MCX > 100%, NSE > 150%, Currency > 100% (positive only).
 *
 * FUDKOI is independent of FUKAA/FUDKII — both can trade simultaneously.
 * Submitted to SignalBufferService.submitIndependentSignal() which skips
 * Layer 1 (per-scrip dedup) and goes directly to FUDKOI-specific batch
 * with 100% OI ranking.
 */
@Service
@Slf4j
@RequiredArgsConstructor
public class FUDKOISignalConsumer {

    private static final ZoneId IST = ZoneId.of("Asia/Kolkata");
    private static final long BACKTEST_THRESHOLD_SECONDS = 120;

    private final TradingHoursService tradingHoursService;
    private final BacktestEngine backtestEngine;
    private final BacktestTradeRepository backtestRepository;
    private final Cache<String, Boolean> processedSignalsCache;
    private final SignalBufferService signalBufferService;

    private final ObjectMapper objectMapper = new ObjectMapper()
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    @Value("${fudkoi.min.trigger.score:50.0}")
    private double minTriggerScore;

    @KafkaListener(
            topics = "kotsin_FUDKOI",
            groupId = "${app.kafka.consumer.fudkoi-group-id:fudkoi-executor}",
            containerFactory = "curatedSignalKafkaListenerContainerFactory"
    )
    public void processFUDKOISignal(String payload, ConsumerRecord<?, ?> rec, Acknowledgment ack) {
        final String topic = rec.topic();
        final int partition = rec.partition();
        final long offset = rec.offset();
        final Instant receivedAt = Instant.now();

        try {
            JsonNode root = objectMapper.readTree(payload);

            // ========== Extract Core Fields ==========
            String scripCode = root.path("scripCode").asText();
            if (scripCode == null || scripCode.isEmpty()) {
                scripCode = root.path("familyId").asText();
            }

            if (scripCode == null || scripCode.isEmpty()) {
                log.debug("fudkoi_no_scripcode topic={} partition={} offset={}", topic, partition, offset);
                if (ack != null) ack.acknowledge();
                return;
            }

            // Only process triggered signals
            boolean triggered = root.path("triggered").asBoolean(false);
            if (!triggered) {
                log.debug("fudkoi_not_triggered scrip={}", scripCode);
                if (ack != null) ack.acknowledge();
                return;
            }

            // ========== Parse Signal ==========
            String companyName = root.path("companyName").asText(
                    root.path("symbol").asText(scripCode));
            String direction = root.path("direction").asText("");
            double triggerPrice = root.path("triggerPrice").asDouble(0);
            double triggerScore = root.path("triggerScore").asDouble(0);

            // BB-SuperTrend components (for logging/rationale)
            double bbUpper = root.path("bbUpper").asDouble(0);
            double bbLower = root.path("bbLower").asDouble(0);
            double superTrend = root.path("superTrend").asDouble(0);
            String trend = root.path("trend").asText("");

            // OI fields — FUDKOI's primary ranking metric
            double oiChangeRatio = root.path("oiChangeRatio").asDouble(0);
            String oiLabel = root.path("oiLabel").asText("");

            // Volume fields (for logging)
            double surgeT = root.path("surgeT").asDouble(0);
            double volumeT = root.path("volumeT").asDouble(0);

            // Check minimum trigger score threshold
            if (triggerScore < minTriggerScore) {
                log.debug("fudkoi_below_threshold scrip={} score={} min={}",
                        scripCode, triggerScore, minTriggerScore);
                if (ack != null) ack.acknowledge();
                return;
            }

            // Parse timestamp from triggerTime ISO string
            String triggerTimeStr = root.path("triggerTime").asText("");
            long timestamp;
            try {
                Instant triggerInstant = Instant.parse(triggerTimeStr);
                timestamp = triggerInstant.toEpochMilli();
            } catch (Exception e) {
                timestamp = root.path("timestamp").asLong(System.currentTimeMillis());
            }

            // Determine direction
            if (direction.isEmpty()) {
                direction = "UP".equalsIgnoreCase(trend) ? "BULLISH" : "BEARISH";
            }
            boolean longSignal = "BULLISH".equalsIgnoreCase(direction);
            boolean shortSignal = "BEARISH".equalsIgnoreCase(direction);

            // Read pivot-enriched targets from Kafka (computed by PivotTargetCalculator in streaming candle)
            double stopLoss = root.path("stopLoss").asDouble(0);
            double target1 = root.path("target1").asDouble(0);
            double target2 = root.path("target2").asDouble(0);
            double target3 = root.path("target3").asDouble(0);
            double target4 = root.path("target4").asDouble(0);
            double riskReward = root.path("riskReward").asDouble(
                    root.path("riskRewardRatio").asDouble(0));
            boolean pivotSource = root.path("pivotSource").asBoolean(false);
            double atr30m = root.path("atr30m").asDouble(0);

            // Validate trade parameters — reject if pivot data missing
            if (triggerPrice <= 0 || stopLoss <= 0 || target1 <= 0) {
                log.warn("fudkoi_invalid_params scrip={} entry={} sl={} t1={} pivotSource={}",
                        scripCode, triggerPrice, stopLoss, target1, pivotSource);
                if (ack != null) ack.acknowledge();
                return;
            }

            // ========== Idempotency Check ==========
            String idKey = "FUDKOI|" + scripCode + "|" + triggerTimeStr;
            if (processedSignalsCache.asMap().putIfAbsent(idKey, Boolean.TRUE) != null) {
                log.info("fudkoi_duplicate key={} scrip={}", idKey, scripCode);
                if (ack != null) ack.acknowledge();
                return;
            }

            // ========== Build Instrument Display Name ==========
            // tradeExecutionModule trades EQUITY — use plain company name.
            // Option suffix (e.g. "3020 CE") is only for StrategyTradeExecutor option positions.
            String instrumentSymbol = companyName;

            // ========== Convert to StrategySignal ==========
            String rationale = String.format(
                    "FUDKOI: OI=%.1f%% %s | Score=%.2f | BB[%.2f-%.2f] ST=%.2f | SurgeT=%.2fx",
                    oiChangeRatio, oiLabel, triggerScore,
                    bbLower, bbUpper, superTrend, surgeT);

            StrategySignal signal = StrategySignal.builder()
                    .scripCode(scripCode)
                    .companyName(companyName)
                    .instrumentSymbol(instrumentSymbol)
                    .timestamp(timestamp)
                    .signal("FUDKOI_" + (longSignal ? "LONG" : "SHORT"))
                    .confidence(Math.min(1.0, triggerScore / 100.0))
                    .rationale(rationale)
                    .direction(direction)
                    .longSignal(longSignal)
                    .shortSignal(shortSignal)
                    .entryPrice(triggerPrice)
                    .stopLoss(stopLoss)
                    .target1(target1)
                    .target2(target2)
                    .target3(target3)
                    .target4(target4)
                    .riskRewardRatio(riskReward)
                    .pivotSource(pivotSource)
                    .atr30m(atr30m)
                    .oiChangeRatio(oiChangeRatio)
                    .oiLabel(oiLabel)
                    .volumeT(volumeT)
                    .surgeT(surgeT)
                    .positionSizeMultiplier(1.0)
                    .xfactorFlag(oiChangeRatio >= 200)  // Extreme OI = strong conviction
                    .exchange(root.path("exchange").asText("N"))
                    .build();

            signal.parseScripCode();

            // ========== Age Check for Routing ==========
            final Instant signalTs = Instant.ofEpochMilli(timestamp);
            final ZonedDateTime signalTimeIst = signalTs.atZone(IST);
            long ageSeconds = Math.abs(receivedAt.getEpochSecond() - signalTs.getEpochSecond());

            if (ageSeconds > BACKTEST_THRESHOLD_SECONDS) {
                // BACKTEST MODE
                log.info("FUDKOI_backtest_mode scrip={} ageSeconds={} score={} oiRatio={}",
                        scripCode, ageSeconds, triggerScore, oiChangeRatio);

                BacktestTrade result = backtestEngine.runBacktest(signal, signalTimeIst.toLocalDateTime());
                log.info("fudkoi_backtest_complete scrip={} profit={}", result.getScripCode(), result.getProfit());

            } else {
                // LIVE MODE
                log.info("FUDKOI_live_mode scrip={} ageSeconds={} score={} oiRatio={} oiLabel={}",
                        scripCode, ageSeconds, triggerScore, oiChangeRatio, oiLabel);

                // Check trading hours
                String exchange = signal.getExchange() != null ? signal.getExchange() : "N";
                final ZonedDateTime receivedIst = receivedAt.atZone(IST);

                if (!tradingHoursService.shouldProcessTrade(exchange, receivedIst.toLocalDateTime())) {
                    log.info("fudkoi_outside_hours scrip={} exch={}", scripCode, exchange);
                    if (ack != null) ack.acknowledge();
                    return;
                }

                // Create virtual trade as PENDING (will be updated by SignalBufferService)
                BacktestTrade virtualTrade = BacktestTrade.fromSignal(signal, signalTimeIst.toLocalDateTime());
                virtualTrade.setStatus(BacktestTrade.TradeStatus.PENDING);
                virtualTrade.setRationale(rationale);
                backtestRepository.save(virtualTrade);

                log.info("FUDKOI_virtual_trade created id={} scrip={} oiRatio={} → submitting to independent buffer",
                        virtualTrade.getId(), scripCode, oiChangeRatio);

                // Submit to SignalBufferService independent category — skips Layer 1 entirely
                // FUDKOI races independently: doesn't compete with FUKAA/FUDKII
                signalBufferService.submitIndependentSignal("FUDKOI", signal, virtualTrade,
                        rationale, receivedIst.toLocalDateTime());
            }

            // Acknowledge offset
            if (ack != null) ack.acknowledge();

        } catch (Exception e) {
            log.error("fudkoi_processing_error topic={} partition={} offset={} err={}",
                    topic, partition, offset, e.toString(), e);
        } finally {
            // Always acknowledge — even on error, don't replay failed messages forever
            if (ack != null) ack.acknowledge();
        }
    }
}
