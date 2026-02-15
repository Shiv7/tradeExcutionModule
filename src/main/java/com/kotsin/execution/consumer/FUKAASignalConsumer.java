package com.kotsin.execution.consumer;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.benmanes.caffeine.cache.Cache;
import com.kotsin.execution.logic.TradeManager;
import com.kotsin.execution.model.BacktestTrade;
import com.kotsin.execution.model.StrategySignal;
import com.kotsin.execution.repository.BacktestTradeRepository;
import com.kotsin.execution.service.BacktestEngine;
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
 * FUKAASignalConsumer - Consumes volume-confirmed FUKAA strategy signals
 *
 * Topic: kotsin_FUKAA
 *
 * FUKAA signals are volume-filtered FUDKII signals that passed the 2x average
 * volume surge check on T-1, T, or T+1 candles. Only IMMEDIATE_PASS and
 * T_PLUS_1_PASS outcomes are published to this topic.
 *
 * Signal Format:
 * - scripCode, companyName, direction (BULLISH/BEARISH)
 * - triggerPrice, triggerTime, triggerScore (0-100)
 * - bbUpper, bbLower, superTrend (for SL/target derivation)
 * - fukaaOutcome (IMMEDIATE_PASS / T_PLUS_1_PASS)
 * - passedCandle (T_MINUS_1 / T / T_PLUS_1)
 * - rank (volume surge magnitude)
 * - volumeT, volumeTMinus1, avgVolume, surgeT, surgeTMinus1
 */
@Service
@Slf4j
@RequiredArgsConstructor
public class FUKAASignalConsumer {

    private static final ZoneId IST = ZoneId.of("Asia/Kolkata");
    private static final long BACKTEST_THRESHOLD_SECONDS = 120;

    private final TradeManager tradeManager;
    private final TradingHoursService tradingHoursService;
    private final BacktestEngine backtestEngine;
    private final BacktestTradeRepository backtestRepository;
    private final Cache<String, Boolean> processedSignalsCache;

    private final ObjectMapper objectMapper = new ObjectMapper()
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    @Value("${trading.mode.live:true}")
    private boolean liveTradeEnabled;

    @Value("${fukaa.min.trigger.score:50.0}")
    private double minTriggerScore;

    @KafkaListener(
            topics = "kotsin_FUKAA",
            groupId = "${app.kafka.consumer.fukaa-group-id:fukaa-executor}",
            containerFactory = "curatedSignalKafkaListenerContainerFactory"
    )
    public void processFUKAASignal(String payload, ConsumerRecord<?, ?> rec) {
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
                log.debug("fukaa_no_scripcode topic={} partition={} offset={}", topic, partition, offset);
                return;
            }

            // Only process triggered signals
            boolean triggered = root.path("triggered").asBoolean(false);
            if (!triggered) {
                log.debug("fukaa_not_triggered scrip={}", scripCode);
                return;
            }

            // ========== Parse FUKAA Signal ==========
            String companyName = root.path("companyName").asText(
                    root.path("symbol").asText(scripCode));
            String direction = root.path("direction").asText("");
            double triggerPrice = root.path("triggerPrice").asDouble(0);
            double triggerScore = root.path("triggerScore").asDouble(0);

            // FUKAA-specific fields
            String fukaaOutcome = root.path("fukaaOutcome").asText("");
            String passedCandle = root.path("passedCandle").asText("");
            double rank = root.path("rank").asDouble(0);

            // BB-SuperTrend components for SL/target derivation
            double bbUpper = root.path("bbUpper").asDouble(0);
            double bbLower = root.path("bbLower").asDouble(0);
            double superTrend = root.path("superTrend").asDouble(0);
            String trend = root.path("trend").asText("");

            // Volume fields (for logging/rationale)
            double surgeT = root.path("surgeT").asDouble(0);
            double surgeTMinus1 = root.path("surgeTMinus1").asDouble(0);

            // Check minimum trigger score threshold
            if (triggerScore < minTriggerScore) {
                log.debug("fukaa_below_threshold scrip={} score={} min={}",
                        scripCode, triggerScore, minTriggerScore);
                return;
            }

            // Parse timestamp from triggerTime ISO string
            String triggerTimeStr = root.path("triggerTime").asText("");
            long timestamp;
            try {
                Instant triggerInstant = Instant.parse(triggerTimeStr);
                timestamp = triggerInstant.toEpochMilli();
            } catch (Exception e) {
                // Fallback: try timestamp field or current time
                timestamp = root.path("timestamp").asLong(System.currentTimeMillis());
            }

            // Determine direction
            if (direction.isEmpty()) {
                direction = "UP".equalsIgnoreCase(trend) ? "BULLISH" : "BEARISH";
            }
            boolean longSignal = "BULLISH".equalsIgnoreCase(direction);
            boolean shortSignal = "BEARISH".equalsIgnoreCase(direction);

            // ========== Read Pivot-Enriched Targets (preferred) or Derive from BB/ST ==========
            double stopLoss = root.path("stopLoss").asDouble(0);
            double target1 = root.path("target1").asDouble(0);
            double target2 = root.path("target2").asDouble(0);
            double riskReward = root.path("riskReward").asDouble(
                    root.path("riskRewardRatio").asDouble(0));
            boolean pivotSource = root.path("pivotSource").asBoolean(false);

            // Derive SL/targets from BB/ST if pivot targets not present
            if (stopLoss <= 0 && triggerPrice > 0) {
                stopLoss = longSignal
                        ? Math.min(bbLower, superTrend)
                        : Math.max(bbUpper, superTrend);
            }
            double risk = Math.abs(triggerPrice - stopLoss);
            if (target1 <= 0 && risk > 0) {
                target1 = longSignal
                        ? triggerPrice + 2 * risk
                        : triggerPrice - 2 * risk;
            }
            if (target2 <= 0 && risk > 0) {
                target2 = longSignal
                        ? triggerPrice + 3 * risk
                        : triggerPrice - 3 * risk;
            }
            if (riskReward <= 0 && risk > 0) {
                riskReward = Math.abs(target1 - triggerPrice) / risk;
            }

            // Validate trade parameters
            if (triggerPrice <= 0 || stopLoss <= 0 || target1 <= 0) {
                log.warn("fukaa_invalid_params scrip={} entry={} sl={} t1={} bbL={} bbU={} st={}",
                        scripCode, triggerPrice, stopLoss, target1, bbLower, bbUpper, superTrend);
                return;
            }

            // ========== Idempotency Check ==========
            String idKey = "FUKAA|" + scripCode + "|" + triggerTimeStr;
            if (processedSignalsCache.asMap().putIfAbsent(idKey, Boolean.TRUE) != null) {
                log.info("fukaa_duplicate key={} scrip={}", idKey, scripCode);
                return;
            }

            // ========== Convert to StrategySignal ==========
            String rationale = String.format("FUKAA: %s via %s | Rank=%.2f Score=%.2f | BB[%.2f-%.2f] ST=%.2f | SurgeT=%.2fx T-1=%.2fx",
                    fukaaOutcome, passedCandle, rank, triggerScore,
                    bbLower, bbUpper, superTrend, surgeT, surgeTMinus1);

            StrategySignal signal = StrategySignal.builder()
                    .scripCode(scripCode)
                    .companyName(companyName)
                    .timestamp(timestamp)
                    .signal("FUKAA_" + (longSignal ? "LONG" : "SHORT"))
                    .confidence(Math.min(1.0, triggerScore / 100.0))
                    .rationale(rationale)
                    .direction(direction)
                    .longSignal(longSignal)
                    .shortSignal(shortSignal)
                    .entryPrice(triggerPrice)
                    .stopLoss(stopLoss)
                    .target1(target1)
                    .target2(target2)
                    .riskRewardRatio(riskReward)
                    .positionSizeMultiplier(1.0)
                    .xfactorFlag(rank >= 10.0)
                    .build();

            signal.parseScripCode();

            // ========== Age Check for Routing ==========
            final Instant signalTs = Instant.ofEpochMilli(timestamp);
            final ZonedDateTime signalTimeIst = signalTs.atZone(IST);
            long ageSeconds = Math.abs(receivedAt.getEpochSecond() - signalTs.getEpochSecond());

            if (ageSeconds > BACKTEST_THRESHOLD_SECONDS) {
                // BACKTEST MODE
                log.info("FUKAA_backtest_mode scrip={} ageSeconds={} score={} outcome={} rank={}",
                        scripCode, ageSeconds, triggerScore, fukaaOutcome, rank);

                BacktestTrade result = backtestEngine.runBacktest(signal, signalTimeIst.toLocalDateTime());
                log.info("fukaa_backtest_complete scrip={} profit={}", result.getScripCode(), result.getProfit());

            } else {
                // LIVE MODE
                log.info("FUKAA_live_mode scrip={} ageSeconds={} score={} outcome={} passedCandle={} rank={}",
                        scripCode, ageSeconds, triggerScore, fukaaOutcome, passedCandle, String.format("%.2f", rank));

                // Check trading hours
                String exchange = signal.getExchange() != null ? signal.getExchange() : "N";
                final ZonedDateTime receivedIst = receivedAt.atZone(IST);

                if (!tradingHoursService.shouldProcessTrade(exchange, receivedIst.toLocalDateTime())) {
                    log.info("fukaa_outside_hours scrip={} exch={}", scripCode, exchange);
                    return;
                }

                // Create virtual trade
                BacktestTrade virtualTrade = BacktestTrade.fromSignal(signal, signalTimeIst.toLocalDateTime());
                virtualTrade.setStatus(BacktestTrade.TradeStatus.ACTIVE);
                virtualTrade.setRationale(rationale);
                backtestRepository.save(virtualTrade);

                log.info("FUKAA_virtual_trade created id={} scrip={} score={} outcome={}",
                        virtualTrade.getId(), scripCode, triggerScore, fukaaOutcome);

                // Forward to TradeManager for execution
                if (liveTradeEnabled) {
                    tradeManager.addSignalToWatchlist(signal, receivedIst.toLocalDateTime());
                }
            }

        } catch (Exception e) {
            log.error("fukaa_processing_error topic={} partition={} offset={} err={}",
                    topic, partition, offset, e.toString(), e);
        }
    }
}
