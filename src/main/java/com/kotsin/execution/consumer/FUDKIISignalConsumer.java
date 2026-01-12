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
 * FUDKIISignalConsumer - Consumes standalone FUDKII strategy signals
 *
 * Topic: kotsin_FUDKII
 *
 * FUDKII (Fear, Uncertainty, Doubt, Knowledge, Information, Intelligence) is a
 * standalone BB-SuperTrend based ignition strategy that identifies high-probability
 * breakout/breakdown signals.
 *
 * Signal Format:
 * - scripCode, companyName, timestamp
 * - direction: BULLISH/BEARISH
 * - strength: -1.0 to +1.0
 * - entryPrice, stopLoss, target1, target2
 * - bbBand: UPPER/LOWER (Bollinger Band touch)
 * - superTrendDirection: UP/DOWN
 */
@Service
@Slf4j
@RequiredArgsConstructor
public class FUDKIISignalConsumer {

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

    @Value("${fudkii.min.strength:0.55}")
    private double minStrength;

    /**
     * DISABLED: FUDKII signals now flow through unified trading-signals-v2 topic
     * and are processed by QuantSignalConsumer
     */
    // @KafkaListener(
    //         topics = "kotsin_FUDKII",
    //         groupId = "${app.kafka.consumer.fudkii-group-id:fudkii-executor}",
    //         containerFactory = "curatedSignalKafkaListenerContainerFactory"
    // )
    public void processFUDKIISignal(String payload, ConsumerRecord<?, ?> rec) {
        final String topic = rec.topic();
        final int partition = rec.partition();
        final long offset = rec.offset();
        final Instant receivedAt = Instant.now();

        try {
            JsonNode root = objectMapper.readTree(payload);

            // ========== Extract Core Fields ==========
            String scripCode = root.path("scripCode").asText();
            if (scripCode == null || scripCode.isEmpty()) {
                // Try familyId as fallback
                scripCode = root.path("familyId").asText();
            }

            if (scripCode == null || scripCode.isEmpty()) {
                log.debug("fudkii_no_scripcode topic={} partition={} offset={}", topic, partition, offset);
                return;
            }

            // ========== Parse FUDKII Signal ==========
            String companyName = root.path("companyName").asText(scripCode);
            long timestamp = root.path("timestamp").asLong(System.currentTimeMillis());

            // Get FUDKII strength (-1.0 to +1.0)
            double strength = root.path("strength").asDouble(0);

            // Check minimum strength threshold
            if (Math.abs(strength) < minStrength) {
                log.debug("fudkii_below_threshold scrip={} strength={} min={}",
                        scripCode, strength, minStrength);
                return;
            }

            // Direction
            String direction = root.path("direction").asText();
            if (direction == null || direction.isEmpty()) {
                direction = strength > 0 ? "BULLISH" : "BEARISH";
            }
            boolean longSignal = "BULLISH".equalsIgnoreCase(direction) || strength > 0;
            boolean shortSignal = "BEARISH".equalsIgnoreCase(direction) || strength < 0;

            // BB-SuperTrend components
            String bbBand = root.path("bbBand").asText("NEUTRAL");
            String superTrendDirection = root.path("superTrendDirection").asText("");

            // Get entry/exit levels
            double entryPrice = root.path("entryPrice").asDouble(0);
            double stopLoss = root.path("stopLoss").asDouble(0);
            double target1 = root.path("target1").asDouble(0);
            double target2 = root.path("target2").asDouble(0);
            double riskReward = root.path("riskRewardRatio").asDouble(0);

            // Try alternative field names if primary ones are 0
            if (entryPrice <= 0) {
                entryPrice = root.path("price").asDouble(root.path("currentPrice").asDouble(0));
            }
            if (stopLoss <= 0) {
                stopLoss = root.path("sl").asDouble(0);
            }
            if (target1 <= 0) {
                target1 = root.path("tp1").asDouble(root.path("target").asDouble(0));
            }

            // Validate trade parameters
            if (entryPrice <= 0 || stopLoss <= 0 || target1 <= 0) {
                log.warn("fudkii_invalid_params scrip={} entry={} sl={} t1={}",
                        scripCode, entryPrice, stopLoss, target1);
                return;
            }

            // ========== Idempotency Check ==========
            String idKey = "FUDKII|" + scripCode + "|" + timestamp;
            if (processedSignalsCache.asMap().putIfAbsent(idKey, Boolean.TRUE) != null) {
                log.info("fudkii_duplicate key={} scrip={}", idKey, scripCode);
                return;
            }

            // ========== Convert to StrategySignal ==========
            String rationale = String.format("FUDKII: BB=%s ST=%s strength=%.2f",
                    bbBand, superTrendDirection, strength);

            StrategySignal signal = StrategySignal.builder()
                    .scripCode(scripCode)
                    .companyName(companyName)
                    .timestamp(timestamp)
                    .signal("FUDKII_" + (longSignal ? "LONG" : "SHORT"))
                    .confidence(Math.abs(strength))
                    .rationale(rationale)
                    .direction(direction)
                    .longSignal(longSignal)
                    .shortSignal(shortSignal)
                    .entryPrice(entryPrice)
                    .stopLoss(stopLoss)
                    .target1(target1)
                    .target2(target2)
                    .riskRewardRatio(riskReward)
                    .positionSizeMultiplier(1.0)
                    .xfactorFlag(Math.abs(strength) > 0.8)
                    .build();

            signal.parseScripCode();

            // ========== Age Check for Routing ==========
            final Instant signalTs = Instant.ofEpochMilli(timestamp);
            final ZonedDateTime signalTimeIst = signalTs.atZone(IST);
            long ageSeconds = Math.abs(receivedAt.getEpochSecond() - signalTs.getEpochSecond());

            if (ageSeconds > BACKTEST_THRESHOLD_SECONDS) {
                // BACKTEST MODE
                log.info("FUDKII_backtest_mode scrip={} ageSeconds={} strength={} bb={} st={}",
                        scripCode, ageSeconds, strength, bbBand, superTrendDirection);

                BacktestTrade result = backtestEngine.runBacktest(signal, signalTimeIst.toLocalDateTime());
                log.info("fudkii_backtest_complete scrip={} profit={}", result.getScripCode(), result.getProfit());

            } else {
                // LIVE MODE
                log.info("FUDKII_live_mode scrip={} ageSeconds={} strength={} bb={} st={}",
                        scripCode, ageSeconds, String.format("%.3f", strength), bbBand, superTrendDirection);

                // Check trading hours
                String exchange = signal.getExchange() != null ? signal.getExchange() : "N";
                final ZonedDateTime receivedIst = receivedAt.atZone(IST);

                if (!tradingHoursService.shouldProcessTrade(exchange, receivedIst.toLocalDateTime())) {
                    log.info("fudkii_outside_hours scrip={} exch={}", scripCode, exchange);
                    return;
                }

                // Create virtual trade
                BacktestTrade virtualTrade = BacktestTrade.fromSignal(signal, signalTimeIst.toLocalDateTime());
                virtualTrade.setStatus(BacktestTrade.TradeStatus.ACTIVE);
                virtualTrade.setRationale(rationale);
                backtestRepository.save(virtualTrade);

                log.info("FUDKII_virtual_trade created id={} scrip={} strength={}",
                        virtualTrade.getId(), scripCode, strength);

                // Forward to TradeManager for execution
                if (liveTradeEnabled) {
                    tradeManager.addSignalToWatchlist(signal, receivedIst.toLocalDateTime());
                }
            }

        } catch (Exception e) {
            log.error("fudkii_processing_error topic={} partition={} offset={} err={}",
                    topic, partition, offset, e.toString(), e);
        }
    }
}
