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
 * MereSignalConsumer - Consumes MERE (Mean Exhaustion Reversion Engine) strategy signals
 *
 * Topic: kotsin_MERE
 *
 * MERE is a multi-phase mean reversion strategy that identifies price exhaustion at
 * BB extremes via 3 phases: SCAN (M30 BB gate) → QUALIFY (5-layer scoring) → TRIGGER (M15/M5 confirm).
 *
 * Signals are submitted as independent signals to SignalBufferService (like FUDKOI)
 * since MERE trades counter-trend setups that should not compete with trend-following strategies.
 */
@Service
@Slf4j
@RequiredArgsConstructor
public class MereSignalConsumer {

    private static final ZoneId IST = ZoneId.of("Asia/Kolkata");
    private static final long BACKTEST_THRESHOLD_SECONDS = 120;

    private final TradingHoursService tradingHoursService;
    private final BacktestEngine backtestEngine;
    private final BacktestTradeRepository backtestRepository;
    private final Cache<String, Boolean> processedSignalsCache;
    private final SignalBufferService signalBufferService;

    private final ObjectMapper objectMapper = new ObjectMapper()
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    @Value("${mere.min.trigger.score:60.0}")
    private double minTriggerScore;

    @KafkaListener(
            topics = "kotsin_MERE",
            groupId = "${app.kafka.consumer.mere-group-id:mere-executor}",
            containerFactory = "curatedSignalKafkaListenerContainerFactory"
    )
    public void processMERESignal(String payload, ConsumerRecord<?, ?> rec, Acknowledgment ack) {
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
                log.debug("mere_no_scripcode topic={} partition={} offset={}", topic, partition, offset);
                if (ack != null) ack.acknowledge();
                return;
            }

            // ========== Parse MERE Signal ==========
            // Read variant (MERE_SCALP, MERE_SWING, MERE_POSITIONAL) from payload
            String mereVariant = root.path("strategy").asText("MERE");
            if (!mereVariant.startsWith("MERE")) mereVariant = "MERE";

            String companyName = root.path("companyName").asText(
                    root.path("symbol").asText(scripCode));

            boolean triggered = root.path("triggered").asBoolean(false);
            if (!triggered) {
                log.debug("mere_not_triggered scrip={}", scripCode);
                if (ack != null) ack.acknowledge();
                return;
            }

            // Parse triggerTime
            String triggerTimeStr = root.path("triggerTime").asText("");
            long timestamp;
            try {
                Instant triggerInstant = Instant.parse(triggerTimeStr);
                timestamp = triggerInstant.toEpochMilli();
            } catch (Exception e) {
                timestamp = root.path("timestamp").asLong(System.currentTimeMillis());
            }

            // MERE score (uses triggerScore or mereScore)
            double triggerScore = root.path("triggerScore").asDouble(
                    root.path("mereScore").asDouble(0));

            // Check autoExecute flag — WATCHING signals (score 30-84) are display-only
            boolean autoExecute = root.path("autoExecute").asBoolean(true);
            String tradeStatus = root.path("tradeStatus").asText("ACTIVE");

            if (!autoExecute) {
                log.info("mere_watching_only scrip={} score={} status={}",
                        scripCode, triggerScore, tradeStatus);
                if (ack != null) ack.acknowledge();
                return;
            }

            if (triggerScore < minTriggerScore) {
                log.debug("mere_below_threshold scrip={} score={} min={}",
                        scripCode, triggerScore, minTriggerScore);
                if (ack != null) ack.acknowledge();
                return;
            }

            // Direction
            String direction = root.path("direction").asText();
            if (direction == null || direction.isEmpty()) {
                String trend = root.path("trend").asText("");
                direction = "UP".equalsIgnoreCase(trend) ? "BULLISH" : "BEARISH";
            }
            boolean longSignal = "BULLISH".equalsIgnoreCase(direction);
            boolean shortSignal = "BEARISH".equalsIgnoreCase(direction);

            // Trade parameters
            double entryPrice = root.path("triggerPrice").asDouble(0);
            if (entryPrice <= 0) {
                entryPrice = root.path("entryPrice").asDouble(
                        root.path("price").asDouble(0));
            }

            double stopLoss = root.path("stopLoss").asDouble(
                    root.path("equitySl").asDouble(0));
            double target1 = root.path("target1").asDouble(
                    root.path("equityT1").asDouble(0));
            double target2 = root.path("target2").asDouble(
                    root.path("equityT2").asDouble(0));
            double target3 = root.path("target3").asDouble(0);
            double target4 = root.path("target4").asDouble(0);
            double riskReward = root.path("riskReward").asDouble(
                    root.path("riskRewardRatio").asDouble(0));
            boolean pivotSource = root.path("pivotSource").asBoolean(false);
            double atr30m = root.path("atr30m").asDouble(0);

            // OI + Volume fields
            double oiChangeRatio = root.path("oiChangeRatio").asDouble(
                    root.path("oiChangePct").asDouble(0));
            String oiLabel = root.path("oiLabel").asText(
                    root.path("oiInterpretation").asText(""));
            double volumeT = root.path("volumeT").asDouble(0);
            double surgeTVal = root.path("surgeT").asDouble(0);
            double blockTradeVol = root.path("blockTradeVol").asDouble(0);
            double blockTradePct = root.path("blockTradePct").asDouble(0);
            double oiBuildupPct = root.path("oiBuildupPct").asDouble(0);

            // Option enrichment fields
            boolean optionAvailable = root.path("optionAvailable").asBoolean(false);
            String optionScripCode = root.path("optionScripCode").asText("");
            double optionStrike = root.path("optionStrike").asDouble(0);
            String optionType = root.path("optionType").asText("");
            double optionLtp = root.path("optionLtp").asDouble(0);
            String optionExpiry = root.path("optionExpiry").asText("");
            int optionLotSize = root.path("optionLotSize").asInt(0);
            int optionMultiplier = root.path("optionMultiplier").asInt(1);
            String optionSymbol = root.path("optionSymbol").asText("");
            String optionExchange = root.path("optionExchange").asText("");
            String optionExchangeType = root.path("optionExchangeType").asText("");

            // ========== KII Score (for ranking) ==========
            double kiiScore = (Math.abs(oiChangeRatio) + surgeTVal * 100.0) / 2.0;

            // MERE-specific score breakdown
            int mereL1 = root.path("mereL1Extension").asInt(root.path("mereLayer1").asInt(0));
            int mereL2 = root.path("mereL2Exhaustion").asInt(root.path("mereLayer2").asInt(0));
            int mereL3 = root.path("mereL3Options").asInt(root.path("mereLayer3").asInt(0));
            String mereReasons = root.path("mereReasons").asText("");
            String entryReason = root.path("entryReason").asText("");
            String confirmReasons = root.path("confirmReasons").asText("");

            log.info("mere_signal_accepted scrip={} score={} L1={} L2={} L3={} OI={}% surge={}x KII={} entry={} reason={}",
                    scripCode, triggerScore, mereL1, mereL2, mereL3,
                    oiChangeRatio, surgeTVal, kiiScore, entryReason, confirmReasons);

            // Validate trade parameters
            if (entryPrice <= 0 || stopLoss <= 0 || target1 <= 0) {
                log.warn("mere_invalid_params scrip={} entry={} sl={} t1={}",
                        scripCode, entryPrice, stopLoss, target1);
                if (ack != null) ack.acknowledge();
                return;
            }

            // ========== Idempotency Check ==========
            String idKey = "MERE|" + scripCode + "|" + triggerTimeStr;
            if (processedSignalsCache.asMap().putIfAbsent(idKey, Boolean.TRUE) != null) {
                log.info("mere_duplicate key={} scrip={}", idKey, scripCode);
                if (ack != null) ack.acknowledge();
                return;
            }

            // ========== Convert to StrategySignal ==========
            String rationale = String.format("MERE: %s score=%.0f | L1=%d L2=%d L3=%d | %s | %s",
                    direction, triggerScore, mereL1, mereL2, mereL3,
                    entryReason, mereReasons);

            StrategySignal signal = StrategySignal.builder()
                    .scripCode(scripCode)
                    .companyName(companyName)
                    .instrumentSymbol(companyName)
                    .timestamp(timestamp)
                    .signal("MERE_" + (longSignal ? "LONG" : "SHORT"))
                    .confidence(Math.min(1.0, triggerScore / 100.0))
                    .rationale(rationale)
                    .direction(direction)
                    .longSignal(longSignal)
                    .shortSignal(shortSignal)
                    .entryPrice(entryPrice)
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
                    .surgeT(surgeTVal)
                    .kiiScore(kiiScore)
                    .blockTradeVol(blockTradeVol)
                    .blockTradePct(blockTradePct)
                    .oiBuildupPct(oiBuildupPct)
                    .optionAvailable(optionAvailable)
                    .optionScripCode(optionScripCode)
                    .optionStrike(optionStrike)
                    .optionType(optionType)
                    .optionLtp(optionLtp)
                    .optionExpiry(optionExpiry)
                    .optionLotSize(optionLotSize)
                    .optionMultiplier(optionMultiplier)
                    .optionSymbol(optionSymbol)
                    .optionExchange(optionExchange)
                    .optionExchangeType(optionExchangeType)
                    .positionSizeMultiplier(1.0)
                    .xfactorFlag(triggerScore >= 80)
                    .exchange(root.path("exchange").asText("N"))
                    .build();

            signal.parseScripCode();

            // ========== Age Check for Routing ==========
            final Instant signalTs = Instant.ofEpochMilli(timestamp);
            final ZonedDateTime signalTimeIst = signalTs.atZone(IST);
            long ageSeconds = Math.abs(receivedAt.getEpochSecond() - signalTs.getEpochSecond());

            if (ageSeconds > BACKTEST_THRESHOLD_SECONDS) {
                // BACKTEST MODE
                log.info("MERE_backtest_mode scrip={} ageSeconds={} score={}",
                        scripCode, ageSeconds, triggerScore);

                BacktestTrade result = backtestEngine.runBacktest(signal, signalTimeIst.toLocalDateTime());
                log.info("mere_backtest_complete scrip={} profit={}", result.getScripCode(), result.getProfit());

            } else {
                // LIVE MODE
                log.info("MERE_live_mode scrip={} ageSeconds={} score={}",
                        scripCode, ageSeconds, triggerScore);

                String exchange = signal.getExchange() != null ? signal.getExchange() : "N";
                final ZonedDateTime receivedIst = receivedAt.atZone(IST);

                if (!tradingHoursService.shouldProcessTrade(exchange, receivedIst.toLocalDateTime())) {
                    log.info("mere_outside_hours scrip={} exch={}", scripCode, exchange);
                    if (ack != null) ack.acknowledge();
                    return;
                }

                // Create virtual trade as PENDING
                BacktestTrade virtualTrade = BacktestTrade.fromSignal(signal, signalTimeIst.toLocalDateTime());
                virtualTrade.setStatus(BacktestTrade.TradeStatus.PENDING);
                virtualTrade.setRationale(rationale);
                backtestRepository.save(virtualTrade);

                log.info("MERE_virtual_trade created id={} scrip={} score={} → submitting to buffer",
                        virtualTrade.getId(), scripCode, triggerScore);

                // Submit as independent signal (MERE is counter-trend, should not compete with FUDKII/FUKAA)
                signalBufferService.submitIndependentSignal(mereVariant, signal, virtualTrade,
                        rationale, receivedIst.toLocalDateTime());
            }

            if (ack != null) ack.acknowledge();

        } catch (Exception e) {
            log.error("mere_processing_error topic={} partition={} offset={} err={}",
                    topic, partition, offset, e.toString(), e);
        } finally {
            if (ack != null) ack.acknowledge();
        }
    }
}
