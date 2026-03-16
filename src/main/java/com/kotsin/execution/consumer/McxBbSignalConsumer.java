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
 * McxBbSignalConsumer - Consumes MCX Bollinger Band strategy signals
 *
 * Topic: kotsin_MCX_BB
 *
 * MCX-BB is a commodity-focused Bollinger Band breakout strategy targeting
 * MCX instruments. Signals are submitted to SignalBufferService for
 * cross-strategy dedup and fund allocation.
 *
 * Default exchange is MCX ("M") since this strategy exclusively targets
 * commodity instruments.
 */
@Service
@Slf4j
@RequiredArgsConstructor
public class McxBbSignalConsumer {

    private static final ZoneId IST = ZoneId.of("Asia/Kolkata");
    private static final long BACKTEST_THRESHOLD_SECONDS = 120;

    private final TradingHoursService tradingHoursService;
    private final BacktestEngine backtestEngine;
    private final BacktestTradeRepository backtestRepository;
    private final Cache<String, Boolean> processedSignalsCache;
    private final SignalBufferService signalBufferService;

    private final ObjectMapper objectMapper = new ObjectMapper()
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    @Value("${mcxbb.min.trigger.score:40.0}")
    private double minTriggerScore;

    @KafkaListener(
            topics = "kotsin_MCX_BB",
            groupId = "${app.kafka.consumer.mcxbb-group-id:mcxbb-executor}",
            containerFactory = "curatedSignalKafkaListenerContainerFactory"
    )
    public void processMcxBbSignal(String payload, ConsumerRecord<?, ?> rec, Acknowledgment ack) {
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
                log.debug("mcxbb_no_scripcode topic={} partition={} offset={}", topic, partition, offset);
                if (ack != null) ack.acknowledge();
                return;
            }

            // ========== Parse MCX-BB Signal ==========
            String companyName = root.path("companyName").asText(
                    root.path("symbol").asText(scripCode));

            // Only process triggered signals
            boolean triggered = root.path("triggered").asBoolean(false);
            if (!triggered) {
                log.debug("mcxbb_not_triggered scrip={}", scripCode);
                if (ack != null) ack.acknowledge();
                return;
            }

            // Parse triggerTime ISO string to epoch millis (primary), fallback to timestamp field
            String triggerTimeStr = root.path("triggerTime").asText("");
            long timestamp;
            try {
                Instant triggerInstant = Instant.parse(triggerTimeStr);
                timestamp = triggerInstant.toEpochMilli();
            } catch (Exception e) {
                timestamp = root.path("timestamp").asLong(System.currentTimeMillis());
            }

            // Get triggerScore (0-100)
            double triggerScore = root.path("triggerScore").asDouble(0);

            // Check minimum trigger score threshold
            if (triggerScore < minTriggerScore) {
                log.debug("mcxbb_below_threshold scrip={} score={} min={}",
                        scripCode, triggerScore, minTriggerScore);
                if (ack != null) ack.acknowledge();
                return;
            }

            // Direction
            String direction = root.path("direction").asText();
            String trend = root.path("trend").asText("");
            if (direction == null || direction.isEmpty()) {
                direction = "UP".equalsIgnoreCase(trend) ? "BULLISH" : "BEARISH";
            }
            boolean longSignal = "BULLISH".equalsIgnoreCase(direction);
            boolean shortSignal = "BEARISH".equalsIgnoreCase(direction);

            // BB components (for logging/rationale — no SuperTrend for MCX-BB)
            double bbUpper = root.path("bbUpper").asDouble(0);
            double bbLower = root.path("bbLower").asDouble(0);

            // Get entry price (triggerPrice is the primary field in MCX-BB signals)
            double entryPrice = root.path("triggerPrice").asDouble(0);
            if (entryPrice <= 0) {
                entryPrice = root.path("entryPrice").asDouble(
                        root.path("price").asDouble(0));
            }

            // Read pivot-enriched targets from Kafka (computed by PivotTargetCalculator in streaming candle)
            // No BB fallback — trust pivot values as-is. null = DM, 0 = ERR.
            double stopLoss = root.path("stopLoss").asDouble(0);
            double target1 = root.path("target1").asDouble(0);
            double target2 = root.path("target2").asDouble(0);
            double target3 = root.path("target3").asDouble(0);
            double target4 = root.path("target4").asDouble(0);
            double riskReward = root.path("riskReward").asDouble(
                    root.path("riskRewardRatio").asDouble(0));
            boolean pivotSource = root.path("pivotSource").asBoolean(false);
            double atr30m = root.path("atr30m").asDouble(0);

            // OI + Volume fields for cross-instrument ranking
            double oiChangeRatio = root.path("oiChangeRatio").asDouble(0);
            String oiLabel = root.path("oiLabel").asText("");
            double volumeT = root.path("volumeT").asDouble(0);
            double surgeTVal = root.path("surgeT").asDouble(0);
            double blockTradeVol = root.path("blockTradeVol").asDouble(0);
            double blockTradePct = root.path("blockTradePct").asDouble(0);
            double oiBuildupPct = root.path("oiBuildupPct").asDouble(0);

            // Option enrichment fields (from OptionDataEnricher in StreamingCandle)
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
            boolean optionSwapped = root.path("optionSwapped").asBoolean(false);
            boolean optionIsITM = root.path("optionIsITM").asBoolean(false);

            // ========== KII Score (for ranking only, no gate) ==========
            // KII_Score = (|OIChange%| + VolumeSurge%) / 2
            double kiiScore = (Math.abs(oiChangeRatio) + surgeTVal * 100.0) / 2.0;

            log.info("mcxbb_signal_accepted scrip={} OI={}% buildup={}% surge={}x KII={} blockVol={} blockPct={}%",
                    scripCode, oiChangeRatio, oiBuildupPct, surgeTVal, kiiScore, blockTradeVol, blockTradePct);

            // Validate trade parameters — reject if pivot data missing (no trade without proper SL)
            if (entryPrice <= 0 || stopLoss <= 0 || target1 <= 0) {
                log.warn("mcxbb_invalid_params scrip={} entry={} sl={} t1={} pivotSource={}",
                        scripCode, entryPrice, stopLoss, target1, pivotSource);
                if (ack != null) ack.acknowledge();
                return;
            }

            // ========== Idempotency Check ==========
            String idKey = "MCX_BB|" + scripCode + "|" + triggerTimeStr;
            if (processedSignalsCache.asMap().putIfAbsent(idKey, Boolean.TRUE) != null) {
                log.info("mcxbb_duplicate key={} scrip={}", idKey, scripCode);
                if (ack != null) ack.acknowledge();
                return;
            }

            // ========== Build Instrument Display Name ==========
            // tradeExecutionModule trades EQUITY — use plain company name.
            // Option suffix (e.g. "3020 CE") is only for StrategyTradeExecutor option positions.
            String instrumentSymbol = companyName;

            // ========== Convert to StrategySignal ==========
            String rationale = String.format("MCX-BB: %s score=%.0f | BB[%.2f-%.2f] surge=%.1fx oiChg=%.1f%%",
                    direction, triggerScore, bbLower, bbUpper, surgeTVal, oiChangeRatio);

            StrategySignal signal = StrategySignal.builder()
                    .scripCode(scripCode)
                    .companyName(companyName)
                    .instrumentSymbol(instrumentSymbol)
                    .timestamp(timestamp)
                    .signal("MCX_BB_" + (longSignal ? "LONG" : "SHORT"))
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
                    .optionIsITM(optionIsITM)
                    .optionLtpDeferred(root.path("optionLtpDeferred").asBoolean(false))
                    .greekDelta(root.path("greekDelta").asDouble(0))
                    .greekGamma(root.path("greekGamma").asDouble(0))
                    .greekTheta(root.path("greekTheta").asDouble(0))
                    .greekVega(root.path("greekVega").asDouble(0))
                    .greekRho(root.path("greekRho").asDouble(0))
                    .greekIV(root.path("greekIV").asDouble(0))
                    .greekDte(root.path("greekDte").asInt(0))
                    .greekMoneynessType(root.path("greekMoneynessType").asText(null))
                    .greekTheoreticalPrice(root.path("greekTheoreticalPrice").asDouble(0))
                    .greekMispricing(root.path("greekMispricing").asDouble(0))
                    .greekLeverage(root.path("greekLeverage").asDouble(0))
                    .greekTimeValue(root.path("greekTimeValue").asDouble(0))
                    .greekIntrinsicValue(root.path("greekIntrinsicValue").asDouble(0))
                    .greekThetaImpaired(root.path("greekThetaImpaired").asBoolean(false))
                    .greekSlMethod(root.path("greekSlMethod").asText(null))
                    .greekSlIvFloor(root.path("greekSlIvFloor").asDouble(0))
                    .greekGammaBoost(root.path("greekGammaBoost").asDouble(0))
                    .optionSL(root.path("optionSL").asDouble(0))
                    .optionT1(root.path("optionT1").asDouble(0))
                    .optionT2(root.path("optionT2").asDouble(0))
                    .optionT3(root.path("optionT3").asDouble(0))
                    .optionT4(root.path("optionT4").asDouble(0))
                    .optionRR(root.path("optionRR").asDouble(0))
                    .optionRRpassed(root.path("optionRRpassed").asBoolean(false))
                    .optionLotAllocation(root.path("optionLotAllocation").asText(null))
                    .positionSizeMultiplier(1.0)
                    .xfactorFlag(triggerScore >= 80)
                    .exchange(root.path("exchange").asText("M"))
                    .build();

            signal.parseScripCode();

            // ========== Age Check for Routing ==========
            final Instant signalTs = Instant.ofEpochMilli(timestamp);
            final ZonedDateTime signalTimeIst = signalTs.atZone(IST);
            long ageSeconds = Math.abs(receivedAt.getEpochSecond() - signalTs.getEpochSecond());

            if (ageSeconds > BACKTEST_THRESHOLD_SECONDS) {
                // BACKTEST MODE
                log.info("mcxbb_backtest_mode scrip={} ageSeconds={} score={} pivot={}",
                        scripCode, ageSeconds, triggerScore, pivotSource);

                BacktestTrade result = backtestEngine.runBacktest(signal, signalTimeIst.toLocalDateTime());
                log.info("mcxbb_backtest_complete scrip={} profit={}", result.getScripCode(), result.getProfit());

            } else {
                // LIVE MODE
                log.info("mcxbb_live_mode scrip={} ageSeconds={} score={} pivot={}",
                        scripCode, ageSeconds, triggerScore, pivotSource);

                // Check trading hours
                String exchange = signal.getExchange() != null ? signal.getExchange() : "M";
                final ZonedDateTime receivedIst = receivedAt.atZone(IST);

                if (!tradingHoursService.shouldProcessTrade(exchange, receivedIst.toLocalDateTime())) {
                    log.info("mcxbb_outside_hours scrip={} exch={}", scripCode, exchange);
                    if (ack != null) ack.acknowledge();
                    return;
                }

                // Create virtual trade as PENDING (will be updated by SignalBufferService)
                BacktestTrade virtualTrade = BacktestTrade.fromSignal(signal, signalTimeIst.toLocalDateTime());
                virtualTrade.setStatus(BacktestTrade.TradeStatus.PENDING);
                virtualTrade.setRationale(rationale);
                backtestRepository.save(virtualTrade);

                log.info("mcxbb_virtual_trade created id={} scrip={} score={} -> submitting to buffer",
                        virtualTrade.getId(), scripCode, triggerScore);

                // Submit to SignalBufferService for cross-strategy dedup
                signalBufferService.submitSignal("MCX_BB", signal, virtualTrade,
                        rationale, receivedIst.toLocalDateTime());
            }

            // Acknowledge offset — bookmark this message as processed
            if (ack != null) ack.acknowledge();

        } catch (Exception e) {
            log.error("mcxbb_processing_error topic={} partition={} offset={} err={}",
                    topic, partition, offset, e.toString(), e);
        } finally {
            // Always acknowledge — even on error, don't replay failed messages forever
            if (ack != null) ack.acknowledge();
        }
    }
}
