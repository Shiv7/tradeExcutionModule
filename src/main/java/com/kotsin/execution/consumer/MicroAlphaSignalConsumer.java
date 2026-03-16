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
 * MicroAlphaSignalConsumer - Consumes MicroAlpha conviction-scored signals.
 *
 * Topic: microalpha-signals
 *
 * MicroAlpha uses a 6-component conviction score (flow, OI, gamma, options, session, block)
 * with market-regime-aware mode detection (MEAN_REVERSION, BREAKOUT, etc.).
 */
@Service
@Slf4j
@RequiredArgsConstructor
public class MicroAlphaSignalConsumer {

    private static final ZoneId IST = ZoneId.of("Asia/Kolkata");
    private static final long BACKTEST_THRESHOLD_SECONDS = 120;

    private final TradingHoursService tradingHoursService;
    private final BacktestEngine backtestEngine;
    private final BacktestTradeRepository backtestRepository;
    private final Cache<String, Boolean> processedSignalsCache;
    private final SignalBufferService signalBufferService;

    private final ObjectMapper objectMapper = new ObjectMapper()
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    @Value("${microalpha.min.conviction:12.0}")
    private double minConviction;

    @Value("${microalpha.min.rr:1.0}")
    private double minRiskReward;

    @KafkaListener(
            topics = "microalpha-signals",
            groupId = "${app.kafka.consumer.microalpha-group-id:microalpha-executor}",
            containerFactory = "curatedSignalKafkaListenerContainerFactory"
    )
    public void processMicroAlphaSignal(String payload, ConsumerRecord<?, ?> rec, Acknowledgment ack) {
        final String topic = rec.topic();
        final int partition = rec.partition();
        final long offset = rec.offset();
        final Instant receivedAt = Instant.now();

        try {
            JsonNode root = objectMapper.readTree(payload);

            String scripCode = root.path("scripCode").asText();
            if (scripCode == null || scripCode.isEmpty()) {
                if (ack != null) ack.acknowledge();
                return;
            }

            boolean triggered = root.path("triggered").asBoolean(false);
            if (!triggered) {
                if (ack != null) ack.acknowledge();
                return;
            }

            // Parse core fields
            String companyName = root.path("symbol").asText(scripCode);
            String direction = root.path("direction").asText("");
            double conviction = root.path("absConviction").asDouble(
                    root.path("conviction").asDouble(0));
            double riskReward = root.path("riskReward").asDouble(0);
            double entryPrice = root.path("entryPrice").asDouble(0);
            double stopLoss = root.path("stopLoss").asDouble(0);
            double target1 = root.path("target").asDouble(0);
            String tradingMode = root.path("tradingMode").asText("UNKNOWN");

            // Timestamp
            String triggerTimeStr = root.path("triggerTime").asText("");
            long timestamp;
            try {
                timestamp = Instant.parse(triggerTimeStr).toEpochMilli();
            } catch (Exception e) {
                timestamp = root.path("timestamp").asLong(System.currentTimeMillis());
            }

            // Gates
            if (Math.abs(conviction) < minConviction) {
                log.debug("microalpha_below_conviction scrip={} conv={} min={}",
                        scripCode, conviction, minConviction);
                if (ack != null) ack.acknowledge();
                return;
            }

            boolean longSignal = "BULLISH".equalsIgnoreCase(direction);
            boolean shortSignal = "BEARISH".equalsIgnoreCase(direction);
            if (!longSignal && !shortSignal) {
                if (ack != null) ack.acknowledge();
                return;
            }

            if (entryPrice <= 0 || stopLoss <= 0 || target1 <= 0) {
                log.warn("microalpha_invalid_params scrip={} entry={} sl={} t1={}",
                        scripCode, entryPrice, stopLoss, target1);
                if (ack != null) ack.acknowledge();
                return;
            }

            // Idempotency
            String idKey = "MICROALPHA|" + scripCode + "|" + triggerTimeStr;
            if (processedSignalsCache.asMap().putIfAbsent(idKey, Boolean.TRUE) != null) {
                if (ack != null) ack.acknowledge();
                return;
            }

            // Option enrichment
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
            boolean optionIsITM = root.path("optionIsITM").asBoolean(false);

            // Futures fallback
            boolean futuresAvailable = root.path("futuresAvailable").asBoolean(false);
            if (!optionAvailable && futuresAvailable) {
                optionAvailable = true;
                optionScripCode = root.path("futuresScripCode").asText("");
                optionLtp = root.path("futuresLtp").asDouble(0);
                optionLotSize = root.path("futuresLotSize").asInt(0);
                optionMultiplier = root.path("futuresMultiplier").asInt(1);
                optionSymbol = root.path("futuresSymbol").asText("");
                optionExchange = root.path("futuresExchange").asText("");
                optionExchangeType = root.path("futuresExchangeType").asText("");
            }

            String rationale = String.format(
                    "MICROALPHA: %s mode=%s conv=%.0f%% R:R=%.2f",
                    direction, tradingMode, conviction, riskReward);

            StrategySignal signal = StrategySignal.builder()
                    .scripCode(scripCode)
                    .companyName(companyName)
                    .instrumentSymbol(companyName)
                    .timestamp(timestamp)
                    .signal("MICROALPHA_" + (longSignal ? "LONG" : "SHORT"))
                    .confidence(Math.min(1.0, Math.abs(conviction) / 100.0))
                    .rationale(rationale)
                    .direction(direction)
                    .longSignal(longSignal)
                    .shortSignal(shortSignal)
                    .entryPrice(entryPrice)
                    .stopLoss(stopLoss)
                    .target1(target1)
                    .target2(0)
                    .target3(0)
                    .target4(0)
                    .riskRewardRatio(riskReward)
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
                    .exchange(root.path("exchange").asText("N"))
                    .build();

            signal.parseScripCode();

            log.info("microalpha_signal_accepted scrip={} dir={} mode={} conv={} rr={} opt={} fut={}",
                    scripCode, direction, tradingMode, conviction, riskReward,
                    optionAvailable, futuresAvailable);

            // Age check
            final Instant signalTs = Instant.ofEpochMilli(timestamp);
            final ZonedDateTime signalTimeIst = signalTs.atZone(IST);
            long ageSeconds = Math.abs(receivedAt.getEpochSecond() - signalTs.getEpochSecond());

            if (ageSeconds > BACKTEST_THRESHOLD_SECONDS) {
                BacktestTrade result = backtestEngine.runBacktest(signal, signalTimeIst.toLocalDateTime());
                log.info("microalpha_backtest_complete scrip={} profit={}", result.getScripCode(), result.getProfit());
            } else {
                String exchange = signal.getExchange() != null ? signal.getExchange() : "N";
                final ZonedDateTime receivedIst = receivedAt.atZone(IST);

                if (!tradingHoursService.shouldProcessTrade(exchange, receivedIst.toLocalDateTime())) {
                    log.info("microalpha_outside_hours scrip={} exch={}", scripCode, exchange);
                    if (ack != null) ack.acknowledge();
                    return;
                }

                BacktestTrade virtualTrade = BacktestTrade.fromSignal(signal, signalTimeIst.toLocalDateTime());
                virtualTrade.setStatus(BacktestTrade.TradeStatus.PENDING);
                virtualTrade.setRationale(rationale);
                backtestRepository.save(virtualTrade);

                log.info("microalpha_live scrip={} conv={} rr={} → submitting to buffer",
                        scripCode, conviction, riskReward);

                signalBufferService.submitSignal("MICROALPHA", signal, virtualTrade,
                        rationale, receivedIst.toLocalDateTime());
            }

            if (ack != null) ack.acknowledge();

        } catch (Exception e) {
            log.error("microalpha_processing_error topic={} partition={} offset={} err={}",
                    topic, partition, offset, e.toString(), e);
        } finally {
            if (ack != null) ack.acknowledge();
        }
    }
}
