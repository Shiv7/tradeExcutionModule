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
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;

/**
 * PivotConfluenceSignalConsumer - Consumes multi-timeframe pivot confluence signals
 *
 * Topic: pivot-confluence-signals
 *
 * Pivot Confluence is a pure price-action strategy that combines:
 * - HTF bias (Daily/4H) for direction
 * - LTF confirmation (15m/5m) for entry
 * - Multi-timeframe pivot level confluence (daily/weekly/monthly)
 * - CPR analysis, SMC zones, retest detection
 *
 * Supports pause/resume via Redis flag: pivot:auto-trade:paused
 * Min score: 55, Min R:R: 1.2 (configurable)
 * Works on all exchanges: NSE, MCX, Currency
 */
@Service
@Slf4j
@RequiredArgsConstructor
public class PivotConfluenceSignalConsumer {

    private static final ZoneId IST = ZoneId.of("Asia/Kolkata");
    private static final long BACKTEST_THRESHOLD_SECONDS = 120;
    private static final String PAUSE_REDIS_KEY = "pivot:auto-trade:paused";

    private final TradingHoursService tradingHoursService;
    private final BacktestEngine backtestEngine;
    private final BacktestTradeRepository backtestRepository;
    private final Cache<String, Boolean> processedSignalsCache;
    private final SignalBufferService signalBufferService;
    private final RedisTemplate<String, String> redisTemplate;

    private final ObjectMapper objectMapper = new ObjectMapper()
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    @Value("${pivot.min.trigger.score:55.0}")
    private double minTriggerScore;

    @Value("${pivot.min.rr:1.2}")
    private double minRiskReward;

    /**
     * Check if auto-trading is paused via Redis flag.
     */
    public boolean isPaused() {
        try {
            String val = redisTemplate.opsForValue().get(PAUSE_REDIS_KEY);
            return "true".equalsIgnoreCase(val);
        } catch (Exception e) {
            log.warn("pivot_pause_check_failed: {}", e.getMessage());
            return false;
        }
    }

    /**
     * Set pause state.
     */
    public void setPaused(boolean paused) {
        try {
            redisTemplate.opsForValue().set(PAUSE_REDIS_KEY, String.valueOf(paused));
            log.info("pivot_auto_trade paused={}", paused);
        } catch (Exception e) {
            log.error("pivot_pause_set_failed: {}", e.getMessage());
        }
    }

    @KafkaListener(
            topics = "pivot-confluence-signals",
            groupId = "${app.kafka.consumer.pivot-group-id:pivot-executor-v1}",
            containerFactory = "curatedSignalKafkaListenerContainerFactory"
    )
    public void processPivotSignal(String payload, ConsumerRecord<?, ?> rec, Acknowledgment ack) {
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
                log.debug("pivot_no_scripcode topic={} partition={} offset={}", topic, partition, offset);
                if (ack != null) ack.acknowledge();
                return;
            }

            // Only process triggered signals
            boolean triggered = root.path("triggered").asBoolean(false);
            if (!triggered) {
                log.debug("pivot_not_triggered scrip={}", scripCode);
                if (ack != null) ack.acknowledge();
                return;
            }

            // ========== Pause Check ==========
            if (isPaused()) {
                log.info("pivot_paused scrip={} — auto-trade disabled, skipping", scripCode);
                if (ack != null) ack.acknowledge();
                return;
            }

            // ========== Parse Pivot Signal ==========
            String companyName = root.path("companyName").asText(
                    root.path("symbol").asText(scripCode));

            // Parse triggerTime ISO string to epoch millis
            String triggerTimeStr = root.path("triggerTime").asText("");
            long timestamp;
            try {
                Instant triggerInstant = Instant.parse(triggerTimeStr);
                timestamp = triggerInstant.toEpochMilli();
            } catch (Exception e) {
                timestamp = root.path("timestamp").asLong(System.currentTimeMillis());
            }

            // Score & R:R gates
            double score = root.path("score").asDouble(0);
            double riskReward = root.path("riskReward").asDouble(
                    root.path("riskRewardRatio").asDouble(0));

            if (score < minTriggerScore) {
                log.debug("pivot_below_score scrip={} score={} min={}",
                        scripCode, score, minTriggerScore);
                if (ack != null) ack.acknowledge();
                return;
            }

            if (riskReward < minRiskReward) {
                log.debug("pivot_below_rr scrip={} rr={} min={}",
                        scripCode, riskReward, minRiskReward);
                if (ack != null) ack.acknowledge();
                return;
            }

            // Direction
            String direction = root.path("direction").asText("");
            String htfDirection = root.path("htfDirection").asText("");
            if (direction.isEmpty()) {
                direction = htfDirection;
            }
            boolean longSignal = "BULLISH".equalsIgnoreCase(direction);
            boolean shortSignal = "BEARISH".equalsIgnoreCase(direction);

            if (!longSignal && !shortSignal) {
                log.debug("pivot_no_direction scrip={} dir={}", scripCode, direction);
                if (ack != null) ack.acknowledge();
                return;
            }

            // Entry, SL, Targets
            double entryPrice = root.path("entryPrice").asDouble(
                    root.path("pivotCurrentPrice").asDouble(0));
            double stopLoss = root.path("stopLoss").asDouble(0);
            double target1 = root.path("target").asDouble(
                    root.path("target1").asDouble(0));
            double target2 = root.path("target2").asDouble(0);
            double target3 = root.path("target3").asDouble(0);
            double target4 = root.path("target4").asDouble(0);

            // HTF/LTF details (for rationale)
            double htfStrength = root.path("htfStrength").asDouble(0);
            boolean ltfConfirmed = root.path("ltfConfirmed").asBoolean(false);
            double ltfAlignmentScore = root.path("ltfAlignmentScore").asDouble(0);
            int pivotNearbyLevels = root.path("pivotNearbyLevels").asInt(0);
            String cprPosition = root.path("cprPosition").asText("");

            // SMC
            boolean smcInOrderBlock = root.path("smcInOrderBlock").asBoolean(false);
            boolean hasConfirmedRetest = root.path("hasConfirmedRetest").asBoolean(false);
            boolean hasActiveBreakout = root.path("hasActiveBreakout").asBoolean(false);

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

            // Futures fallback fields (MCX instruments without options)
            boolean futuresAvailable = root.path("futuresAvailable").asBoolean(false);
            String futuresScripCode = root.path("futuresScripCode").asText("");
            double futuresLtp = root.path("futuresLtp").asDouble(0);
            int futuresLotSize = root.path("futuresLotSize").asInt(0);
            int futuresMultiplier = root.path("futuresMultiplier").asInt(1);
            String futuresSymbol = root.path("futuresSymbol").asText("");
            String futuresExchange = root.path("futuresExchange").asText("");
            String futuresExchangeType = root.path("futuresExchangeType").asText("");

            // ML enrichment
            boolean mlAvailable = root.path("mlAvailable").asBoolean(false);
            double mlConfidence = root.path("mlConfidence").asDouble(0);
            double positionSizeMultiplier = root.path("mlPositionSizeMultiplier").asDouble(1.0);

            // Validate trade parameters
            if (entryPrice <= 0 || stopLoss <= 0 || target1 <= 0) {
                log.warn("pivot_invalid_params scrip={} entry={} sl={} t1={}",
                        scripCode, entryPrice, stopLoss, target1);
                if (ack != null) ack.acknowledge();
                return;
            }

            // ========== Idempotency Check ==========
            String idKey = "PIVOT|" + scripCode + "|" + triggerTimeStr;
            if (processedSignalsCache.asMap().putIfAbsent(idKey, Boolean.TRUE) != null) {
                log.info("pivot_duplicate key={} scrip={}", idKey, scripCode);
                if (ack != null) ack.acknowledge();
                return;
            }

            String instrumentSymbol = companyName;

            // ========== Convert to StrategySignal ==========
            String rationale = String.format(
                    "PIVOT: %s score=%.0f R:R=%.2f | HTF=%s(%.0f%%) LTF=%s(%.0f) | levels=%d CPR=%s | retest=%s breakout=%s OB=%s",
                    direction, score, riskReward,
                    htfDirection, htfStrength * 100, ltfConfirmed ? "YES" : "NO", ltfAlignmentScore,
                    pivotNearbyLevels, cprPosition,
                    hasConfirmedRetest ? "YES" : "NO",
                    hasActiveBreakout ? "YES" : "NO",
                    smcInOrderBlock ? "YES" : "NO");

            // Use option or futures enrichment for the signal
            // Prefer options, fallback to futures (MCX)
            if (!optionAvailable && futuresAvailable) {
                optionAvailable = true;
                optionScripCode = futuresScripCode;
                optionLtp = futuresLtp;
                optionLotSize = futuresLotSize;
                optionMultiplier = futuresMultiplier;
                optionSymbol = futuresSymbol;
                optionExchange = futuresExchange;
                optionExchangeType = futuresExchangeType;
            }

            StrategySignal signal = StrategySignal.builder()
                    .scripCode(scripCode)
                    .companyName(companyName)
                    .instrumentSymbol(instrumentSymbol)
                    .timestamp(timestamp)
                    .signal("PIVOT_" + (longSignal ? "LONG" : "SHORT"))
                    .confidence(Math.min(1.0, score / 100.0))
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
                    .pivotSource(true)
                    .atr30m(root.path("atr30m").asDouble(0))
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
                    .positionSizeMultiplier(positionSizeMultiplier > 0 ? positionSizeMultiplier : 1.0)
                    .xfactorFlag(score >= 80 && riskReward >= 2.0)
                    .exchange(root.path("exchange").asText("N"))
                    .build();

            signal.parseScripCode();

            log.info("pivot_signal_accepted scrip={} dir={} score={} rr={} htf={} ltf={} levels={} ml={} opt={} fut={}",
                    scripCode, direction, score, riskReward, htfDirection,
                    ltfConfirmed, pivotNearbyLevels, mlAvailable,
                    optionAvailable, futuresAvailable);

            // ========== Age Check for Routing ==========
            final Instant signalTs = Instant.ofEpochMilli(timestamp);
            final ZonedDateTime signalTimeIst = signalTs.atZone(IST);
            long ageSeconds = Math.abs(receivedAt.getEpochSecond() - signalTs.getEpochSecond());

            if (ageSeconds > BACKTEST_THRESHOLD_SECONDS) {
                // BACKTEST MODE
                log.info("PIVOT_backtest_mode scrip={} ageSeconds={} score={} rr={}",
                        scripCode, ageSeconds, score, riskReward);

                BacktestTrade result = backtestEngine.runBacktest(signal, signalTimeIst.toLocalDateTime());
                log.info("pivot_backtest_complete scrip={} profit={}", result.getScripCode(), result.getProfit());

            } else {
                // LIVE MODE
                log.info("PIVOT_live_mode scrip={} ageSeconds={} score={} rr={} exch={}",
                        scripCode, ageSeconds, score, riskReward, signal.getExchange());

                // Check trading hours
                String exchange = signal.getExchange() != null ? signal.getExchange() : "N";
                final ZonedDateTime receivedIst = receivedAt.atZone(IST);

                if (!tradingHoursService.shouldProcessTrade(exchange, receivedIst.toLocalDateTime())) {
                    log.info("pivot_outside_hours scrip={} exch={}", scripCode, exchange);
                    if (ack != null) ack.acknowledge();
                    return;
                }

                // Create virtual trade as PENDING
                BacktestTrade virtualTrade = BacktestTrade.fromSignal(signal, signalTimeIst.toLocalDateTime());
                virtualTrade.setStatus(BacktestTrade.TradeStatus.PENDING);
                virtualTrade.setRationale(rationale);
                backtestRepository.save(virtualTrade);

                log.info("PIVOT_virtual_trade created id={} scrip={} score={} rr={} → submitting to buffer",
                        virtualTrade.getId(), scripCode, score, riskReward);

                // Submit to SignalBufferService for fund allocation & execution
                signalBufferService.submitSignal("PIVOT_CONFLUENCE", signal, virtualTrade,
                        rationale, receivedIst.toLocalDateTime());
            }

            if (ack != null) ack.acknowledge();

        } catch (Exception e) {
            log.error("pivot_processing_error topic={} partition={} offset={} err={}",
                    topic, partition, offset, e.toString(), e);
        } finally {
            if (ack != null) ack.acknowledge();
        }
    }
}
