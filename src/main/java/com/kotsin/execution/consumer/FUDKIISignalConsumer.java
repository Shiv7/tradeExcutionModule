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
import com.kotsin.execution.virtual.PriceProvider;
import com.kotsin.execution.virtual.VirtualEngineService;
import com.kotsin.execution.virtual.VirtualWalletRepository;
import com.kotsin.execution.virtual.model.VirtualOrder;
import com.kotsin.execution.virtual.model.VirtualPosition;
import com.kotsin.execution.virtual.model.VirtualSettings;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Optional;

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
    private final VirtualEngineService virtualEngine;
    private final VirtualWalletRepository walletRepo;
    private final PriceProvider priceProvider;

    private final ObjectMapper objectMapper = new ObjectMapper()
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    @Value("${trading.mode.live:true}")
    private boolean liveTradeEnabled;

    @Value("${fudkii.min.trigger.score:50.0}")
    private double minTriggerScore;

    @KafkaListener(
            topics = "kotsin_FUDKII",
            groupId = "${app.kafka.consumer.fudkii-group-id:fudkii-executor}",
            containerFactory = "curatedSignalKafkaListenerContainerFactory"
    )
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
            String companyName = root.path("companyName").asText(
                    root.path("symbol").asText(scripCode));

            // Only process triggered signals
            boolean triggered = root.path("triggered").asBoolean(false);
            if (!triggered) {
                log.debug("fudkii_not_triggered scrip={}", scripCode);
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

            // Get triggerScore (0-100) -- NOT "strength" which doesn't exist in FUDKII signals
            double triggerScore = root.path("triggerScore").asDouble(0);

            // Check minimum trigger score threshold
            if (triggerScore < minTriggerScore) {
                log.debug("fudkii_below_threshold scrip={} score={} min={}",
                        scripCode, triggerScore, minTriggerScore);
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

            // BB-SuperTrend components (for SL/target fallback if pivot targets missing)
            double bbUpper = root.path("bbUpper").asDouble(0);
            double bbLower = root.path("bbLower").asDouble(0);
            double superTrend = root.path("superTrend").asDouble(0);

            // Get entry price (triggerPrice is the primary field in FUDKII signals)
            double entryPrice = root.path("triggerPrice").asDouble(0);
            if (entryPrice <= 0) {
                entryPrice = root.path("entryPrice").asDouble(
                        root.path("price").asDouble(0));
            }

            // Get pivot-enriched targets (preferred) or derive from BB/ST
            double stopLoss = root.path("stopLoss").asDouble(0);
            double target1 = root.path("target1").asDouble(0);
            double target2 = root.path("target2").asDouble(0);
            double riskReward = root.path("riskReward").asDouble(
                    root.path("riskRewardRatio").asDouble(0));
            boolean pivotSource = root.path("pivotSource").asBoolean(false);

            // Derive SL/targets from BB/ST if pivot targets not present
            if (stopLoss <= 0 && entryPrice > 0) {
                stopLoss = longSignal
                        ? Math.min(bbLower, superTrend)
                        : Math.max(bbUpper, superTrend);
            }
            double risk = Math.abs(entryPrice - stopLoss);
            if (target1 <= 0 && risk > 0) {
                target1 = longSignal
                        ? entryPrice + 2 * risk
                        : entryPrice - 2 * risk;
            }
            if (target2 <= 0 && risk > 0) {
                target2 = longSignal
                        ? entryPrice + 3 * risk
                        : entryPrice - 3 * risk;
            }
            if (riskReward <= 0 && risk > 0) {
                riskReward = Math.abs(target1 - entryPrice) / risk;
            }

            // Validate trade parameters
            if (entryPrice <= 0 || stopLoss <= 0 || target1 <= 0) {
                log.warn("fudkii_invalid_params scrip={} entry={} sl={} t1={} bbL={} bbU={} st={}",
                        scripCode, entryPrice, stopLoss, target1, bbLower, bbUpper, superTrend);
                return;
            }

            // ========== Idempotency Check ==========
            String idKey = "FUDKII|" + scripCode + "|" + triggerTimeStr;
            if (processedSignalsCache.asMap().putIfAbsent(idKey, Boolean.TRUE) != null) {
                log.info("fudkii_duplicate key={} scrip={}", idKey, scripCode);
                return;
            }

            // ========== Convert to StrategySignal ==========
            String rationale = String.format("FUDKII: %s score=%.0f | BB[%.2f-%.2f] ST=%.2f | pivot=%s",
                    direction, triggerScore, bbLower, bbUpper, superTrend, pivotSource);

            StrategySignal signal = StrategySignal.builder()
                    .scripCode(scripCode)
                    .companyName(companyName)
                    .timestamp(timestamp)
                    .signal("FUDKII_" + (longSignal ? "LONG" : "SHORT"))
                    .confidence(Math.min(1.0, triggerScore / 100.0))
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
                    .xfactorFlag(triggerScore >= 80)
                    .build();

            signal.parseScripCode();

            // ========== Age Check for Routing ==========
            final Instant signalTs = Instant.ofEpochMilli(timestamp);
            final ZonedDateTime signalTimeIst = signalTs.atZone(IST);
            long ageSeconds = Math.abs(receivedAt.getEpochSecond() - signalTs.getEpochSecond());

            if (ageSeconds > BACKTEST_THRESHOLD_SECONDS) {
                // BACKTEST MODE
                log.info("FUDKII_backtest_mode scrip={} ageSeconds={} score={} pivot={}",
                        scripCode, ageSeconds, triggerScore, pivotSource);

                BacktestTrade result = backtestEngine.runBacktest(signal, signalTimeIst.toLocalDateTime());
                log.info("fudkii_backtest_complete scrip={} profit={}", result.getScripCode(), result.getProfit());

            } else {
                // LIVE MODE
                log.info("FUDKII_live_mode scrip={} ageSeconds={} score={} pivot={}",
                        scripCode, ageSeconds, triggerScore, pivotSource);

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

                log.info("FUDKII_virtual_trade created id={} scrip={} score={}",
                        virtualTrade.getId(), scripCode, triggerScore);

                // Forward to TradeManager for execution
                if (liveTradeEnabled) {
                    tradeManager.addSignalToWatchlist(signal, receivedIst.toLocalDateTime());
                }

                // SWITCH detection + Paper trade via VirtualEngineService
                handlePaperTrade(signal, scripCode, companyName, longSignal, "FUDKII");
            }

        } catch (Exception e) {
            log.error("fudkii_processing_error topic={} partition={} offset={} err={}",
                    topic, partition, offset, e.toString(), e);
        }
    }

    private void handlePaperTrade(StrategySignal signal, String scripCode,
                                  String companyName, boolean longSignal, String source) {
        try {
            String numericScrip = signal.getNumericScripCode() != null ? signal.getNumericScripCode() : scripCode;

            // SWITCH detection: if opposite position exists, close it first
            Optional<VirtualPosition> existingPos = walletRepo.getPosition(numericScrip)
                    .filter(p -> p.getQtyOpen() > 0);
            if (existingPos.isPresent()) {
                VirtualPosition pos = existingPos.get();
                boolean existingIsLong = pos.getSide() == VirtualPosition.Side.LONG;
                if (existingIsLong != longSignal) {
                    log.info("{}_SWITCH scrip={} oldSide={} newSide={}", source, numericScrip,
                             pos.getSide(), longSignal ? "LONG" : "SHORT");
                    virtualEngine.closePosition(numericScrip);
                } else {
                    log.debug("{}_same_direction_skip scrip={}", source, numericScrip);
                    return;
                }
            }

            // Create paper trade
            double price = signal.getEntryPrice();
            Double ltp = priceProvider.getLtp(numericScrip);
            if (ltp != null && ltp > 0) price = ltp;

            VirtualSettings settings = walletRepo.loadSettings();
            int qty = price > 0 ? (int) Math.floor(settings.getAccountValue() * 0.02 / price) : 1;
            if (qty < 1) qty = 1;

            VirtualOrder order = new VirtualOrder();
            order.setScripCode(numericScrip);
            order.setSide(longSignal ? VirtualOrder.Side.BUY : VirtualOrder.Side.SELL);
            order.setType(VirtualOrder.Type.MARKET);
            order.setQty(qty);
            order.setCurrentPrice(price);
            order.setSl(signal.getStopLoss());
            order.setTp1(signal.getTarget1());
            order.setTp2(signal.getTarget2());
            order.setTp1ClosePercent(0.5);  // 50% at T1
            order.setTrailingType("PCT");
            order.setTrailingValue(1.0);    // 1% trail after T1
            order.setTrailingStep(0.5);
            order.setSignalId(signal.getSignal() + "_" + numericScrip + "_" + signal.getTimestamp());
            order.setSignalType(signal.getSignal());
            order.setSignalSource(source);

            VirtualOrder executed = virtualEngine.createOrder(order);
            log.info("{}_paper_trade scrip={} status={} qty={} entry={} SL={} T1={}",
                     source, numericScrip, executed.getStatus(), qty, price,
                     signal.getStopLoss(), signal.getTarget1());
        } catch (Exception e) {
            log.error("{}_paper_trade_error scrip={} err={}", source, scripCode, e.getMessage());
        }
    }
}
