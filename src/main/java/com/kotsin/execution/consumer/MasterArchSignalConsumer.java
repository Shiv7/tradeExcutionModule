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
 * MasterArchSignalConsumer - Consumes signals from MASTER ARCHITECTURE
 * 
 * Topic: score-final-opportunity
 * 
 * This consumer handles FinalOpportunityScore from the new Master Architecture:
 * - Normalized scores [-1.0, +1.0]
 * - Trade decisions: ENTER_NOW, WATCHLIST, MONITOR, REJECT
 * - Position sizing: 0-2 lots with hedge recommendations
 * 
 * Converts Master Arch signals to StrategySignal format for unified trade execution.
 */
@Service
@Slf4j
@RequiredArgsConstructor
public class MasterArchSignalConsumer {

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

    /**
     * DISABLED: Master Architecture signals now flow through unified trading-signals-v2 topic
     * and are processed by QuantSignalConsumer
     */
    // @KafkaListener(
    //         topics = {"kotsin_FF1", "score-final-opportunity"},
    //         groupId = "${app.kafka.consumer.masterarch-group-id:masterarch-executor}",
    //         containerFactory = "curatedSignalKafkaListenerContainerFactory"
    // )
    public void processMasterArchSignal(String payload, ConsumerRecord<?, ?> rec) {
        final String topic = rec.topic();
        final int partition = rec.partition();
        final long offset = rec.offset();
        final Instant receivedAt = Instant.now();

        try {
            JsonNode root = objectMapper.readTree(payload);

            // ========== Extract Core Fields ==========
            String scripCode = root.path("scripCode").asText();
            if (scripCode == null || scripCode.isEmpty()) {
                log.debug("masterarch_no_scripcode topic={} partition={} offset={}", topic, partition, offset);
                return;
            }

            // Check if actionable
            String decision = root.path("decision").asText("REJECT");
            boolean isActionable = root.path("isActionable").asBoolean(false);
            
            if (!isActionable || !"ENTER_NOW".equals(decision)) {
                log.debug("masterarch_not_actionable scrip={} decision={}", scripCode, decision);
                return;
            }

            // ========== Parse FinalOpportunityScore ==========
            String companyName = root.path("companyName").asText(scripCode);
            long timestamp = root.path("timestamp").asLong(System.currentTimeMillis());
            
            // Get final score components
            JsonNode finalScoreNode = root.path("finalScore");
            double finalScore = finalScoreNode.path("current").asDouble(0);
            double directionConfidence = root.path("directionConfidence").asDouble(0);
            
            // Determine direction from score
            String direction = finalScore > 0 ? "BULLISH" : (finalScore < 0 ? "BEARISH" : "NEUTRAL");
            boolean longSignal = finalScore > 0;
            boolean shortSignal = finalScore < 0;
            
            // Get position sizing
            int recommendedLots = root.path("recommendedLots").asInt(0);
            boolean hedgeRecommended = root.path("hedgeRecommended").asBoolean(false);
            
            // Get entry/exit levels (if available from position node)
            JsonNode position = root.path("position");
            double entryPrice = position.path("entryPrice").asDouble(0);
            double stopLoss = position.path("stopLoss").asDouble(0);
            double target1 = position.path("target1").asDouble(0);
            double target2 = position.path("target2").asDouble(0);
            double riskReward = position.path("riskRewardRatio").asDouble(0);
            
            // Validate trade parameters
            if (entryPrice <= 0 || stopLoss <= 0 || target1 <= 0) {
                log.warn("masterarch_invalid_params scrip={} entry={} sl={} t1={}", 
                        scripCode, entryPrice, stopLoss, target1);
                return;
            }

            // ========== Idempotency Check ==========
            String idKey = "MASTERARCH|" + scripCode + "|" + timestamp;
            if (processedSignalsCache.asMap().putIfAbsent(idKey, Boolean.TRUE) != null) {
                log.info("masterarch_duplicate key={} scrip={}", idKey, scripCode);
                return;
            }

            // ========== Convert to StrategySignal ==========
            StrategySignal signal = StrategySignal.builder()
                    .scripCode(scripCode)
                    .companyName(companyName)
                    .timestamp(timestamp)
                    .signal("MASTER_ARCH_" + (longSignal ? "LONG" : "SHORT"))
                    .confidence(directionConfidence)
                    .rationale("Master Architecture: " + decision + " with score " + String.format("%.2f", finalScore))
                    .direction(direction)
                    .longSignal(longSignal)
                    .shortSignal(shortSignal)
                    .entryPrice(entryPrice)
                    .stopLoss(stopLoss)
                    .target1(target1)
                    .target2(target2)
                    .riskRewardRatio(riskReward)
                    .positionSizeMultiplier(recommendedLots > 0 ? recommendedLots : 1.0)
                    .vcpCombinedScore(root.path("signalStrengthScore").asDouble(0))
                    .ipuFinalScore(root.path("securityContextScore").asDouble(0))
                    .xfactorFlag(directionConfidence > 0.8)
                    .build();

            signal.parseScripCode();

            // ========== Age Check for Routing ==========
            final Instant signalTs = Instant.ofEpochMilli(timestamp);
            final ZonedDateTime signalTimeIst = signalTs.atZone(IST);
            long ageSeconds = Math.abs(receivedAt.getEpochSecond() - signalTs.getEpochSecond());

            if (ageSeconds > BACKTEST_THRESHOLD_SECONDS) {
                // BACKTEST MODE
                log.info("🎯 masterarch_backtest_mode scrip={} ageSeconds={} score={} decision={}",
                        scripCode, ageSeconds, finalScore, decision);
                
                BacktestTrade result = backtestEngine.runBacktest(signal, signalTimeIst.toLocalDateTime());
                log.info("masterarch_backtest_complete scrip={} profit={}", result.getScripCode(), result.getProfit());
                
            } else {
                // LIVE MODE
                log.info("masterarch_live_mode scrip={} ageSeconds={} score={} decision={} lots={} hedge={}",
                        scripCode, ageSeconds, String.format("%.3f", finalScore), decision, recommendedLots, hedgeRecommended);

                // Check trading hours
                String exchange = signal.getExchange() != null ? signal.getExchange() : "N";
                final ZonedDateTime receivedIst = receivedAt.atZone(IST);

                if (!tradingHoursService.shouldProcessTrade(exchange, receivedIst.toLocalDateTime())) {
                    log.info("masterarch_outside_hours scrip={} exch={}", scripCode, exchange);
                    return;
                }

                // Create virtual trade
                BacktestTrade virtualTrade = BacktestTrade.fromSignal(signal, signalTimeIst.toLocalDateTime());
                virtualTrade.setStatus(BacktestTrade.TradeStatus.ACTIVE);
                // Store Master Arch specific info in rationale (BacktestTrade doesn't have extraField)
                virtualTrade.setRationale(String.format("MASTER_ARCH | lots=%d | hedge=%s | %s", 
                        recommendedLots, hedgeRecommended, signal.getRationale()));
                backtestRepository.save(virtualTrade);
                
                log.info("🎯 masterarch_virtual_trade created id={} scrip={} lots={}", 
                        virtualTrade.getId(), scripCode, recommendedLots);

                // Forward to TradeManager for execution
                if (liveTradeEnabled) {
                    tradeManager.addSignalToWatchlist(signal, receivedIst.toLocalDateTime());
                    
                    if (hedgeRecommended) {
                        log.info("🛡️ masterarch_hedge_recommended scrip={} - Consider adding hedge position", scripCode);
                    }
                }
            }

        } catch (Exception e) {
            log.error("masterarch_processing_error topic={} partition={} offset={} err={}",
                    topic, partition, offset, e.toString(), e);
        }
    }
}
