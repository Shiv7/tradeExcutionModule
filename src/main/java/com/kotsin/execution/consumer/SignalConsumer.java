package com.kotsin.execution.consumer;

import com.github.benmanes.caffeine.cache.Cache;
import com.kotsin.execution.config.TradeProps;
import com.kotsin.execution.logic.TradeManager;
import com.kotsin.execution.model.BacktestTrade;
import com.kotsin.execution.model.StrategySignal;
import com.kotsin.execution.repository.BacktestTradeRepository;
import com.kotsin.execution.service.BacktestEngine;
import com.kotsin.execution.service.TradingHoursService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;

/**
 * SignalConsumer - Consumes signals from StreamingCandle's trading-signals topic
 * 
 * AUTOMATIC ROUTING:
 * - Signal > 2 min old → BACKTEST mode (historical candles, save to DB)
 * - Signal ≤ 2 min old → LIVE mode (both virtual + 5paisa broker)
 * 
 * In LIVE mode:
 * - Creates virtual trade for monitoring
 * - Forwards to TradeManager for real 5paisa execution
 * - Monitors via forwardtesting-data Kafka topic
 */
@Service
@Slf4j
@RequiredArgsConstructor
public class SignalConsumer {

    private static final ZoneId IST = ZoneId.of("Asia/Kolkata");
    private static final long BACKTEST_THRESHOLD_SECONDS = 120; // 2 minutes

    private final TradeManager tradeManager;
    private final TradingHoursService tradingHoursService;
    private final BacktestEngine backtestEngine;
    private final BacktestTradeRepository backtestRepository;

    // Short-TTL cache for idempotency
    private final Cache<String, Boolean> processedSignalsCache;

    // App config
    private final TradeProps tradeProps;
    
    @Value("${trading.mode.live:true}")
    private boolean liveTradeEnabled;

    @KafkaListener(
            topics = "${trade.topics.signals:trading-signals}",
            containerFactory = "strategySignalKafkaListenerContainerFactory",
            errorHandler = "bulletproofErrorHandler"
    )
    public void processStrategySignal(
            StrategySignal raw,
            Acknowledgment ack,
            @Header(KafkaHeaders.RECEIVED_TIMESTAMP) long kafkaTimestamp,
            ConsumerRecord<?, ?> rec
    ) {
        final String topic = rec.topic();
        final int partition = rec.partition();
        final long offset = rec.offset();
        final Instant receivedAt = Instant.now();

        try {
            // ========== Validation ==========
            if (raw == null || StringUtils.isBlank(raw.getScripCode())) {
                log.warn("signal_invalid scripCode=blank topic={} partition={} offset={}",
                        topic, partition, offset);
                ack.acknowledge();
                return;
            }

            // Parse scripCode to extract exchange/exchangeType
            raw.parseScripCode();

            // 🛡️ CRITICAL FIX #4: Comprehensive signal validation
            com.kotsin.execution.validation.ValidationResult validation = raw.validate();
            if (!validation.isValid()) {
                log.warn("signal_validation_failed scrip={} errors={} topic={} partition={} offset={}",
                        raw.getScripCode(), validation.getErrors(), topic, partition, offset);
                ack.acknowledge();
                return;
            }

            // Log warnings but continue processing
            if (validation.hasWarnings()) {
                log.info("signal_validation_warnings scrip={} warnings={}",
                        raw.getScripCode(), validation.getWarnings());
            }

            // Check if signal is actionable
            if (!raw.isActionable()) {
                log.debug("signal_not_actionable scrip={} signal={} topic={} partition={} offset={}",
                        raw.getScripCode(), raw.getSignal(), topic, partition, offset);
                ack.acknowledge();
                return;
            }

            // ========== Timestamp & Age Calculation ==========
            long tsMillis = raw.getTimestamp();
            boolean plausible = tsMillis > 946684800000L && tsMillis < 4102444800000L; // 2000-2100
            final Instant signalTs = plausible ? Instant.ofEpochMilli(tsMillis) : receivedAt;
            final ZonedDateTime signalTimeIst = signalTs.atZone(IST);
            
            long ageSeconds = Math.abs(receivedAt.getEpochSecond() - signalTs.getEpochSecond());

            // ========== Idempotency Check ==========
            final String idKey = buildIdKey(raw, signalTs);
            if (processedSignalsCache.asMap().putIfAbsent(idKey, Boolean.TRUE) != null) {
                log.info("signal_duplicate key={} scrip={} topic={} partition={} offset={}",
                        idKey, raw.getScripCode(), topic, partition, offset);
                ack.acknowledge();
                return;
            }

            // ========== AUTOMATIC ROUTING BASED ON SIGNAL AGE ==========
            if (ageSeconds > BACKTEST_THRESHOLD_SECONDS) {
                // BACKTEST MODE: Signal is delayed, run backtest with historical data
                log.info("signal_backtest_mode scrip={} ageSeconds={} signal={} offset={}",
                        raw.getScripCode(), ageSeconds, raw.getSignal(), offset);
                
                BacktestTrade result = backtestEngine.runBacktest(raw, signalTimeIst.toLocalDateTime());
                
                log.info("backtest_complete scrip={} entry={} exit={} profit={} reason={} status={}",
                        result.getScripCode(), result.getEntryPrice(), result.getExitPrice(),
                        result.getProfit(), result.getExitReason(), result.getStatus());
                
            } else {
                // LIVE MODE: Signal is fresh, execute both virtual and real trades
                log.info("signal_live_mode scrip={} ageSeconds={} signal={} offset={}",
                        raw.getScripCode(), ageSeconds, raw.getSignal(), offset);
                
                // Check trading hours for live execution
                String exchange = raw.getExchange() != null ? raw.getExchange() : "N";
                final ZonedDateTime receivedIst = receivedAt.atZone(IST);
                
                if (!tradingHoursService.shouldProcessTrade(exchange, receivedIst.toLocalDateTime())) {
                    log.info("signal_outside_hours scrip={} exch={} ts={} - switching to backtest mode",
                            raw.getScripCode(), exchange, receivedIst);
                    BacktestTrade result = backtestEngine.runBacktest(raw, signalTimeIst.toLocalDateTime());
                    log.info("backtest_complete (outside hours) scrip={} profit={}", 
                            result.getScripCode(), result.getProfit());
                    ack.acknowledge();
                    return;
                }
                
                // 1. Create virtual trade for monitoring (saved to DB)
                BacktestTrade virtualTrade = BacktestTrade.fromSignal(raw, signalTimeIst.toLocalDateTime());
                virtualTrade.setStatus(BacktestTrade.TradeStatus.ACTIVE);
                backtestRepository.save(virtualTrade);
                log.info("virtual_trade_created id={} scrip={}", virtualTrade.getId(), virtualTrade.getScripCode());
                
                // 2. Forward to TradeManager for real 5paisa execution
                if (liveTradeEnabled) {
                    log.info("signal_accepted scrip={} company={} signal={} direction={} confidence={} " +
                            "entry={} sl={} t1={} t2={} rr={}",
                            raw.getScripCode(), raw.getCompanyName(), raw.getSignal(), raw.getDirection(),
                            String.format("%.2f", raw.getConfidence()), raw.getEntryPrice(), raw.getStopLoss(),
                            raw.getTarget1(), raw.getTarget2(), String.format("%.2f", raw.getRiskRewardRatio()));
                    
                    tradeManager.addSignalToWatchlist(raw, receivedIst.toLocalDateTime());
                } else {
                    log.info("live_trade_disabled scrip={} - virtual trade only", raw.getScripCode());
                }
            }

            ack.acknowledge();

        } catch (Exception e) {
            log.error("signal_processing_error topic={} partition={} offset={} err={}",
                    topic, partition, offset, e.toString(), e);
            throw e;
        }
    }

    /**
     * Build idempotency key from signal
     */
    private String buildIdKey(StrategySignal s, Instant signalTs) {
        String signal = StringUtils.defaultString(s.getSignal(), "NA");
        String direction = StringUtils.defaultString(s.getDirection(), "NA");
        return s.getScripCode() + '|' + signal + '|' + direction + '|' + signalTs.toEpochMilli();
    }
}
