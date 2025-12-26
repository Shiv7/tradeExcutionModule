package com.kotsin.execution.consumer;

import com.github.benmanes.caffeine.cache.Cache;
import com.kotsin.execution.config.TradeProps;
import com.kotsin.execution.logic.TradeManager;
import com.kotsin.execution.model.StrategySignal;
import com.kotsin.execution.service.TradingHoursService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Objects;

/**
 * SignalConsumer - Consumes signals from StreamingCandle's trading-signals topic
 * 
 * Aligned with TradingSignal schema from the 16-module quant framework.
 * Filters actionable signals (longSignal=true OR shortSignal=true) and
 * forwards them to TradeManager for execution.
 */
@Service
@Slf4j
@RequiredArgsConstructor
public class SignalConsumer {

    private static final ZoneId IST = ZoneId.of("Asia/Kolkata");

    private final TradeManager tradeManager;
    private final TradingHoursService tradingHoursService;

    // Short-TTL cache for idempotency
    private final Cache<String, Boolean> processedSignalsCache;

    // App config
    private final TradeProps tradeProps;

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

            // Check if signal is actionable
            if (!raw.isActionable()) {
                log.debug("signal_not_actionable scrip={} signal={} topic={} partition={} offset={}",
                        raw.getScripCode(), raw.getSignal(), topic, partition, offset);
                ack.acknowledge();
                return;
            }

            // Validate trade parameters
            if (raw.getStopLoss() <= 0 || raw.getTarget1() <= 0 || raw.getEntryPrice() <= 0) {
                log.warn("signal_invalid_params scrip={} entry={} sl={} t1={} topic={} partition={} offset={}",
                        raw.getScripCode(), raw.getEntryPrice(), raw.getStopLoss(), 
                        raw.getTarget1(), topic, partition, offset);
                ack.acknowledge();
                return;
            }

            // ========== Timestamp Validation ==========
            long tsMillis = raw.getTimestamp();
            boolean plausible = tsMillis > 946684800000L && tsMillis < 4102444800000L; // 2000-2100
            final Instant signalTs = plausible ? Instant.ofEpochMilli(tsMillis) : receivedAt;

            long ageSec = Math.abs(receivedAt.getEpochSecond() - signalTs.getEpochSecond());
            if (ageSec > tradeProps.maxSkewSeconds()) {
                log.warn("signal_stale scrip={} ageSec={} maxSkew={} topic={} partition={} offset={}",
                        raw.getScripCode(), ageSec, tradeProps.maxSkewSeconds(), topic, partition, offset);
                ack.acknowledge();
                return;
            }

            // ========== Trading Hours Check ==========
            final ZonedDateTime receivedIst = receivedAt.atZone(IST);
            String exchange = raw.getExchange() != null ? raw.getExchange() : "N";
            
            if (!tradingHoursService.shouldProcessTrade(exchange, receivedIst.toLocalDateTime())) {
                log.info("signal_outside_hours scrip={} exch={} ts={} topic={} partition={} offset={}",
                        raw.getScripCode(), exchange, receivedIst, topic, partition, offset);
                ack.acknowledge();
                return;
            }

            // ========== Idempotency Check ==========
            final String idKey = buildIdKey(raw, signalTs);
            if (processedSignalsCache.asMap().putIfAbsent(idKey, Boolean.TRUE) != null) {
                log.info("signal_duplicate key={} scrip={} topic={} partition={} offset={}",
                        idKey, raw.getScripCode(), topic, partition, offset);
                ack.acknowledge();
                return;
            }

            // ========== Forward to TradeManager ==========
            log.info("signal_accepted scrip={} company={} signal={} direction={} confidence={} " +
                    "entry={} sl={} t1={} t2={} rr={} topic={} partition={} offset={}",
                    raw.getScripCode(), raw.getCompanyName(), raw.getSignal(), raw.getDirection(),
                    String.format("%.2f", raw.getConfidence()), raw.getEntryPrice(), raw.getStopLoss(),
                    raw.getTarget1(), raw.getTarget2(), String.format("%.2f", raw.getRiskRewardRatio()),
                    topic, partition, offset);

            tradeManager.addSignalToWatchlist(raw, receivedIst.toLocalDateTime());

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
