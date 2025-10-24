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
import java.util.Locale;
import java.util.Objects;

@Service
@Slf4j
@RequiredArgsConstructor
public class SignalConsumer {

    private static final ZoneId IST = ZoneId.of("Asia/Kolkata");

    private final TradeManager tradeManager;
    private final TradingHoursService tradingHoursService;

    // Inject a short-ttl cache for idempotency (defined in config below)
    private final Cache<String, Boolean> processedSignalsCache;

    // App config (max skew, topic names, defaults etc.)
    private final TradeProps tradeProps;

    @KafkaListener(
            topics = "${trade.topics.signals:enhanced-30m-signals}",
            containerFactory = "strategySignalKafkaListenerContainerFactory",
            errorHandler = "bulletproofErrorHandler"
    )
    public void processStrategySignal(
            StrategySignal raw,
            Acknowledgment ack,
            @Header(KafkaHeaders.RECEIVED_TIMESTAMP) long kafkaTimestamp, // kept (not used for 'now')
            ConsumerRecord<?, ?> rec
    ) {
        final String topic = rec.topic();
        final int partition = rec.partition();
        final long offset = rec.offset();

        // Use local clock for freshness checks
        final Instant receivedAt = Instant.now();

        try {
            if (raw == null || StringUtils.isBlank(raw.getScripCode())) {
                log.warn("signal_invalid scripCode=blank topic={} partition={} offset={} ts={}",
                        topic, partition, offset, receivedAt);
                ack.acknowledge();
                return;
            }

            final StrategySignal s = sanitize(raw);

            // If side is unknown, drop early
            if (s.getSignal() == null) {
                log.warn("signal_invalid_side scrip={} topic={} partition={} offset={}",
                        s.getScripCode(), topic, partition, offset);
                ack.acknowledge();
                return;
            }

            // Numeric sanity
            if (s.getStopLoss() <= 0 || s.getTarget1() <= 0) {
                log.warn("signal_invalid_numbers scrip={} sl={} t1={} topic={} partition={} offset={}",
                        s.getScripCode(), s.getStopLoss(), s.getTarget1(), topic, partition, offset);
                ack.acknowledge();
                return;
            }

            // Prefer producer ts from payload if plausible
            long tsMillis = s.getTimestamp();
            boolean plausible = tsMillis > 946684800000L && tsMillis < 4102444800000L; // 2000..2100
            final Instant signalTs = plausible ? Instant.ofEpochMilli(tsMillis) : receivedAt;

            long ageSec = Math.abs(receivedAt.getEpochSecond() - signalTs.getEpochSecond());
            if (ageSec > tradeProps.maxSkewSeconds()) {
                log.warn("signal_stale scrip={} ageSec={} maxSkewSec={} topic={} partition={} offset={}",
                        s.getScripCode(), ageSec, tradeProps.maxSkewSeconds(), topic, partition, offset);
                ack.acknowledge();
                return;
            }

            final ZonedDateTime receivedIst = receivedAt.atZone(IST);
            if (!tradingHoursService.shouldProcessTrade(s.getExchange(), receivedIst.toLocalDateTime())) {
                log.info("signal_outside_hours scrip={} exch={} ts={} topic={} partition={} offset={}",
                        s.getScripCode(), s.getExchange(), receivedIst, topic, partition, offset);
                ack.acknowledge();
                return;
            }

            final String idKey = buildIdKey(s, signalTs);
            if (processedSignalsCache.asMap().putIfAbsent(idKey, Boolean.TRUE) != null) {
                log.info("signal_duplicate key={} scrip={} topic={} partition={} offset={}",
                        idKey, s.getScripCode(), topic, partition, offset);
                ack.acknowledge();
                return;
            }

            tradeManager.addSignalToWatchlist(s, receivedIst.toLocalDateTime());

            ack.acknowledge();
            log.info("signal_accepted key={} scrip={} topic={} partition={} offset={}",
                    idKey, s.getScripCode(), topic, partition, offset);

        } catch (Exception e) {
            log.error("signal_processing_error topic={} partition={} offset={} err={}",
                    topic, partition, offset, e.toString(), e);
            throw e;
        }
    }

    private StrategySignal sanitize(StrategySignal in) {
        Objects.requireNonNull(in, "signal");
        StrategySignal s = new StrategySignal();

        s.setScripCode(StringUtils.trim(in.getScripCode()));
        s.setCompanyName(StringUtils.trimToNull(in.getCompanyName()));

        // Normalize signal side
        String side = in.getSignal() == null ? null : in.getSignal().trim().toUpperCase(Locale.ROOT);
        if (!"BULLISH".equals(side) && !"BEARISH".equals(side)) {
            // If unknown, keep null to let downstream reject/ignore according to policy
            side = null;
        }
        s.setSignal(side);

        // Pass-through numerics
        s.setEntryPrice(in.getEntryPrice());
        s.setStopLoss(in.getStopLoss());
        s.setTarget1(in.getTarget1());
        s.setTarget2(in.getTarget2());
        s.setTarget3(in.getTarget3());

        // Timestamp: prefer producer timestamp if present
        s.setTimestamp(in.getTimestamp());

        // Normalize exchange + type
        String exch = StringUtils.defaultIfBlank(in.getExchange(), "N").trim().toUpperCase(Locale.ROOT);
        String exType = StringUtils.defaultIfBlank(in.getExchangeType(), "C").trim().toUpperCase(Locale.ROOT);
        // Keep only expected values
        if (!("N".equals(exch) || "B".equals(exch))) exch = "N";
        if (!("C".equals(exType) || "D".equals(exType))) exType = "C";
        s.setExchange(exch);
        s.setExchangeType(exType);

        // Optional order instrument overrides for option-only execution
        String oCode = StringUtils.trimToNull(in.getOrderScripCode());
        String oEx   = StringUtils.trimToNull(in.getOrderExchange());
        String oExTy = StringUtils.trimToNull(in.getOrderExchangeType());
        if (oCode != null) s.setOrderScripCode(oCode);
        if (oEx != null) s.setOrderExchange(oEx.toUpperCase(Locale.ROOT));
        if (oExTy != null) s.setOrderExchangeType(oExTy.toUpperCase(Locale.ROOT));
        if (in.getOrderLimitPrice() != 0) {
            try { s.setOrderLimitPrice(in.getOrderLimitPrice()); } catch (Exception ignore) {}
        }
        if (in.getOrderLimitPriceEntry() != null) s.setOrderLimitPriceEntry(in.getOrderLimitPriceEntry());
        if (in.getOrderLimitPriceExit() != null) s.setOrderLimitPriceExit(in.getOrderLimitPriceExit());
        if (in.getOrderTickSize() != null) s.setOrderTickSize(in.getOrderTickSize());

        // Optional: normalize strategy/timeframe if you use them in keys
        s.setStrategy(StringUtils.trimToNull(in.getStrategy()));
        s.setTimeframe(StringUtils.trimToNull(in.getTimeframe()));

        return s;
    }

    private String buildIdKey(StrategySignal s, Instant signalTs) {
        String strategy = StringUtils.defaultString(s.getStrategy(), "NA");
        String tf = StringUtils.defaultString(s.getTimeframe(), "NA");
        String side = StringUtils.defaultString(s.getSignal(), "NA");
        return s.getScripCode() + '|' + strategy + '|' + tf + '|' + side + '|' + signalTs.toEpochMilli();
    }
}
