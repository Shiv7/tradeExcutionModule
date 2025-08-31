package com.kotsin.execution.consumer;

import com.kotsin.execution.model.ActiveTrade;
import com.kotsin.execution.model.Candlestick;
import com.kotsin.execution.model.MarketData;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 🛡️ Live market data consumer for forwardtesting-data.
 * Consumes MarketData POJO and forwards 1‑min bars to TradeManager.
 * - Uses event time (MarketData.Time) for minute bucketing when present
 * - Aggregates per-minute volume via TotalQty deltas
 * - Only forwards bars for relevant scrips (waiting or active)
 */
@Component
@Slf4j
@RequiredArgsConstructor
public class LiveMarketDataConsumer {

    private final com.kotsin.execution.logic.TradeManager tradeManager;

    // Per-token minute volume aggregator state
    private final Map<Integer, Long> lastCumQty = new ConcurrentHashMap<>();
    private final Map<Integer, Long> windowStartByToken = new ConcurrentHashMap<>();
    private final Map<Integer, Long> windowVol = new ConcurrentHashMap<>();

    @KafkaListener(
            topics = "forwardtesting-data",
            properties = {"auto.offset.reset=earliest"},
            containerFactory = "marketDataKafkaListenerContainerFactory"
    )
    public void consumeMarketData(
            @Payload MarketData marketData,
            @Header(KafkaHeaders.RECEIVED_TIMESTAMP) long kafkaTimestamp,
            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
            Acknowledgment acknowledgment) {

        try {
            if (marketData == null) {
                acknowledgment.acknowledge();
                return;
            }

            final int token = marketData.getToken();
            final String scripCode = marketData.getUniqueIdentifier();
            if (scripCode == null || scripCode.isBlank()) {
                log.debug("Skipping marketData with blank scripCode: {}", marketData);
                acknowledgment.acknowledge();
                return;
            }

            // Relevance check: only forward bars for waiting or active instrument
            final ActiveTrade activeTrade = tradeManager.getCurrentTrade();
            final List<String> waiting = tradeManager.getWaitingTrade();
            final boolean isRelevant = waiting.contains(scripCode) ||
                    (activeTrade != null && scripCode.equals(activeTrade.getScripCode()));
            if (!isRelevant) {
                acknowledgment.acknowledge();
                return;
            }

            // Event-time minute bucketing (fallback to Kafka receive time)
            final long baseTs = marketData.getTime() > 0 ? marketData.getTime() : kafkaTimestamp;
            final long windowStart = baseTs - (baseTs % 60_000L);

            // Minute volume via TotalQty deltas
            final long cum = marketData.getTotalQuantity();
            final Long prevWindow = windowStartByToken.put(token, windowStart);
            final long prevCum = lastCumQty.getOrDefault(token, cum);
            final long delta = Math.max(0, cum - prevCum);
            lastCumQty.put(token, cum);
            if (prevWindow == null || !prevWindow.equals(windowStart)) {
                windowVol.put(token, delta);
            } else {
                windowVol.merge(token, delta, Long::sum);
            }

            // Build bar
            final double open = marketData.getOpenRate();
            final double high = marketData.getHigh();
            final double low  = marketData.getLow();
            final double close= marketData.getLastRate();

            // Basic sanity guard to avoid corrupted bars
            if (high <= 0 || low <= 0 || close <= 0 || high < Math.max(open, Math.max(low, close)) || low > Math.min(open, Math.min(high, close))) {
                acknowledgment.acknowledge();
                return;
            }

            Candlestick bar = new Candlestick();
            bar.setOpen(open);
            bar.setHigh(high);
            bar.setLow(low);
            bar.setClose(close);
            bar.setVolume(windowVol.getOrDefault(token, 0L));
            bar.setWindowStartMillis(windowStart);
            bar.setExchange(marketData.getExchange());
            bar.setExchangeType(marketData.getExchangeType());

            // Ensure the key matches the trade's companyName (or scrip fallback)
            final String companyName = tradeManager.resolveCompanyName(scripCode);
            bar.setCompanyName(companyName);

            log.debug("Forwarding bar to TradeManager: scrip={} company={} bar={}", scripCode, companyName, bar);
            tradeManager.processCandle(bar);

            acknowledgment.acknowledge();
        } catch (Exception e) {
            log.error("Error processing market data POJO: {}", e.toString(), e);
            // Do not acknowledge on failure; let the container retry/DLQ according to config
            throw e;
        }
    }
}
