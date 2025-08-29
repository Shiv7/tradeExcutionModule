package com.kotsin.execution.consumer;

import com.kotsin.execution.model.ActiveTrade;
import com.kotsin.execution.model.Candlestick;
import com.kotsin.execution.model.MarketData;
import com.kotsin.execution.service.TradingHoursService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 🛡️ BULLETPROOF Consumer for live market data from forwardtesting-data topic.
 * 🔧 FIXED: Now uses MarketData POJO for proper type safety and Token-scripCode linking.
 */
@Component
@Slf4j
@RequiredArgsConstructor
public class LiveMarketDataConsumer {

    private final com.kotsin.execution.logic.TradeManager tradeManager;

    /**
     * 🔧 FIXED: Consume MarketData POJO directly with proper JSON deserialization
     * Uses marketDataKafkaListenerContainerFactory for type-safe POJO conversion
     * Token (market data) = scripCode (strategy signals) for company linking
     * 🎯 Group ID: Configured in application.properties via containerFactory
     */
    @KafkaListener(topics = "forwardtesting-data",
            properties = {"auto.offset.reset=earliest"},
            containerFactory = "marketDataKafkaListenerContainerFactory")
    public void consumeMarketData(
            @Payload MarketData marketData,
            @Header(KafkaHeaders.RECEIVED_TIMESTAMP) long timestamp,
            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
            Acknowledgment acknowledgment) {

        try {
            String scripCode = marketData.getUniqueIdentifier();
            ActiveTrade activeTrade = tradeManager.getCurrentTrade();
            List<String> scripCodeList = tradeManager.getWaitingTrade();
            if (Objects.nonNull(activeTrade)) {
                scripCodeList.add(activeTrade.getScripCode());
            }
            if (scripCodeList.contains(scripCode)) {
                log.info("recevied desired candle for waiting or active : {}", marketData);
                Candlestick candlestick = new Candlestick();
                candlestick.setClose(marketData.getLastRate());
                candlestick.setHigh(marketData.getHigh());
                candlestick.setLow(marketData.getLow());
                candlestick.setOpen(marketData.getOpenRate());
                tradeManager.processCandle(candlestick);
            }
            acknowledgment.acknowledge();
        } catch (Exception e) {
            log.error("🚨 Error processing market data POJO: {}", e.getMessage());
            acknowledgment.acknowledge();
        }
    }
}
