package com.kotsin.execution.consumer;

import com.kotsin.execution.logic.TradeManager;
import com.kotsin.execution.model.StrategySignal;
import com.kotsin.execution.service.TradingHoursService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;

@Service
@Slf4j
@RequiredArgsConstructor
public class SignalConsumer {

    private final TradeManager tradeManager;
    private final TradingHoursService tradingHoursService;

    @KafkaListener(topics = "enhanced-30m-signals", containerFactory = "strategySignalKafkaListenerContainerFactory", errorHandler = "bulletproofErrorHandler")
    public void processStrategySignal(StrategySignal signal, Acknowledgment acknowledgment, @Header(KafkaHeaders.RECEIVED_TIMESTAMP) long kafkaTimestamp) {
        if (signal == null || signal.getScripCode() == null || signal.getScripCode().trim().isEmpty()) {
            log.warn("Invalid signal data received. Discarding.");
            acknowledgment.acknowledge();
            return;
        }

        LocalDateTime signalReceivedTime = LocalDateTime.ofInstant(Instant.ofEpochMilli(kafkaTimestamp), ZoneId.of("Asia/Kolkata"));
        if (!tradingHoursService.shouldProcessTrade(signal.getExchange(), signalReceivedTime)) {
            log.warn("Signal for {} received outside trading hours. Discarding.", signal.getScripCode());
            acknowledgment.acknowledge();
            return;
        }

        tradeManager.addSignalToWatchlist(sanitizeStrategySignal(signal), signalReceivedTime);
        acknowledgment.acknowledge();
    }

    private StrategySignal sanitizeStrategySignal(StrategySignal signal) {
        StrategySignal sanitized = new StrategySignal();
        sanitized.setScripCode(signal.getScripCode().trim());
        sanitized.setSignal(signal.getSignal() != null ? signal.getSignal().trim().toUpperCase() : null);
        sanitized.setCompanyName(signal.getCompanyName() != null ? signal.getCompanyName().trim() : null);
        sanitized.setEntryPrice(signal.getEntryPrice());
        sanitized.setStopLoss(signal.getStopLoss());
        sanitized.setTarget1(signal.getTarget1());
        sanitized.setTarget2(signal.getTarget2());
        sanitized.setTarget3(signal.getTarget3());
        sanitized.setTimestamp(signal.getTimestamp());
        sanitized.setExchange(signal.getExchange() != null ? signal.getExchange().trim() : "N");
        sanitized.setExchangeType(signal.getExchangeType() != null ? signal.getExchangeType().trim() : "C");
        return sanitized;
    }
}
