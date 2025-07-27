package com.kotsin.execution.consumer;

import com.kotsin.execution.logic.TradeManager;
import com.kotsin.execution.model.Candlestick;
import com.kotsin.execution.model.SimulationEndEvent;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.event.EventListener;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class MarketDataHandler {

    private final TradeManager tradeManager;

    @Value("${trading.mode:LIVE}")
    private String tradingMode;

    @KafkaListener(topics = "5-min-candle", containerFactory = "candlestickKafkaListenerContainerFactory", autoStartup = "#{'${trading.mode}'.equalsIgnoreCase('LIVE')}")
    public void process5MinCandle(Candlestick candle) {
        tradeManager.processCandle(candle);
    }

    @Async
    @EventListener
    public void handleSimulationCandle(Candlestick candle) {
        if (!"SIMULATION".equalsIgnoreCase(tradingMode)) return;
        tradeManager.processCandle(candle);
    }

    @EventListener
    public void handleSimulationEnd(SimulationEndEvent event) {
        tradeManager.closeTradeAtSimulationEnd(event.getLastCandle());
    }
}
