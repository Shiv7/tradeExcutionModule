package com.kotsin.execution.service;

import com.kotsin.execution.model.BacktestTrade;
import com.kotsin.execution.model.Candlestick;
import com.kotsin.execution.model.StrategySignal;
import com.kotsin.execution.repository.BacktestTradeRepository;
import com.kotsin.execution.rl.service.RLTrainer;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.List;

/**
 * BacktestEngine - Core backtesting logic
 * 
 * Simulates trades using historical candles from 5paisa API:
 * 1. Receives signal from Kafka
 * 2. Fetches historical candles for signal date + N days
 * 3. Simulates entry/exit based on price action
 * 4. Calculates P&L and saves to MongoDB
 * 5. Records experience to RL trainer
 */
@Service
@Slf4j
@RequiredArgsConstructor
public class BacktestEngine {
    
    private static final ZoneId IST = ZoneId.of("Asia/Kolkata");
    
    private final HistoricalDataClient historicalDataClient;
    private final BacktestTradeRepository repository;
    
    @Autowired(required = false)
    private RLTrainer rlTrainer;
    
    @Value("${backtest.days-after-signal:5}")
    private int daysAfterSignal;
    
    /**
     * Run backtest for a single signal
     */
    public BacktestTrade runBacktest(StrategySignal signal, LocalDateTime signalTime) {
        log.info("Starting backtest for {} signal={} direction={}",
                signal.getScripCode(), signal.getSignal(), signal.getDirection());
        
        // 1. Create trade from signal
        BacktestTrade trade = BacktestTrade.fromSignal(signal, signalTime);
        
        // 2. Parse scripCode for exchange info
        signal.parseScripCode();
        String numericScripCode = signal.getNumericScripCode();
        String exchange = signal.getExchange() != null ? signal.getExchange() : "N";
        String exchangeType = signal.getExchangeType() != null ? signal.getExchangeType() : "D";
        
        // 3. Fetch historical candles (signal date + N days)
        LocalDate signalDate = signalTime.toLocalDate();
        List<Candlestick> allCandles = fetchMultiDayCandles(
                numericScripCode, signalDate, signalDate.plusDays(daysAfterSignal),
                exchange, exchangeType);
        
        if (allCandles.isEmpty()) {
            log.warn("No historical candles found for {} from {} to {}",
                    signal.getScripCode(), signalDate, signalDate.plusDays(daysAfterSignal));
            trade.setStatus(BacktestTrade.TradeStatus.FAILED);
            return repository.save(trade);
        }
        
        log.info("Fetched {} candles for {} from {} to {}",
                allCandles.size(), signal.getScripCode(), signalDate, signalDate.plusDays(daysAfterSignal));
        
        // 4. Filter candles after signal time
        List<Candlestick> relevantCandles = allCandles.stream()
                .filter(c -> {
                    LocalDateTime candleTime = Instant.ofEpochMilli(c.getWindowStartMillis())
                            .atZone(IST).toLocalDateTime();
                    return candleTime.isAfter(signalTime) || candleTime.isEqual(signalTime);
                })
                .toList();
        
        log.info("Processing {} candles after signal time {}", relevantCandles.size(), signalTime);
        
        // 5. Check for empty candles (CRITICAL FIX)
        if (relevantCandles.isEmpty()) {
            log.warn("No candles after signal time for {} - cancelling trade", trade.getScripCode());
            trade.setStatus(BacktestTrade.TradeStatus.CANCELLED);
            return repository.save(trade);
        }
        
        // 6. Simulate trade
        for (Candlestick candle : relevantCandles) {
            LocalDateTime candleTime = Instant.ofEpochMilli(candle.getWindowStartMillis())
                    .atZone(IST).toLocalDateTime();
            
            // Entry logic
            if (!trade.isEntered()) {
                if (shouldEnter(trade, candle)) {
                    executeEntry(trade, candle, candleTime);
                    log.info("BACKTEST ENTRY: {} at {} time={}", 
                            trade.getScripCode(), trade.getEntryPrice(), candleTime);
                }
                continue;
            }
            
            // Exit logic - check both SL and TP, determine which hit first
            ExitResult exitResult = checkExitWithPriority(trade, candle);
            if (exitResult.shouldExit) {
                executeExit(trade, exitResult.exitPrice, exitResult.reason, candleTime);
                log.info("BACKTEST EXIT: {} at {} reason={} P&L={}", 
                        trade.getScripCode(), trade.getExitPrice(), trade.getExitReason(), trade.getProfit());
                break;
            }
        }
        
        // 6. Handle untriggered trades
        if (!trade.isEntered()) {
            trade.setStatus(BacktestTrade.TradeStatus.CANCELLED);
            log.info("Trade not entered for {}: signal expired", trade.getScripCode());
        } else if (!trade.isExited()) {
            // Exit at last candle price (end of period)
            Candlestick lastCandle = relevantCandles.get(relevantCandles.size() - 1);
            LocalDateTime lastTime = Instant.ofEpochMilli(lastCandle.getWindowStartMillis())
                    .atZone(IST).toLocalDateTime();
            executeExit(trade, lastCandle.getClose(), "END_OF_PERIOD", lastTime);
            log.info("Trade force-exited for {} at end of period: P&L={}", 
                    trade.getScripCode(), trade.getProfit());
        }
        
        // 7. Save to DB
        BacktestTrade savedTrade = repository.save(trade);
        
        // 8. Record experience for RL training
        if (rlTrainer != null && savedTrade.getStatus() != BacktestTrade.TradeStatus.PENDING) {
            try {
                rlTrainer.recordBacktestExperience(savedTrade);
                log.debug("Recorded RL experience for {}", savedTrade.getScripCode());
            } catch (Exception e) {
                log.warn("Failed to record RL experience: {}", e.getMessage());
            }
        }
        
        return savedTrade;
    }
    
    /**
     * Entry condition: price touches signal entry level
     * FIXED: Removed close condition requirement - just check if price touched entry
     */
    private boolean shouldEnter(BacktestTrade trade, Candlestick candle) {
        double signalPrice = trade.getSignalPrice();
        
        // For LONG: enter if low touches or goes below entry price
        if (trade.isBullish()) {
            return candle.getLow() <= signalPrice;
        }
        // For SHORT: enter if high touches or goes above entry price
        else {
            return candle.getHigh() >= signalPrice;
        }
    }
    
    /**
     * Execute virtual entry
     */
    private void executeEntry(BacktestTrade trade, Candlestick candle, LocalDateTime time) {
        trade.setEntryTime(time);
        trade.setEntryPrice(trade.getSignalPrice()); // Enter at signal price
        trade.setStatus(BacktestTrade.TradeStatus.ACTIVE);
    }
    
    /**
     * Check exit conditions with proper SL/TP priority
     * FIXED: Uses OHLC logic to determine which hit first on same candle
     */
    private ExitResult checkExitWithPriority(BacktestTrade trade, Candlestick candle) {
        double stopLoss = trade.getStopLoss();
        double target1 = trade.getTarget1();
        double entryPrice = trade.getEntryPrice();
        double open = candle.getOpen();
        
        if (trade.isBullish()) {
            boolean slHit = candle.getLow() <= stopLoss;
            boolean tpHit = candle.getHigh() >= target1;
            
            // Both hit on same candle - determine priority
            if (slHit && tpHit) {
                // If open is closer to SL than entry, SL likely hit first
                // If open is above entry, price moved up first (TP), then down (SL)
                if (open < entryPrice) {
                    return new ExitResult(true, stopLoss, "STOP_LOSS");
                } else {
                    return new ExitResult(true, target1, "TARGET1");
                }
            }
            if (slHit) return new ExitResult(true, stopLoss, "STOP_LOSS");
            if (tpHit) return new ExitResult(true, target1, "TARGET1");
            
        } else {
            // SHORT
            boolean slHit = candle.getHigh() >= stopLoss;
            boolean tpHit = candle.getLow() <= target1;
            
            if (slHit && tpHit) {
                // If open is higher than entry, price went up first (SL)
                if (open > entryPrice) {
                    return new ExitResult(true, stopLoss, "STOP_LOSS");
                } else {
                    return new ExitResult(true, target1, "TARGET1");
                }
            }
            if (slHit) return new ExitResult(true, stopLoss, "STOP_LOSS");
            if (tpHit) return new ExitResult(true, target1, "TARGET1");
        }
        
        return new ExitResult(false, 0, null);
    }
    
    /**
     * Execute virtual exit
     */
    private void executeExit(BacktestTrade trade, double exitPrice, String reason, LocalDateTime time) {
        trade.setExitTime(time);
        trade.setExitPrice(exitPrice);
        trade.setExitReason(reason);
        trade.setStatus(BacktestTrade.TradeStatus.COMPLETED);
        trade.calculatePnL();
    }
    
    /**
     * Fetch candles for date range using single API call
     * HistoricalDataClient handles date capping and validation
     */
    private List<Candlestick> fetchMultiDayCandles(String scripCode, LocalDate from, LocalDate to,
                                                    String exchange, String exchangeType) {
        List<Candlestick> candles = historicalDataClient.getHistoricalCandles(
                scripCode, from, to, exchange, exchangeType);
        
        if (candles == null) {
            return new java.util.ArrayList<>();
        }
        
        // Sort by time (API may not guarantee order)
        candles.sort((a, b) -> Long.compare(a.getWindowStartMillis(), b.getWindowStartMillis()));
        
        return candles;
    }
    
    /**
     * Exit result holder
     */
    private record ExitResult(boolean shouldExit, double exitPrice, String reason) {}
}
