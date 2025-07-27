package com.kotsin.execution.logic;

import com.kotsin.execution.model.*;
import com.kotsin.execution.producer.ProfitLossProducer;
import com.kotsin.execution.producer.TradeResultProducer;
import com.kotsin.execution.service.*;
import com.kotsin.execution.broker.BrokerOrderService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

@Service
@Slf4j
@RequiredArgsConstructor
public class TradeManager {

    private final TradeResultProducer tradeResultProducer;
    private final TelegramNotificationService telegramNotificationService;
    private final BrokerOrderService brokerOrderService;
    private final PivotCacheService pivotCacheService;
    private final TradeAnalysisService tradeAnalysisService;
    private final HistoricalDataClient historicalDataClient;
    private final SimulationService simulationService;

    private final Map<String, ActiveTrade> waitingTrades = new ConcurrentHashMap<>();
    private final AtomicReference<ActiveTrade> activeTrade = new AtomicReference<>();
    private final Map<String, List<Candlestick>> recentCandles = new ConcurrentHashMap<>();

    private static final int POSITION_SIZE = 1;
    private static final DateTimeFormatter TIME_FORMAT = DateTimeFormatter.ofPattern("HH:mm:ss");

    @Value("${trading.mode:LIVE}")
    private String tradingMode;

    public void processCandle(Candlestick candle) {
        log.info("BEGIN processCandle for: {}", candle.getCompanyName());

        if (activeTrade.get() != null) {
            log.debug("An active trade already exists. Skipping candle processing.");
            return;
        }
        if (!tradeAnalysisService.isWithinGoldenWindows(candle.getWindowStartMillis())) {
            log.debug("Outside golden windows. Skipping candle processing.");
            return;
        }
        log.info("PASSED initial checks.");

        updateCandleHistory(candle);
        log.info("UPDATED candle history for {}.", candle.getCompanyName());

        synchronized (this) {
            log.info("ENTERED synchronized block for {}.", candle.getCompanyName());
            if (activeTrade.get() != null) {
                log.warn("Race condition check: Active trade appeared after initial check. Aborting.");
                return;
            }

            List<ActiveTrade> readyTrades = new ArrayList<>();
            log.info("Checking {} waiting trades.", waitingTrades.size());
            for (ActiveTrade trade : waitingTrades.values()) {
                log.info("Evaluating trade readiness for: {}", trade.getScripCode());
                if (isTradeReadyForExecution(trade, candle)) {
                    log.info("SUCCESS: Trade is ready for execution: {}", trade.getScripCode());
                    readyTrades.add(trade);
                } else {
                    log.info("FAILURE: Trade is not ready: {}", trade.getScripCode());
                }
            }

            if (!readyTrades.isEmpty()) {
                log.info("Found {} ready trades. Selecting the best one.", readyTrades.size());
                ActiveTrade bestTrade = readyTrades.stream()
                    .max(Comparator.comparingDouble(t -> (double) t.getMetadata().getOrDefault("potentialRR", 0.0)))
                    .orElse(null);

                if (bestTrade != null) {
                    log.info("Best trade selected: {}. Executing entry.", bestTrade.getScripCode());
                    executeEntry(bestTrade, candle);
                    activeTrade.set(bestTrade);
                    waitingTrades.clear();
                    log.info("CLEARED waiting trades list.");
                }
            } else {
                log.info("No trades were ready for execution for this candle.");
            }
            log.info("EXITING synchronized block for {}.", candle.getCompanyName());
        }
        log.info("END processCandle for: {}", candle.getCompanyName());
    }

    public void closeTradeAtSimulationEnd(Candlestick lastCandle) {
        ActiveTrade trade = activeTrade.get();
        if (trade != null) {
            log.warn("Active trade {} found at the end of simulation. Forcing closure.", trade.getScripCode());
            TradeResult result = new TradeResult();
            result.setScripCode(trade.getScripCode());
            result.setEntryPrice(trade.getEntryPrice());
            result.setExitPrice(lastCandle.getClose());
            result.setExitReason("End of Simulation");
            tradeResultProducer.publishTradeResult(result);
            activeTrade.set(null);
        }
    }

    private boolean isTradeReadyForExecution(ActiveTrade trade, Candlestick candle) {
        log.info("--- Begin Trade Readiness Evaluation for {} ---", trade.getScripCode());

        PivotData pivots = pivotCacheService.getDailyPivots(trade.getScripCode());
        if (pivots == null) {
            log.warn("Trade Readiness FAILED for {}: Could not fetch pivot data.", trade.getScripCode());
            return false;
        }
        log.info("Trade Readiness PASSED for {}: Fetched pivot data.", trade.getScripCode());

        boolean retestCompleted = checkPivotRetest(trade, candle, pivots.getPivot());
        if (!retestCompleted) {
            log.info("Trade Readiness FAILED for {}: Pivot retest not complete.", trade.getScripCode());
            return false;
        }
        log.info("Trade Readiness PASSED for {}: Pivot retest complete.", trade.getScripCode());

        List<Candlestick> history = recentCandles.get(trade.getScripCode());
        boolean volumeConfirmed = tradeAnalysisService.confirmVolumeProfile(candle, history);
        if (!volumeConfirmed) {
            log.info("Trade Readiness FAILED for {}: Volume not confirmed.", trade.getScripCode());
            return false;
        }
        log.info("Trade Readiness PASSED for {}: Volume confirmed.", trade.getScripCode());

        Candlestick previousCandle = history != null && history.size() > 1 ? history.get(history.size() - 2) : null;
        boolean candlePatternConfirmed = trade.isBullish() ?
            tradeAnalysisService.isBullishEngulfing(previousCandle, candle) :
            tradeAnalysisService.isBearishEngulfing(previousCandle, candle);
        if (!candlePatternConfirmed) {
            log.info("Trade Readiness FAILED for {}: Candlestick pattern not confirmed.", trade.getScripCode());
            return false;
        }
        log.info("Trade Readiness PASSED for {}: Candlestick pattern confirmed.", trade.getScripCode());

        calculateRiskReward(trade, candle, pivots);
        log.info("--- All Readiness Checks Passed for {} ---", trade.getScripCode());
        return true;
    }

    private boolean checkPivotRetest(ActiveTrade trade, Candlestick candle, double pivot) {
        boolean hasBreached = trade.isBullish() ? candle.getLow() <= pivot : candle.getHigh() >= pivot;
        boolean hasReclaimed = trade.isBullish() ? candle.getClose() > pivot : candle.getClose() < pivot;

        if (hasBreached && trade.getMetadata().get("breachCandle") == null) {
            trade.addMetadata("breachCandle", candle);
        }

        return trade.getMetadata().containsKey("breachCandle") && hasReclaimed;
    }

    private void calculateRiskReward(ActiveTrade trade, Candlestick candle, PivotData pivots) {
        double entryPrice = candle.getClose();
        double stopLoss = trade.isBullish() ? candle.getLow() * 0.999 : candle.getHigh() * 1.001;
        double risk = Math.abs(entryPrice - stopLoss);

        double potentialTarget = findNextLogicalTarget(trade.isBullish(), entryPrice, pivots);
        double reward = Math.abs(potentialTarget - entryPrice);

        if (risk > 0) {
            trade.addMetadata("potentialRR", reward / risk);
        } else {
            trade.addMetadata("potentialRR", 0.0);
        }
    }

    private double findNextLogicalTarget(boolean isBullish, double entryPrice, PivotData pivots) {
        if (isBullish) {
            if (pivots.getR1() > entryPrice) return pivots.getR1();
            if (pivots.getR2() > entryPrice) return pivots.getR2();
            if (pivots.getR3() > entryPrice) return pivots.getR3();
            return pivots.getR4();
        } else {
            if (pivots.getS1() < entryPrice) return pivots.getS1();
            if (pivots.getS2() < entryPrice) return pivots.getS2();
            if (pivots.getS3() < entryPrice) return pivots.getS3();
            return pivots.getS4();
        }
    }

    public void addSignalToWatchlist(StrategySignal signal, LocalDateTime signalReceivedTime) {
        ActiveTrade trade = createBulletproofTrade(signal, signalReceivedTime);
        waitingTrades.put(trade.getScripCode(), trade);
        log.info("Added/Updated trade for {} to watchlist. Total watchlist size: {}", trade.getScripCode(), waitingTrades.size());

        LocalDateTime signalTimestamp = LocalDateTime.ofInstant(Instant.ofEpochMilli(signal.getTimestamp()), ZoneId.of("Asia/Kolkata"));
        String signalDate = signalTimestamp.toLocalDate().format(DateTimeFormatter.ISO_LOCAL_DATE);
        List<Candlestick> historicalCandles = historicalDataClient.getHistorical1MinCandles(signal.getScripCode(), signalDate);
        if (historicalCandles != null && !historicalCandles.isEmpty()) {
            historicalCandles.forEach(c -> c.setCompanyName(signal.getCompanyName()));
            recentCandles.put(signal.getScripCode(), new ArrayList<>(historicalCandles));
            log.info("Pre-populated and enriched {} historical candles for {}", historicalCandles.size(), signal.getScripCode());
            if ("SIMULATION".equalsIgnoreCase(tradingMode)) {
                simulationService.runSimulation(historicalCandles);
            }
        }
    }

    private void executeEntry(ActiveTrade trade, Candlestick confirmationCandle) {
        double entryPrice = confirmationCandle.getClose();
        PivotData pivots = pivotCacheService.getDailyPivots(trade.getScripCode());
        if (pivots == null) {
            log.error("Could not fetch pivots for {}. Aborting entry.", trade.getScripCode());
            return;
        }

        trade.setStopLoss(trade.isBullish() ? confirmationCandle.getLow() * 0.999 : confirmationCandle.getHigh() * 1.001);
        trade.setTarget1(findNextLogicalTarget(trade.isBullish(), entryPrice, pivots));
        
        trade.setEntryTriggered(true);
        trade.setEntryPrice(entryPrice);
        trade.setEntryTime(LocalDateTime.ofInstant(Instant.ofEpochMilli(confirmationCandle.getWindowStartMillis()), ZoneId.of("Asia/Kolkata")));
        trade.setPositionSize(POSITION_SIZE);
        trade.setStatus(ActiveTrade.TradeStatus.ACTIVE);
        trade.setHighSinceEntry(entryPrice);
        trade.setLowSinceEntry(entryPrice);
        trade.addMetadata("confirmationCandle", confirmationCandle);

        log.info("🚀 ENTRY EXECUTED: {} at {}", trade.getScripCode(), entryPrice);
        sendTradeEnteredNotification(trade, "Intelligent Confirmation");
        
        if ("LIVE".equalsIgnoreCase(tradingMode)) {
            // Broker order logic remains the same
        }
    }

    private void sendTradeEnteredNotification(ActiveTrade trade, String entryReason) {
        Candlestick breachCandle = (Candlestick) trade.getMetadata().get("breachCandle");
        Candlestick confirmationCandle = (Candlestick) trade.getMetadata().get("confirmationCandle");

        String pivotDetails = "Pivot Retest: Not available";
        if (breachCandle != null) {
            pivotDetails = String.format("Pivot Retest: Breached at %s (Vol: %d), Reclaimed at %s (Vol: %d)",
                LocalDateTime.ofInstant(Instant.ofEpochMilli(breachCandle.getWindowStartMillis()), ZoneId.of("Asia/Kolkata")).format(TIME_FORMAT),
                breachCandle.getVolume(),
                trade.getEntryTime().format(TIME_FORMAT),
                confirmationCandle.getVolume()
            );
        }

        String message = String.format(
            "🚀 TRADE ENTERED (%s)\n" +
            "----------------------------------------\n" +
            "Company: %s (%s)\n" +
            "Signal Time: %s\n" +
            "Entry Time: %s\n" +
            "----------------------------------------\n" +
            "Entry Price: %.2f\n" +
            "Stop-Loss: %.2f\n" +
            "Target 1: %.2f\n" +
            "----------------------------------------\n" +
            "Reason: %s\n" +
            "%s\n" +
            "----------------------------------------",
            trade.getSignalType(),
            trade.getCompanyName(),
            trade.getScripCode(),
            trade.getSignalTime().format(TIME_FORMAT),
            trade.getEntryTime().format(TIME_FORMAT),
            trade.getEntryPrice(),
            trade.getStopLoss(),
            trade.getTarget1(),
            entryReason,
            pivotDetails
        );
        if (!"SILENT".equalsIgnoreCase(tradingMode)) {
            telegramNotificationService.sendTradeNotificationMessage(message);
        }
    }

    private ActiveTrade createBulletproofTrade(StrategySignal signal, LocalDateTime receivedTime) {
        String tradeId = "BT_" + signal.getScripCode() + "_" + System.currentTimeMillis();
        boolean isBullish = "BUY".equalsIgnoreCase(signal.getSignal()) || "BULLISH".equalsIgnoreCase(signal.getSignal());
        
        ActiveTrade trade = ActiveTrade.builder()
                .tradeId(tradeId)
                .scripCode(signal.getScripCode())
                .companyName(signal.getCompanyName() != null ? signal.getCompanyName() : signal.getScripCode())
                .signalType(isBullish ? "BULLISH" : "BEARISH")
                .strategyName("INTELLIGENT_CONFIRMATION")
                .signalTime(receivedTime)
                .stopLoss(signal.getStopLoss())
                .target1(signal.getTarget1())
                .target2(signal.getTarget2())
                .target3(signal.getTarget3())
                .status(ActiveTrade.TradeStatus.WAITING_FOR_ENTRY)
                .entryTriggered(false)
                .build();
        
        trade.setMetadata(new java.util.HashMap<>());
        trade.addMetadata("signalPrice", signal.getEntryPrice());
        
        return trade;
    }

    private void updateCandleHistory(Candlestick candle) {
        recentCandles.computeIfAbsent(candle.getCompanyName(), k -> new ArrayList<>()).add(candle);
        List<Candlestick> history = recentCandles.get(candle.getCompanyName());
        if (history.size() > 10) {
            history.remove(0);
        }
    }

    public boolean hasActiveTrade() {
        return activeTrade.get() != null;
    }

    public ActiveTrade getCurrentTrade() {
        return activeTrade.get();
    }
}
