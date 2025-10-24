package com.kotsin.execution.logic;

import com.kotsin.execution.model.*;
import com.kotsin.execution.producer.TradeResultProducer;
import com.kotsin.execution.producer.ProfitLossProducer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.RedisTemplate;
import com.kotsin.execution.service.*;
import com.kotsin.execution.broker.BrokerOrderService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

@Service
@Slf4j
@RequiredArgsConstructor
public class TradeManager {

    private final TradeResultProducer tradeResultProducer;
    private final TelegramNotificationService telegramNotificationService;
    private final ProfitLossProducer profitLossProducer;
    private final BrokerOrderService brokerOrderService;
    private final PivotService pivotCacheService;
    private final TradeAnalysisService tradeAnalysisService;
    private final HistoricalDataClient historicalDataClient;
    private final RedisTemplate<String, String> executionStringRedisTemplate;

    @Value("${trade.options.slippage.ticks.exit:1}")
    private int optionSlippageTicksExit;

    /** Waiting trades keyed by scripCode (unique per instrument). */
    private final Map<String, ActiveTrade> waitingTrades = new ConcurrentHashMap<>();
    /** Single active trade at a time (per current design). */
    private final AtomicReference<ActiveTrade> activeTrade = new AtomicReference<>();
    /**
     * Recent candles keyed by **companyName** (matches Candlestick model);
     * we standardize on companyName here for consistency with live/historical candles.
     */
    private final Map<String, List<Candlestick>> recentCandles = new ConcurrentHashMap<>();

    /** TODO: externalize via TradeProps later */
    private static final int POSITION_SIZE = 1;
    private static final ZoneId IST = ZoneId.of("Asia/Kolkata");
    private static final DateTimeFormatter TIME_FORMAT = DateTimeFormatter.ofPattern("HH:mm:ss");
    private static final DateTimeFormatter DATE_TIME_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    // Trailing configuration (in R units)
    @Value("${trade.trail.stage1.r:1.0}")
    private double trailStage1R;
    @Value("${trade.trail.stage2.r:1.5}")
    private double trailStage2R;
    @Value("${trade.trail.stage3.r:2.0}")
    private double trailStage3R;
    @Value("${trade.trail.stage1.stopR:0.0}")
    private double trailStage1StopR;
    @Value("${trade.trail.stage2.stopR:0.5}")
    private double trailStage2StopR;
    @Value("${trade.trail.stage3.stopR:1.0}")
    private double trailStage3StopR;

    /** Main entry: evaluate a new candle and, if eligible, execute the best ready trade. */
    public void processCandle(Candlestick candle) {
        if (candle == null || candle.getCompanyName() == null || candle.getCompanyName().isBlank()) {
            log.debug("processCandle: missing companyName, skipping.");
            return;
        }
        log.info("BEGIN processCandle for: {}", candle.getCompanyName());

        // Manage open trade first (exit on TP/SL), then stop
        ActiveTrade open = activeTrade.get();
        if (open != null) {
            evaluateAndMaybeExit(open, candle);
            return;
        }

        // Time window guard (based on candle timestamp)
        if (!tradeAnalysisService.isWithinGoldenWindows(candle.getWindowStartMillis())) {
            log.debug("Outside golden windows. Skipping candle processing.");
            return;
        }
        log.info("PASSED initial checks.");

        // Maintain per-instrument history used by readiness checks
        updateCandleHistory(candle);
        log.info("UPDATED candle history for {}.", candle.getCompanyName());

        synchronized (this) {
            log.info("ENTERED synchronized block for {}.", candle.getCompanyName());
            if (activeTrade.get() != null) {
                log.warn("Race condition check: Active trade appeared after initial check. Aborting.");
                return;
            }

            // Evaluate all waiting trades for readiness w.r.t this candle
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



    /** Readiness evaluation: pivots retest → volume profile → candle pattern → RR calc. */
    private boolean isTradeReadyForExecution(ActiveTrade trade, Candlestick candle) {
        log.info("--- Begin Trade Readiness Evaluation for {} ---", trade.getScripCode());

        PivotData pivots = pivotCacheService.getDailyPivots(trade.getScripCode(), trade.getSignalTime().toLocalDate());
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

        // Use companyName as canonical key for candles (aligned with Candlestick model)
        List<Candlestick> history = recentCandles.get(trade.getCompanyName());
        boolean volumeConfirmed = tradeAnalysisService.confirmVolumeProfile(candle, history);
        if (!volumeConfirmed) {
            log.info("Trade Readiness FAILED for {}: Volume not confirmed.", trade.getScripCode());
            return false;
        }
        log.info("Trade Readiness PASSED for {}: Volume confirmed.", trade.getScripCode());

        Candlestick previousCandle = (history != null && history.size() > 1) ? history.get(history.size() - 2) : null;
        boolean candlePatternConfirmed = trade.isBullish()
                ? tradeAnalysisService.isBullishEngulfing(previousCandle, candle)
                : tradeAnalysisService.isBearishEngulfing(previousCandle, candle);
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

        trade.setStopLoss(stopLoss);
        trade.setTarget1(potentialTarget);
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

    /** Add/refresh a trade candidate and pre-load recent candles for that instrument. */
    public boolean addSignalToWatchlist(StrategySignal signal, LocalDateTime signalReceivedTime) {
        ActiveTrade trade = createBulletproofTrade(signal, signalReceivedTime);
        waitingTrades.put(trade.getScripCode(), trade);
        log.info("Added/Updated trade for {} to watchlist. Total watchlist size: {}", trade.getScripCode(), waitingTrades.size());

        // Preload 1-min history for the signal date; keep canonical key as companyName
        LocalDate signalDate = LocalDateTime.ofInstant(Instant.ofEpochMilli(signal.getTimestamp()), IST).toLocalDate();
        List<Candlestick> historicalCandles = historicalDataClient.getHistorical1MinCandles(
                signal.getScripCode(), signalDate.toString(), signal.getExchange(), signal.getExchangeType());

        if (historicalCandles != null && !historicalCandles.isEmpty()) {
            String name = signal.getCompanyName() != null ? signal.getCompanyName() : signal.getScripCode();
            historicalCandles.forEach(c -> c.setCompanyName(name));
            recentCandles.put(name, new ArrayList<>(historicalCandles));
            log.info("Pre-populated and enriched {} historical candles for {}", historicalCandles.size(), name);
        }
        return true;
    }

    /** Execute entry: set trade fields, (later) place broker order, emit notifications. */
    private void executeEntry(ActiveTrade trade, Candlestick confirmationCandle) {
        double entryPrice = confirmationCandle.getClose();
        PivotData pivots = pivotCacheService.getDailyPivots(trade.getScripCode(), trade.getSignalTime().toLocalDate());
        if (pivots == null) {
            log.error("Could not fetch pivots for {}. Aborting entry.", trade.getScripCode());
            return;
        }

        // Stop/Target were (re)calculated in calculateRiskReward()
        trade.setEntryTriggered(true);
        trade.setEntryPrice(entryPrice);
        trade.setEntryTime(LocalDateTime.ofInstant(Instant.ofEpochMilli(confirmationCandle.getWindowStartMillis()), IST));
        trade.setPositionSize(POSITION_SIZE);
        trade.setStatus(ActiveTrade.TradeStatus.ACTIVE);
        trade.setHighSinceEntry(entryPrice);
        trade.setLowSinceEntry(entryPrice);
        trade.addMetadata("confirmationCandle", confirmationCandle);


        String formattedEntryTime = trade.getEntryTime().format(DATE_TIME_FORMAT);
        log.info("ENTRY EXECUTED: {} at {} on {}", trade.getScripCode(), entryPrice, formattedEntryTime);

        // Send Telegram entry notification (uses formatted message internally)
        try {
            telegramNotificationService.sendTradeNotification(trade);
        } catch (Exception ex) {
            log.warn("Failed to send Telegram notification for {}: {}", trade.getScripCode(), ex.toString());
        }

        // Place the actual order via broker (spread-aware for options/MCX)
        try {
            String orderScrip = String.valueOf(trade.getMetadata().getOrDefault("orderScripCode", trade.getScripCode()));
            String exch = String.valueOf(trade.getMetadata().getOrDefault("exchange", "N"));
            String exchType = String.valueOf(trade.getMetadata().getOrDefault("exchangeType", "C"));
            String orderEx = String.valueOf(trade.getMetadata().getOrDefault("orderExchange", exch));
            String orderExType = String.valueOf(trade.getMetadata().getOrDefault("orderExchangeType", exchType));
            BrokerOrderService.Side side = trade.isBullish() ? BrokerOrderService.Side.BUY : BrokerOrderService.Side.SELL;

            String orderId;
            boolean isOptionOrMcx = "M".equalsIgnoreCase(orderEx) || "D".equalsIgnoreCase(orderExType);
            if (isOptionOrMcx) {
                double limit = entryPrice;
                Object olpEntry = trade.getMetadata().get("orderLimitPriceEntry");
                if (!(olpEntry instanceof Number)) olpEntry = trade.getMetadata().get("orderLimitPrice");
                if (olpEntry instanceof Number n) limit = n.doubleValue();
                orderId = brokerOrderService.placeLimitOrder(orderScrip, orderEx, orderExType, side, trade.getPositionSize(), limit);
            } else {
                orderId = brokerOrderService.placeMarketOrder(orderScrip, orderEx, orderExType, side, trade.getPositionSize());
            }
            trade.addMetadata("brokerOrderId", orderId);
            log.info("Broker order placed: id={} scrip={} side={} qty={} exch={} exType={}", orderId, orderScrip, side, trade.getPositionSize(), orderEx, orderExType);
            // Paper trading: record fill price for P&L (simulated at limit/market used) and publish entry
            double fill = ("M".equalsIgnoreCase(orderEx) || "D".equalsIgnoreCase(orderExType)) ?
                    (trade.getMetadata().get("orderLimitPriceEntry") instanceof Number n ? n.doubleValue() : entryPrice)
                    : entryPrice;
            trade.addMetadata("fillEntryPrice", fill);
            try { profitLossProducer.publishTradeEntry(trade, fill); } catch (Exception ignore) {}
        } catch (Exception ex) {
            trade.addMetadata("brokerError", ex.toString());
            log.error("Broker order failed for {}: {}", trade.getScripCode(), ex.toString(), ex);
            // Keep trade ACTIVE for now; you may choose to revert status or trigger a retry policy here.
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

        trade.setMetadata(new HashMap<>());
        trade.addMetadata("signalPrice", signal.getEntryPrice());
        trade.addMetadata("exchange", signal.getExchange());
        trade.addMetadata("exchangeType", signal.getExchangeType());
        // Execution instrument overrides (option-only execution)
        if (signal.getOrderScripCode() != null) trade.addMetadata("orderScripCode", signal.getOrderScripCode());
        if (signal.getOrderExchange() != null) trade.addMetadata("orderExchange", signal.getOrderExchange());
        if (signal.getOrderExchangeType() != null) trade.addMetadata("orderExchangeType", signal.getOrderExchangeType());
        if (signal.getOrderLimitPrice() != null) trade.addMetadata("orderLimitPrice", signal.getOrderLimitPrice());
        if (signal.getOrderLimitPriceEntry() != null) trade.addMetadata("orderLimitPriceEntry", signal.getOrderLimitPriceEntry());
        if (signal.getOrderLimitPriceExit() != null) trade.addMetadata("orderLimitPriceExit", signal.getOrderLimitPriceExit());
        if (signal.getOrderTickSize() != null) trade.addMetadata("tickSize", signal.getOrderTickSize());
        return trade;
    }

    /** Maintain a rolling window of the most recent candles per instrument. */
    private void updateCandleHistory(Candlestick candle) {
        String key = candle.getCompanyName();
        List<Candlestick> history = recentCandles.computeIfAbsent(key, k -> new ArrayList<>());
        history.add(candle);
        // Keep a modest tail to support pattern checks (increase if needed)
        int max = 100;
        if (history.size() > max) {
            history.subList(0, history.size() - max).clear();
        }
    }

    /** Helpers / Queries */
    public String resolveCompanyName(String scripCode) {
        ActiveTrade at = activeTrade.get();
        if (at != null && scripCode.equals(at.getScripCode())) return at.getCompanyName();
        ActiveTrade wt = waitingTrades.get(scripCode);
        return wt != null ? wt.getCompanyName() : scripCode;
    }

    public boolean hasActiveTrade() {
        return activeTrade.get() != null;
    }

    public ActiveTrade getCurrentTrade() {
        return activeTrade.get();
    }

    public List<String> getWaitingTrade() {
        List<String> scripCodeList = new ArrayList<>();
        for (ActiveTrade t : waitingTrades.values()) {
            scripCodeList.add(t.getScripCode());
        }
        return scripCodeList;
    }

    /** Evaluate TP/SL against the current bar and exit if hit. */
    private void evaluateAndMaybeExit(ActiveTrade trade, Candlestick bar) {
        // Trailing stop logic using forwardtesting 1-min bars
        try {
            double entry = trade.getEntryPrice();
            double r = Math.abs(entry - trade.getStopLoss());
            if (r > 0.0 && trade.getStatus() == ActiveTrade.TradeStatus.ACTIVE) {
                Integer stage = (Integer) trade.getMetadata().getOrDefault("trailStage", 0);
                if (trade.isBullish()) {
                    double up = Math.max(trade.getHighSinceEntry(), bar.getHigh());
                    if (stage < 1 && up >= entry + trailStage1R * r) { trade.setStopLoss(entry + trailStage1StopR * r); trade.getMetadata().put("trailStage", 1); log.info("trail_s1 scrip={}", trade.getScripCode()); }
                    else if (stage < 2 && up >= entry + trailStage2R * r) { trade.setStopLoss(entry + trailStage2StopR * r); trade.getMetadata().put("trailStage", 2); log.info("trail_s2 scrip={}", trade.getScripCode()); }
                    else if (stage < 3 && up >= entry + trailStage3R * r) { trade.setStopLoss(entry + trailStage3StopR * r); trade.getMetadata().put("trailStage", 3); log.info("trail_s3 scrip={}", trade.getScripCode()); }
                } else {
                    double dn = Math.min(trade.getLowSinceEntry(), bar.getLow());
                    if (stage < 1 && dn <= entry - trailStage1R * r) { trade.setStopLoss(entry - trailStage1StopR * r); trade.getMetadata().put("trailStage", 1); log.info("trail_s1 scrip={} (bear)", trade.getScripCode()); }
                    else if (stage < 2 && dn <= entry - trailStage2R * r) { trade.setStopLoss(entry - trailStage2StopR * r); trade.getMetadata().put("trailStage", 2); log.info("trail_s2 scrip={} (bear)", trade.getScripCode()); }
                    else if (stage < 3 && dn <= entry - trailStage3R * r) { trade.setStopLoss(entry - trailStage3StopR * r); trade.getMetadata().put("trailStage", 3); log.info("trail_s3 scrip={} (bear)", trade.getScripCode()); }
                }
            }
        } catch (Exception ignore) {}
        // Update extrema since entry
        trade.setHighSinceEntry(Math.max(trade.getHighSinceEntry(), bar.getHigh()));
        trade.setLowSinceEntry(Math.min(trade.getLowSinceEntry(), bar.getLow()));

        boolean hitSL = trade.isBullish() ? bar.getLow() <= trade.getStopLoss()
                : bar.getHigh() >= trade.getStopLoss();
        boolean hitT1 = trade.isBullish() ? bar.getHigh() >= trade.getTarget1()
                : bar.getLow() <= trade.getTarget1();

        if (!hitSL && !hitT1) return;

        String reason = hitSL ? "STOP_LOSS" : "TARGET1";
        double exitPrice = hitSL ? trade.getStopLoss() : trade.getTarget1();

        // Place exit (market/limit) with the broker (re-price using Redis if available)
        try {
            String orderScrip = String.valueOf(trade.getMetadata().getOrDefault("orderScripCode", trade.getScripCode()));
            String exch = String.valueOf(trade.getMetadata().getOrDefault("exchange", "N"));
            String exType = String.valueOf(trade.getMetadata().getOrDefault("exchangeType", "C"));
            String orderEx = String.valueOf(trade.getMetadata().getOrDefault("orderExchange", exch));
            String orderExType = String.valueOf(trade.getMetadata().getOrDefault("orderExchangeType", exType));
            BrokerOrderService.Side sideToClose = trade.isBullish() ? BrokerOrderService.Side.SELL : BrokerOrderService.Side.BUY;

            String exitOrderId;
            boolean isOptionOrMcx = "M".equalsIgnoreCase(orderEx) || "D".equalsIgnoreCase(orderExType);
            if (isOptionOrMcx) {
                double limit = exitPrice;
                try {
                    if (executionStringRedisTemplate != null) {
                        String key = "orderbook:" + orderScrip + ":latest";
                        String json = executionStringRedisTemplate.opsForValue().get(key);
                        if (json != null && !json.isBlank()) {
                            var node = new com.fasterxml.jackson.databind.ObjectMapper().readTree(json);
                            double bestBid = node.path("bestBid").asDouble(0.0);
                            double bestAsk = node.path("bestAsk").asDouble(0.0);
                            double tick = 0.05;
                            Object t = trade.getMetadata().get("tickSize");
                            if (t instanceof Number n) tick = n.doubleValue();
                            limit = trade.isBullish() ? (bestBid > 0 ? bestBid - tick * optionSlippageTicksExit : limit)
                                                      : (bestAsk > 0 ? bestAsk + tick * optionSlippageTicksExit : limit);
                        }
                    }
                } catch (Exception ignore) {}
                Object olpExit = trade.getMetadata().get("orderLimitPriceExit");
                if (!(olpExit instanceof Number)) olpExit = trade.getMetadata().get("orderLimitPrice");
                if (olpExit instanceof Number n) limit = n.doubleValue();
                exitOrderId = brokerOrderService.placeLimitOrder(orderScrip, orderEx, orderExType, sideToClose, trade.getPositionSize(), limit);
            } else {
                exitOrderId = brokerOrderService.placeMarketOrder(orderScrip, orderEx, orderExType, sideToClose, trade.getPositionSize());
            }
            trade.addMetadata("exitOrderId", exitOrderId);
            log.info("Exit order placed: id={} scrip={} reason={}", exitOrderId, orderScrip, reason);
        } catch (Exception ex) {
            trade.addMetadata("brokerExitError", ex.toString());
            log.error("Broker exit failed for {}: {}", trade.getScripCode(), ex.toString(), ex);
            return; // keep trade active; will retry next bar
        }

        // Publish result & clear
        TradeResult result = new TradeResult();
        result.setTradeId(trade.getTradeId());
        result.setScripCode(trade.getScripCode());
        result.setEntryPrice(trade.getEntryPrice());
        result.setExitPrice(exitPrice);
        result.setExitReason(reason);
        tradeResultProducer.publishTradeResult(result);

        // Paper P&L
        try {
            double fillEntry = 0.0;
            Object fe = trade.getMetadata().get("fillEntryPrice");
            if (fe instanceof Number n) fillEntry = n.doubleValue(); else fillEntry = trade.getEntryPrice();
            double fillExit = exitPrice;
            Object ox = trade.getMetadata().get("orderLimitPriceExit");
            if (ox instanceof Number n) fillExit = n.doubleValue();
            double pnl = (trade.isBullish() ? (fillExit - fillEntry) : (fillEntry - fillExit)) * trade.getPositionSize();
            profitLossProducer.publishTradeExit(trade, fillExit, reason, pnl);
        } catch (Exception ignore) {}

        trade.setStatus(ActiveTrade.TradeStatus.COMPLETED);
        activeTrade.set(null);
        log.info("Trade completed: {} reason={} PnL={}", trade.getScripCode(), reason,
                (trade.isBullish() ? exitPrice - trade.getEntryPrice() : trade.getEntryPrice() - exitPrice));
    }
}
