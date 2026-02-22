package com.kotsin.execution.service;

import com.kotsin.execution.logic.TradeManager;
import com.kotsin.execution.model.BacktestTrade;
import com.kotsin.execution.model.StrategySignal;
import com.kotsin.execution.repository.BacktestTradeRepository;
import com.kotsin.execution.virtual.PriceProvider;
import com.kotsin.execution.virtual.VirtualEngineService;
import com.kotsin.execution.virtual.VirtualWalletRepository;
import com.kotsin.execution.virtual.model.VirtualOrder;
import com.kotsin.execution.virtual.model.VirtualPosition;
import com.kotsin.execution.virtual.model.VirtualSettings;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.Comparator;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.*;

/**
 * SignalBufferService - Cross-strategy signal deduplication with time buffer
 * and cross-instrument best-signal-per-timeframe selection.
 *
 * Layer 1 (per-scripCode, 35s): FUKAA > FUDKII for same instrument
 * Layer 2 (global batch, 60s): Pick best signal across all instruments by OI + Volume
 *
 * Flow:
 * 1. Consumer calls submitSignal(source, signal, metadata)
 * 2. Signal stored in per-scripCode buffer (35s timer)
 * 3. When per-scrip timer fires: resolve FUKAA > FUDKII → winner goes to batch
 * 4. Batch collects all per-scrip winners within 60s window
 * 5. When batch timer fires: rank by OI+Volume, execute only the BEST signal
 */
@Service
@Slf4j
public class SignalBufferService {

    private static final long BUFFER_SECONDS = 35;

    private final ConcurrentHashMap<String, BufferedSignalGroup> buffer = new ConcurrentHashMap<>();
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(4, r -> {
        Thread t = new Thread(r, "signal-buffer-timer");
        t.setDaemon(true);
        return t;
    });

    @Autowired
    private VirtualEngineService virtualEngine;

    @Autowired
    private VirtualWalletRepository walletRepo;

    @Autowired
    private PriceProvider priceProvider;

    @Autowired
    private TradeManager tradeManager;

    @Autowired
    private BacktestTradeRepository backtestRepository;

    @Value("${trading.mode.live:true}")
    private boolean liveTradeEnabled;

    @Value("${signal.batch.enabled:true}")
    private boolean batchEnabled;

    @Value("${signal.batch.window.seconds:60}")
    private int batchWindowSeconds;

    // ========== Per-ScripCode Buffer (Layer 1) ==========

    static class BufferedSignalGroup {
        volatile StrategySignal fudkiiSignal;
        volatile StrategySignal fukaaSignal;
        volatile BacktestTrade fudkiiVirtualTrade;
        volatile BacktestTrade fukaaVirtualTrade;
        volatile String fudkiiRationale;
        volatile String fukaaRationale;
        volatile LocalDateTime receivedTimeIst;
        volatile ScheduledFuture<?> timerFuture;
        final long createdAtMillis = System.currentTimeMillis();
    }

    // ========== Cross-Instrument Batch (Layer 2) ==========

    static class ResolvedSignal {
        String scripCode;
        String source;
        StrategySignal signal;
        BacktestTrade virtualTrade;
        String rationale;
        LocalDateTime receivedTimeIst;
        double rankScore;
    }

    static class TimeframeBatch {
        final long createdAtMillis = System.currentTimeMillis();
        final ConcurrentHashMap<String, ResolvedSignal> resolvedSignals = new ConcurrentHashMap<>();
        volatile ScheduledFuture<?> batchTimerFuture;
    }

    private volatile TimeframeBatch currentBatch;
    private final Object batchLock = new Object();

    // Independent category batches (FUDKOI, future strategies)
    // Each category has its own batch — doesn't compete with FUKAA/FUDKII pipeline
    private final ConcurrentHashMap<String, TimeframeBatch> categoryBatches = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Object> categoryLocks = new ConcurrentHashMap<>();

    // ========== Public API ==========

    /**
     * Submit a signal from a consumer. Called by both FUDKIISignalConsumer and FUKAASignalConsumer.
     */
    public void submitSignal(String source, StrategySignal signal, BacktestTrade virtualTrade,
                             String rationale, LocalDateTime receivedIst) {
        String scripCode = signal.getNumericScripCode() != null
                ? signal.getNumericScripCode() : signal.getScripCode();

        log.info("BUFFER_submit source={} scrip={} direction={} score={} oiRatio={} oiLabel={} surgeT={}",
                source, scripCode, signal.getDirection(), signal.getConfidence(),
                signal.getOiChangeRatio(), signal.getOiLabel(), signal.getSurgeT());

        BufferedSignalGroup group = buffer.compute(scripCode, (key, existing) -> {
            if (existing == null) {
                existing = new BufferedSignalGroup();
                existing.receivedTimeIst = receivedIst;
            }

            if ("FUKAA".equals(source)) {
                existing.fukaaSignal = signal;
                existing.fukaaVirtualTrade = virtualTrade;
                existing.fukaaRationale = rationale;
            } else {
                existing.fudkiiSignal = signal;
                existing.fudkiiVirtualTrade = virtualTrade;
                existing.fudkiiRationale = rationale;
            }

            return existing;
        });

        // Schedule timer on first signal for this scrip (only once)
        synchronized (group) {
            if (group.timerFuture == null) {
                group.timerFuture = scheduler.schedule(
                        () -> evaluateAndExecute(scripCode),
                        BUFFER_SECONDS, TimeUnit.SECONDS
                );
                log.info("BUFFER_timer_started scrip={} source={} bufferSec={}",
                        scripCode, source, BUFFER_SECONDS);
            } else {
                log.info("BUFFER_signal_added scrip={} source={} (timer already running)",
                        scripCode, source);
            }
        }
    }

    /**
     * Submit a signal for an independent strategy category (e.g., FUDKOI).
     * Skips Layer 1 (per-scrip dedup) — goes directly to category-specific batch.
     * Each category races independently: FUDKOI and FUKAA can both trade simultaneously.
     */
    public void submitIndependentSignal(String category, StrategySignal signal,
                                         BacktestTrade virtualTrade, String rationale,
                                         LocalDateTime receivedIst) {
        String scripCode = signal.getNumericScripCode() != null
                ? signal.getNumericScripCode() : signal.getScripCode();
        double rankScore = computeRankScoreForCategory(category, signal);

        ResolvedSignal resolved = new ResolvedSignal();
        resolved.scripCode = scripCode;
        resolved.source = category;
        resolved.signal = signal;
        resolved.virtualTrade = virtualTrade;
        resolved.rationale = rationale;
        resolved.receivedTimeIst = receivedIst;
        resolved.rankScore = rankScore;

        log.info("BATCH_{}_add scrip={} rankScore={} oiRatio={} oiLabel={}",
                category, scripCode, String.format("%.2f", rankScore),
                signal.getOiChangeRatio(), signal.getOiLabel());

        Object lock = categoryLocks.computeIfAbsent(category, k -> new Object());
        synchronized (lock) {
            TimeframeBatch batch = categoryBatches.computeIfAbsent(category, k -> new TimeframeBatch());
            batch.resolvedSignals.put(scripCode, resolved);

            if (batch.batchTimerFuture == null) {
                final TimeframeBatch b = batch;
                final String cat = category;
                batch.batchTimerFuture = scheduler.schedule(
                        () -> evaluateCategoryBatch(cat, b),
                        batchWindowSeconds, TimeUnit.SECONDS);
                log.info("BATCH_{}_timer_started windowSec={} firstScrip={}", category, batchWindowSeconds, scripCode);
            }
        }
    }

    // ========== Layer 1: Per-ScripCode Resolution ==========

    /**
     * Called when the per-scripCode buffer timer fires.
     * Resolves FUKAA > FUDKII for this instrument, then forwards to batch.
     */
    private void evaluateAndExecute(String scripCode) {
        BufferedSignalGroup group = buffer.remove(scripCode);
        if (group == null) {
            log.warn("BUFFER_expired_empty scrip={}", scripCode);
            return;
        }

        // Determine winner: FUKAA > FUDKII
        boolean hasFukaa = group.fukaaSignal != null;
        boolean hasFudkii = group.fudkiiSignal != null;

        String winnerSource;
        StrategySignal winnerSignal;
        BacktestTrade winnerTrade;
        BacktestTrade loserTrade = null;

        if (hasFukaa) {
            winnerSource = "FUKAA";
            winnerSignal = group.fukaaSignal;
            winnerTrade = group.fukaaVirtualTrade;
            if (hasFudkii) {
                loserTrade = group.fudkiiVirtualTrade;
                log.info("BUFFER_resolved scrip={} BOTH_FIRED → FUKAA wins (volume-confirmed). " +
                                "FUDKII score={} FUKAA score={}",
                        scripCode, group.fudkiiSignal.getConfidence(), group.fukaaSignal.getConfidence());
            } else {
                log.info("BUFFER_resolved scrip={} FUKAA_ONLY → executing FUKAA", scripCode);
            }
        } else if (hasFudkii) {
            winnerSource = "FUDKII";
            winnerSignal = group.fudkiiSignal;
            winnerTrade = group.fudkiiVirtualTrade;
            log.info("BUFFER_resolved scrip={} FUDKII_ONLY → executing FUDKII after {}s buffer",
                    scripCode, BUFFER_SECONDS);
        } else {
            log.warn("BUFFER_empty_group scrip={}", scripCode);
            return;
        }

        // Mark loser trade as superseded
        if (loserTrade != null) {
            loserTrade.setStatus(BacktestTrade.TradeStatus.FAILED);
            loserTrade.setExitReason("SUPERSEDED_BY_" + winnerSource);
            backtestRepository.save(loserTrade);
            log.info("BUFFER_loser_marked scrip={} loserId={} reason=SUPERSEDED_BY_{}",
                    scripCode, loserTrade.getId(), winnerSource);
        }

        // Layer 2: Add to cross-instrument batch (or execute directly if batching disabled)
        if (batchEnabled) {
            addToBatch(scripCode, winnerSource, winnerSignal, winnerTrade,
                    winnerSource.equals("FUKAA") ? group.fukaaRationale : group.fudkiiRationale,
                    group.receivedTimeIst);
        } else {
            // Direct execution (legacy behavior)
            executeSignal(scripCode, winnerSource, winnerSignal, winnerTrade, group.receivedTimeIst);
        }
    }

    // ========== Layer 2: Cross-Instrument Batch ==========

    /**
     * Add a per-scripCode winner to the global batch. Start batch timer on first entry.
     */
    private void addToBatch(String scripCode, String source, StrategySignal signal,
                            BacktestTrade virtualTrade, String rationale, LocalDateTime receivedTimeIst) {
        double rankScore = computeRankScore(signal);

        ResolvedSignal resolved = new ResolvedSignal();
        resolved.scripCode = scripCode;
        resolved.source = source;
        resolved.signal = signal;
        resolved.virtualTrade = virtualTrade;
        resolved.rationale = rationale;
        resolved.receivedTimeIst = receivedTimeIst;
        resolved.rankScore = rankScore;

        log.info("BATCH_add scrip={} source={} rankScore={} oiRatio={} oiLabel={} surgeT={}",
                scripCode, source, String.format("%.2f", rankScore),
                signal.getOiChangeRatio(), signal.getOiLabel(), signal.getSurgeT());

        synchronized (batchLock) {
            if (currentBatch == null) {
                currentBatch = new TimeframeBatch();
            }
            currentBatch.resolvedSignals.put(scripCode, resolved);

            // Start batch timer if not already running
            if (currentBatch.batchTimerFuture == null) {
                final TimeframeBatch batch = currentBatch;
                batch.batchTimerFuture = scheduler.schedule(
                        () -> evaluateBatch(batch),
                        batchWindowSeconds, TimeUnit.SECONDS
                );
                log.info("BATCH_timer_started windowSec={} firstScrip={}", batchWindowSeconds, scripCode);
            }
        }
    }

    /**
     * Compute OI score component. Extracted for reuse by category-specific ranking.
     * Direction-aligned OI gets 2x boost, counter-direction gets 1x.
     */
    private double computeOiScore(StrategySignal signal) {
        double oiRatio = signal.getOiChangeRatio();
        String oiLabel = signal.getOiLabel();
        boolean bullish = signal.isBullish();

        if (bullish) {
            if ("LONG_BUILDUP".equals(oiLabel)) return Math.abs(oiRatio) * 2.0;
            else if ("SHORT_COVERING".equals(oiLabel)) return Math.abs(oiRatio) * 1.0;
        } else {
            if ("SHORT_BUILDUP".equals(oiLabel)) return Math.abs(oiRatio) * 2.0;
            else if ("LONG_UNWINDING".equals(oiLabel)) return Math.abs(oiRatio) * 1.0;
        }
        return 0;
    }

    /**
     * Compute ranking score for cross-instrument selection (FUKAA/FUDKII pipeline).
     * Higher = better. Factors: OI in trade direction (60%) + Volume surge (40%).
     */
    private double computeRankScore(StrategySignal signal) {
        double oiScore = computeOiScore(signal);
        double volumeScore = Math.min(signal.getSurgeT(), 10.0);
        return oiScore * 0.6 + volumeScore * 0.4;
    }

    /**
     * Compute ranking score for independent category strategies.
     * FUDKOI uses 100% OI ranking; others fall back to default.
     */
    private double computeRankScoreForCategory(String category, StrategySignal signal) {
        if ("FUDKOI".equals(category)) {
            return computeOiScore(signal);
        }
        return computeRankScore(signal);
    }

    /**
     * Evaluate an independent category batch. Cleans up category state, then
     * delegates to the shared evaluateBatch() for winner selection and execution.
     */
    private void evaluateCategoryBatch(String category, TimeframeBatch batch) {
        synchronized (categoryLocks.computeIfAbsent(category, k -> new Object())) {
            if (categoryBatches.get(category) == batch) {
                categoryBatches.remove(category);
            }
        }
        // Reuse the exact same batch evaluation logic
        evaluateBatch(batch);
    }

    /**
     * Evaluate the batch: pick the best signal by rankScore, execute only that one.
     */
    private void evaluateBatch(TimeframeBatch batch) {
        synchronized (batchLock) {
            if (currentBatch == batch) {
                currentBatch = null; // Reset for next batch
            }
        }

        if (batch.resolvedSignals.isEmpty()) {
            log.warn("BATCH_empty no signals to evaluate");
            return;
        }

        int candidateCount = batch.resolvedSignals.size();

        // If only 1 signal, just execute it directly
        if (candidateCount == 1) {
            ResolvedSignal only = batch.resolvedSignals.values().iterator().next();
            log.info("BATCH_single_signal scrip={} source={} rankScore={} → executing directly",
                    only.scripCode, only.source, String.format("%.2f", only.rankScore));
            executeSignal(only.scripCode, only.source, only.signal, only.virtualTrade, only.receivedTimeIst);
            return;
        }

        // Multiple signals: pick the best by rankScore
        ResolvedSignal best = batch.resolvedSignals.values().stream()
                .max(Comparator.comparingDouble(r -> r.rankScore))
                .orElse(null);

        if (best == null) {
            log.warn("BATCH_no_winner after ranking {} candidates", candidateCount);
            return;
        }

        log.info("BATCH_WINNER scrip={} source={} rankScore={} oiRatio={} oiLabel={} surgeT={} (from {} candidates)",
                best.scripCode, best.source, String.format("%.2f", best.rankScore),
                best.signal.getOiChangeRatio(), best.signal.getOiLabel(),
                best.signal.getSurgeT(), candidateCount);

        // Log all candidates for audit
        for (Map.Entry<String, ResolvedSignal> entry : batch.resolvedSignals.entrySet()) {
            ResolvedSignal r = entry.getValue();
            String marker = r == best ? "★ WINNER" : "  SKIPPED";
            log.info("BATCH_candidate {} scrip={} source={} rankScore={} oiRatio={} surgeT={} dir={}",
                    marker, r.scripCode, r.source, String.format("%.2f", r.rankScore),
                    r.signal.getOiChangeRatio(), r.signal.getSurgeT(), r.signal.getDirection());
        }

        // Execute only the winner
        executeSignal(best.scripCode, best.source, best.signal, best.virtualTrade, best.receivedTimeIst);

        // Mark all others as superseded
        for (Map.Entry<String, ResolvedSignal> entry : batch.resolvedSignals.entrySet()) {
            ResolvedSignal r = entry.getValue();
            if (r != best && r.virtualTrade != null) {
                r.virtualTrade.setStatus(BacktestTrade.TradeStatus.FAILED);
                r.virtualTrade.setExitReason("SUPERSEDED_BY_BEST_" + best.scripCode);
                backtestRepository.save(r.virtualTrade);
                log.info("BATCH_superseded scrip={} source={} rankScore={} → lost to {}",
                        r.scripCode, r.source, String.format("%.2f", r.rankScore), best.scripCode);
            }
        }
    }

    // ========== Signal Execution ==========

    /**
     * Execute a resolved signal: forward to TradeManager + create paper trade.
     */
    private void executeSignal(String scripCode, String source, StrategySignal signal,
                               BacktestTrade virtualTrade, LocalDateTime receivedTimeIst) {
        // Forward to TradeManager for live execution
        if (liveTradeEnabled) {
            try {
                tradeManager.addSignalToWatchlist(signal, receivedTimeIst);
            } catch (Exception ex) {
                log.warn("BUFFER_watchlist_error scrip={} source={} err={}",
                        scripCode, source, ex.getMessage());
            }
        }

        // Execute paper trade
        boolean longSignal = signal.isLongSignal();
        String paperResult = handlePaperTrade(signal, scripCode,
                signal.getCompanyName(), longSignal, source);

        // Update trade status
        if ("FILLED".equals(paperResult)) {
            virtualTrade.setStatus(BacktestTrade.TradeStatus.ACTIVE);
        } else if ("SAME_DIRECTION".equals(paperResult)) {
            virtualTrade.setStatus(BacktestTrade.TradeStatus.FAILED);
            virtualTrade.setExitReason("SAME_DIRECTION_SKIP");
        } else {
            virtualTrade.setStatus(BacktestTrade.TradeStatus.FAILED);
            virtualTrade.setExitReason(paperResult);
        }
        backtestRepository.save(virtualTrade);

        log.info("BUFFER_execution_complete scrip={} source={} result={} tradeId={}",
                scripCode, source, paperResult, virtualTrade.getId());
    }

    /**
     * Execute paper trade via VirtualEngineService.
     */
    private String handlePaperTrade(StrategySignal signal, String scripCode,
                                    String companyName, boolean longSignal, String source) {
        try {
            String numericScrip = signal.getNumericScripCode() != null
                    ? signal.getNumericScripCode() : scripCode;

            // SWITCH detection: if opposite position exists, close it first
            Optional<VirtualPosition> existingPos = walletRepo.getPosition(numericScrip)
                    .filter(p -> p.getQtyOpen() > 0);
            if (existingPos.isPresent()) {
                VirtualPosition pos = existingPos.get();
                boolean existingIsLong = pos.getSide() == VirtualPosition.Side.LONG;
                if (existingIsLong != longSignal) {
                    log.info("{}_SWITCH scrip={} oldSide={} newSide={}", source, numericScrip,
                            pos.getSide(), longSignal ? "LONG" : "SHORT");
                    virtualEngine.closePosition(numericScrip);
                } else {
                    log.info("{}_same_direction_skip scrip={}", source, numericScrip);
                    return "SAME_DIRECTION";
                }
            }

            // Create paper trade
            double price = signal.getEntryPrice();
            Double ltp = priceProvider.getLtp(numericScrip);
            if (ltp != null && ltp > 0) price = ltp;

            VirtualSettings settings = walletRepo.loadSettings();
            double capitalPerTrade = Math.min(settings.getAccountValue() * 0.50, 50000.0);
            int qty = price > 0 ? (int) Math.floor(capitalPerTrade / price) : 1;
            if (qty < 1) qty = 1;

            VirtualOrder order = new VirtualOrder();
            order.setScripCode(numericScrip);
            order.setSide(longSignal ? VirtualOrder.Side.BUY : VirtualOrder.Side.SELL);
            order.setType(VirtualOrder.Type.MARKET);
            order.setQty(qty);
            order.setCurrentPrice(price);
            order.setSl(signal.getStopLoss());
            order.setTp1(signal.getTarget1());
            order.setTp2(signal.getTarget2());
            order.setTp1ClosePercent(0.5);  // 50% at T1
            order.setTrailingType("PCT");
            order.setTrailingValue(1.0);    // 1% trail after T1
            order.setTrailingStep(0.5);
            order.setSignalId(signal.getSignal() + "_" + numericScrip + "_" + signal.getTimestamp());
            order.setSignalType(signal.getSignal());
            order.setSignalSource(source);
            order.setExchange(signal.getExchange() != null ? signal.getExchange() : "N");
            order.setInstrumentSymbol(signal.getInstrumentSymbol() != null
                    ? signal.getInstrumentSymbol() : companyName);

            VirtualOrder executed = virtualEngine.createOrder(order);
            log.info("{}_paper_trade scrip={} status={} qty={} entry={} SL={} T1={}",
                    source, numericScrip, executed.getStatus(), qty, price,
                    signal.getStopLoss(), signal.getTarget1());
            return executed.getStatus() == VirtualOrder.Status.FILLED ? "FILLED" : "ORDER_REJECTED";
        } catch (Exception e) {
            log.error("{}_paper_trade_error scrip={} err={}", source, scripCode, e.getMessage());
            return "ERROR:" + e.getMessage();
        }
    }

    @PreDestroy
    public void shutdown() {
        log.info("SignalBufferService shutting down...");
        scheduler.shutdown();
        try {
            if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                scheduler.shutdownNow();
            }
        } catch (InterruptedException e) {
            scheduler.shutdownNow();
            Thread.currentThread().interrupt();
        }

        // Flush any active batch
        TimeframeBatch batch = currentBatch;
        if (batch != null && !batch.resolvedSignals.isEmpty()) {
            log.info("SignalBufferService flushing active batch with {} signals", batch.resolvedSignals.size());
            evaluateBatch(batch);
        }

        // Flush independent category batches
        for (Map.Entry<String, TimeframeBatch> entry : categoryBatches.entrySet()) {
            if (!entry.getValue().resolvedSignals.isEmpty()) {
                log.info("SignalBufferService flushing {} batch with {} signals",
                        entry.getKey(), entry.getValue().resolvedSignals.size());
                evaluateBatch(entry.getValue());
            }
        }

        // Execute any remaining per-scrip buffered signals
        for (String scripCode : buffer.keySet()) {
            try {
                evaluateAndExecute(scripCode);
            } catch (Exception e) {
                log.error("BUFFER_shutdown_flush_error scrip={} err={}", scripCode, e.getMessage());
            }
        }
        log.info("SignalBufferService shutdown complete. Remaining buffered: {}", buffer.size());
    }
}
