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
import com.kotsin.execution.service.LotSizeLookupService;
import com.kotsin.execution.options.BlackScholesCalculator;
import com.kotsin.execution.options.OptionGreeks;
import com.kotsin.execution.options.OptionGreeks.OptionType;
import com.kotsin.execution.wallet.service.FundAllocationService;
import com.kotsin.execution.wallet.service.SignalQueueService;
import com.kotsin.execution.wallet.service.StrategyWalletResolver;
import com.kotsin.execution.wallet.service.WalletTransactionService;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.*;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.util.*;
import java.util.concurrent.*;

/**
 * SignalBufferService - Unified cross-strategy signal batching with priority-based dedup.
 *
 * All strategies (FUKAA, FUDKII, FUDKOI, MERE) go into a single shared batch.
 * Priority: FUKAA > FUDKOI > FUDKII > MERE — same scrip only trades under highest-priority strategy.
 *
 * Dynamic batch windows (time-of-day aware):
 *   Opening session (9:00–10:00 IST): 3s  — high signal density, fast price moves
 *   Rest of day NSE (10:00–15:25):    5s  — sparse signals, less urgency
 *   After NSE close (15:25+):         2s  — only MCX/CDS active, fast movers
 *
 * Flow:
 * 1. Consumer calls submitSignal(source, signal, metadata)
 * 2. Signal added directly to shared batch (no per-scrip buffer delay)
 * 3. When batch timer fires: group by strategy, process in priority order,
 *    cross-strategy dedup, fund allocation, execute
 */
@Service
@Slf4j
public class SignalBufferService {

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
    private LotSizeLookupService lotSizeLookup;

    @Autowired
    private PriceProvider priceProvider;

    @Autowired
    private TradeManager tradeManager;

    @Autowired
    private BacktestTradeRepository backtestRepository;

    @Autowired(required = false)
    private FundAllocationService fundAllocationService;

    @Autowired(required = false)
    private SignalQueueService signalQueueService;

    @Autowired(required = false)
    private WalletTransactionService walletTransactionService;

    @Autowired(required = false)
    private com.kotsin.execution.wallet.repository.WalletRepository strategyWalletRepository;

    @Autowired(required = false)
    private McxMiniFallbackService mcxMiniFallbackService;

    @Autowired(required = false)
    private BlackScholesCalculator blackScholesCalculator;

    @Value("${option.greek.sl.iv.multiplier:1.5}")
    private double greekSlIvMultiplier;

    @Value("${option.greek.sl.min.floor:0.08}")
    private double greekSlMinFloor;

    @Value("${option.greek.gamma.boost.multiplier:15.0}")
    private double gammaBoostMultiplier;

    @Value("${option.greek.gamma.boost.cap:0.5}")
    private double gammaBoostCap;

    @Value("${option.greek.theta.impairment.threshold:0.05}")
    private double thetaImpairmentThreshold;

    @Value("${option.greek.min.dte:2}")
    private int greekMinDte;

    @Value("${option.greek.min.rr:1.0}")
    private double greekMinRR;

    private final RestTemplate restTemplate = new RestTemplate();

    @Value("${dashboard.backend.url:http://localhost:8085}")
    private String dashboardBackendUrl;

    @Value("${option.producer.base.url:http://localhost:8208}")
    private String optionProducerBaseUrl;

    @Value("${option.swap.retry.count:5}")
    private int optionSwapRetryCount;

    @Value("${option.swap.retry.delay.ms:400}")
    private int optionSwapRetryDelayMs;

    @Value("${trading.mode.live:true}")
    private boolean liveTradeEnabled;

    @Value("${signal.batch.enabled:true}")
    private boolean batchEnabled;

    @Value("${signal.batch.window.seconds:5}")
    private int batchWindowSeconds;

    @Value("${strategy.wallet.enabled:false}")
    private boolean strategyWalletEnabled;

    // ========== Cross-Instrument Batch ==========

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

    // Cross-strategy dedup — scripCode → strategyKey, prevents same scrip executing in multiple strategies
    private final ConcurrentHashMap<String, CrossBatchEntry> recentlyExecutedScrips = new ConcurrentHashMap<>();

    // Opening batch tracking: "FUDKII:N:2026-02-26" → true = opening batch already fired for this strategy+exchange+date
    private final ConcurrentHashMap<String, Boolean> openingBatchFired = new ConcurrentHashMap<>();
    private static final ZoneId IST = ZoneId.of("Asia/Kolkata");

    // Per-exchange first 30m candle close times
    private static final LocalTime NSE_FIRST_30M_CLOSE = LocalTime.of(9, 45);
    private static final LocalTime MCX_FIRST_30M_CLOSE = LocalTime.of(9, 30);
    private static final LocalTime CDS_FIRST_30M_CLOSE = LocalTime.of(9, 30);
    // Opening batch window: signals within this many minutes of first 30m close are "opening"
    private static final int OPENING_BATCH_WINDOW_MINUTES = 5;
    // Top N signals to push from opening batch
    private static final int OPENING_BATCH_TOP_N = 3;

    // Strategy priority order — higher priority strategies get first pick
    private static final List<String> STRATEGY_PRIORITY = List.of("FUKAA", "FUDKOI", "FUDKII", "MERE", "MCX_BB", "MCX_BBT1");

    // Dynamic batch window thresholds
    private static final LocalTime OPENING_SESSION_START = LocalTime.of(9, 0);
    private static final LocalTime OPENING_SESSION_END   = LocalTime.of(10, 0);
    private static final LocalTime NSE_CLOSE_TIME        = LocalTime.of(15, 25);

    static class CrossBatchEntry {
        final String strategyKey;
        final long timestamp;
        CrossBatchEntry(String strategyKey) {
            this.strategyKey = strategyKey;
            this.timestamp = System.currentTimeMillis();
        }
    }

    // ========== NSE No-Trade Window (3:15 PM – 3:25 PM IST) ==========
    // NSE positions close at 15:25 — block new entries 10 min before
    private static final LocalTime NSE_NO_TRADE_START = LocalTime.of(15, 15);
    private static final LocalTime NSE_NO_TRADE_END   = LocalTime.of(15, 25);

    /**
     * Returns true if the signal is within the NSE no-trade window (3:15–3:25 PM IST).
     * Only applies to NSE exchange ("N" or null/empty which defaults to NSE).
     */
    private boolean isInNseNoTradeWindow(StrategySignal signal) {
        String exchange = signal.getExchange();
        // Only applies to NSE (exchange "N" or unset which defaults to NSE)
        if (exchange != null && !exchange.isEmpty() && !"N".equalsIgnoreCase(exchange)) {
            return false;
        }
        LocalTime now = LocalTime.now(IST);
        return !now.isBefore(NSE_NO_TRADE_START) && !now.isAfter(NSE_NO_TRADE_END);
    }

    // ========== Public API ==========

    /**
     * Submit a signal from any consumer. All strategies go into the shared batch.
     * Cross-strategy dedup (FUKAA > FUDKOI > FUDKII > MERE) is handled at batch evaluation time.
     */
    public void submitSignal(String source, StrategySignal signal, BacktestTrade virtualTrade,
                             String rationale, LocalDateTime receivedIst) {
        String scripCode = signal.getNumericScripCode() != null
                ? signal.getNumericScripCode() : signal.getScripCode();

        // Block new NSE entries during 3:15–3:25 PM IST window
        if (isInNseNoTradeWindow(signal)) {
            log.info("NSE_NO_TRADE_WINDOW source={} scrip={} — signal blocked (3:15–3:25 PM IST), card still visible on dashboard",
                    source, scripCode);
            return;
        }

        log.info("SIGNAL_submit source={} scrip={} direction={} score={} oiRatio={} oiLabel={} surgeT={}",
                source, scripCode, signal.getDirection(), signal.getConfidence(),
                signal.getOiChangeRatio(), signal.getOiLabel(), signal.getSurgeT());

        // Build ResolvedSignal and add directly to shared batch (no per-scrip buffer delay)
        double rankScore = computeRankScoreForCategory(source, signal);

        ResolvedSignal resolved = new ResolvedSignal();
        resolved.scripCode = scripCode;
        resolved.source = source;
        resolved.signal = signal;
        resolved.virtualTrade = virtualTrade;
        resolved.rationale = rationale;
        resolved.receivedTimeIst = receivedIst;
        resolved.rankScore = rankScore;

        if (batchEnabled) {
            addToBatch(scripCode, source, signal, virtualTrade, rationale, receivedIst, resolved);
        } else {
            executeSignal(scripCode, source, signal, virtualTrade, receivedIst, 0);
        }
    }

    /**
     * Submit a signal for an independent strategy category (e.g., FUDKOI, MERE).
     * Now unified — delegates to the shared batch just like submitSignal().
     */
    public void submitIndependentSignal(String category, StrategySignal signal,
                                         BacktestTrade virtualTrade, String rationale,
                                         LocalDateTime receivedIst) {
        submitSignal(category, signal, virtualTrade, rationale, receivedIst);
    }

    // ========== Cross-Instrument Batch ==========

    /**
     * Compute dynamic batch window based on time of day (IST).
     *   Opening session (9:00–10:00): 3s — high density, co-arriving signals
     *   Rest of day NSE (10:00–15:25): 5s — sparse signals
     *   After NSE close (15:25+): 2s — only MCX/CDS, fast movers
     */
    private int getDynamicBatchWindowSeconds() {
        LocalTime now = LocalTime.now(IST);
        if (!now.isBefore(OPENING_SESSION_START) && now.isBefore(OPENING_SESSION_END)) {
            return 3; // Opening session: fast batch
        }
        if (!now.isBefore(NSE_CLOSE_TIME)) {
            return 2; // After NSE close: only MCX/CDS active
        }
        return batchWindowSeconds; // Rest of day: configurable (default 5s)
    }

    /**
     * Add a signal directly to the shared batch. Start batch timer on first entry.
     * All strategies share one batch — cross-strategy dedup happens at evaluation time.
     */
    private void addToBatch(String scripCode, String source, StrategySignal signal,
                            BacktestTrade virtualTrade, String rationale,
                            LocalDateTime receivedTimeIst, ResolvedSignal resolved) {
        // Use strategy-qualified key: "FUKAA|12345" so same scrip from different strategies coexists in batch
        String batchKey = source + "|" + scripCode;

        log.info("BATCH_add scrip={} source={} rankScore={} oiRatio={} oiLabel={} surgeT={}",
                scripCode, source, String.format("%.2f", resolved.rankScore),
                signal.getOiChangeRatio(), signal.getOiLabel(), signal.getSurgeT());

        synchronized (batchLock) {
            if (currentBatch == null) {
                currentBatch = new TimeframeBatch();
            }
            currentBatch.resolvedSignals.put(batchKey, resolved);

            // Start batch timer if not already running
            if (currentBatch.batchTimerFuture == null) {
                int windowSec = getDynamicBatchWindowSeconds();
                final TimeframeBatch batch = currentBatch;
                batch.batchTimerFuture = scheduler.schedule(
                        () -> evaluateBatch(batch),
                        windowSec, TimeUnit.SECONDS
                );
                log.info("BATCH_timer_started windowSec={} firstScrip={} source={}", windowSec, scripCode, source);
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
        // When OI data unavailable (most NSE equities), use volume surge as sole ranking factor
        if (oiScore == 0 && volumeScore > 0) {
            return volumeScore;
        }
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
     * Evaluate the batch using centralized fund allocation.
     *
     * Fund allocation rules (per strategy wallet):
     * - Available fund = 50% of wallet balance
     * - First window of day (per exchange): proportional allocation across ALL signals by rankScore
     * - Subsequent windows: single winner gets 50% of remaining deployable fund
     * - P&L recycled to wallet when trades close
     *
     * Falls back to legacy single-winner behavior if fund allocation is disabled.
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

        // Log all candidates for audit
        for (Map.Entry<String, ResolvedSignal> entry : batch.resolvedSignals.entrySet()) {
            ResolvedSignal r = entry.getValue();
            log.info("BATCH_candidate scrip={} source={} rankScore={} oiRatio={} surgeT={} dir={} exchange={}",
                    r.scripCode, r.source, String.format("%.2f", r.rankScore),
                    r.signal.getOiChangeRatio(), r.signal.getSurgeT(),
                    r.signal.getDirection(), r.signal.getExchange());
        }

        // Use centralized fund allocation if strategy wallets are enabled
        if (strategyWalletEnabled && fundAllocationService != null) {
            evaluateBatchWithFundAllocation(batch);
            return;
        }

        // Legacy behavior: single winner per batch
        evaluateBatchLegacy(batch);
    }

    /**
     * Fund-allocation-aware batch evaluation with cross-strategy dedup.
     *
     * Process strategies in priority order: FUKAA → FUDKOI → FUDKII → MERE.
     * Same scrip only trades under the highest-priority strategy that fired for it.
     * Uses strategy-specific ranking (surgeT for FUKAA, OI for FUDKOI, KII for FUDKII).
     */
    private void evaluateBatchWithFundAllocation(TimeframeBatch batch) {
        // Clean up expired cross-batch dedup entries (2 × current dynamic window)
        long expiryMs = getDynamicBatchWindowSeconds() * 2000L;
        long now = System.currentTimeMillis();
        recentlyExecutedScrips.entrySet().removeIf(e -> (now - e.getValue().timestamp) > expiryMs);

        // Clean up old opening batch tracking entries (keep only today)
        String todayStr = LocalDate.now(IST).toString();
        openingBatchFired.keySet().removeIf(k -> !k.endsWith(todayStr));

        // Group by strategy key (derived from source)
        Map<String, List<ResolvedSignal>> byStrategy = new LinkedHashMap<>();
        for (ResolvedSignal r : batch.resolvedSignals.values()) {
            String strategyKey = StrategyWalletResolver.resolveStrategyKey(r.source, null);
            if (strategyKey == null) strategyKey = r.source; // fallback
            byStrategy.computeIfAbsent(strategyKey, k -> new ArrayList<>()).add(r);
        }

        // Process in priority order — FUKAA first, then FUDKOI, then FUDKII, then MERE
        List<String> orderedStrategies = new ArrayList<>();
        for (String priority : STRATEGY_PRIORITY) {
            if (byStrategy.containsKey(priority)) orderedStrategies.add(priority);
        }
        // Add any remaining strategies not in priority list
        for (String key : byStrategy.keySet()) {
            if (!orderedStrategies.contains(key)) orderedStrategies.add(key);
        }

        Set<String> executedScrips = new HashSet<>();

        for (String strategyKey : orderedStrategies) {
            List<ResolvedSignal> signals = byStrategy.get(strategyKey);

            // Re-rank signals using strategy-specific scoring
            for (ResolvedSignal r : signals) {
                r.rankScore = computeStrategySpecificRank(strategyKey, r);
            }

            // Filter out scrips already executed by higher-priority strategy
            List<ResolvedSignal> eligible = new ArrayList<>();
            for (ResolvedSignal r : signals) {
                // Within-batch dedup
                if (executedScrips.contains(r.scripCode)) {
                    log.info("CROSS_STRATEGY_DEDUP scrip={} skipped for {} (already executed in this batch)",
                            r.scripCode, strategyKey);
                    if (r.virtualTrade != null) {
                        r.virtualTrade.setStatus(BacktestTrade.TradeStatus.FAILED);
                        r.virtualTrade.setExitReason("CROSS_STRATEGY_DEDUP_WITHIN_BATCH");
                        backtestRepository.save(r.virtualTrade);
                    }
                    continue;
                }
                // Cross-batch dedup
                CrossBatchEntry alreadyBy = recentlyExecutedScrips.get(r.scripCode);
                if (alreadyBy != null) {
                    int alreadyPriority = STRATEGY_PRIORITY.indexOf(alreadyBy.strategyKey);
                    int currentPriority = STRATEGY_PRIORITY.indexOf(strategyKey);
                    if (alreadyPriority >= 0 && currentPriority >= 0 && alreadyPriority < currentPriority) {
                        log.info("CROSS_BATCH_DEDUP scrip={} already executed by {} (higher priority than {})",
                                r.scripCode, alreadyBy.strategyKey, strategyKey);
                        if (r.virtualTrade != null) {
                            r.virtualTrade.setStatus(BacktestTrade.TradeStatus.FAILED);
                            r.virtualTrade.setExitReason("CROSS_BATCH_DEDUP_BY_" + alreadyBy.strategyKey);
                            backtestRepository.save(r.virtualTrade);
                        }
                        continue;
                    }
                }
                eligible.add(r);
            }

            if (eligible.isEmpty()) continue;

            // ========== FUDKII Opening Batch: Top 3 by KII_Score ==========
            // At each exchange's first 30m close, pick the top 3 FUDKII signals.
            // Ranking: KII_Score. Tiebreaker: blockTradeVol × entryPrice (INR block deal value).
            if ("FUDKII".equals(strategyKey) && isOpeningBatch(strategyKey, eligible)) {
                executeOpeningBatchTop3(strategyKey, eligible, executedScrips);
                continue;
            }

            // Build allocation requests (include minLotCost for slot consolidation)
            List<FundAllocationService.SignalAllocationRequest> requests = eligible.stream()
                    .map(r -> {
                        double minLotCost = computeMinLotCost(r);
                        return FundAllocationService.SignalAllocationRequest.builder()
                            .scripCode(r.scripCode)
                            .rankScore(r.rankScore)
                            .oiChangeRatio(r.signal.getOiChangeRatio())
                            .exchange(r.signal.getExchange() != null ? r.signal.getExchange() : "N")
                            .confidence(r.signal.getConfidence())
                            .riskRewardRatio(r.signal.getRiskRewardRatio())
                            .minLotCost(minLotCost)
                            .build();
                    })
                    .toList();

            // Get allocations from centralized service (slot-based confidence sizing)
            Map<String, Double> allocations = fundAllocationService.computeBatchAllocation(
                    strategyKey, requests);

            // Execute allocated signals with LTP validation and capital cascade
            double cascadeCapital = 0; // Capital from rejected signals, passed to next in rank
            int executionRank = 0;     // Track execution order for rank labels
            Set<String> rejectedScrips = new HashSet<>();

            for (ResolvedSignal r : eligible) {
                Double allocatedCapital = allocations.get(r.scripCode);
                double totalCapital = (allocatedCapital != null ? allocatedCapital : 0) + cascadeCapital;

                if (totalCapital > 0) {
                    executionRank++;
                    // Set rank label on signal for dashboard display
                    r.signal.setRationale(r.signal.getRationale() + " | Rank #" + executionRank);

                    log.info("BATCH_ALLOCATED scrip={} source={} strategy={} capital={} rankScore={} rank=#{}{}",
                            r.scripCode, r.source, strategyKey,
                            String.format("%.0f", totalCapital),
                            String.format("%.2f", r.rankScore), executionRank,
                            cascadeCapital > 0 ? String.format(" (includes %.0f cascade)", cascadeCapital) : "");

                    cascadeCapital = 0; // Reset cascade — this signal gets it

                    boolean filled = executeSignal(r.scripCode, r.source, r.signal, r.virtualTrade,
                            r.receivedTimeIst, totalCapital);
                    if (filled) {
                        executedScrips.add(r.scripCode);
                        recentlyExecutedScrips.put(r.scripCode, new CrossBatchEntry(strategyKey));
                        // Cascade leftover capital from lot rounding to next rank
                        double leftover = r.signal.getLeftoverCapital();
                        if (leftover > 0) {
                            cascadeCapital = leftover;
                            log.info("BATCH_LEFTOVER scrip={} rank=#{} leftover={} — cascading to next",
                                r.scripCode, executionRank, String.format("%.0f", leftover));
                        }
                    } else {
                        // Check if rejected due to LTP out of range — cascade FULL capital to next
                        String exitReason = r.virtualTrade != null ? r.virtualTrade.getExitReason() : "";
                        if ("LTP_OUT_OF_RANGE".equals(exitReason)) {
                            cascadeCapital = totalCapital;
                            rejectedScrips.add(r.scripCode);
                            log.info("BATCH_LTP_REJECTED scrip={} strategy={} rank=#{} — capital {} cascading to next",
                                    r.scripCode, strategyKey, executionRank, String.format("%.0f", totalCapital));
                        } else {
                            log.info("BATCH_EXEC_FAILED scrip={} strategy={} rank=#{} reason={}",
                                    r.scripCode, strategyKey, executionRank, exitReason);
                        }
                    }
                }
            }

            // Mark non-allocated and rejected eligible signals
            for (ResolvedSignal r : eligible) {
                if (!executedScrips.contains(r.scripCode) && !rejectedScrips.contains(r.scripCode)
                        && r.virtualTrade != null) {
                    r.virtualTrade.setStatus(BacktestTrade.TradeStatus.FAILED);
                    r.virtualTrade.setExitReason("FUND_ALLOC_NOT_SELECTED");
                    backtestRepository.save(r.virtualTrade);
                    log.info("BATCH_not_allocated scrip={} source={} strategy={} rankScore={}",
                            r.scripCode, r.source, strategyKey,
                            String.format("%.2f", r.rankScore));
                }
            }
        }
    }

    /**
     * Compute strategy-specific rank score.
     * FUDKII: KII_Score = (|OIChange%| + surgeT×100) / 2 (from signal.kiiScore)
     * FUKAA: volume surge (surgeT) is primary ranking.
     * FUDKOI: OI is primary ranking.
     * Others: default composite (OI 60% + Volume 40%).
     */
    private double computeStrategySpecificRank(String strategyKey, ResolvedSignal r) {
        if ("FUDKII".equals(strategyKey)) {
            // KII_Score — precomputed by FUDKIISignalConsumer
            double kii = r.signal.getKiiScore();
            if (kii > 0) return kii;
            // Fallback: compute KII from raw fields
            return (Math.abs(r.signal.getOiChangeRatio()) + r.signal.getSurgeT() * 100.0) / 2.0;
        } else if ("FUKAA".equals(strategyKey)) {
            return Math.min(r.signal.getSurgeT(), 20.0); // volume surge is primary for FUKAA
        } else if ("FUDKOI".equals(strategyKey)) {
            return computeOiScore(r.signal); // OI is primary for FUDKOI
        }
        return r.rankScore; // default composite
    }

    /**
     * Compute minimum cost to buy 1 lot for a signal.
     * For option signals: optionLtp × optionLotSize.
     * For equity/commodity signals: entryPrice × multiplier (from LotSizeLookupService).
     *
     * MCX MINI FALLBACK: For MCX futures-only commodities with a fallback chain,
     * reports the cheapest affordable variant's lot cost so FundAllocationService
     * can allocate slots based on the mini variant cost rather than the original.
     * This prevents SLOT_LOT_TOO_EXPENSIVE rejection for expensive MCX lots
     * that have cheaper mini variants (e.g., ALUMINIUM -> ALUMINI).
     */
    private double computeMinLotCost(ResolvedSignal r) {
        try {
            // Option path: use option pricing
            if (r.signal.isOptionAvailable() && r.signal.getOptionLtp() > 0 && r.signal.getOptionLotSize() > 0) {
                return r.signal.getOptionLtp() * r.signal.getOptionLotSize();
            }
            // Equity/commodity path: use lot size lookup
            double price = r.signal.getEntryPrice();
            if (price <= 0) return 0;
            int multiplier = lotSizeLookup.getMultiplier(r.scripCode);
            double originalCost = price * multiplier;

            // MCX mini fallback: if original lot is expensive and a cheaper variant exists,
            // report the cheapest variant's cost so FundAllocationService doesn't reject it.
            // We use findCheapestVariantCost() which returns the smallest variant's lot cost
            // from the fallback chain (e.g., SILVERMIC for SILVER, GOLDTEN for GOLD).
            if ("M".equalsIgnoreCase(r.signal.getExchange()) && mcxMiniFallbackService != null) {
                String symbolRoot = McxMiniFallbackService.extractSymbolRoot(r.signal.getCompanyName());
                if (symbolRoot != null && !mcxMiniFallbackService.isOptionsWhitelisted(symbolRoot)
                        && mcxMiniFallbackService.hasFallbackChain(symbolRoot)) {
                    double cheapestCost = mcxMiniFallbackService.findCheapestVariantCost(symbolRoot, price);
                    if (cheapestCost > 0 && cheapestCost < originalCost) {
                        log.debug("[MCX-FALLBACK] minLotCost for {} reduced from {} to {} (cheapest mini)",
                                symbolRoot, String.format("%.0f", originalCost),
                                String.format("%.0f", cheapestCost));
                        return cheapestCost;
                    }
                }
            }

            return originalCost;
        } catch (Exception e) {
            log.warn("ERR [LOT-COST] Failed to compute minLotCost for scrip={}: {}", r.scripCode, e.getMessage());
            return 0;
        }
    }

    // ========== Opening Batch Logic ==========

    /**
     * Determine if this is the opening batch for the given strategy.
     * Opening batch = first FUDKII batch of the day, near the exchange's first 30m candle close.
     * NSE: 9:45, MCX/Currency: 9:30.
     */
    private boolean isOpeningBatch(String strategyKey, List<ResolvedSignal> signals) {
        if (signals.isEmpty()) return false;
        LocalDateTime now = LocalDateTime.now(IST);
        LocalDate today = now.toLocalDate();
        LocalTime nowTime = now.toLocalTime();

        // Check each exchange represented in the signals
        Set<String> exchanges = new HashSet<>();
        for (ResolvedSignal r : signals) {
            String exch = r.signal.getExchange() != null ? r.signal.getExchange() : "N";
            exchanges.add(exch);
        }

        for (String exch : exchanges) {
            String key = strategyKey + ":" + exch + ":" + today;
            if (openingBatchFired.containsKey(key)) continue;

            // Determine first 30m close time for this exchange
            LocalTime first30mClose;
            if ("M".equalsIgnoreCase(exch)) {
                first30mClose = MCX_FIRST_30M_CLOSE;
            } else if ("C".equalsIgnoreCase(exch)) {
                first30mClose = CDS_FIRST_30M_CLOSE;
            } else {
                first30mClose = NSE_FIRST_30M_CLOSE;
            }

            // Check if current time is within the opening window
            LocalTime windowEnd = first30mClose.plusMinutes(OPENING_BATCH_WINDOW_MINUTES);
            if (!nowTime.isBefore(first30mClose) && nowTime.isBefore(windowEnd)) {
                log.info("OPENING_BATCH_DETECTED strategy={} exchange={} first30m={} now={}",
                        strategyKey, exch, first30mClose, nowTime);
                return true;
            }
        }
        return false;
    }

    /**
     * Execute the opening batch: pick top 3 by KII_Score, tiebreak by block deal INR value.
     * Marks this exchange+strategy+date as "opening batch fired" to prevent re-triggering.
     */
    private void executeOpeningBatchTop3(String strategyKey, List<ResolvedSignal> signals,
                                          Set<String> executedScrips) {
        LocalDate today = LocalDate.now(IST);

        // Mark opening batch as fired for each exchange in these signals
        Set<String> exchanges = new HashSet<>();
        for (ResolvedSignal r : signals) {
            String exch = r.signal.getExchange() != null ? r.signal.getExchange() : "N";
            exchanges.add(exch);
            openingBatchFired.put(strategyKey + ":" + exch + ":" + today, true);
        }

        // Sort by KII_Score (rankScore already set to KII by computeStrategySpecificRank)
        // Tiebreaker: block deal INR value = blockTradeVol × entryPrice
        signals.sort((a, b) -> {
            int cmp = Double.compare(b.rankScore, a.rankScore); // descending KII
            if (cmp != 0) return cmp;
            // Tiebreaker: block deal INR value (higher wins)
            double blockInrA = a.signal.getBlockTradeVol() * a.signal.getEntryPrice();
            double blockInrB = b.signal.getBlockTradeVol() * b.signal.getEntryPrice();
            return Double.compare(blockInrB, blockInrA);
        });

        // Log all candidates
        log.info("OPENING_BATCH_EVAL strategy={} exchanges={} candidates={}", strategyKey, exchanges, signals.size());
        for (int i = 0; i < signals.size(); i++) {
            ResolvedSignal r = signals.get(i);
            double blockInr = r.signal.getBlockTradeVol() * r.signal.getEntryPrice();
            log.info("OPENING_BATCH_RANK #{} scrip={} KII={} OI={}% surge={}x blockINR={} dir={}",
                    i + 1, r.scripCode, String.format("%.1f", r.rankScore),
                    r.signal.getOiChangeRatio(), r.signal.getSurgeT(),
                    String.format("%.0f", blockInr), r.signal.getDirection());
        }

        // Pick top N
        int topN = Math.min(OPENING_BATCH_TOP_N, signals.size());
        List<ResolvedSignal> winners = signals.subList(0, topN);

        // Build allocation requests for the top N signals (include minLotCost for slot consolidation)
        List<FundAllocationService.SignalAllocationRequest> requests = winners.stream()
                .map(r -> {
                    double minLotCost = computeMinLotCost(r);
                    return FundAllocationService.SignalAllocationRequest.builder()
                        .scripCode(r.scripCode)
                        .rankScore(r.rankScore)
                        .oiChangeRatio(r.signal.getOiChangeRatio())
                        .exchange(r.signal.getExchange() != null ? r.signal.getExchange() : "N")
                        .confidence(r.signal.getConfidence())
                        .riskRewardRatio(r.signal.getRiskRewardRatio())
                        .minLotCost(minLotCost)
                        .build();
                })
                .toList();

        // Get fund allocations for the top N (slot-based confidence sizing)
        Map<String, Double> allocations = fundAllocationService.computeBatchAllocation(
                strategyKey, requests);

        // Execute top N with LTP validation and capital cascade
        double openingCascadeCapital = 0;
        int openingRank = 0;
        for (ResolvedSignal r : winners) {
            Double allocatedCapital = allocations.get(r.scripCode);
            double totalCapital = (allocatedCapital != null ? allocatedCapital : 0) + openingCascadeCapital;

            if (totalCapital > 0) {
                openingRank++;
                r.signal.setRationale(r.signal.getRationale() + " | Rank #" + openingRank);

                double blockInr = r.signal.getBlockTradeVol() * r.signal.getEntryPrice();
                log.info("OPENING_BATCH_EXECUTE scrip={} KII={} capital={} blockINR={} rank=#{}{}",
                        r.scripCode, String.format("%.1f", r.rankScore),
                        String.format("%.0f", totalCapital), String.format("%.0f", blockInr),
                        openingRank,
                        openingCascadeCapital > 0 ? String.format(" (includes %.0f cascade)", openingCascadeCapital) : "");

                openingCascadeCapital = 0;

                boolean filled = executeSignal(r.scripCode, r.source, r.signal, r.virtualTrade,
                        r.receivedTimeIst, totalCapital);
                if (filled) {
                    executedScrips.add(r.scripCode);
                    recentlyExecutedScrips.put(r.scripCode, new CrossBatchEntry(strategyKey));
                    double leftover = r.signal.getLeftoverCapital();
                    if (leftover > 0) {
                        openingCascadeCapital = leftover;
                        log.info("OPENING_LEFTOVER scrip={} rank=#{} leftover={} — cascading to next",
                            r.scripCode, openingRank, String.format("%.0f", leftover));
                    }
                } else {
                    String exitReason = r.virtualTrade != null ? r.virtualTrade.getExitReason() : "";
                    if ("LTP_OUT_OF_RANGE".equals(exitReason)) {
                        openingCascadeCapital = totalCapital;
                        log.info("OPENING_LTP_REJECTED scrip={} rank=#{} — capital {} cascading to next",
                                r.scripCode, openingRank, String.format("%.0f", totalCapital));
                    } else {
                        log.info("OPENING_DEDUP_SKIP scrip={} strategy={} rank=#{} — execution failed",
                                r.scripCode, strategyKey, openingRank);
                    }
                }
            }
        }

        // Mark non-winners as not selected
        for (int i = topN; i < signals.size(); i++) {
            ResolvedSignal r = signals.get(i);
            if (r.virtualTrade != null) {
                r.virtualTrade.setStatus(BacktestTrade.TradeStatus.FAILED);
                r.virtualTrade.setExitReason("OPENING_BATCH_NOT_TOP_" + OPENING_BATCH_TOP_N);
                backtestRepository.save(r.virtualTrade);
                log.info("OPENING_BATCH_SKIPPED scrip={} KII={} rank=#{} (only top {} selected)",
                        r.scripCode, String.format("%.1f", r.rankScore), i + 1, OPENING_BATCH_TOP_N);
            }
        }

        log.info("OPENING_BATCH_COMPLETE strategy={} executed={} skipped={}",
                strategyKey, topN, signals.size() - topN);
    }

    /**
     * Legacy batch evaluation: single winner per batch (fallback when strategy wallets disabled).
     */
    private void evaluateBatchLegacy(TimeframeBatch batch) {
        int candidateCount = batch.resolvedSignals.size();

        if (candidateCount == 1) {
            ResolvedSignal only = batch.resolvedSignals.values().iterator().next();
            log.info("BATCH_single_signal scrip={} source={} rankScore={} → executing directly",
                    only.scripCode, only.source, String.format("%.2f", only.rankScore));
            executeSignal(only.scripCode, only.source, only.signal, only.virtualTrade,
                    only.receivedTimeIst, 0);
            return;
        }

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

        executeSignal(best.scripCode, best.source, best.signal, best.virtualTrade,
                best.receivedTimeIst, 0);

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
     *
     * @param allocatedCapital capital allocated by FundAllocationService (0 = use legacy sizing)
     * @return true if the trade was successfully filled (FILLED or OPTION_FILLED), false otherwise
     */
    private boolean executeSignal(String scripCode, String source, StrategySignal signal,
                               BacktestTrade virtualTrade, LocalDateTime receivedTimeIst,
                               double allocatedCapital) {
        // Forward to TradeManager for live equity execution ONLY when no option routing.
        // BUG FIX: When optionAvailable=true, the option pipeline (handlePaperTrade → routeToOptionTrade)
        // handles execution via StrategyTradeExecutor on port 8085. TradeManager is equity-only and
        // would create a duplicate trade on the underlying scripCode, causing phantom P&L and
        // rejected broker orders (wrong exchange type, no margin for cash equity).
        if (liveTradeEnabled && !(signal.isOptionAvailable() && signal.getOptionLtp() > 0)) {
            try {
                tradeManager.addSignalToWatchlist(signal, receivedTimeIst);
            } catch (Exception ex) {
                log.warn("BUFFER_watchlist_error scrip={} source={} err={}",
                        scripCode, source, ex.getMessage());
            }
        }

        // Execute paper trade with allocated capital
        boolean longSignal = signal.isLongSignal();
        String paperResult = handlePaperTrade(signal, scripCode,
                signal.getCompanyName(), longSignal, source, allocatedCapital);

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

        log.info("BUFFER_execution_complete scrip={} source={} result={} capital={} tradeId={}",
                scripCode, source, paperResult,
                allocatedCapital > 0 ? String.format("%.0f", allocatedCapital) : "legacy",
                virtualTrade.getId());

        // Return true only if trade was actually filled — not queued, rejected, or failed
        return "FILLED".equals(paperResult) || "OPTION_FILLED".equals(paperResult);
    }

    /**
     * Execute paper trade via VirtualEngineService.
     *
     * @param allocatedCapital capital from FundAllocationService. If > 0, uses this instead of
     *                         legacy calculation. If 0, falls back to legacy 50%/50k cap.
     */
    private String handlePaperTrade(StrategySignal signal, String scripCode,
                                    String companyName, boolean longSignal, String source,
                                    double allocatedCapital) {
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

            // Create paper trade — use real-time LTP for accurate entry
            double price = signal.getEntryPrice();
            Double ltp = priceProvider.getLtp(numericScrip);
            if (ltp != null && ltp > 0) {
                // Validate real-time LTP is within trade setup (SL < LTP < T1)
                double sl = signal.getStopLoss();
                double t1 = signal.getTarget1();
                if (sl > 0 && t1 > 0 && (ltp < sl || ltp > t1)) {
                    log.warn("EQUITY_LTP_OUT_OF_RANGE scrip={} signalEntry={} realtimeLtp={} SL={} T1={}",
                        numericScrip, String.format("%.2f", price), String.format("%.2f", ltp),
                        String.format("%.2f", sl), String.format("%.2f", t1));
                    return "LTP_OUT_OF_RANGE";
                }
                price = ltp;
            } else {
                log.warn("STALE_PRICE_FALLBACK scrip={} source={} — PriceProvider returned null, using signal entry={}",
                    numericScrip, source, String.format("%.2f", price));
            }

            // Position sizing: use allocated capital from FundAllocationService, or legacy fallback
            // Cap per-trade capital at 20% of wallet balance to prevent single-trade concentration
            double maxPerTradeCapPct = 0.20;
            double capitalPerTrade;
            if (allocatedCapital > 0) {
                capitalPerTrade = allocatedCapital;
            } else if (strategyWalletEnabled && signalQueueService != null) {
                // BUG-013A FIX: Fund allocation returned 0 → queue the signal
                log.warn("FUND_ALLOC_ZERO scrip={} source={} → queuing signal", numericScrip, source);
                VirtualOrder queueOrder = new VirtualOrder();
                queueOrder.setScripCode(numericScrip);
                queueOrder.setSide(longSignal ? VirtualOrder.Side.BUY : VirtualOrder.Side.SELL);
                queueOrder.setType(VirtualOrder.Type.MARKET);
                queueOrder.setQty(1); // placeholder
                queueOrder.setCurrentPrice(price);
                queueOrder.setSl(signal.getStopLoss());
                queueOrder.setTp1(signal.getTarget1());
                queueOrder.setTp2(signal.getTarget2());
                queueOrder.setSignalSource(source);
                queueOrder.setSignalType(signal.getSignal());
                queueOrder.setExchange(signal.getExchange() != null ? signal.getExchange() : "N");
                String stratKey = StrategyWalletResolver.resolveStrategyKey(source, null);
                String wId = StrategyWalletResolver.walletIdForStrategy(stratKey != null ? stratKey : source);
                signalQueueService.queueSignal(queueOrder, price, wId, stratKey != null ? stratKey : source);
                return "QUEUED_NO_FUND_ALLOCATION";
            } else {
                // Legacy fallback (strategy wallets NOT enabled)
                VirtualSettings settings = walletRepo.loadSettings();
                capitalPerTrade = Math.min(settings.getAccountValue() * 0.50, 50000.0);
            }

            // Cap per-trade capital at 20% of wallet balance to prevent concentration
            if (strategyWalletEnabled && strategyWalletRepository != null) {
                try {
                    String stratKey2 = StrategyWalletResolver.resolveStrategyKey(source, null);
                    String wId2 = StrategyWalletResolver.walletIdForStrategy(stratKey2 != null ? stratKey2 : source);
                    var walletOpt = strategyWalletRepository.getWallet(wId2);
                    if (walletOpt.isPresent()) {
                        double maxCap = walletOpt.get().getCurrentBalance() * maxPerTradeCapPct;
                        if (capitalPerTrade > maxCap) {
                            log.info("CAPITAL_CAP scrip={} allocated={} capped={} ({}% of balance={})",
                                    numericScrip, String.format("%.0f", capitalPerTrade),
                                    String.format("%.0f", maxCap), maxPerTradeCapPct * 100,
                                    String.format("%.0f", walletOpt.get().getCurrentBalance()));
                            capitalPerTrade = maxCap;
                        }
                    }
                } catch (Exception e) {
                    log.debug("CAPITAL_CAP wallet lookup failed: {}", e.getMessage());
                }
            }

            // ── OPTION ROUTING: If option data available, ALWAYS trade option — never fall back to equity ──
            // OTM is guaranteed by OptionDataEnricher (Streaming Candle). ITM options are never published
            // as optionAvailable=true. No swap logic needed here.
            // Deferred LTP: when swap succeeded but LTP wasn't available during enrichment (Kafka round-trip),
            // resolve it now from OptionProducer's LivePriceCache (by batch time, ticks have arrived).
            if (signal.isOptionAvailable() && signal.getOptionLotSize() > 0) {
                if (signal.isOptionLtpDeferred() || signal.getOptionLtp() <= 0) {
                    if (!resolveDeferredOptionLtp(signal, numericScrip)) {
                        log.warn("DEFERRED_LTP_FAILED scrip={} optionScrip={} — falling to equity",
                            numericScrip, signal.getOptionScripCode());
                    }
                }
            }
            if (signal.isOptionAvailable() && signal.getOptionLtp() > 0 && signal.getOptionLotSize() > 0) {
                String optResult = routeToOptionTrade(signal, source, capitalPerTrade, price, longSignal, numericScrip);
                log.info("OPTION_ROUTE scrip={} source={} strike={} type={} result={}",
                        numericScrip, source, signal.getOptionStrike(), signal.getOptionType(), optResult);
                return optResult;
            }

            // LOT-SIZE: qty MUST be lot-rounded via LotSizeLookupService
            int lotSize = lotSizeLookup.getLotSize(numericScrip);
            int multiplier = lotSizeLookup.getMultiplier(numericScrip);
            double costPerLot = price * multiplier;
            int lots = costPerLot > 0 ? (int) Math.floor(capitalPerTrade / costPerLot) : 0;
            // Track leftover capital from lot rounding for cascade
            if (lots > 0 && costPerLot > 0) {
                signal.setLeftoverCapital(capitalPerTrade - (lots * costPerLot));
            }

            // BUG-014 FIX: If can't afford even 1 lot, try MCX mini fallback before queuing
            if (lots < 1) {
                // ── MCX MINI FALLBACK: Try smaller commodity variant before giving up ──
                // Only for MCX futures (exchange "M") that are NOT on the options whitelist.
                // Commodities with options (CRUDEOIL, NATURALGAS, ZINC, COPPER) should use
                // option routing via OptionDataEnricher, not futures fallback.
                String exchCode = signal.getExchange();
                if ("M".equalsIgnoreCase(exchCode) && mcxMiniFallbackService != null) {
                    String symbolRoot = McxMiniFallbackService.extractSymbolRoot(companyName);
                    if (symbolRoot != null && !mcxMiniFallbackService.isOptionsWhitelisted(symbolRoot)
                            && mcxMiniFallbackService.hasFallbackChain(symbolRoot)) {
                        McxMiniFallbackService.McxFallbackResult fallback =
                                mcxMiniFallbackService.findAffordableMiniVariant(
                                        symbolRoot, exchCode, capitalPerTrade, price);
                        if (fallback != null) {
                            // Substitute the mini variant into the trade
                            log.info("MCX_MINI_FALLBACK: {} -> {} (scripCode {} -> {}, " +
                                            "lot cost {} -> {}, lots={})",
                                    symbolRoot, fallback.symbolRoot,
                                    numericScrip, fallback.scripCode,
                                    String.format("%.0f", costPerLot),
                                    String.format("%.0f", fallback.lotCost),
                                    fallback.affordableLots);

                            // Override trade parameters with mini variant
                            numericScrip = fallback.scripCode;
                            lotSize = fallback.lotSize;
                            multiplier = fallback.multiplier;
                            costPerLot = price * fallback.multiplier;
                            lots = fallback.affordableLots;
                            companyName = fallback.name;
                            // SL and targets stay the same — underlying price is identical for futures
                        }
                    }
                }

                // If still can't afford (no mini variant found or mini also too expensive)
                if (lots < 1) {
                    if (strategyWalletEnabled && signalQueueService != null && allocatedCapital > 0) {
                        log.warn("FUND_ALLOC_INSUFFICIENT scrip={} allocated={} costPerLot={} -> queuing",
                                numericScrip, String.format("%.0f", capitalPerTrade), String.format("%.2f", costPerLot));
                        // Build placeholder order for queue
                        VirtualOrder queueOrder = new VirtualOrder();
                        queueOrder.setScripCode(numericScrip);
                        queueOrder.setSide(longSignal ? VirtualOrder.Side.BUY : VirtualOrder.Side.SELL);
                        queueOrder.setType(VirtualOrder.Type.MARKET);
                        queueOrder.setQty(lotSize); // minimum lot
                        queueOrder.setLotSize(lotSize);
                        queueOrder.setCurrentPrice(price);
                        queueOrder.setSl(signal.getStopLoss());
                        queueOrder.setTp1(signal.getTarget1());
                        queueOrder.setTp2(signal.getTarget2());
                        queueOrder.setSignalSource(source);
                        queueOrder.setSignalType(signal.getSignal());
                        queueOrder.setExchange(signal.getExchange() != null ? signal.getExchange() : "N");
                        String strategyKey = StrategyWalletResolver.resolveStrategyKey(source, null);
                        String walletId = StrategyWalletResolver.walletIdForStrategy(strategyKey != null ? strategyKey : source);
                        signalQueueService.queueSignal(queueOrder, costPerLot, walletId, strategyKey != null ? strategyKey : source);
                        return "QUEUED_INSUFFICIENT_FOR_LOT";
                    }
                    // Legacy fallback: force 1 lot when strategy wallets not enabled
                    lots = 1;
                }
            }

            int qty = lots * lotSize;
            double actualUsed = lots * costPerLot;
            double leftover = capitalPerTrade - actualUsed;
            log.info("FUND_ALLOC_LOT_CALC scrip={} allocated={} costPerLot={} lots={} qty={} actualUsed={} leftover={}",
                    numericScrip, String.format("%.0f", capitalPerTrade), String.format("%.2f", costPerLot),
                    lots, qty, String.format("%.0f", actualUsed), String.format("%.0f", leftover));

            VirtualOrder order = new VirtualOrder();
            order.setScripCode(numericScrip);
            order.setSide(longSignal ? VirtualOrder.Side.BUY : VirtualOrder.Side.SELL);
            order.setType(VirtualOrder.Type.MARKET);
            order.setQty(qty);
            order.setLotSize(lotSize);
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
            log.info("{}_paper_trade scrip={} status={} qty={} capital={} entry={} SL={} T1={}",
                    source, numericScrip, executed.getStatus(), qty,
                    String.format("%.0f", capitalPerTrade), price,
                    signal.getStopLoss(), signal.getTarget1());
            return executed.getStatus() == VirtualOrder.Status.FILLED ? "FILLED" : "ORDER_REJECTED";
        } catch (Exception e) {
            log.error("{}_paper_trade_error scrip={} err={}", source, scripCode, e.getMessage());
            return "ERROR:" + e.getMessage();
        }
    }

    // ========== Real-Time OTM Option Swap via OptionProducer (port 8208) ==========

    /**
     * When OPTION-GATE detects an ITM option at execution time, call OptionProducer's
     * swap API to dynamically find the correct OTM option, subscribe to it, and update
     * the signal with fresh option data (scripCode, strike, LTP, lotSize, etc.).
     *
     * This is the SAME API that Streaming Candle's OptionDataEnricher uses — we're just
     * calling it at execution time as a safety net when enrichment data is stale.
     *
     * @return true if swap succeeded and signal was updated with new OTM option data
     */
    @SuppressWarnings("unchecked")
    private boolean swapToOTMOption(StrategySignal signal, double currentSpot, boolean isBullish,
                                     String numericScrip, String source) {
        String url = optionProducerBaseUrl + "/api/ws/swap-option";
        String symbolRoot = signal.getCompanyName();
        // Extract symbol root from option symbol (e.g. "IEX 30MAR 120 CE" → "IEX")
        if (signal.getOptionSymbol() != null && !signal.getOptionSymbol().isEmpty()) {
            symbolRoot = signal.getOptionSymbol().split("\\s+")[0];
        }

        log.info("[OPTION-SWAP] Requesting OTM swap: symbol={} spot={} bullish={} oldOption={} exchange={} source={}",
                symbolRoot, String.format("%.2f", currentSpot), isBullish,
                signal.getOptionScripCode(), signal.getExchange(), source);

        try {
            Map<String, Object> swapReq = new LinkedHashMap<>();
            swapReq.put("underlyingSymbolRoot", symbolRoot);
            swapReq.put("currentSpotPrice", currentSpot);
            swapReq.put("bullish", isBullish);
            swapReq.put("oldOptionScripCode", signal.getOptionScripCode());
            swapReq.put("exchange", signal.getExchange() != null ? signal.getExchange() : "N");
            swapReq.put("exchangeType", signal.getExchangeType() != null ? signal.getExchangeType() : "D");

            HttpHeaders headers = new HttpHeaders();
            headers.setContentType(MediaType.APPLICATION_JSON);
            HttpEntity<Map<String, Object>> entity = new HttpEntity<>(swapReq, headers);

            ResponseEntity<Map> response = restTemplate.exchange(url, HttpMethod.POST, entity, Map.class);

            if (!response.getStatusCode().is2xxSuccessful() || response.getBody() == null) {
                log.warn("[OPTION-SWAP] API returned non-200: {} scrip={}", response.getStatusCode(), numericScrip);
                return false;
            }

            Map<String, Object> body = response.getBody();
            if (!Boolean.TRUE.equals(body.get("success"))) {
                log.warn("[OPTION-SWAP] API returned failure: {} scrip={}", body.get("message"), numericScrip);
                return false;
            }

            Map<String, Object> newOpt = (Map<String, Object>) body.get("newOption");
            if (newOpt == null) {
                log.warn("[OPTION-SWAP] API returned no newOption data scrip={}", numericScrip);
                return false;
            }

            // Extract new option metadata
            String newScripCode = String.valueOf(newOpt.get("scripCode"));
            String newStrike = String.valueOf(newOpt.get("strikeRate"));
            String newType = String.valueOf(newOpt.get("scripType"));
            String newExpiry = String.valueOf(newOpt.get("expiry"));
            String newSymbol = String.valueOf(newOpt.getOrDefault("companyName", ""));
            String newLotSize = String.valueOf(newOpt.getOrDefault("lotSize", "1"));
            String newExch = String.valueOf(newOpt.getOrDefault("exch", signal.getExchange()));
            String newExchType = String.valueOf(newOpt.getOrDefault("exchType", "D"));

            // Wait for LTP: OptionProducer subscribes to the new option, need to wait for tick
            Double newLtp = null;
            for (int i = 0; i < optionSwapRetryCount; i++) {
                Thread.sleep(optionSwapRetryDelayMs);
                newLtp = priceProvider.getLtp(newScripCode);
                if (newLtp != null && newLtp > 0) break;
            }

            if (newLtp == null || newLtp <= 0) {
                log.warn("[OPTION-SWAP] No LTP received for new option after {}ms: newScripCode={} scrip={}",
                        optionSwapRetryCount * optionSwapRetryDelayMs, newScripCode, numericScrip);
                return false;
            }

            // Verify new option is actually OTM
            double strikeVal = Double.parseDouble(newStrike);
            boolean stillITM = "CE".equals(newType) ? strikeVal < currentSpot : strikeVal > currentSpot;
            if (stillITM) {
                log.warn("[OPTION-SWAP] Swapped option is still ITM: strike={} spot={} type={} scrip={}",
                        newStrike, currentSpot, newType, numericScrip);
                return false;
            }

            // Log before updating so we can see old → new
            double oldStrike = signal.getOptionStrike();
            String oldScripCode = signal.getOptionScripCode();

            // Update signal with new OTM option data
            signal.setOptionScripCode(newScripCode);
            signal.setOptionStrike(strikeVal);
            signal.setOptionType(newType);
            signal.setOptionLtp(newLtp);
            signal.setOptionExpiry(newExpiry);
            signal.setOptionSymbol(newSymbol);
            signal.setOptionExchange(newExch);
            signal.setOptionExchangeType(newExchType);
            signal.setOptionIsITM(false);
            try { signal.setOptionLotSize(Integer.parseInt(newLotSize)); } catch (NumberFormatException e) { /* keep existing */ }

            log.info("[OPTION-SWAP] SUCCESS: scrip={} strike={} → {} scripCode={} → {} ltp={} type={} source={}",
                    numericScrip, oldStrike, strikeVal, oldScripCode, newScripCode,
                    String.format("%.2f", newLtp), newType, source);

            return true;

        } catch (Exception e) {
            log.error("[OPTION-SWAP] Failed for scrip={}: {}", numericScrip, e.getMessage());
            return false;
        }
    }

    // ========== Option Routing via Dashboard Backend StrategyTradeExecutor ==========

    /**
     * Route a signal to StrategyTradeExecutor on dashboard backend (port 8085)
     * for option trading. Builds a StrategyTradeRequest and POSTs to /api/strategy-trades.
     *
     * @return "OPTION_FILLED" on success, "OPTION_INSUFFICIENT_CAPITAL" if can't afford 1 lot,
     *         "OPTION_ROUTE_FAILED" if HTTP call fails (caller falls back to equity)
     */
    private String routeToOptionTrade(StrategySignal signal, String source,
                                       double capitalPerTrade, double equityPrice,
                                       boolean longSignal, String numericScrip) {
        try {
            double optionLtp = signal.getOptionLtp();
            int optionLotSize = signal.getOptionLotSize();
            int optionMultiplier = signal.getOptionMultiplier() > 0 ? signal.getOptionMultiplier() : 1;

            // Compute lots: floor(capitalPerTrade / (optionLtp * optionLotSize))
            double costPerLot = optionLtp * optionLotSize;
            int lots = costPerLot > 0 ? (int) Math.floor(capitalPerTrade / costPerLot) : 0;
            if (lots < 1) {
                log.info("OPTION_INSUFFICIENT_CAPITAL scrip={} capital={} costPerLot={} optLtp={} lotSize={}",
                        numericScrip, String.format("%.0f", capitalPerTrade),
                        String.format("%.2f", costPerLot), optionLtp, optionLotSize);
                return "OPTION_INSUFFICIENT_CAPITAL";
            }

            int qty = lots * optionLotSize;
            double capitalUsed = lots * costPerLot;
            double leftoverCapital = capitalPerTrade - capitalUsed;
            if (leftoverCapital > 0) {
                log.info("CAPITAL_LEFTOVER scrip={} allocated={} used={} leftover={} lots={}",
                    numericScrip, String.format("%.0f", capitalPerTrade),
                    String.format("%.0f", capitalUsed), String.format("%.0f", leftoverCapital), lots);
            }
            // Store leftover on signal for batch loop to cascade to next rank
            signal.setLeftoverCapital(leftoverCapital);

            String strategyKey = StrategyWalletResolver.resolveStrategyKey(source, null);
            if (strategyKey == null) strategyKey = source;
            String walletId = StrategyWalletResolver.walletIdForStrategy(strategyKey);

            // NOTE: Do NOT deduct margin here — StrategyTradeExecutor.openTrade() on dashboard
            // backend (port 8085) calls lockStrategyWalletMargin() which handles margin deduction.
            // Deducting here too would cause DOUBLE DEDUCTION (Bug #3 fix, 2026-03-05).

            // Option side: always BUY (buy CE for bullish, buy PE for bearish)
            String side = "BUY";
            String direction = longSignal ? "BULLISH" : "BEARISH";

            double equitySl = signal.getStopLoss();
            double equityT1 = signal.getTarget1();
            double equityT2 = signal.getTarget2();
            double equityT3 = signal.getTarget3();
            double equityT4 = signal.getTarget4();

            // Use Greek-enriched option targets from Streaming Candle (Step 1) when available.
            // These are computed using Black-Scholes delta/gamma/IV — far more accurate than
            // the old delta=0.5 ATM estimate. Falls back to delta=0.5 mapping only if Greeks absent.
            boolean hasGreekTargets = signal.getOptionSL() > 0 && signal.getOptionT1() > 0;
            double delta;
            double optSl, optT1, optT2, optT3, optT4;

            if (hasGreekTargets) {
                delta = signal.getGreekDelta() > 0 ? signal.getGreekDelta() : 0.5;
                optSl = signal.getOptionSL();
                optT1 = signal.getOptionT1();
                optT2 = signal.getOptionT2();
                optT3 = signal.getOptionT3();
                optT4 = signal.getOptionT4();
                log.info("OPTION_GREEK_TARGETS scrip={} delta={} IV={}% SL={} T1={} T2={} T3={} T4={} RR={} method={}",
                    numericScrip, String.format("%.3f", delta), String.format("%.1f", signal.getGreekIV()),
                    String.format("%.2f", optSl), String.format("%.2f", optT1),
                    String.format("%.2f", optT2), String.format("%.2f", optT3),
                    String.format("%.2f", optT4), String.format("%.2f", signal.getOptionRR()),
                    signal.getGreekSlMethod());
            } else {
                // Legacy fallback: delta=0.5 ATM mapping
                delta = 0.5;
                optSl = Math.max(optionLtp - Math.abs(equityPrice - equitySl) * delta, optionLtp * 0.3);
                optT1 = equityT1 > 0 ? optionLtp + Math.abs(equityT1 - equityPrice) * delta : 0;
                optT2 = equityT2 > 0 ? optionLtp + Math.abs(equityT2 - equityPrice) * delta : 0;
                optT3 = equityT3 > 0 ? optionLtp + Math.abs(equityT3 - equityPrice) * delta : 0;
                optT4 = equityT4 > 0 ? optionLtp + Math.abs(equityT4 - equityPrice) * delta : 0;
                log.info("OPTION_LEGACY_TARGETS scrip={} delta=0.5 (no Greek enrichment)", numericScrip);
            }

            String exch = signal.getOptionExchange() != null && !signal.getOptionExchange().isEmpty()
                    ? signal.getOptionExchange()
                    : (signal.getExchange() != null ? signal.getExchange() : "N");

            // Build JSON request body
            Map<String, Object> body = new LinkedHashMap<>();
            body.put("scripCode", signal.getOptionScripCode());
            // ── REAL-TIME LTP VALIDATION: Fetch current option price before entry ──
            // Signal's optionLtp is from enrichment time (potentially 1-2 min stale).
            // Fetch current LTP and validate it's still within the trade setup (SL < LTP < T1).
            Double realtimeLtp = priceProvider.getLtp(signal.getOptionScripCode());
            if (realtimeLtp != null && realtimeLtp > 0) {
                double sl = hasGreekTargets ? signal.getOptionSL() : optSl;
                double t1 = hasGreekTargets ? signal.getOptionT1() : optT1;
                if (realtimeLtp < sl || realtimeLtp > t1) {
                    log.warn("LTP_OUT_OF_RANGE scrip={} optionScrip={} signalLtp={} realtimeLtp={} SL={} T1={} — trade setup invalidated",
                        numericScrip, signal.getOptionScripCode(),
                        String.format("%.2f", optionLtp), String.format("%.2f", realtimeLtp),
                        String.format("%.2f", sl), String.format("%.2f", t1));
                    return "LTP_OUT_OF_RANGE";
                }
                // Use real-time LTP as entry price for accurate P&L tracking
                log.info("REALTIME_LTP scrip={} signalLtp={} realtimeLtp={} deviation={}%",
                    numericScrip, String.format("%.2f", optionLtp), String.format("%.2f", realtimeLtp),
                    String.format("%.1f", Math.abs(realtimeLtp - optionLtp) / optionLtp * 100));
                optionLtp = realtimeLtp;
            } else {
                log.warn("STALE_OPTION_PRICE_FALLBACK scrip={} optionScrip={} — PriceProvider returned null, using signal optionLtp={}",
                    numericScrip, signal.getOptionScripCode(), String.format("%.2f", optionLtp));
            }

            body.put("instrumentSymbol", signal.getOptionSymbol());
            body.put("instrumentType", "OPTION");
            body.put("underlyingScripCode", numericScrip);
            body.put("underlyingSymbol", signal.getCompanyName());
            body.put("side", side);
            body.put("quantity", qty);
            body.put("lots", lots);
            body.put("lotSize", optionLotSize);
            body.put("multiplier", optionMultiplier);
            body.put("entryPrice", optionLtp);
            body.put("sl", optSl);
            body.put("t1", optT1);
            body.put("t2", optT2);
            body.put("t3", optT3);
            body.put("t4", optT4);
            body.put("equitySpot", equityPrice);
            body.put("equitySl", equitySl);
            body.put("equityT1", equityT1);
            body.put("equityT2", equityT2);
            body.put("equityT3", equityT3);
            body.put("equityT4", equityT4);
            body.put("delta", delta);
            body.put("optionType", signal.getOptionType());
            body.put("optionExpiry", signal.getOptionExpiry());
            body.put("strike", signal.getOptionStrike());
            body.put("strategy", strategyKey);
            body.put("executionMode", "AUTO");
            body.put("exchange", exch);
            body.put("direction", direction);
            body.put("confidence", signal.getConfidence() * 100);

            // Pass Greek enrichment data so Dashboard can use authoritative targets
            if (hasGreekTargets) {
                body.put("greekEnriched", true);
                body.put("greekDelta", signal.getGreekDelta());
                body.put("greekGamma", signal.getGreekGamma());
                body.put("greekTheta", signal.getGreekTheta());
                body.put("greekVega", signal.getGreekVega());
                body.put("greekIV", signal.getGreekIV());
                body.put("greekDte", signal.getGreekDte());
                body.put("greekMoneynessType", signal.getGreekMoneynessType());
                body.put("greekThetaImpaired", signal.isGreekThetaImpaired());
                body.put("greekSlMethod", signal.getGreekSlMethod());
                body.put("greekGammaBoost", signal.getGreekGammaBoost());
                body.put("optionRR", signal.getOptionRR());
                // Lot allocation from Streaming Candle (theta-aware: "100,0,0,0" for theta-impaired)
                if (signal.getOptionLotAllocation() != null) {
                    body.put("lotAllocation", signal.getOptionLotAllocation());
                }
                // Cross-instrument futures SL/targets
                if (signal.getFuturesSL() > 0) {
                    body.put("futuresSL", signal.getFuturesSL());
                    body.put("futuresT1", signal.getFuturesT1());
                    body.put("futuresT2", signal.getFuturesT2());
                    body.put("futuresT3", signal.getFuturesT3());
                    body.put("futuresT4", signal.getFuturesT4());
                }
            }

            HttpHeaders headers = new HttpHeaders();
            headers.setContentType(MediaType.APPLICATION_JSON);
            HttpEntity<Map<String, Object>> request = new HttpEntity<>(body, headers);

            String url = dashboardBackendUrl + "/api/strategy-trades";
            log.info("OPTION_ROUTE_REQUEST scrip={} optionScrip={} symbol={} lots={} qty={} optLtp={} → {}",
                    numericScrip, signal.getOptionScripCode(), signal.getOptionSymbol(),
                    lots, qty, optionLtp, url);

            ResponseEntity<Map> response = restTemplate.exchange(url, HttpMethod.POST, request, Map.class);

            if (response.getStatusCode().is2xxSuccessful() && response.getBody() != null) {
                Object success = response.getBody().get("success");
                if (Boolean.TRUE.equals(success)) {
                    log.info("OPTION_FILLED scrip={} optionScrip={} lots={} qty={} via StrategyTradeExecutor tradeId={}",
                            numericScrip, signal.getOptionScripCode(), lots, qty,
                            response.getBody().get("tradeId"));
                    return "OPTION_FILLED";
                }
            }

            log.warn("OPTION_ROUTE_REJECTED scrip={} response={}", numericScrip, response.getBody());
            // No margin to release here — margin is only deducted by dashboard backend on success
            return "OPTION_ROUTE_FAILED";

        } catch (Exception e) {
            log.error("OPTION_ROUTE_ERROR scrip={} err={}", numericScrip, e.getMessage(), e);
            return "OPTION_ROUTE_FAILED";
        }
    }

    /**
     * Resolve deferred option LTP and compute Greeks at batch evaluation time.
     * Called when Streaming Candle's OptionDataEnricher found the correct OTM option via swap
     * but couldn't get the LTP in time (WebSocket tick round-trips through Kafka in 2-5s).
     * By batch time (3-5s later), OptionProducer's LivePriceCache should have the tick.
     *
     * @return true if LTP resolved and signal is ready for option trading, false if unavailable
     */
    private boolean resolveDeferredOptionLtp(StrategySignal signal, String numericScrip) {
        String optionScripCode = signal.getOptionScripCode();
        if (optionScripCode == null || optionScripCode.isEmpty()) return false;

        Double ltp = priceProvider.getLtp(optionScripCode);

        if (ltp == null || ltp <= 0) {
            log.info("DEFERRED_LTP_STILL_MISSING scrip={} optionScrip={}", numericScrip, optionScripCode);
            signal.setOptionAvailable(false);
            return false;
        }

        signal.setOptionLtp(ltp);
        signal.setOptionLtpDeferred(false);
        log.info("DEFERRED_LTP_RESOLVED scrip={} optionScrip={} LTP={}",
            numericScrip, optionScripCode, String.format("%.2f", ltp));

        // Compute Greeks using Black-Scholes if calculator is available
        if (blackScholesCalculator == null) {
            log.debug("DEFERRED_GREEKS_SKIP BlackScholesCalculator not available");
            return true; // LTP resolved, trade with legacy delta=0.5 path
        }

        try {
            double strike = signal.getOptionStrike();
            String optType = signal.getOptionType();
            String expiryStr = signal.getOptionExpiry();

            // Get fresh underlying spot price
            Double spotPrice = priceProvider.getLtp(numericScrip);
            if (spotPrice == null || spotPrice <= 0) spotPrice = (double) signal.getEntryPrice();

            if (strike <= 0 || optType == null || expiryStr == null || spotPrice <= 0) {
                log.debug("DEFERRED_GREEKS_SKIP missing fields: strike={} type={} expiry={} spot={}",
                    strike, optType, expiryStr, spotPrice);
                return true;
            }

            LocalDate expiry;
            try {
                expiry = LocalDate.parse(expiryStr, java.time.format.DateTimeFormatter.ISO_LOCAL_DATE);
            } catch (Exception e) {
                try {
                    expiry = LocalDate.parse(expiryStr, java.time.format.DateTimeFormatter.ofPattern("yyyy-MM-dd"));
                } catch (Exception e2) {
                    log.warn("DEFERRED_GREEKS cannot parse expiry '{}': {}", expiryStr, e2.getMessage());
                    return true;
                }
            }

            OptionType bsType = "CE".equals(optType) ? OptionType.CALL : OptionType.PUT;
            int dte = (int) java.time.temporal.ChronoUnit.DAYS.between(LocalDate.now(), expiry);

            // DTE gate: reject OTM with DTE < minimum
            if (dte < greekMinDte) {
                double intrinsic = bsType == OptionType.CALL
                    ? Math.max(0, spotPrice - strike) : Math.max(0, strike - spotPrice);
                if (intrinsic <= 0) {
                    log.warn("DEFERRED_DTE_GATE dte={} < {} for OTM option scrip={}", dte, greekMinDte, optionScripCode);
                    signal.setOptionAvailable(false);
                    return false;
                }
            }

            OptionGreeks greeks = blackScholesCalculator.calculateGreeks(spotPrice, strike, expiry, bsType, ltp);

            double delta = greeks.getDelta();
            double absDelta = Math.abs(delta);
            double gamma = greeks.getGamma();
            double theta = greeks.getTheta();
            double iv = greeks.getImpliedVolatility();

            boolean thetaImpaired = ltp > 0 && (Math.abs(theta) / ltp) > thetaImpairmentThreshold;

            signal.setGreekDelta(delta);
            signal.setGreekGamma(gamma);
            signal.setGreekTheta(theta);
            signal.setGreekVega(greeks.getVega());
            signal.setGreekRho(greeks.getRho());
            signal.setGreekIV(iv);
            signal.setGreekDte(dte);
            signal.setGreekMoneynessType(greeks.getMoneynessType() != null ? greeks.getMoneynessType().name() : "UNKNOWN");
            signal.setGreekTheoreticalPrice(greeks.getTheoreticalPrice());
            signal.setGreekMispricing(greeks.getMispricing());
            signal.setGreekLeverage(greeks.getLeverage());
            signal.setGreekTimeValue(greeks.getTimeValue());
            signal.setGreekIntrinsicValue(greeks.getIntrinsicValue());
            signal.setGreekThetaImpaired(thetaImpaired);

            // Greek-aware SL and targets (same formulas as OptionDataEnricher.enrichWithGreeks)
            double equityEntry = signal.getEntryPrice();
            double equitySl = signal.getStopLoss();
            double equityT1 = signal.getTarget1();
            double equityT2 = signal.getTarget2();
            double equityT3 = signal.getTarget3();
            double equityT4 = signal.getTarget4();

            if (equityEntry > 0 && equitySl > 0 && equityT1 > 0) {
                double equityRiskDistance = Math.abs(equityEntry - equitySl);
                double deltaSL = ltp - equityRiskDistance * absDelta;

                double ivDecimal = iv / 100.0;
                double tYears = Math.max(dte / 365.0, 0.001);
                double ivFloor = ltp * ivDecimal * Math.sqrt(tYears) * greekSlIvMultiplier;
                double absoluteFloor = ltp * greekSlMinFloor;
                double minSLdistance = Math.max(ivFloor, absoluteFloor);

                double optionSL;
                if ((ltp - deltaSL) < minSLdistance) {
                    optionSL = ltp - minSLdistance;
                    signal.setGreekSlMethod("IV_FLOOR");
                } else {
                    optionSL = deltaSL;
                    signal.setGreekSlMethod("DELTA");
                }
                optionSL = Math.max(optionSL, 0.05);
                signal.setGreekSlIvFloor(minSLdistance);

                double gammaBoost = Math.min(gamma * gammaBoostMultiplier, gammaBoostCap);
                signal.setGreekGammaBoost(gammaBoost);

                double optT1 = equityT1 > 0 ? ltp + Math.abs(equityT1 - equityEntry) * absDelta : 0;
                double optT2 = equityT2 > 0 ? ltp + Math.abs(equityT2 - equityEntry) * absDelta * (1 + gammaBoost) : 0;
                double optT3 = equityT3 > 0 ? ltp + Math.abs(equityT3 - equityEntry) * absDelta * (1 + gammaBoost) : 0;
                double optT4 = equityT4 > 0 ? ltp + Math.abs(equityT4 - equityEntry) * absDelta * (1 + gammaBoost) : 0;

                double optionRisk = ltp - optionSL;
                double optionReward = optT1 - ltp;
                double optionRR = optionRisk > 0 ? optionReward / optionRisk : 0;
                boolean rrPassed = optionRR >= greekMinRR;

                String lotAllocation = thetaImpaired ? "100,0,0,0" : "40,30,20,10";

                signal.setOptionSL(optionSL);
                signal.setOptionT1(optT1);
                signal.setOptionT2(optT2);
                signal.setOptionT3(optT3);
                signal.setOptionT4(optT4);
                signal.setOptionRR(optionRR);
                signal.setOptionRRpassed(rrPassed);
                signal.setOptionLotAllocation(lotAllocation);

                log.info("DEFERRED_GREEKS_COMPUTED scrip={} delta={} IV={}% SL={} T1={} RR={} method={} thetaImpaired={}",
                    numericScrip, String.format("%.3f", delta), String.format("%.1f", iv),
                    String.format("%.2f", optionSL), String.format("%.2f", optT1),
                    String.format("%.2f", optionRR), signal.getGreekSlMethod(), thetaImpaired);
            }

            return true;
        } catch (Exception e) {
            log.error("DEFERRED_GREEKS_ERROR scrip={}: {}", numericScrip, e.getMessage());
            return true; // LTP resolved, Greeks failed — tradeable with legacy delta=0.5
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

        log.info("SignalBufferService shutdown complete.");
    }
}
