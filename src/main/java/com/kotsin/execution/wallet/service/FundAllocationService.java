package com.kotsin.execution.wallet.service;

import com.kotsin.execution.virtual.VirtualWalletRepository;
import com.kotsin.execution.virtual.model.VirtualPosition;
import com.kotsin.execution.wallet.model.WalletEntity;
import com.kotsin.execution.wallet.repository.WalletRepository;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.time.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Centralized Fund Allocation Service — Slot-Based Confidence Sizing.
 *
 * Rules:
 * 1. Available margin = currentBalance - usedMargin
 * 2. Exchange budgets partition available margin — TIME-PHASED:
 *    Phase 1 (9:00–15:25):  NSE 60%, MCX 30%, CDS 10%  (all markets open)
 *    Phase 2 (15:25–17:00): NSE 0%,  MCX 75%, CDS 25%  (NSE closed, funds flow to MCX+CDS)
 *    Phase 3 (17:00–MCX close): NSE 0%, MCX 100%, CDS 0% (only MCX open, all funds → MCX)
 *    MCX close is seasonal: 23:30 IST (US DST) / 23:55 IST (US standard time)
 * 3. Each exchange has max concurrent positions (slots) — also time-phased
 * 4. Confidence gate: signal.confidence < threshold (60%) → rejected
 * 5. High conviction: confidence > 80% AND RR >= 3.0 → double slot (2x capital)
 * 6. Capital per slot = (availableMargin × exchangeBudget%) / maxPositions
 * 7. Signals sorted by rankScore DESC — best-ranked get first pick at slots
 */
@Service
@Slf4j
public class FundAllocationService {

    private final WalletRepository walletRepository;

    @Autowired(required = false)
    private VirtualWalletRepository virtualWalletRepository;

    // Per-wallet locks to prevent concurrent over-allocation
    private final ConcurrentHashMap<String, ReentrantLock> walletLocks = new ConcurrentHashMap<>();

    /** MCX-only strategies: get 100% MCX budget, no NSE/CDS allocation */
    private static final Set<String> MCX_ONLY_STRATEGIES = Set.of("MCX_BB", "MCX_BBT1");

    @Value("${strategy.wallet.initial.capital:1000000}")
    private double initialCapital;

    // Exchange budget config
    @Value("${fund.allocation.exchange.nse.alloc-pct:60.0}")
    private double nseAllocPct;
    @Value("${fund.allocation.exchange.nse.max-positions:5}")
    private int nseMaxPositions;
    @Value("${fund.allocation.exchange.mcx.alloc-pct:30.0}")
    private double mcxAllocPct;
    @Value("${fund.allocation.exchange.mcx.max-positions:3}")
    private int mcxMaxPositions;
    @Value("${fund.allocation.exchange.cds.alloc-pct:10.0}")
    private double cdsAllocPct;
    @Value("${fund.allocation.exchange.cds.max-positions:2}")
    private int cdsMaxPositions;

    // Phase 2 config: after NSE close (15:25), before CDS close (17:00)
    @Value("${fund.allocation.phase2.mcx.alloc-pct:75.0}")
    private double phase2McxAllocPct;
    @Value("${fund.allocation.phase2.mcx.max-positions:5}")
    private int phase2McxMaxPositions;
    @Value("${fund.allocation.phase2.cds.alloc-pct:25.0}")
    private double phase2CdsAllocPct;
    @Value("${fund.allocation.phase2.cds.max-positions:3}")
    private int phase2CdsMaxPositions;

    // Phase 3 config: after CDS close (17:00), until MCX close (seasonal)
    @Value("${fund.allocation.phase3.mcx.alloc-pct:100.0}")
    private double phase3McxAllocPct;
    @Value("${fund.allocation.phase3.mcx.max-positions:7}")
    private int phase3McxMaxPositions;

    private static final ZoneId IST = ZoneId.of("Asia/Kolkata");
    private static final ZoneId US_EASTERN = ZoneId.of("America/New_York");
    private static final LocalTime NSE_CLOSE = LocalTime.of(15, 25);
    private static final LocalTime CDS_CLOSE = LocalTime.of(17, 0);

    // Confidence thresholds
    @Value("${fund.allocation.confidence.threshold:60.0}")
    private double confidenceThreshold;
    @Value("${fund.allocation.confidence.high:80.0}")
    private double confidenceHigh;
    @Value("${fund.allocation.rr.high:3.0}")
    private double rrHigh;

    public FundAllocationService(WalletRepository walletRepository) {
        this.walletRepository = walletRepository;
    }

    /**
     * Compute fund allocation for a batch of signals from the same strategy.
     * Uses slot-based confidence sizing with exchange-partitioned budgets.
     *
     * @param strategyKey  canonical strategy key (FUKAA, FUDKII, FUDKOI, etc.)
     * @param signals      list of signals
     * @return Map of scripCode → allocated capital (only entries that should be executed)
     */
    public Map<String, Double> computeBatchAllocation(
            String strategyKey, List<SignalAllocationRequest> signals) {

        if (signals == null || signals.isEmpty()) {
            return Collections.emptyMap();
        }

        String walletId = StrategyWalletResolver.walletIdForStrategy(strategyKey);
        ReentrantLock lock = walletLocks.computeIfAbsent(walletId, k -> new ReentrantLock());
        lock.lock();
        try {
            WalletEntity wallet = walletRepository.getWallet(walletId)
                    .orElse(walletRepository.getOrCreateStrategyWallet(walletId, initialCapital));

            double availableMargin = wallet.getCurrentBalance() - wallet.getUsedMargin();

            if (availableMargin <= 0) {
                log.warn("SLOT_ALLOC_EMPTY strategy={} wallet={} balance={} used={} available=0",
                        strategyKey, walletId, wallet.getCurrentBalance(), wallet.getUsedMargin());
                return Collections.emptyMap();
            }

            int phase = getCurrentPhase();
            log.info("SLOT_ALLOC_PHASE strategy={} phase={} mcxBudget={}% cdsBudget={}% nseBudget={}% mcxClose={} mcxOnly={}",
                    strategyKey, phase,
                    getExchangeAllocPct("MCX", strategyKey), getExchangeAllocPct("CDS", strategyKey),
                    getExchangeAllocPct("NSE", strategyKey),
                    getMcxCloseTime(), MCX_ONLY_STRATEGIES.contains(strategyKey));

            // Count open positions per exchange for this strategy
            Map<String, Integer> openByExchange = countOpenPositionsByExchange(strategyKey);

            // Track slots consumed within this batch
            Map<String, Integer> batchSlotUsage = new HashMap<>();

            // Sort by rankScore DESC — best-ranked signals get first pick at slots
            List<SignalAllocationRequest> sorted = new ArrayList<>(signals);
            sorted.sort(Comparator.comparingDouble((SignalAllocationRequest s) -> s.rankScore).reversed());

            Map<String, Double> allocations = new LinkedHashMap<>();

            for (SignalAllocationRequest sig : sorted) {
                String exchange = normalizeExchange(sig.exchange);
                double conf = sig.confidence * 100.0; // convert 0-1 → 0-100

                // Confidence gate
                if (conf < confidenceThreshold) {
                    log.info("SLOT_CONFIDENCE_REJECT strategy={} scrip={} confidence={} threshold={}",
                            strategyKey, sig.scripCode, String.format("%.1f", conf), confidenceThreshold);
                    continue;
                }

                // Exchange budget lookup (MCX_BB/MCX_BBT1 get 100% MCX, no NSE/CDS)
                double allocPct = getExchangeAllocPct(exchange, strategyKey);
                int maxPositions = getExchangeMaxPositions(exchange, strategyKey);

                // Available slots = max - already open - consumed in this batch
                int openInExchange = openByExchange.getOrDefault(exchange, 0);
                int usedInBatch = batchSlotUsage.getOrDefault(exchange, 0);
                int availableSlots = maxPositions - openInExchange - usedInBatch;

                if (availableSlots <= 0) {
                    log.info("SLOT_EXCHANGE_FULL strategy={} scrip={} exchange={} open={} batchUsed={} max={}",
                            strategyKey, sig.scripCode, exchange, openInExchange, usedInBatch, maxPositions);
                    continue;
                }

                // Capital calculation
                double capitalPerSlot = (availableMargin * (allocPct / 100.0)) / maxPositions;

                // Determine slots to use: start with 1
                int slotsToUse = 1;

                // Slot consolidation for expensive lots (e.g., MCX ALUMINI needs 339K but slot is 142K)
                // If minimum lot cost exceeds per-slot capital, combine multiple slots to cover it
                if (sig.minLotCost > 0 && sig.minLotCost > capitalPerSlot) {
                    int slotsNeeded = (int) Math.ceil(sig.minLotCost / capitalPerSlot);
                    if (slotsNeeded > availableSlots) {
                        log.info("SLOT_LOT_TOO_EXPENSIVE strategy={} scrip={} exchange={} minLotCost={} " +
                                        "perSlot={} slotsNeeded={} available={}",
                                strategyKey, sig.scripCode, exchange,
                                String.format("%.0f", sig.minLotCost),
                                String.format("%.0f", capitalPerSlot),
                                slotsNeeded, availableSlots);
                        continue;
                    }
                    slotsToUse = slotsNeeded;
                    log.info("SLOT_CONSOLIDATE strategy={} scrip={} exchange={} minLotCost={} " +
                                    "perSlot={} slotsConsolidated={}",
                            strategyKey, sig.scripCode, exchange,
                            String.format("%.0f", sig.minLotCost),
                            String.format("%.0f", capitalPerSlot), slotsToUse);
                }

                // High conviction: confidence > 80% AND RR >= 3.0 → double slot (2x capital)
                // Only upgrade if we haven't already consolidated for lot cost
                if (slotsToUse == 1 && conf > confidenceHigh && sig.riskRewardRatio >= rrHigh && availableSlots >= 2) {
                    slotsToUse = 2;
                }

                double allocatedCapital = capitalPerSlot * slotsToUse;

                allocations.put(sig.scripCode, allocatedCapital);
                batchSlotUsage.merge(exchange, slotsToUse, Integer::sum);

                log.info("SLOT_ALLOC strategy={} scrip={} exchange={} confidence={} RR={} slots={} " +
                                "capital={} (perSlot={} availMargin={} budgetPct={}%)",
                        strategyKey, sig.scripCode, exchange,
                        String.format("%.1f", conf), String.format("%.1f", sig.riskRewardRatio),
                        slotsToUse, String.format("%.0f", allocatedCapital),
                        String.format("%.0f", capitalPerSlot),
                        String.format("%.0f", availableMargin), allocPct);
            }

            log.info("SLOT_ALLOC_RESULT strategy={} availableMargin={} allocated={} rejected={}",
                    strategyKey, String.format("%.0f", availableMargin),
                    formatAllocations(allocations), signals.size() - allocations.size());
            return allocations;
        } finally {
            lock.unlock();
        }
    }

    // ==================== Position Counting ====================

    private Map<String, Integer> countOpenPositionsByExchange(String strategyKey) {
        Map<String, Integer> counts = new HashMap<>();
        if (virtualWalletRepository == null) return counts;

        String walletId = StrategyWalletResolver.walletIdForStrategy(strategyKey);
        try {
            for (VirtualPosition pos : virtualWalletRepository.listPositions()) {
                if (pos.getQtyOpen() > 0 && walletId.equals(pos.getWalletId())) {
                    String exch = normalizeExchange(pos.getExchange());
                    counts.merge(exch, 1, Integer::sum);
                }
            }
        } catch (Exception e) {
            log.warn("SLOT_ALLOC position count failed: {}", e.getMessage());
        }
        return counts;
    }

    // ==================== Exchange Budget Helpers (Time-Phased) ====================

    /**
     * Determine current allocation phase based on IST time:
     *   Phase 1: before 15:25 (all markets open)
     *   Phase 2: 15:25–17:00 (NSE closed, MCX + CDS active)
     *   Phase 3: after 17:00 until MCX seasonal close (MCX only)
     */
    private int getCurrentPhase() {
        LocalTime now = LocalTime.now(IST);
        if (!now.isBefore(CDS_CLOSE)) return 3;
        if (!now.isBefore(NSE_CLOSE)) return 2;
        return 1;
    }

    private double getExchangeAllocPct(String exchange, String strategyKey) {
        // MCX-only strategies: 100% to MCX, 0% to everything else
        if (MCX_ONLY_STRATEGIES.contains(strategyKey)) {
            return "MCX".equals(exchange) ? 100.0 : 0.0;
        }
        int phase = getCurrentPhase();
        return switch (phase) {
            case 3 -> switch (exchange) {
                case "MCX" -> phase3McxAllocPct;    // 100%
                default -> 0.0;                     // NSE & CDS closed
            };
            case 2 -> switch (exchange) {
                case "MCX" -> phase2McxAllocPct;    // 75%
                case "CDS" -> phase2CdsAllocPct;    // 25%
                default -> 0.0;                     // NSE closed
            };
            default -> switch (exchange) {          // Phase 1: all markets
                case "MCX" -> mcxAllocPct;          // 30%
                case "CDS" -> cdsAllocPct;          // 10%
                default -> nseAllocPct;             // 60%
            };
        };
    }

    private int getExchangeMaxPositions(String exchange, String strategyKey) {
        // MCX-only strategies: 7 MCX positions, 0 for everything else
        if (MCX_ONLY_STRATEGIES.contains(strategyKey)) {
            return "MCX".equals(exchange) ? 7 : 0;
        }
        int phase = getCurrentPhase();
        return switch (phase) {
            case 3 -> switch (exchange) {
                case "MCX" -> phase3McxMaxPositions;  // 7
                default -> 0;
            };
            case 2 -> switch (exchange) {
                case "MCX" -> phase2McxMaxPositions;  // 5
                case "CDS" -> phase2CdsMaxPositions;  // 3
                default -> 0;
            };
            default -> switch (exchange) {
                case "MCX" -> mcxMaxPositions;        // 3
                case "CDS" -> cdsMaxPositions;        // 2
                default -> nseMaxPositions;           // 5
            };
        };
    }

    /**
     * MCX close time — seasonal, aligned with US DST (COMEX/NYMEX):
     *   US DST active  → 23:30 IST
     *   US standard    → 23:55 IST
     */
    public static LocalTime getMcxCloseTime() {
        return getMcxCloseTime(LocalDate.now(IST));
    }

    public static LocalTime getMcxCloseTime(LocalDate date) {
        ZonedDateTime usNoon = date.atTime(12, 0).atZone(US_EASTERN);
        boolean usDstToday = usNoon.getZone().getRules().isDaylightSavings(usNoon.toInstant());
        if (usDstToday) return LocalTime.of(23, 30);

        ZonedDateTime usTomorrow = date.plusDays(1).atTime(12, 0).atZone(US_EASTERN);
        if (usTomorrow.getZone().getRules().isDaylightSavings(usTomorrow.toInstant())) {
            return LocalTime.of(23, 30);
        }

        ZonedDateTime usLastWeek = date.minusDays(6).atTime(12, 0).atZone(US_EASTERN);
        if (usLastWeek.getZone().getRules().isDaylightSavings(usLastWeek.toInstant())) {
            return LocalTime.of(23, 30);
        }

        return LocalTime.of(23, 55);
    }

    // ==================== Helpers ====================

    static String normalizeExchange(String exchange) {
        if (exchange == null || exchange.isBlank()) return "NSE";
        String upper = exchange.trim().toUpperCase();
        return switch (upper) {
            case "N", "NSE", "B", "BSE" -> "NSE";
            case "M", "MCX" -> "MCX";
            case "C", "CURRENCY", "CDS" -> "CDS";
            default -> "NSE";
        };
    }

    private String formatAllocations(Map<String, Double> alloc) {
        return alloc.entrySet().stream()
                .map(e -> e.getKey() + "=" + String.format("%.0f", e.getValue()))
                .collect(java.util.stream.Collectors.joining(", ", "{", "}"));
    }

    // ==================== Request DTO ====================

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class SignalAllocationRequest {
        private String scripCode;
        private double rankScore;
        private double oiChangeRatio;
        private String exchange;
        private double confidence;       // 0.0-1.0 scale
        private double riskRewardRatio;  // e.g. 2.5, 3.0
        private double minLotCost;       // minimum capital to buy 1 lot (price * multiplier)
    }
}
