package com.kotsin.execution.wallet.service;

import com.kotsin.execution.virtual.VirtualWalletRepository;
import com.kotsin.execution.virtual.model.VirtualPosition;
import com.kotsin.execution.wallet.repository.WalletRepository;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.*;

/**
 * Post-session wallet margin reconciliation.
 *
 * Problem: If a trade-close event is lost (process restart, Kafka lag, exception),
 * usedMargin stays locked forever — "margin leak." Over time this silently eats
 * the entire wallet, causing "insufficient funds" on every new signal.
 *
 * Fix: After each exchange session closes, compare wallet.usedMargin against the
 * actual sum of avgEntry x qtyOpen for all open positions. Correct any mismatch
 * using atomic Lua script — safe against concurrent writes from port 8085.
 *
 * Schedule:
 *   - 08:50 AM IST  — pre-market safety net (before WalletDailyResetService runs)
 *   - 15:30 PM IST  — after NSE close (positions should be exited at 15:25)
 *   - 17:05 PM IST  — after CDS close (positions exited at 16:55)
 *   - 23:35 PM IST  — after MCX close (DST season, exited at 23:25)
 *   - 23:59 PM IST  — after MCX close (standard season, exited at 23:50)
 */
@Service
@Slf4j
public class WalletMarginReconciliationService {

    @Autowired
    private WalletRepository walletRepository;

    @Autowired(required = false)
    private VirtualWalletRepository virtualWalletRepository;

    @Scheduled(cron = "0 50 8 * * MON-FRI", zone = "Asia/Kolkata")
    public void reconcilePreMarket() {
        reconcileAll("PRE-MARKET 08:50");
    }

    @Scheduled(cron = "0 30 15 * * MON-FRI", zone = "Asia/Kolkata")
    public void reconcileAfterNse() {
        reconcileAll("POST-NSE 15:30");
    }

    @Scheduled(cron = "0 5 17 * * MON-FRI", zone = "Asia/Kolkata")
    public void reconcileAfterCds() {
        reconcileAll("POST-CDS 17:05");
    }

    @Scheduled(cron = "0 35 23 * * MON-FRI", zone = "Asia/Kolkata")
    public void reconcileAfterMcxDst() {
        reconcileAll("POST-MCX-DST 23:35");
    }

    @Scheduled(cron = "0 59 23 * * MON-FRI", zone = "Asia/Kolkata")
    public void reconcileAfterMcxStandard() {
        reconcileAll("POST-MCX-STD 23:59");
    }

    /**
     * Core reconciliation: for each wallet, compute actual usedMargin from open positions
     * and atomically fix any mismatch via Lua script.
     */
    public void reconcileAll(String trigger) {
        log.info("[MARGIN-RECONCILE] {} — starting reconciliation", trigger);

        Map<String, Double> actualMarginByWallet = computeActualMarginByWallet();

        Set<String> walletIds = walletRepository.getAllWalletIds();
        int fixedCount = 0;
        int okCount = 0;

        for (String walletId : walletIds) {
            try {
                double actualUsed = actualMarginByWallet.getOrDefault(walletId, 0.0);

                // Atomic reconciliation via Lua — reads current state and fixes if needed
                String result = walletRepository.atomicReconcileMargin(walletId, actualUsed);
                if (result == null || "-1".equals(result)) {
                    continue;
                }

                if (result.startsWith("FIXED")) {
                    fixedCount++;
                    String[] parts = result.split("\\|");
                    String oldUsed = parts.length > 1 ? parts[1] : "?";
                    String newUsed = parts.length > 2 ? parts[2] : "?";
                    String available = parts.length > 3 ? parts[3] : "?";
                    log.warn("[MARGIN-RECONCILE] FIXED {} usedMargin: {} -> {} (avail now {})",
                            walletId, oldUsed, newUsed, available);
                } else {
                    okCount++;
                    log.debug("[MARGIN-RECONCILE] OK {} usedMargin={}", walletId,
                            result.replace("OK|", ""));
                }
            } catch (Exception e) {
                log.error("[MARGIN-RECONCILE] Error reconciling {}: {}", walletId, e.getMessage());
            }
        }

        log.info("[MARGIN-RECONCILE] {} — complete. {} fixed, {} ok, {} total wallets",
                trigger, fixedCount, okCount, walletIds.size());
    }

    /**
     * Compute the actual usedMargin per wallet by summing avgEntry x qtyOpen
     * for all positions with qtyOpen > 0.
     */
    private Map<String, Double> computeActualMarginByWallet() {
        Map<String, Double> marginByWallet = new HashMap<>();

        if (virtualWalletRepository == null) {
            log.warn("[MARGIN-RECONCILE] VirtualWalletRepository not available");
            return marginByWallet;
        }

        try {
            List<VirtualPosition> positions = virtualWalletRepository.listPositions();
            int openCount = 0;
            for (VirtualPosition pos : positions) {
                if (pos.getQtyOpen() > 0) {
                    String walletId = pos.getWalletId();
                    if (walletId == null || walletId.isBlank()) continue;
                    double posMargin = pos.getAvgEntry() * pos.getQtyOpen();
                    marginByWallet.merge(walletId, posMargin, Double::sum);
                    openCount++;
                }
            }
            log.info("[MARGIN-RECONCILE] Scanned {} positions, {} open across {} wallets",
                    positions.size(), openCount, marginByWallet.size());
        } catch (Exception e) {
            log.error("[MARGIN-RECONCILE] Failed to scan positions: {}", e.getMessage());
        }

        return marginByWallet;
    }
}
