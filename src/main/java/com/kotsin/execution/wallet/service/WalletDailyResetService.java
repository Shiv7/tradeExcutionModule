package com.kotsin.execution.wallet.service;

import com.kotsin.execution.wallet.repository.WalletRepository;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.Set;

/**
 * Pre-market daily wallet reset service.
 *
 * Runs at 8:55 AM IST (weekdays) to atomically:
 *   1. Reset daily counters (dayPnl, dayTradeCount, etc.)
 *   2. Set maxDailyLoss = 10% of currentBalance
 *   3. Clear any tripped circuit breakers
 *
 * Uses atomic Lua script — safe against concurrent writes from port 8085.
 */
@Service
@Slf4j
public class WalletDailyResetService {

    @Autowired
    private WalletRepository walletRepository;

    @Scheduled(cron = "0 55 8 * * MON-FRI", zone = "Asia/Kolkata")
    public void preMarketDailyReset() {
        log.info("[WALLET-RESET] 8:55 AM pre-market daily reset starting...");
        Set<String> walletIds = walletRepository.getAllWalletIds();
        int resetCount = 0;

        for (String walletId : walletIds) {
            try {
                String result = walletRepository.atomicDailyReset(walletId);
                if (result == null || "-1".equals(result)) {
                    log.warn("[WALLET-RESET] Skipped {} (wallet not found or Lua failed)", walletId);
                    continue;
                }

                resetCount++;
                String[] parts = result.split("\\|");
                String balance = parts.length > 1 ? parts[1] : "?";
                String usedMargin = parts.length > 2 ? parts[2] : "?";
                String maxDailyLoss = parts.length > 3 ? parts[3] : "?";

                log.info("[WALLET-RESET] {} -> balance={} usedMargin={} maxDailyLoss={}",
                        walletId, balance, usedMargin, maxDailyLoss);
            } catch (Exception e) {
                log.error("[WALLET-RESET] Failed to reset wallet {}: {}", walletId, e.getMessage());
            }
        }
        log.info("[WALLET-RESET] Pre-market reset complete. {} wallets reset.", resetCount);
    }
}
