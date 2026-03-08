package com.kotsin.execution.wallet.service;

import com.kotsin.execution.virtual.VirtualWalletRepository;
import com.kotsin.execution.virtual.model.VirtualPosition;
import com.kotsin.execution.wallet.model.WalletEntity;
import com.kotsin.execution.wallet.repository.WalletRepository;
import lombok.extern.slf4j.Slf4j;
import org.bson.Document;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Component;

import jakarta.annotation.PostConstruct;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * One-time migrator that creates per-strategy wallets from historical trade_outcomes.
 *
 * Runs on startup ONLY if Redis flag "strategy-wallet-migration:completed" is absent.
 * For each strategy: currentBalance = 100,000 + sumRealizedPnl from trade_outcomes.
 * Creates wallets for all 6 strategies regardless (some may have 0 trades).
 */
@Component
@Slf4j
public class StrategyWalletMigrator {

    private static final String MIGRATION_FLAG_KEY = "strategy-wallet-migration:completed";

    @Autowired
    private WalletRepository walletRepository;

    @Autowired
    private MongoTemplate mongoTemplate;

    @Autowired
    @Qualifier("executionStringRedisTemplate")
    private RedisTemplate<String, String> redis;

    @Autowired
    private VirtualWalletRepository positionRepo;

    @Value("${strategy.wallet.initial.capital:1000000}")
    private double initialCapital;

    @PostConstruct
    public void migrate() {
        try {
            // Check if migration already ran
            String flag = redis.opsForValue().get(MIGRATION_FLAG_KEY);
            if ("true".equals(flag)) {
                log.info("[WALLET-MIGRATE] Migration already completed. Skipping.");
                return;
            }

            log.info("[WALLET-MIGRATE] Starting one-time strategy wallet migration...");

            // Aggregate P&L per strategy from trade_outcomes
            Map<String, StrategyStats> statsMap = new HashMap<>();
            for (String key : StrategyWalletResolver.ALL_STRATEGY_KEYS) {
                statsMap.put(key, new StrategyStats());
            }

            int totalDocs = 0;
            int mappedDocs = 0;
            int unmappedDocs = 0;

            try {
                for (Document doc : mongoTemplate.getCollection("trade_outcomes").find()) {
                    totalDocs++;

                    String signalSource = extractStrategy(doc);
                    String strategyKey = StrategyWalletResolver.resolveStrategyKey(signalSource, null);

                    if (strategyKey == null || !statsMap.containsKey(strategyKey)) {
                        unmappedDocs++;
                        continue;
                    }

                    mappedDocs++;
                    StrategyStats stats = statsMap.get(strategyKey);
                    double pnl = getDouble(doc, "pnl");
                    boolean isWin = pnl > 0;

                    stats.totalPnl += pnl;
                    stats.totalTrades++;
                    if (isWin) stats.wins++;
                    else stats.losses++;
                }
            } catch (Exception e) {
                log.warn("ERR [WALLET-MIGRATE] Could not read trade_outcomes: {}. Creating wallets with initial capital only.", e.getMessage());
            }

            log.info("[WALLET-MIGRATE] Scanned {} trade_outcomes: {} mapped, {} unmapped",
                    totalDocs, mappedDocs, unmappedDocs);

            // Create wallets for ALL strategies
            for (String key : StrategyWalletResolver.ALL_STRATEGY_KEYS) {
                String walletId = StrategyWalletResolver.walletIdForStrategy(key);
                StrategyStats stats = statsMap.get(key);

                // Check if wallet already exists (don't overwrite)
                if (walletRepository.getWallet(walletId).isPresent()) {
                    log.info("[WALLET-MIGRATE] Wallet {} already exists, skipping creation", walletId);
                    continue;
                }

                double balance = initialCapital + stats.totalPnl;
                WalletEntity wallet = WalletEntity.createDefaultVirtual(walletId, initialCapital);

                // Apply migrated stats
                wallet.setCurrentBalance(balance);
                wallet.setAvailableMargin(balance);
                wallet.setRealizedPnl(stats.totalPnl);
                wallet.setTotalPnl(stats.totalPnl);
                wallet.setTotalTradeCount(stats.totalTrades);
                wallet.setTotalWinCount(stats.wins);
                wallet.setTotalLossCount(stats.losses);
                wallet.setPeakBalance(Math.max(initialCapital, balance));
                if (stats.totalTrades > 0) {
                    wallet.setWinRate((double) stats.wins / stats.totalTrades * 100);
                }

                walletRepository.saveWallet(wallet);
                log.info("✅ [WALLET-MIGRATE] Created {} | balance={} | pnl={} | trades={} ({}W/{}L)",
                        walletId, String.format("%.0f", balance), String.format("%.0f", stats.totalPnl),
                        stats.totalTrades, stats.wins, stats.losses);
            }

            // Account for open positions' margin in wallets
            applyOpenPositionMargins();

            // Set flag to prevent re-running
            redis.opsForValue().set(MIGRATION_FLAG_KEY, "true");
            log.info("✅ [WALLET-MIGRATE] Migration complete. Created {} strategy wallets.",
                    StrategyWalletResolver.ALL_STRATEGY_KEYS.size());

        } catch (Exception e) {
            log.error("ERR [WALLET-MIGRATE] Migration failed: {}", e.getMessage(), e);
            // Don't set flag — allow retry on next startup
        }
    }

    /**
     * Extract strategy/signalSource from a trade_outcomes document.
     * Checks multiple field names used across different Kafka producers.
     */
    private String extractStrategy(Document doc) {
        // Primary: signalSource field
        String strategy = doc.getString("signalSource");
        if (strategy != null && !strategy.isBlank()) return strategy;

        // Fallback: strategy field
        strategy = doc.getString("strategy");
        if (strategy != null && !strategy.isBlank()) return strategy;

        // Fallback: source field
        strategy = doc.getString("source");
        if (strategy != null && !strategy.isBlank()) return strategy;

        return null;
    }

    private double getDouble(Document doc, String field) {
        Object val = doc.get(field);
        if (val instanceof Number) return ((Number) val).doubleValue();
        if (val instanceof String) {
            try { return Double.parseDouble((String) val); }
            catch (NumberFormatException e) { return 0; }
        }
        return 0;
    }

    /**
     * Scan Redis for open virtual positions and deduct their margin from the
     * corresponding strategy wallet. Also sets walletId on positions that lack it.
     */
    private void applyOpenPositionMargins() {
        try {
            List<VirtualPosition> positions = positionRepo.listPositions();
            Map<String, Double> marginByWallet = new HashMap<>();

            for (VirtualPosition p : positions) {
                if (p.getQtyOpen() <= 0) continue;

                String strategyKey = StrategyWalletResolver.resolveStrategyKey(
                        p.getSignalSource(), p.getSignalType());
                if (strategyKey == null) continue;

                String walletId = StrategyWalletResolver.walletIdForStrategy(strategyKey);

                // Set walletId on position if missing
                if (p.getWalletId() == null || p.getWalletId().isBlank()) {
                    p.setWalletId(walletId);
                    positionRepo.savePosition(p);
                    log.info("[WALLET-MIGRATE] Set walletId={} on open position scripCode={}", walletId, p.getScripCode());
                }

                double margin = p.getAvgEntry() * p.getQtyOpen();
                marginByWallet.merge(walletId, margin, Double::sum);
            }

            for (Map.Entry<String, Double> entry : marginByWallet.entrySet()) {
                String walletId = entry.getKey();
                double usedMargin = entry.getValue();

                walletRepository.getWallet(walletId).ifPresent(wallet -> {
                    wallet.setUsedMargin(usedMargin);
                    wallet.recalcAvailableMargin();
                    walletRepository.saveWallet(wallet);
                    log.info("✅ [WALLET-MIGRATE] Applied open-position margin to {} | usedMargin={} | availableMargin={}",
                            walletId, String.format("%.0f", usedMargin), String.format("%.0f", wallet.getAvailableMargin()));
                });
            }
        } catch (Exception e) {
            log.warn("ERR [WALLET-MIGRATE] Failed to apply open position margins: {}", e.getMessage());
        }
    }

    private static class StrategyStats {
        double totalPnl = 0;
        int totalTrades = 0;
        int wins = 0;
        int losses = 0;
    }
}
