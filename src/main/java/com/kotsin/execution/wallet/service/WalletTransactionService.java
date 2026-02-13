package com.kotsin.execution.wallet.service;

import com.kotsin.execution.wallet.model.WalletEntity;
import com.kotsin.execution.wallet.model.WalletTransaction;
import com.kotsin.execution.wallet.repository.WalletRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Service for managing wallet transactions, margin, and P&L.
 * Thread-safe operations for concurrent trading.
 */
@Service
@Slf4j
@RequiredArgsConstructor
public class WalletTransactionService {

    private final WalletRepository walletRepository;

    @Value("${wallet.initial.capital:100000}")
    private double initialCapital;

    // Per-wallet locks for thread safety
    private final ConcurrentHashMap<String, ReentrantLock> walletLocks = new ConcurrentHashMap<>();

    private ReentrantLock getLock(String walletId) {
        return walletLocks.computeIfAbsent(walletId, k -> new ReentrantLock());
    }

    /**
     * Get or create the default virtual wallet
     */
    public WalletEntity getDefaultWallet() {
        return walletRepository.getOrCreateDefaultWallet(initialCapital);
    }

    /**
     * Check if margin is available for a new position
     */
    public MarginCheckResult checkMarginAvailable(String walletId, double requiredMargin, int currentOpenPositions) {
        ReentrantLock lock = getLock(walletId);
        lock.lock();
        try {
            WalletEntity wallet = walletRepository.getWallet(walletId)
                    .orElse(walletRepository.getOrCreateDefaultWallet(initialCapital));

            // Check trading day reset
            ensureTradingDayReset(wallet);

            // Check circuit breaker
            if (wallet.isCircuitBreakerTripped()) {
                return MarginCheckResult.failed("Circuit breaker tripped: " + wallet.getCircuitBreakerReason());
            }

            // Check daily loss limit
            if (wallet.isDailyLossLimitBreached()) {
                tripCircuitBreaker(wallet, "Daily loss limit breached");
                return MarginCheckResult.failed("Daily loss limit breached. Trading halted for today.");
            }

            // Check drawdown limit
            if (wallet.isDrawdownLimitBreached()) {
                tripCircuitBreaker(wallet, "Drawdown limit breached");
                return MarginCheckResult.failed("Max drawdown limit breached. Trading halted.");
            }

            // Check position limits
            if (currentOpenPositions >= wallet.getMaxOpenPositions()) {
                return MarginCheckResult.failed(String.format(
                        "Max open positions limit reached: %d/%d",
                        currentOpenPositions, wallet.getMaxOpenPositions()));
            }

            // Check available margin
            double effectiveAvailable = wallet.getEffectiveAvailableMargin();
            if (requiredMargin > effectiveAvailable) {
                return MarginCheckResult.failed(String.format(
                        "Insufficient margin: required %.2f, available %.2f",
                        requiredMargin, effectiveAvailable));
            }

            // Check position size limit
            if (wallet.getMaxPositionSize() > 0 && requiredMargin > wallet.getMaxPositionSize()) {
                return MarginCheckResult.failed(String.format(
                        "Position size %.2f exceeds max limit %.2f",
                        requiredMargin, wallet.getMaxPositionSize()));
            }

            double maxByPercent = wallet.getCurrentBalance() * wallet.getMaxPositionSizePercent() / 100;
            if (wallet.getMaxPositionSizePercent() > 0 && requiredMargin > maxByPercent) {
                return MarginCheckResult.failed(String.format(
                        "Position size %.2f exceeds %.1f%% of capital (max %.2f)",
                        requiredMargin, wallet.getMaxPositionSizePercent(), maxByPercent));
            }

            return MarginCheckResult.success(effectiveAvailable, requiredMargin);

        } finally {
            lock.unlock();
        }
    }

    /**
     * Deduct margin when an order is filled
     */
    public WalletOperationResult deductMargin(
            String walletId, String orderId, String scripCode, String symbol,
            String side, int qty, double fillPrice) {

        ReentrantLock lock = getLock(walletId);
        lock.lock();
        try {
            WalletEntity wallet = walletRepository.getWallet(walletId)
                    .orElse(walletRepository.getOrCreateDefaultWallet(initialCapital));

            ensureTradingDayReset(wallet);

            double marginAmount = fillPrice * qty;
            double balanceBefore = wallet.getCurrentBalance();
            double marginBefore = wallet.getUsedMargin();

            // Update wallet
            wallet.setUsedMargin(wallet.getUsedMargin() + marginAmount);
            wallet.setAvailableMargin(wallet.getCurrentBalance() - wallet.getUsedMargin() - wallet.getReservedMargin());
            wallet.setDayTradeCount(wallet.getDayTradeCount() + 1);
            wallet.setTotalTradeCount(wallet.getTotalTradeCount() + 1);
            wallet.setUpdatedAt(LocalDateTime.now());

            // Create transaction record
            WalletTransaction txn = WalletTransaction.marginDeduct(
                    walletId, orderId, scripCode, symbol, side, qty, fillPrice,
                    balanceBefore, marginBefore);

            // Save
            walletRepository.saveWallet(wallet);
            walletRepository.saveTransaction(txn);

            log.info("WALLET_MARGIN_DEDUCT walletId={} orderId={} scrip={} margin={} usedMargin={} available={}",
                    walletId, orderId, scripCode, marginAmount, wallet.getUsedMargin(), wallet.getAvailableMargin());

            return WalletOperationResult.success(wallet, txn);

        } finally {
            lock.unlock();
        }
    }

    /**
     * Credit P&L when a position is closed (partial or full)
     */
    public WalletOperationResult creditPnl(
            String walletId, String positionId, String scripCode, String symbol,
            String side, int qty, double entryPrice, double exitPrice, String exitReason) {

        ReentrantLock lock = getLock(walletId);
        lock.lock();
        try {
            WalletEntity wallet = walletRepository.getWallet(walletId)
                    .orElse(walletRepository.getOrCreateDefaultWallet(initialCapital));

            ensureTradingDayReset(wallet);

            // Calculate P&L
            double pnl;
            if ("LONG".equalsIgnoreCase(side) || "BUY".equalsIgnoreCase(side)) {
                pnl = (exitPrice - entryPrice) * qty;
            } else {
                pnl = (entryPrice - exitPrice) * qty;
            }

            double marginReleased = entryPrice * qty;
            double balanceBefore = wallet.getCurrentBalance();
            double marginBefore = wallet.getUsedMargin();

            // Update wallet
            wallet.setCurrentBalance(wallet.getCurrentBalance() + pnl);
            wallet.setUsedMargin(Math.max(0, wallet.getUsedMargin() - marginReleased));
            wallet.setAvailableMargin(wallet.getCurrentBalance() - wallet.getUsedMargin() - wallet.getReservedMargin());
            wallet.setRealizedPnl(wallet.getRealizedPnl() + pnl);
            wallet.setTotalPnl(wallet.getRealizedPnl() + wallet.getUnrealizedPnl());
            wallet.setDayRealizedPnl(wallet.getDayRealizedPnl() + pnl);
            wallet.setDayPnl(wallet.getDayRealizedPnl() + wallet.getDayUnrealizedPnl());

            // Update win/loss stats
            if (pnl > 0) {
                wallet.setDayWinCount(wallet.getDayWinCount() + 1);
                wallet.setTotalWinCount(wallet.getTotalWinCount() + 1);
            } else if (pnl < 0) {
                wallet.setDayLossCount(wallet.getDayLossCount() + 1);
                wallet.setTotalLossCount(wallet.getTotalLossCount() + 1);
            }

            // Update stats
            updateWinRate(wallet);
            wallet.updatePeakBalance();

            // Track max drawdown hit
            double currentDrawdown = wallet.getPeakBalance() - wallet.getCurrentBalance();
            if (currentDrawdown > wallet.getMaxDrawdownHit()) {
                wallet.setMaxDrawdownHit(currentDrawdown);
            }

            wallet.setUpdatedAt(LocalDateTime.now());

            // Check if we need to trip circuit breaker
            if (wallet.isDailyLossLimitBreached()) {
                tripCircuitBreaker(wallet, "Daily loss limit reached: " + wallet.getDayRealizedPnl());
            } else if (wallet.isDrawdownLimitBreached()) {
                tripCircuitBreaker(wallet, "Drawdown limit reached: " + currentDrawdown);
            }

            // Create transaction record
            WalletTransaction txn = WalletTransaction.pnlCredit(
                    walletId, positionId, scripCode, symbol, side, qty,
                    entryPrice, exitPrice, pnl, exitReason, balanceBefore, marginBefore);

            // Save
            walletRepository.saveWallet(wallet);
            walletRepository.saveTransaction(txn);

            log.info("WALLET_PNL_CREDIT walletId={} positionId={} scrip={} pnl={} balance={} dayPnl={}",
                    walletId, positionId, scripCode, pnl, wallet.getCurrentBalance(), wallet.getDayPnl());

            return WalletOperationResult.success(wallet, txn);

        } finally {
            lock.unlock();
        }
    }

    /**
     * Update unrealized P&L for open positions
     */
    public void updateUnrealizedPnl(String walletId, double totalUnrealizedPnl) {
        ReentrantLock lock = getLock(walletId);
        lock.lock();
        try {
            WalletEntity wallet = walletRepository.getWallet(walletId).orElse(null);
            if (wallet == null) return;

            wallet.setUnrealizedPnl(totalUnrealizedPnl);
            wallet.setTotalPnl(wallet.getRealizedPnl() + totalUnrealizedPnl);
            wallet.setDayUnrealizedPnl(totalUnrealizedPnl);
            wallet.setDayPnl(wallet.getDayRealizedPnl() + totalUnrealizedPnl);
            wallet.setUpdatedAt(LocalDateTime.now());

            walletRepository.saveWallet(wallet);

        } finally {
            lock.unlock();
        }
    }

    /**
     * Get wallet summary for API
     */
    public WalletSummary getWalletSummary(String walletId) {
        WalletEntity wallet = walletRepository.getWallet(walletId)
                .orElse(walletRepository.getOrCreateDefaultWallet(initialCapital));

        ensureTradingDayReset(wallet);
        walletRepository.saveWallet(wallet);

        return WalletSummary.from(wallet);
    }

    /**
     * Get recent transactions
     */
    public List<WalletTransaction> getRecentTransactions(String walletId, int limit) {
        return walletRepository.getRecentTransactions(walletId, limit);
    }

    /**
     * Reset circuit breaker manually
     */
    public void resetCircuitBreaker(String walletId) {
        ReentrantLock lock = getLock(walletId);
        lock.lock();
        try {
            WalletEntity wallet = walletRepository.getWallet(walletId).orElse(null);
            if (wallet != null) {
                wallet.setCircuitBreakerTripped(false);
                wallet.setCircuitBreakerReason(null);
                wallet.setCircuitBreakerTrippedAt(null);
                wallet.setCircuitBreakerResetsAt(null);
                wallet.setUpdatedAt(LocalDateTime.now());
                walletRepository.saveWallet(wallet);
                log.info("Circuit breaker reset for wallet: {}", walletId);
            }
        } finally {
            lock.unlock();
        }
    }

    // ==================== Private Helpers ====================

    private void ensureTradingDayReset(WalletEntity wallet) {
        LocalDate today = LocalDate.now();
        if (wallet.getTradingDate() == null || !wallet.getTradingDate().equals(today)) {
            wallet.resetDailyCounters(today);
            log.info("Trading day reset for wallet: {} on {}", wallet.getWalletId(), today);
        }
    }

    private void tripCircuitBreaker(WalletEntity wallet, String reason) {
        wallet.setCircuitBreakerTripped(true);
        wallet.setCircuitBreakerReason(reason);
        wallet.setCircuitBreakerTrippedAt(LocalDateTime.now());
        // Reset next trading day at 9:00 AM
        wallet.setCircuitBreakerResetsAt(
                LocalDate.now().plusDays(1).atTime(9, 0));
        walletRepository.saveWallet(wallet);
        log.warn("CIRCUIT_BREAKER_TRIPPED walletId={} reason={}", wallet.getWalletId(), reason);
    }

    private void updateWinRate(WalletEntity wallet) {
        int total = wallet.getTotalWinCount() + wallet.getTotalLossCount();
        if (total > 0) {
            wallet.setWinRate((double) wallet.getTotalWinCount() / total * 100);
        }
    }

    // ==================== Result Classes ====================

    public static class MarginCheckResult {
        private final boolean success;
        private final String message;
        private final double availableMargin;
        private final double requiredMargin;

        private MarginCheckResult(boolean success, String message, double availableMargin, double requiredMargin) {
            this.success = success;
            this.message = message;
            this.availableMargin = availableMargin;
            this.requiredMargin = requiredMargin;
        }

        public static MarginCheckResult success(double available, double required) {
            return new MarginCheckResult(true, "Margin available", available, required);
        }

        public static MarginCheckResult failed(String reason) {
            return new MarginCheckResult(false, reason, 0, 0);
        }

        public boolean isSuccess() { return success; }
        public String getMessage() { return message; }
        public double getAvailableMargin() { return availableMargin; }
        public double getRequiredMargin() { return requiredMargin; }
    }

    public static class WalletOperationResult {
        private final boolean success;
        private final String message;
        private final WalletEntity wallet;
        private final WalletTransaction transaction;

        private WalletOperationResult(boolean success, String message,
                                      WalletEntity wallet, WalletTransaction transaction) {
            this.success = success;
            this.message = message;
            this.wallet = wallet;
            this.transaction = transaction;
        }

        public static WalletOperationResult success(WalletEntity wallet, WalletTransaction txn) {
            return new WalletOperationResult(true, "Success", wallet, txn);
        }

        public static WalletOperationResult failed(String reason) {
            return new WalletOperationResult(false, reason, null, null);
        }

        public boolean isSuccess() { return success; }
        public String getMessage() { return message; }
        public WalletEntity getWallet() { return wallet; }
        public WalletTransaction getTransaction() { return transaction; }
    }

    @lombok.Data
    @lombok.Builder
    public static class WalletSummary {
        private String walletId;
        private String mode;
        private double initialCapital;
        private double currentBalance;
        private double availableMargin;
        private double usedMargin;
        private double realizedPnl;
        private double unrealizedPnl;
        private double totalPnl;
        private double dayPnl;
        private int dayTradeCount;
        private int dayWinCount;
        private int dayLossCount;
        private double winRate;
        private boolean circuitBreakerTripped;
        private String circuitBreakerReason;
        private Map<String, Object> limits;

        public static WalletSummary from(WalletEntity wallet) {
            return WalletSummary.builder()
                    .walletId(wallet.getWalletId())
                    .mode(wallet.getMode().name())
                    .initialCapital(wallet.getInitialCapital())
                    .currentBalance(wallet.getCurrentBalance())
                    .availableMargin(wallet.getAvailableMargin())
                    .usedMargin(wallet.getUsedMargin())
                    .realizedPnl(wallet.getRealizedPnl())
                    .unrealizedPnl(wallet.getUnrealizedPnl())
                    .totalPnl(wallet.getTotalPnl())
                    .dayPnl(wallet.getDayPnl())
                    .dayTradeCount(wallet.getDayTradeCount())
                    .dayWinCount(wallet.getDayWinCount())
                    .dayLossCount(wallet.getDayLossCount())
                    .winRate(wallet.getWinRate())
                    .circuitBreakerTripped(wallet.isCircuitBreakerTripped())
                    .circuitBreakerReason(wallet.getCircuitBreakerReason())
                    .limits(Map.of(
                            "maxDailyLoss", wallet.getMaxDailyLoss(),
                            "maxDrawdown", wallet.getMaxDrawdown(),
                            "maxOpenPositions", wallet.getMaxOpenPositions(),
                            "maxPositionSizePercent", wallet.getMaxPositionSizePercent()
                    ))
                    .build();
        }
    }
}
