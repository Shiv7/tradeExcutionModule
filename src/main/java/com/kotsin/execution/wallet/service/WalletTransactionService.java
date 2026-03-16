package com.kotsin.execution.wallet.service;

import com.kotsin.execution.wallet.model.WalletEntity;
import com.kotsin.execution.wallet.model.WalletTransaction;
import com.kotsin.execution.wallet.repository.WalletRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.List;
import java.util.Map;

/**
 * Service for managing wallet transactions, margin, and P&L.
 *
 * All wallet state mutations use atomic Lua scripts in WalletRepository,
 * eliminating cross-JVM race conditions with StrategyTradeExecutor (port 8085).
 * ReentrantLock is no longer needed — Redis Lua provides atomicity.
 */
@Service
@Slf4j
@RequiredArgsConstructor
public class WalletTransactionService {

    private final WalletRepository walletRepository;

    @Value("${strategy.wallet.initial.capital:1000000}")
    private double initialCapital;

    private static final ZoneId IST = ZoneId.of("Asia/Kolkata");

    /**
     * Get or create the default virtual wallet
     */
    public WalletEntity getDefaultWallet() {
        return walletRepository.getOrCreateDefaultWallet(initialCapital);
    }

    /**
     * Check if margin is available for a new position.
     * READ-ONLY — no wallet mutation. Uses atomic daily reset check.
     */
    public MarginCheckResult checkMarginAvailable(String walletId, double requiredMargin, int currentOpenPositions) {
        // Atomic daily reset if needed (no race risk)
        walletRepository.atomicEnsureDailyReset(walletId);

        WalletEntity wallet = walletRepository.getWallet(walletId)
                .orElse(walletRepository.getOrCreateStrategyWallet(walletId, initialCapital));

        if (wallet.isCircuitBreakerTripped()) {
            return MarginCheckResult.failed("Circuit breaker tripped: " + wallet.getCircuitBreakerReason());
        }
        if (wallet.isDailyLossLimitBreached()) {
            walletRepository.atomicTripCircuitBreaker(walletId, "Daily loss limit breached");
            return MarginCheckResult.failed("Daily loss limit breached. Trading halted for today.");
        }
        if (wallet.isDrawdownLimitBreached()) {
            walletRepository.atomicTripCircuitBreaker(walletId, "Drawdown limit breached");
            return MarginCheckResult.failed("Max drawdown limit breached. Trading halted.");
        }
        if (currentOpenPositions >= wallet.getMaxOpenPositions()) {
            return MarginCheckResult.failed(String.format(
                    "Max open positions limit reached: %d/%d",
                    currentOpenPositions, wallet.getMaxOpenPositions()));
        }
        double effectiveAvailable = wallet.getEffectiveAvailableMargin();
        if (requiredMargin > effectiveAvailable) {
            return MarginCheckResult.failed(String.format(
                    "Insufficient margin: required %.2f, available %.2f",
                    requiredMargin, effectiveAvailable));
        }
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
    }

    /**
     * Deduct margin when an order is filled. ATOMIC via Lua script.
     */
    public WalletOperationResult deductMargin(
            String walletId, String orderId, String scripCode, String symbol,
            String side, int qty, double fillPrice) {

        walletRepository.atomicEnsureDailyReset(walletId);

        double marginAmount = fillPrice * qty;

        // Read wallet state BEFORE for transaction record
        WalletEntity walletBefore = walletRepository.getWallet(walletId)
                .orElse(walletRepository.getOrCreateStrategyWallet(walletId, initialCapital));
        double balanceBefore = walletBefore.getCurrentBalance();
        double marginBefore = walletBefore.getUsedMargin();

        // Atomic margin deduction via Lua
        String result = walletRepository.atomicDeductMargin(walletId, marginAmount);
        if (result == null || "-1".equals(result)) {
            log.error("WALLET_MARGIN_DEDUCT_FAILED walletId={} orderId={} scrip={} margin={}",
                    walletId, orderId, scripCode, marginAmount);
            return WalletOperationResult.failed("Atomic margin deduction failed for " + walletId);
        }

        String[] parts = result.split("\\|");
        String usedMargin = parts.length > 0 ? parts[0] : "?";
        String available = parts.length > 1 ? parts[1] : "?";

        // Create transaction record (non-critical, separate from atomic wallet update)
        WalletTransaction txn = WalletTransaction.marginDeduct(
                walletId, orderId, scripCode, symbol, side, qty, fillPrice,
                balanceBefore, marginBefore);
        walletRepository.saveTransaction(txn);

        log.info("WALLET_MARGIN_DEDUCT walletId={} orderId={} scrip={} margin={} usedMargin={} available={}",
                walletId, orderId, scripCode, marginAmount, usedMargin, available);

        // Re-read wallet for return value
        WalletEntity walletAfter = walletRepository.getWallet(walletId).orElse(walletBefore);
        return WalletOperationResult.success(walletAfter, txn);
    }

    /**
     * Credit P&L when a position is closed (partial or full). ATOMIC via Lua script.
     * Backward-compatible overload — zero charges.
     */
    public WalletOperationResult creditPnl(
            String walletId, String positionId, String scripCode, String symbol,
            String side, int qty, double entryPrice, double exitPrice, String exitReason) {
        return creditPnl(walletId, positionId, scripCode, symbol, side, qty,
                entryPrice, exitPrice, exitReason, 0.0);
    }

    /**
     * Credit P&L when a position is closed (partial or full). ATOMIC via Lua script.
     * Deducts totalCharges (Zerodha brokerage+STT+txn+GST+SEBI+stamp) from gross P&L.
     */
    public WalletOperationResult creditPnl(
            String walletId, String positionId, String scripCode, String symbol,
            String side, int qty, double entryPrice, double exitPrice,
            String exitReason, double totalCharges) {

        walletRepository.atomicEnsureDailyReset(walletId);

        // Calculate gross P&L in Java
        double grossPnl;
        if ("LONG".equalsIgnoreCase(side) || "BUY".equalsIgnoreCase(side)) {
            grossPnl = (exitPrice - entryPrice) * qty;
        } else {
            grossPnl = (entryPrice - exitPrice) * qty;
        }

        // Net P&L = gross - charges (charges always reduce P&L)
        double netPnl = grossPnl - totalCharges;

        double marginReleased = entryPrice * qty;

        // Read wallet state BEFORE for transaction record
        WalletEntity walletBefore = walletRepository.getWallet(walletId)
                .orElse(walletRepository.getOrCreateStrategyWallet(walletId, initialCapital));
        double balanceBefore = walletBefore.getCurrentBalance();
        double marginBefore = walletBefore.getUsedMargin();

        // Atomic PnL credit + margin release via Lua — credits NET P&L
        String result = walletRepository.atomicCreditPnl(walletId, netPnl, marginReleased);
        if (result == null || "-1".equals(result)) {
            log.error("WALLET_PNL_CREDIT_FAILED walletId={} positionId={} scrip={} grossPnl={} charges={} netPnl={} marginRelease={}",
                    walletId, positionId, scripCode, grossPnl, totalCharges, netPnl, marginReleased);
            return WalletOperationResult.failed("Atomic PnL credit failed for " + walletId);
        }

        String[] parts = result.split("\\|");
        String balance = parts.length > 0 ? parts[0] : "?";
        String dayPnl = parts.length > 3 ? parts[3] : "?";
        boolean cbTripped = parts.length > 4 && "1".equals(parts[4]);

        if (cbTripped) {
            log.warn("CIRCUIT_BREAKER_TRIPPED walletId={} (triggered by atomic creditPnl)", walletId);
        }

        // Create transaction record with net P&L (non-critical)
        WalletTransaction txn = WalletTransaction.pnlCredit(
                walletId, positionId, scripCode, symbol, side, qty,
                entryPrice, exitPrice, netPnl, exitReason, balanceBefore, marginBefore);
        walletRepository.saveTransaction(txn);

        log.info("WALLET_PNL_CREDIT walletId={} positionId={} scrip={} grossPnl={} charges={} netPnl={} balance={} dayPnl={}",
                walletId, positionId, scripCode,
                String.format("%.2f", grossPnl), String.format("%.2f", totalCharges),
                String.format("%.2f", netPnl), balance, dayPnl);

        // Re-read wallet for return value
        WalletEntity walletAfter = walletRepository.getWallet(walletId).orElse(walletBefore);
        return WalletOperationResult.success(walletAfter, txn);
    }

    /**
     * Update unrealized P&L for open positions. ATOMIC via Lua script.
     */
    public void updateUnrealizedPnl(String walletId, double totalUnrealizedPnl) {
        String updatedAt = LocalDateTime.now(IST).toString();
        boolean success = walletRepository.atomicUpdateUnrealizedPnl(walletId, totalUnrealizedPnl, updatedAt);
        if (!success) {
            log.debug("atomicUpdateUnrealizedPnl skipped for {} (wallet not found)", walletId);
        }
    }

    /**
     * Get wallet summary for API. Uses atomic daily reset check.
     */
    public WalletSummary getWalletSummary(String walletId) {
        walletRepository.atomicEnsureDailyReset(walletId);
        WalletEntity wallet = walletRepository.getWallet(walletId)
                .orElse(walletRepository.getOrCreateStrategyWallet(walletId, initialCapital));
        return WalletSummary.from(wallet);
    }

    /**
     * Get recent transactions
     */
    public List<WalletTransaction> getRecentTransactions(String walletId, int limit) {
        return walletRepository.getRecentTransactions(walletId, limit);
    }

    /**
     * Reset circuit breaker. ATOMIC via Lua script.
     */
    public void resetCircuitBreaker(String walletId) {
        String result = walletRepository.atomicResetCircuitBreaker(walletId);
        if (result != null && !"- 1".equals(result)) {
            log.info("Circuit breaker reset for wallet: {}", walletId);
        }
    }

    /**
     * Lightweight safety gates for strategy wallets.
     * READ-ONLY — uses atomic daily reset check but no wallet mutation.
     */
    public SafetyGateResult checkSafetyGates(String walletId) {
        walletRepository.atomicEnsureDailyReset(walletId);

        WalletEntity wallet = walletRepository.getWallet(walletId)
                .orElse(walletRepository.getOrCreateStrategyWallet(walletId, initialCapital));

        if (wallet.isCircuitBreakerTripped()) {
            return SafetyGateResult.failed("Circuit breaker tripped: " + wallet.getCircuitBreakerReason());
        }
        if (wallet.isDailyLossLimitBreached()) {
            walletRepository.atomicTripCircuitBreaker(walletId, "Daily loss limit breached");
            return SafetyGateResult.failed("Daily loss limit breached. Trading halted for today.");
        }
        if (wallet.isDrawdownLimitBreached()) {
            walletRepository.atomicTripCircuitBreaker(walletId, "Drawdown limit breached");
            return SafetyGateResult.failed("Max drawdown limit breached. Trading halted.");
        }
        return SafetyGateResult.pass();
    }

    public static class SafetyGateResult {
        private final boolean pass;
        private final String reason;

        private SafetyGateResult(boolean pass, String reason) {
            this.pass = pass;
            this.reason = reason;
        }

        public static SafetyGateResult pass() { return new SafetyGateResult(true, null); }
        public static SafetyGateResult failed(String reason) { return new SafetyGateResult(false, reason); }
        public boolean isPass() { return pass; }
        public String getReason() { return reason; }
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
