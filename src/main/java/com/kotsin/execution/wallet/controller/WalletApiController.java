package com.kotsin.execution.wallet.controller;

import com.kotsin.execution.wallet.model.WalletTransaction;
import com.kotsin.execution.wallet.service.WalletTransactionService;
import com.kotsin.execution.wallet.service.WalletTransactionService.WalletSummary;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

/**
 * REST API for wallet operations in Trade Execution Module.
 */
@RestController
@RequestMapping("/api/wallet")
@RequiredArgsConstructor
@Slf4j
public class WalletApiController {

    private final WalletTransactionService walletTransactionService;

    private static final String DEFAULT_WALLET_ID = "virtual-wallet-1";

    /**
     * Get wallet summary
     */
    @GetMapping
    public ResponseEntity<WalletSummary> getWallet() {
        WalletSummary summary = walletTransactionService.getWalletSummary(DEFAULT_WALLET_ID);
        return ResponseEntity.ok(summary);
    }

    /**
     * Get wallet by ID
     */
    @GetMapping("/{walletId}")
    public ResponseEntity<WalletSummary> getWalletById(@PathVariable String walletId) {
        WalletSummary summary = walletTransactionService.getWalletSummary(walletId);
        return ResponseEntity.ok(summary);
    }

    /**
     * Check margin availability
     */
    @PostMapping("/margin/check")
    public ResponseEntity<Map<String, Object>> checkMargin(
            @RequestBody MarginCheckRequest request) {

        var result = walletTransactionService.checkMarginAvailable(
                DEFAULT_WALLET_ID,
                request.getRequiredMargin(),
                request.getCurrentOpenPositions());

        return ResponseEntity.ok(Map.of(
                "success", result.isSuccess(),
                "message", result.getMessage(),
                "availableMargin", result.getAvailableMargin(),
                "requiredMargin", result.getRequiredMargin()
        ));
    }

    /**
     * Get recent transactions
     */
    @GetMapping("/transactions")
    public ResponseEntity<List<WalletTransaction>> getTransactions(
            @RequestParam(defaultValue = "50") int limit) {

        List<WalletTransaction> transactions =
                walletTransactionService.getRecentTransactions(DEFAULT_WALLET_ID, limit);
        return ResponseEntity.ok(transactions);
    }

    /**
     * Reset circuit breaker (admin endpoint)
     */
    @PostMapping("/circuit-breaker/reset")
    public ResponseEntity<Map<String, Object>> resetCircuitBreaker() {
        walletTransactionService.resetCircuitBreaker(DEFAULT_WALLET_ID);
        return ResponseEntity.ok(Map.of(
                "success", true,
                "message", "Circuit breaker reset successfully"
        ));
    }

    /**
     * Health check
     */
    @GetMapping("/health")
    public ResponseEntity<Map<String, Object>> health() {
        try {
            WalletSummary summary = walletTransactionService.getWalletSummary(DEFAULT_WALLET_ID);
            return ResponseEntity.ok(Map.of(
                    "status", "UP",
                    "walletId", summary.getWalletId(),
                    "balance", summary.getCurrentBalance(),
                    "circuitBreaker", summary.isCircuitBreakerTripped() ? "TRIPPED" : "OK"
            ));
        } catch (Exception e) {
            return ResponseEntity.ok(Map.of(
                    "status", "DEGRADED",
                    "error", e.getMessage()
            ));
        }
    }

    @lombok.Data
    public static class MarginCheckRequest {
        private double requiredMargin;
        private int currentOpenPositions;
    }
}
