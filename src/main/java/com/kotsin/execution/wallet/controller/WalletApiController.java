package com.kotsin.execution.wallet.controller;

import com.kotsin.execution.wallet.model.WalletEntity;
import com.kotsin.execution.wallet.model.WalletTransaction;
import com.kotsin.execution.wallet.repository.WalletRepository;
import com.kotsin.execution.wallet.service.SignalQueueService;
import com.kotsin.execution.wallet.service.StrategyWalletResolver;
import com.kotsin.execution.wallet.service.WalletTransactionService;
import com.kotsin.execution.wallet.service.WalletTransactionService.WalletSummary;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * REST API for wallet operations in Trade Execution Module.
 */
@RestController
@RequestMapping("/api/wallet")
@RequiredArgsConstructor
@Slf4j
public class WalletApiController {

    private final WalletTransactionService walletTransactionService;

    @Autowired
    private WalletRepository walletRepository;

    @Autowired(required = false)
    private SignalQueueService signalQueueService;

    @Autowired(required = false)
    private KafkaTemplate<String, Object> kafkaTemplate;

    private static final String DEFAULT_WALLET_ID = "virtual-wallet-1";

    // ==================== Default Wallet Endpoints ====================

    @GetMapping
    public ResponseEntity<WalletSummary> getWallet() {
        WalletSummary summary = walletTransactionService.getWalletSummary(DEFAULT_WALLET_ID);
        return ResponseEntity.ok(summary);
    }

    @GetMapping("/{walletId}")
    public ResponseEntity<WalletSummary> getWalletById(@PathVariable String walletId) {
        WalletSummary summary = walletTransactionService.getWalletSummary(walletId);
        return ResponseEntity.ok(summary);
    }

    @PostMapping("/margin/check")
    public ResponseEntity<Map<String, Object>> checkMargin(@RequestBody MarginCheckRequest request) {
        var result = walletTransactionService.checkMarginAvailable(
                DEFAULT_WALLET_ID, request.getRequiredMargin(), request.getCurrentOpenPositions());
        return ResponseEntity.ok(Map.of(
                "success", result.isSuccess(),
                "message", result.getMessage(),
                "availableMargin", result.getAvailableMargin(),
                "requiredMargin", result.getRequiredMargin()
        ));
    }

    @GetMapping("/transactions")
    public ResponseEntity<List<WalletTransaction>> getTransactions(
            @RequestParam(defaultValue = "50") int limit) {
        return ResponseEntity.ok(walletTransactionService.getRecentTransactions(DEFAULT_WALLET_ID, limit));
    }

    @PostMapping("/circuit-breaker/reset")
    public ResponseEntity<Map<String, Object>> resetCircuitBreaker() {
        walletTransactionService.resetCircuitBreaker(DEFAULT_WALLET_ID);
        return ResponseEntity.ok(Map.of("success", true, "message", "Circuit breaker reset successfully"));
    }

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
            return ResponseEntity.ok(Map.of("status", "DEGRADED", "error", e.getMessage()));
        }
    }

    // ==================== Strategy Wallet Endpoints ====================

    /**
     * Add funds to a strategy wallet, then retry queued signals.
     */
    @PostMapping("/strategy/{walletId}/add-funds")
    public ResponseEntity<Map<String, Object>> addFunds(
            @PathVariable String walletId, @RequestBody AddFundsRequest request) {
        if (request.getAmount() <= 0) {
            return ResponseEntity.badRequest().body(Map.of("success", false, "message", "Amount must be positive"));
        }

        Optional<WalletEntity> result = walletRepository.addFunds(walletId, request.getAmount());
        if (result.isEmpty()) {
            return ResponseEntity.notFound().build();
        }

        // Retry queued signals
        int retried = (signalQueueService != null) ? signalQueueService.retryQueuedSignals(walletId) : 0;

        // Publish FUND_ADDED event
        if (kafkaTemplate != null) {
            try {
                kafkaTemplate.send("wallet-events", walletId, Map.of(
                        "eventType", "FUND_ADDED",
                        "walletId", walletId,
                        "amount", request.getAmount(),
                        "newBalance", result.get().getCurrentBalance(),
                        "retriedSignals", retried
                ));
            } catch (Exception e) {
                log.error("ERR [WALLET-API] Failed to publish FUND_ADDED event: {}", e.getMessage());
            }
        }

        log.info("[WALLET-API] Funds added walletId={} amount={} newBalance={} retriedSignals={}",
                walletId, request.getAmount(), result.get().getCurrentBalance(), retried);

        return ResponseEntity.ok(Map.of(
                "success", true,
                "newBalance", result.get().getCurrentBalance(),
                "retriedSignals", retried
        ));
    }

    /**
     * Get strategy wallet summary.
     */
    @GetMapping("/strategy/{walletId}")
    public ResponseEntity<WalletSummary> getStrategyWallet(@PathVariable String walletId) {
        WalletSummary summary = walletTransactionService.getWalletSummary(walletId);
        return ResponseEntity.ok(summary);
    }

    /**
     * Get strategy wallet transactions.
     */
    @GetMapping("/strategy/{walletId}/transactions")
    public ResponseEntity<List<WalletTransaction>> getStrategyTransactions(
            @PathVariable String walletId, @RequestParam(defaultValue = "50") int limit) {
        return ResponseEntity.ok(walletTransactionService.getRecentTransactions(walletId, limit));
    }

    /**
     * List all strategy wallet summaries.
     */
    @GetMapping("/strategies")
    public ResponseEntity<List<WalletSummary>> getAllStrategies() {
        List<WalletSummary> summaries = new ArrayList<>();
        for (String key : StrategyWalletResolver.ALL_STRATEGY_KEYS) {
            String walletId = StrategyWalletResolver.walletIdForStrategy(key);
            summaries.add(walletTransactionService.getWalletSummary(walletId));
        }
        return ResponseEntity.ok(summaries);
    }

    /**
     * Get queued signals for a wallet.
     */
    @GetMapping("/strategy/{walletId}/queued")
    public ResponseEntity<Map<String, Object>> getQueuedSignals(@PathVariable String walletId) {
        if (signalQueueService == null) {
            return ResponseEntity.ok(Map.of("count", 0, "signals", List.of()));
        }
        var pending = signalQueueService.getPendingSignals(walletId);
        return ResponseEntity.ok(Map.of("count", pending.size(), "signals", pending));
    }

    // ==================== Request DTOs ====================

    @lombok.Data
    public static class MarginCheckRequest {
        private double requiredMargin;
        private int currentOpenPositions;
    }

    @lombok.Data
    public static class AddFundsRequest {
        private double amount;
    }
}
