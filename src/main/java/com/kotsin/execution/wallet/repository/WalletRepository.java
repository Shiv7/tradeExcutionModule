package com.kotsin.execution.wallet.repository;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.kotsin.execution.wallet.model.WalletEntity;
import com.kotsin.execution.wallet.model.WalletTransaction;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Repository;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Repository for persisting Wallet data in Redis.
 */
@Repository
@Slf4j
public class WalletRepository {

    private static final String WALLET_KEY_PREFIX = "wallet:entity:";
    private static final String TRANSACTION_KEY_PREFIX = "wallet:txn:";
    private static final String TRANSACTION_LIST_PREFIX = "wallet:txn-list:";
    private static final long TRANSACTION_RETENTION_DAYS = 30;

    private final RedisTemplate<String, String> redis;
    private final ObjectMapper objectMapper;

    @Autowired
    public WalletRepository(@Qualifier("executionStringRedisTemplate") RedisTemplate<String, String> redis) {
        this.redis = redis;
        this.objectMapper = new ObjectMapper();
        this.objectMapper.registerModule(new JavaTimeModule());
        // FIX: Ignore unknown fields like "dailyLossLimitBreached" from stale Redis data
        this.objectMapper.configure(com.fasterxml.jackson.databind.DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }

    /**
     * Save wallet entity
     */
    public void saveWallet(WalletEntity wallet) {
        try {
            String key = WALLET_KEY_PREFIX + wallet.getWalletId();
            wallet.setVersion(wallet.getVersion() + 1);
            String json = objectMapper.writeValueAsString(wallet);
            redis.opsForValue().set(key, json);
            log.debug("Saved wallet: {}", wallet.getWalletId());
        } catch (JsonProcessingException e) {
            log.error("Failed to save wallet {}: {}", wallet.getWalletId(), e.getMessage());
            throw new RuntimeException("Failed to save wallet", e);
        }
    }

    /**
     * Get wallet by ID
     */
    public Optional<WalletEntity> getWallet(String walletId) {
        try {
            String key = WALLET_KEY_PREFIX + walletId;
            String json = redis.opsForValue().get(key);
            if (json == null) {
                return Optional.empty();
            }
            return Optional.of(objectMapper.readValue(json, WalletEntity.class));
        } catch (JsonProcessingException e) {
            log.error("Failed to read wallet {}: {}", walletId, e.getMessage());
            return Optional.empty();
        }
    }

    /**
     * Get or create default virtual wallet
     */
    public WalletEntity getOrCreateDefaultWallet(double initialCapital) {
        String defaultWalletId = "virtual-wallet-1";
        Optional<WalletEntity> existing = getWallet(defaultWalletId);

        if (existing.isPresent()) {
            WalletEntity wallet = existing.get();
            // Check if it's a new trading day
            if (wallet.getTradingDate() == null || !wallet.getTradingDate().equals(LocalDate.now())) {
                wallet.resetDailyCounters(LocalDate.now());
                saveWallet(wallet);
                log.info("Reset daily counters for wallet: {}", wallet.getWalletId());
            }
            return wallet;
        }

        // Create new default wallet
        WalletEntity newWallet = WalletEntity.createDefaultVirtual(defaultWalletId, initialCapital);
        saveWallet(newWallet);
        log.info("Created default virtual wallet with capital: {}", initialCapital);
        return newWallet;
    }

    /**
     * Save transaction and append to wallet's transaction list
     */
    public void saveTransaction(WalletTransaction transaction) {
        try {
            // Save transaction
            String key = TRANSACTION_KEY_PREFIX + transaction.getTransactionId();
            String json = objectMapper.writeValueAsString(transaction);
            redis.opsForValue().set(key, json, TRANSACTION_RETENTION_DAYS, TimeUnit.DAYS);

            // Append to wallet's transaction list
            String listKey = TRANSACTION_LIST_PREFIX + transaction.getWalletId();
            redis.opsForList().rightPush(listKey, transaction.getTransactionId());

            // Keep list bounded (last 1000 transactions)
            long size = redis.opsForList().size(listKey);
            if (size > 1000) {
                redis.opsForList().trim(listKey, size - 1000, -1);
            }

            log.debug("Saved transaction: {} for wallet: {}",
                    transaction.getTransactionId(), transaction.getWalletId());
        } catch (JsonProcessingException e) {
            log.error("Failed to save transaction: {}", e.getMessage());
        }
    }

    /**
     * Get recent transactions for wallet
     */
    public List<WalletTransaction> getRecentTransactions(String walletId, int limit) {
        List<WalletTransaction> transactions = new ArrayList<>();
        try {
            String listKey = TRANSACTION_LIST_PREFIX + walletId;
            List<String> txnIds = redis.opsForList().range(listKey, -limit, -1);

            if (txnIds != null) {
                for (String txnId : txnIds) {
                    String key = TRANSACTION_KEY_PREFIX + txnId;
                    String json = redis.opsForValue().get(key);
                    if (json != null) {
                        transactions.add(objectMapper.readValue(json, WalletTransaction.class));
                    }
                }
            }
        } catch (Exception e) {
            log.error("Failed to get transactions for wallet {}: {}", walletId, e.getMessage());
        }
        return transactions;
    }

    /**
     * Get all wallet IDs
     */
    public Set<String> getAllWalletIds() {
        Set<String> keys = redis.keys(WALLET_KEY_PREFIX + "*");
        if (keys == null) return Set.of();
        return keys.stream()
                .map(k -> k.replace(WALLET_KEY_PREFIX, ""))
                .collect(java.util.stream.Collectors.toSet());
    }

    /**
     * Delete wallet (for testing)
     */
    public void deleteWallet(String walletId) {
        redis.delete(WALLET_KEY_PREFIX + walletId);
        redis.delete(TRANSACTION_LIST_PREFIX + walletId);
    }
}
