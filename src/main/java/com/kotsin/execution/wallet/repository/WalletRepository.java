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

import org.springframework.data.redis.core.script.DefaultRedisScript;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Repository for persisting Wallet data in Redis.
 *
 * ALL wallet state mutations use atomic Lua scripts to prevent cross-JVM race
 * conditions with StrategyTradeExecutor on port 8085, which also writes to the
 * same wallet:entity:* keys using its own Lua scripts.
 *
 * The plain saveWallet() method is retained ONLY for wallet creation (no race
 * risk since the key doesn't exist yet). All field-level mutations MUST use
 * the atomic* methods.
 */
@Repository
@Slf4j
public class WalletRepository {

    private static final String WALLET_KEY_PREFIX = "wallet:entity:";
    private static final String TRANSACTION_KEY_PREFIX = "wallet:txn:";
    private static final String TRANSACTION_LIST_PREFIX = "wallet:txn-list:";
    private static final long TRANSACTION_RETENTION_DAYS = 30;
    private static final ZoneId IST = ZoneId.of("Asia/Kolkata");

    private final RedisTemplate<String, String> redis;
    private final ObjectMapper objectMapper;

    // ========================================================================
    // ATOMIC LUA SCRIPTS — every wallet mutation is atomic at Redis level
    // ========================================================================

    /** Atomically increment usedMargin and trade counts. Mirrors 8085's LUA_LOCK_MARGIN. */
    private static final String LUA_DEDUCT_MARGIN =
        "local json = redis.call('GET', KEYS[1])\n" +
        "if not json then return '-1' end\n" +
        "local w = cjson.decode(json)\n" +
        "local margin = tonumber(ARGV[1])\n" +
        "w['usedMargin'] = (tonumber(w['usedMargin']) or 0) + margin\n" +
        "w['availableMargin'] = math.max(0, (tonumber(w['currentBalance']) or 0) - w['usedMargin'])\n" +
        "w['dayTradeCount'] = (tonumber(w['dayTradeCount']) or 0) + 1\n" +
        "w['totalTradeCount'] = (tonumber(w['totalTradeCount']) or 0) + 1\n" +
        "w['updatedAt'] = ARGV[2]\n" +
        "w['version'] = (tonumber(w['version']) or 0) + 1\n" +
        "redis.call('SET', KEYS[1], cjson.encode(w))\n" +
        "return tostring(w['usedMargin']) .. '|' .. tostring(w['availableMargin'])\n";

    /** Atomically credit PnL, release margin, update stats. Mirrors 8085's LUA_CREDIT_PNL. */
    private static final String LUA_CREDIT_PNL =
        "local json = redis.call('GET', KEYS[1])\n" +
        "if not json then return '-1' end\n" +
        "local w = cjson.decode(json)\n" +
        "local pnl = tonumber(ARGV[1])\n" +
        "local marginRelease = tonumber(ARGV[2])\n" +
        "local now = ARGV[3]\n" +
        "w['currentBalance'] = (tonumber(w['currentBalance']) or 0) + pnl\n" +
        "w['usedMargin'] = math.max(0, (tonumber(w['usedMargin']) or 0) - marginRelease)\n" +
        "w['availableMargin'] = math.max(0, w['currentBalance'] - w['usedMargin'])\n" +
        "w['realizedPnl'] = (tonumber(w['realizedPnl']) or 0) + pnl\n" +
        "w['dayRealizedPnl'] = (tonumber(w['dayRealizedPnl']) or 0) + pnl\n" +
        "w['totalPnl'] = w['realizedPnl'] + (tonumber(w['unrealizedPnl']) or 0)\n" +
        "w['dayPnl'] = w['dayRealizedPnl'] + (tonumber(w['dayUnrealizedPnl']) or 0)\n" +
        "if pnl > 0 then\n" +
        "  w['totalWinCount'] = (tonumber(w['totalWinCount']) or 0) + 1\n" +
        "  w['dayWinCount'] = (tonumber(w['dayWinCount']) or 0) + 1\n" +
        "elseif pnl < 0 then\n" +
        "  w['totalLossCount'] = (tonumber(w['totalLossCount']) or 0) + 1\n" +
        "  w['dayLossCount'] = (tonumber(w['dayLossCount']) or 0) + 1\n" +
        "end\n" +
        "local totalTrades = (tonumber(w['totalWinCount']) or 0) + (tonumber(w['totalLossCount']) or 0)\n" +
        "w['winRate'] = totalTrades > 0 and ((tonumber(w['totalWinCount']) or 0) / totalTrades * 100) or 0\n" +
        "local peak = tonumber(w['peakBalance']) or 0\n" +
        "if w['currentBalance'] > peak then peak = w['currentBalance'] end\n" +
        "w['peakBalance'] = peak\n" +
        "local dd = peak - w['currentBalance']\n" +
        "local maxDD = tonumber(w['maxDrawdownHit']) or 0\n" +
        "if dd > maxDD then maxDD = dd end\n" +
        "w['maxDrawdownHit'] = maxDD\n" +
        "local cbTripped = '0'\n" +
        "local dailyLoss = -(tonumber(w['dayRealizedPnl']) or 0)\n" +
        "local maxDailyLoss = tonumber(w['maxDailyLoss']) or 999999999\n" +
        "if dailyLoss > 0 and dailyLoss >= maxDailyLoss then\n" +
        "  w['circuitBreakerTripped'] = true\n" +
        "  w['circuitBreakerReason'] = 'Daily loss limit reached: ' .. string.format('%.0f', w['dayRealizedPnl'])\n" +
        "  cbTripped = '1'\n" +
        "end\n" +
        "local maxDrawdown = tonumber(w['maxDrawdown']) or 999999999\n" +
        "if dd >= maxDrawdown and dd > 0 then\n" +
        "  w['circuitBreakerTripped'] = true\n" +
        "  w['circuitBreakerReason'] = 'Drawdown limit reached: ' .. string.format('%.0f', dd)\n" +
        "  cbTripped = '1'\n" +
        "end\n" +
        "w['updatedAt'] = now\n" +
        "w['version'] = (tonumber(w['version']) or 0) + 1\n" +
        "redis.call('SET', KEYS[1], cjson.encode(w))\n" +
        "return tostring(w['currentBalance']) .. '|' .. tostring(w['usedMargin']) .. '|' .. " +
        "tostring(w['availableMargin']) .. '|' .. tostring(w['dayRealizedPnl']) .. '|' .. cbTripped\n";

    /** Atomically reconcile usedMargin to actual value from open positions. */
    private static final String LUA_RECONCILE_MARGIN =
        "local json = redis.call('GET', KEYS[1])\n" +
        "if not json then return '-1' end\n" +
        "local w = cjson.decode(json)\n" +
        "local actualMargin = tonumber(ARGV[1])\n" +
        "local now = ARGV[2]\n" +
        "local oldUsed = tonumber(w['usedMargin']) or 0\n" +
        "if math.abs(oldUsed - actualMargin) <= 0.01 then\n" +
        "  return 'OK|' .. tostring(oldUsed)\n" +
        "end\n" +
        "w['usedMargin'] = actualMargin\n" +
        "w['availableMargin'] = math.max(0, (tonumber(w['currentBalance']) or 0) - actualMargin)\n" +
        "w['updatedAt'] = now\n" +
        "w['version'] = (tonumber(w['version']) or 0) + 1\n" +
        "redis.call('SET', KEYS[1], cjson.encode(w))\n" +
        "return 'FIXED|' .. tostring(oldUsed) .. '|' .. tostring(actualMargin) .. '|' .. tostring(w['availableMargin'])\n";

    /** Atomically reset daily counters, maxDailyLoss, and circuit breaker. */
    private static final String LUA_DAILY_RESET =
        "local json = redis.call('GET', KEYS[1])\n" +
        "if not json then return '-1' end\n" +
        "local w = cjson.decode(json)\n" +
        "local tradingDate = ARGV[1]\n" +
        "local now = ARGV[2]\n" +
        "local balance = tonumber(w['currentBalance']) or 0\n" +
        "w['tradingDate'] = tradingDate\n" +
        "w['dayStartBalance'] = balance\n" +
        "w['dayRealizedPnl'] = 0\n" +
        "w['dayUnrealizedPnl'] = 0\n" +
        "w['dayPnl'] = 0\n" +
        "w['dayTradeCount'] = 0\n" +
        "w['dayWinCount'] = 0\n" +
        "w['dayLossCount'] = 0\n" +
        "w['maxDailyLoss'] = balance * 0.10\n" +
        "w['maxDailyLossPercent'] = 10.0\n" +
        "w['circuitBreakerTripped'] = false\n" +
        "w['circuitBreakerReason'] = cjson.null\n" +
        "w['circuitBreakerTrippedAt'] = cjson.null\n" +
        "w['circuitBreakerResetsAt'] = cjson.null\n" +
        "w['availableMargin'] = math.max(0, balance - (tonumber(w['usedMargin']) or 0))\n" +
        "w['updatedAt'] = now\n" +
        "w['version'] = (tonumber(w['version']) or 0) + 1\n" +
        "redis.call('SET', KEYS[1], cjson.encode(w))\n" +
        "return 'OK|' .. tostring(balance) .. '|' .. tostring(w['usedMargin']) .. '|' .. tostring(w['maxDailyLoss'])\n";

    /** Atomically trip circuit breaker without overwriting other fields. */
    private static final String LUA_TRIP_CIRCUIT_BREAKER =
        "local json = redis.call('GET', KEYS[1])\n" +
        "if not json then return '-1' end\n" +
        "local w = cjson.decode(json)\n" +
        "w['circuitBreakerTripped'] = true\n" +
        "w['circuitBreakerReason'] = ARGV[1]\n" +
        "w['circuitBreakerTrippedAt'] = ARGV[2]\n" +
        "w['circuitBreakerResetsAt'] = ARGV[3]\n" +
        "w['updatedAt'] = ARGV[4]\n" +
        "w['version'] = (tonumber(w['version']) or 0) + 1\n" +
        "redis.call('SET', KEYS[1], cjson.encode(w))\n" +
        "return 'OK'\n";

    /** Atomically reset circuit breaker without overwriting other fields. */
    private static final String LUA_RESET_CIRCUIT_BREAKER =
        "local json = redis.call('GET', KEYS[1])\n" +
        "if not json then return '-1' end\n" +
        "local w = cjson.decode(json)\n" +
        "w['circuitBreakerTripped'] = false\n" +
        "w['circuitBreakerReason'] = cjson.null\n" +
        "w['circuitBreakerTrippedAt'] = cjson.null\n" +
        "w['circuitBreakerResetsAt'] = cjson.null\n" +
        "w['updatedAt'] = ARGV[1]\n" +
        "w['version'] = (tonumber(w['version']) or 0) + 1\n" +
        "redis.call('SET', KEYS[1], cjson.encode(w))\n" +
        "return 'OK'\n";

    /** Atomically add funds to wallet. */
    private static final String LUA_ADD_FUNDS =
        "local json = redis.call('GET', KEYS[1])\n" +
        "if not json then return '-1' end\n" +
        "local w = cjson.decode(json)\n" +
        "local amount = tonumber(ARGV[1])\n" +
        "local now = ARGV[2]\n" +
        "local balanceBefore = tonumber(w['currentBalance']) or 0\n" +
        "w['currentBalance'] = balanceBefore + amount\n" +
        "w['availableMargin'] = math.max(0, w['currentBalance'] - (tonumber(w['usedMargin']) or 0))\n" +
        "local peak = tonumber(w['peakBalance']) or 0\n" +
        "if w['currentBalance'] > peak then w['peakBalance'] = w['currentBalance'] end\n" +
        "w['updatedAt'] = now\n" +
        "w['version'] = (tonumber(w['version']) or 0) + 1\n" +
        "redis.call('SET', KEYS[1], cjson.encode(w))\n" +
        "return tostring(balanceBefore) .. '|' .. tostring(w['currentBalance']) .. '|' .. tostring(w['availableMargin'])\n";

    /** Atomically update ONLY unrealized PnL fields (existing script, unchanged). */
    private static final String LUA_UPDATE_UNREALIZED_PNL =
        "local json = redis.call('GET', KEYS[1])\n" +
        "if not json then return 0 end\n" +
        "local wallet = cjson.decode(json)\n" +
        "local unrealized = tonumber(ARGV[1])\n" +
        "local realizedPnl = tonumber(wallet['realizedPnl'] or 0)\n" +
        "local dayRealizedPnl = tonumber(wallet['dayRealizedPnl'] or 0)\n" +
        "wallet['unrealizedPnl'] = unrealized\n" +
        "wallet['totalPnl'] = realizedPnl + unrealized\n" +
        "wallet['dayUnrealizedPnl'] = unrealized\n" +
        "wallet['dayPnl'] = dayRealizedPnl + unrealized\n" +
        "wallet['updatedAt'] = ARGV[2]\n" +
        "wallet['version'] = (tonumber(wallet['version']) or 0) + 1\n" +
        "redis.call('SET', KEYS[1], cjson.encode(wallet))\n" +
        "return 1\n";

    /** Conditionally reset daily counters + circuit breaker if trading date changed. */
    private static final String LUA_ENSURE_DAILY_RESET =
        "local json = redis.call('GET', KEYS[1])\n" +
        "if not json then return '-1' end\n" +
        "local w = cjson.decode(json)\n" +
        "local today = ARGV[1]\n" +
        "local now = ARGV[2]\n" +
        "local changed = false\n" +
        "if tostring(w['tradingDate']) ~= today then\n" +
        "  w['tradingDate'] = today\n" +
        "  w['dayStartBalance'] = tonumber(w['currentBalance']) or 0\n" +
        "  w['dayRealizedPnl'] = 0\n" +
        "  w['dayUnrealizedPnl'] = 0\n" +
        "  w['dayPnl'] = 0\n" +
        "  w['dayTradeCount'] = 0\n" +
        "  w['dayWinCount'] = 0\n" +
        "  w['dayLossCount'] = 0\n" +
        "  w['availableMargin'] = math.max(0, (tonumber(w['currentBalance']) or 0) - (tonumber(w['usedMargin']) or 0))\n" +
        "  changed = true\n" +
        "end\n" +
        "if w['circuitBreakerTripped'] == true and w['circuitBreakerResetsAt'] ~= nil and w['circuitBreakerResetsAt'] ~= cjson.null then\n" +
        "  if now >= tostring(w['circuitBreakerResetsAt']) then\n" +
        "    w['circuitBreakerTripped'] = false\n" +
        "    w['circuitBreakerReason'] = cjson.null\n" +
        "    w['circuitBreakerTrippedAt'] = cjson.null\n" +
        "    w['circuitBreakerResetsAt'] = cjson.null\n" +
        "    changed = true\n" +
        "  end\n" +
        "end\n" +
        "if changed then\n" +
        "  w['updatedAt'] = now\n" +
        "  w['version'] = (tonumber(w['version']) or 0) + 1\n" +
        "  redis.call('SET', KEYS[1], cjson.encode(w))\n" +
        "  return 'RESET'\n" +
        "end\n" +
        "return 'OK'\n";

    // ========================================================================

    @Autowired
    public WalletRepository(@Qualifier("executionStringRedisTemplate") RedisTemplate<String, String> redis) {
        this.redis = redis;
        this.objectMapper = new ObjectMapper();
        this.objectMapper.registerModule(new JavaTimeModule());
        this.objectMapper.configure(com.fasterxml.jackson.databind.DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }

    /**
     * Save wallet entity — ONLY for wallet creation or migration.
     * For field-level mutations during trading, use atomic* methods instead.
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
        return getOrCreateStrategyWallet("virtual-wallet-1", initialCapital);
    }

    /**
     * Get or create a strategy wallet by ID.
     * Uses atomic Lua for daily reset check on existing wallets.
     */
    public WalletEntity getOrCreateStrategyWallet(String walletId, double initialCapital) {
        Optional<WalletEntity> existing = getWallet(walletId);

        if (existing.isPresent()) {
            // Atomic daily reset check — safe against cross-JVM races
            atomicEnsureDailyReset(walletId);
            // Re-read after potential reset
            return getWallet(walletId).orElse(existing.get());
        }

        // Create new wallet — no race risk (key doesn't exist yet)
        WalletEntity newWallet = WalletEntity.createDefaultVirtual(walletId, initialCapital);
        saveWallet(newWallet);
        log.info("Created strategy wallet {} with capital: {}", walletId, initialCapital);
        return newWallet;
    }

    // ========================================================================
    // ATOMIC MUTATION METHODS
    // ========================================================================

    /**
     * Atomically deduct margin when an order fills.
     * @return "usedMargin|availableMargin" or "-1" if wallet not found
     */
    public String atomicDeductMargin(String walletId, double marginAmount) {
        String key = WALLET_KEY_PREFIX + walletId;
        String now = LocalDateTime.now(IST).toString();
        try {
            DefaultRedisScript<String> script = new DefaultRedisScript<>(LUA_DEDUCT_MARGIN, String.class);
            String result = redis.execute(script, Collections.singletonList(key),
                String.valueOf(marginAmount), now);
            return result;
        } catch (Exception e) {
            log.error("Lua atomicDeductMargin failed for {}: {}", walletId, e.getMessage());
            return null;
        }
    }

    /**
     * Atomically credit PnL, release margin, update win/loss stats, check circuit breaker.
     * @return "balance|usedMargin|available|dayPnl|cbTripped" or "-1" if wallet not found
     */
    public String atomicCreditPnl(String walletId, double pnl, double marginRelease) {
        String key = WALLET_KEY_PREFIX + walletId;
        String now = LocalDateTime.now(IST).toString();
        try {
            DefaultRedisScript<String> script = new DefaultRedisScript<>(LUA_CREDIT_PNL, String.class);
            String result = redis.execute(script, Collections.singletonList(key),
                String.valueOf(pnl), String.valueOf(marginRelease), now);
            return result;
        } catch (Exception e) {
            log.error("Lua atomicCreditPnl failed for {}: {}", walletId, e.getMessage());
            return null;
        }
    }

    /**
     * Atomically reconcile usedMargin to actual value computed from open positions.
     * @return "FIXED|oldUsed|newUsed|available" or "OK|currentUsed" or "-1"
     */
    public String atomicReconcileMargin(String walletId, double actualMargin) {
        String key = WALLET_KEY_PREFIX + walletId;
        String now = LocalDateTime.now(IST).toString();
        try {
            DefaultRedisScript<String> script = new DefaultRedisScript<>(LUA_RECONCILE_MARGIN, String.class);
            return redis.execute(script, Collections.singletonList(key),
                String.valueOf(actualMargin), now);
        } catch (Exception e) {
            log.error("Lua atomicReconcileMargin failed for {}: {}", walletId, e.getMessage());
            return null;
        }
    }

    /**
     * Atomically reset daily counters, set maxDailyLoss=10% of balance, clear circuit breaker.
     * @return "OK|balance|usedMargin|maxDailyLoss" or "-1"
     */
    public String atomicDailyReset(String walletId) {
        String key = WALLET_KEY_PREFIX + walletId;
        String tradingDate = LocalDate.now(IST).toString();
        String now = LocalDateTime.now(IST).toString();
        try {
            DefaultRedisScript<String> script = new DefaultRedisScript<>(LUA_DAILY_RESET, String.class);
            return redis.execute(script, Collections.singletonList(key), tradingDate, now);
        } catch (Exception e) {
            log.error("Lua atomicDailyReset failed for {}: {}", walletId, e.getMessage());
            return null;
        }
    }

    /**
     * Atomically trip circuit breaker without overwriting other wallet fields.
     * @return "OK" or "-1"
     */
    public String atomicTripCircuitBreaker(String walletId, String reason) {
        String key = WALLET_KEY_PREFIX + walletId;
        String now = LocalDateTime.now(IST).toString();
        String resetsAt = LocalDate.now(IST).plusDays(1).atTime(9, 0).toString();
        try {
            DefaultRedisScript<String> script = new DefaultRedisScript<>(LUA_TRIP_CIRCUIT_BREAKER, String.class);
            return redis.execute(script, Collections.singletonList(key), reason, now, resetsAt, now);
        } catch (Exception e) {
            log.error("Lua atomicTripCircuitBreaker failed for {}: {}", walletId, e.getMessage());
            return null;
        }
    }

    /**
     * Atomically reset circuit breaker without overwriting other wallet fields.
     * @return "OK" or "-1"
     */
    public String atomicResetCircuitBreaker(String walletId) {
        String key = WALLET_KEY_PREFIX + walletId;
        String now = LocalDateTime.now(IST).toString();
        try {
            DefaultRedisScript<String> script = new DefaultRedisScript<>(LUA_RESET_CIRCUIT_BREAKER, String.class);
            return redis.execute(script, Collections.singletonList(key), now);
        } catch (Exception e) {
            log.error("Lua atomicResetCircuitBreaker failed for {}: {}", walletId, e.getMessage());
            return null;
        }
    }

    /**
     * Atomically add funds to wallet.
     * @return "balanceBefore|balanceAfter|availableMargin" or "-1"
     */
    public String atomicAddFunds(String walletId, double amount) {
        String key = WALLET_KEY_PREFIX + walletId;
        String now = LocalDateTime.now(IST).toString();
        try {
            DefaultRedisScript<String> script = new DefaultRedisScript<>(LUA_ADD_FUNDS, String.class);
            return redis.execute(script, Collections.singletonList(key),
                String.valueOf(amount), now);
        } catch (Exception e) {
            log.error("Lua atomicAddFunds failed for {}: {}", walletId, e.getMessage());
            return null;
        }
    }

    /**
     * Atomically update ONLY unrealized PnL fields without overwriting other fields.
     */
    public boolean atomicUpdateUnrealizedPnl(String walletId, double unrealizedPnl, String updatedAt) {
        String key = WALLET_KEY_PREFIX + walletId;
        DefaultRedisScript<Long> script = new DefaultRedisScript<>(LUA_UPDATE_UNREALIZED_PNL, Long.class);
        try {
            Long result = redis.execute(script, Collections.singletonList(key),
                String.valueOf(unrealizedPnl), updatedAt);
            return result != null && result == 1;
        } catch (Exception e) {
            log.error("Lua atomicUpdateUnrealizedPnl failed for {}: {}", walletId, e.getMessage());
            return false;
        }
    }

    /**
     * Conditionally reset daily counters if trading date changed, and auto-reset
     * circuit breaker if resetsAt time has passed. No-op if already current.
     * @return "RESET" if wallet was updated, "OK" if no change needed, "-1" if not found
     */
    public String atomicEnsureDailyReset(String walletId) {
        String key = WALLET_KEY_PREFIX + walletId;
        String today = LocalDate.now(IST).toString();
        String now = LocalDateTime.now(IST).toString();
        try {
            DefaultRedisScript<String> script = new DefaultRedisScript<>(LUA_ENSURE_DAILY_RESET, String.class);
            return redis.execute(script, Collections.singletonList(key), today, now);
        } catch (Exception e) {
            log.error("Lua atomicEnsureDailyReset failed for {}: {}", walletId, e.getMessage());
            return null;
        }
    }

    // ========================================================================
    // LEGACY METHODS (addFunds wrapper kept for backward compat with controller)
    // ========================================================================

    /**
     * Add funds to an existing wallet atomically. Creates a DEPOSIT transaction.
     */
    public Optional<WalletEntity> addFunds(String walletId, double amount) {
        String result = atomicAddFunds(walletId, amount);
        if (result == null || "-1".equals(result)) {
            log.error("ERR [WALLET-REPO] Cannot add funds — wallet not found: {}", walletId);
            return Optional.empty();
        }

        String[] parts = result.split("\\|");
        double balanceBefore = parts.length > 0 ? Double.parseDouble(parts[0]) : 0;
        double balanceAfter = parts.length > 1 ? Double.parseDouble(parts[1]) : 0;

        // Re-read wallet for return value
        Optional<WalletEntity> walletOpt = getWallet(walletId);

        // Create DEPOSIT transaction for audit trail
        WalletTransaction txn = WalletTransaction.builder()
                .transactionId("TXN_" + System.currentTimeMillis() + "_" + String.format("%04d", (int) (Math.random() * 10000)))
                .walletId(walletId)
                .type(WalletTransaction.TransactionType.DEPOSIT)
                .amount(amount)
                .balanceBefore(balanceBefore)
                .balanceAfter(balanceAfter)
                .marginBefore(walletOpt.map(WalletEntity::getUsedMargin).orElse(0.0))
                .marginAfter(walletOpt.map(WalletEntity::getUsedMargin).orElse(0.0))
                .description(String.format("Fund top-up: +%.0f added to %s", amount, walletId))
                .timestamp(LocalDateTime.now(IST))
                .build();
        saveTransaction(txn);

        log.info("[WALLET-REPO] Added funds to {}: +{} | balance: {} -> {}",
                walletId, amount, balanceBefore, balanceAfter);
        return walletOpt;
    }

    // ========================================================================
    // TRANSACTION & UTILITY METHODS
    // ========================================================================

    /**
     * Save transaction and append to wallet's transaction list
     */
    public void saveTransaction(WalletTransaction transaction) {
        try {
            String key = TRANSACTION_KEY_PREFIX + transaction.getTransactionId();
            String json = objectMapper.writeValueAsString(transaction);
            redis.opsForValue().set(key, json, TRANSACTION_RETENTION_DAYS, TimeUnit.DAYS);

            String listKey = TRANSACTION_LIST_PREFIX + transaction.getWalletId();
            redis.opsForList().rightPush(listKey, transaction.getTransactionId());

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
