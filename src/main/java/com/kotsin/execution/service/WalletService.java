package com.kotsin.execution.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Service;

import java.util.UUID;
import java.util.concurrent.TimeUnit;

@Service
@Slf4j
public class WalletService {
    private final RedisTemplate<String, String> redis;
    private final ObjectMapper mapper = new ObjectMapper();

    // FIX: Track wallet operation statistics for monitoring
    private long successfulOrders = 0;
    private long failedOrders = 0;
    private long successfulPositionUpdates = 0;
    private long failedPositionUpdates = 0;

    public WalletService(RedisTemplate<String, String> executionStringRedisTemplate) {
        this.redis = executionStringRedisTemplate;
    }

    /**
     * Record an order in Redis.
     *
     * FIX: Changed from silent failure (returning null) to throwing exception on failure.
     * This ensures callers are aware of failures and can handle them appropriately.
     *
     * @return Order ID if successful
     * @throws WalletOperationException if Redis operation fails
     */
    public String recordOrder(String scripCode, String side, int qty, Double price, String mode) {
        if (scripCode == null || scripCode.isEmpty()) {
            throw new WalletOperationException("scripCode cannot be null or empty");
        }
        if (qty <= 0) {
            throw new WalletOperationException("quantity must be positive, got: " + qty);
        }

        try {
            String orderId = UUID.randomUUID().toString();
            OrderRecord rec = new OrderRecord();
            rec.setOrderId(orderId);
            rec.setScripCode(scripCode);
            rec.setSide(side);
            rec.setQty(qty);
            rec.setPrice(price);
            rec.setMode(mode);
            rec.setTs(System.currentTimeMillis());

            String key = "wallet:orders:" + orderId;
            String value = mapper.writeValueAsString(rec);
            redis.opsForValue().set(key, value);

            // FIX: Verify the write succeeded
            String verification = redis.opsForValue().get(key);
            if (verification == null) {
                failedOrders++;
                throw new WalletOperationException("Redis write verification failed for order: " + orderId);
            }

            successfulOrders++;
            log.info("WALLET_ORDER_RECORDED orderId={} scrip={} side={} qty={} price={} mode={}",
                    orderId, scripCode, side, qty, price, mode);
            return orderId;

        } catch (WalletOperationException e) {
            throw e; // Re-throw our custom exceptions
        } catch (Exception e) {
            failedOrders++;
            log.error("WALLET_ORDER_FAILED scrip={} side={} qty={} error={}",
                    scripCode, side, qty, e.getMessage(), e);
            throw new WalletOperationException("Failed to record order for " + scripCode + ": " + e.getMessage(), e);
        }
    }

    /**
     * Record an order with fallback behavior (for backward compatibility).
     * Returns null on failure instead of throwing exception.
     *
     * @deprecated Use {@link #recordOrder(String, String, int, Double, String)} instead
     */
    @Deprecated
    public String recordOrderSafe(String scripCode, String side, int qty, Double price, String mode) {
        try {
            return recordOrder(scripCode, side, qty, price, mode);
        } catch (WalletOperationException e) {
            log.warn("WALLET_ORDER_SAFE_FAILED scrip={} error={}", scripCode, e.getMessage());
            return null;
        }
    }

    /**
     * Update position with thread-safety using per-scripCode locking.
     * FIX: Added synchronization to prevent race condition on concurrent updates.
     * Uses a ConcurrentHashMap of locks to allow concurrent updates to different scripCodes.
     *
     * FIX: Changed from silent failure to throwing exception on failure.
     *
     * @return Updated position
     * @throws WalletOperationException if Redis operation fails
     */
    private static final java.util.concurrent.ConcurrentHashMap<String, Object> scripLocks =
            new java.util.concurrent.ConcurrentHashMap<>();

    public Position updatePosition(String scripCode, String side, int qty, Double price) {
        if (scripCode == null || scripCode.isEmpty()) {
            throw new WalletOperationException("scripCode cannot be null or empty");
        }
        if (qty <= 0) {
            throw new WalletOperationException("quantity must be positive, got: " + qty);
        }
        if (side == null || (!side.equalsIgnoreCase("BUY") && !side.equalsIgnoreCase("SELL"))) {
            throw new WalletOperationException("side must be BUY or SELL, got: " + side);
        }

        // Get or create lock for this scripCode
        Object lock = scripLocks.computeIfAbsent(scripCode, k -> new Object());

        synchronized (lock) {
            try {
                String key = "wallet:positions:" + scripCode;
                String existing = redis.opsForValue().get(key);
                Position pos = existing != null ? mapper.readValue(existing, Position.class) : new Position();
                if (pos.getScripCode() == null) pos.setScripCode(scripCode);

                int oldQty = pos.getQty();
                double oldAvg = pos.getAvgPrice();

                if ("BUY".equalsIgnoreCase(side)) {
                    int newQty = pos.getQty() + qty;
                    double newAvg = pos.getQty() <= 0 || price == null ? (price != null ? price : pos.getAvgPrice())
                            : ((pos.getAvgPrice() * pos.getQty()) + (price * qty)) / (double) newQty;
                    pos.setQty(newQty);
                    if (price != null) pos.setAvgPrice(newAvg);
                } else if ("SELL".equalsIgnoreCase(side)) {
                    pos.setQty(Math.max(0, pos.getQty() - qty));
                    // avgPrice unchanged on sell
                }

                redis.opsForValue().set(key, mapper.writeValueAsString(pos));

                successfulPositionUpdates++;
                log.info("WALLET_POSITION_UPDATED scrip={} side={} qty={} price={} oldQty={} newQty={} avgPrice={}",
                        scripCode, side, qty, price, oldQty, pos.getQty(), pos.getAvgPrice());

                return pos;

            } catch (Exception e) {
                failedPositionUpdates++;
                log.error("WALLET_POSITION_FAILED scrip={} side={} qty={} error={}",
                        scripCode, side, qty, e.getMessage(), e);
                throw new WalletOperationException("Failed to update position for " + scripCode + ": " + e.getMessage(), e);
            }
        }
    }

    /**
     * Update position with fallback behavior (for backward compatibility).
     * Returns null on failure instead of throwing exception.
     *
     * @deprecated Use {@link #updatePosition(String, String, int, Double)} instead
     */
    @Deprecated
    public Position updatePositionSafe(String scripCode, String side, int qty, Double price) {
        try {
            return updatePosition(scripCode, side, qty, price);
        } catch (WalletOperationException e) {
            log.warn("WALLET_POSITION_SAFE_FAILED scrip={} error={}", scripCode, e.getMessage());
            return null;
        }
    }

    /**
     * Get wallet operation statistics
     */
    public WalletStats getStats() {
        return new WalletStats(successfulOrders, failedOrders, successfulPositionUpdates, failedPositionUpdates);
    }

    @Data
    public static class OrderRecord {
        private String orderId;
        private String scripCode;
        private String side;
        private int qty;
        private Double price;
        private String mode;
        private long ts;
    }

    @Data
    public static class Position {
        private String scripCode;
        private int qty;
        private double avgPrice;
    }

    /**
     * FIX: Custom exception for wallet operations.
     * Allows callers to handle wallet failures explicitly.
     */
    public static class WalletOperationException extends RuntimeException {
        public WalletOperationException(String message) {
            super(message);
        }

        public WalletOperationException(String message, Throwable cause) {
            super(message, cause);
        }
    }

    /**
     * Wallet operation statistics
     */
    @Data
    public static class WalletStats {
        private final long successfulOrders;
        private final long failedOrders;
        private final long successfulPositionUpdates;
        private final long failedPositionUpdates;

        public WalletStats(long successfulOrders, long failedOrders,
                          long successfulPositionUpdates, long failedPositionUpdates) {
            this.successfulOrders = successfulOrders;
            this.failedOrders = failedOrders;
            this.successfulPositionUpdates = successfulPositionUpdates;
            this.failedPositionUpdates = failedPositionUpdates;
        }

        public double getOrderSuccessRate() {
            long total = successfulOrders + failedOrders;
            return total > 0 ? (double) successfulOrders / total : 1.0;
        }

        public double getPositionSuccessRate() {
            long total = successfulPositionUpdates + failedPositionUpdates;
            return total > 0 ? (double) successfulPositionUpdates / total : 1.0;
        }

        @Override
        public String toString() {
            return String.format("WalletStats[orders: %d success / %d failed (%.1f%%), positions: %d success / %d failed (%.1f%%)]",
                    successfulOrders, failedOrders, getOrderSuccessRate() * 100,
                    successfulPositionUpdates, failedPositionUpdates, getPositionSuccessRate() * 100);
        }
    }
}
