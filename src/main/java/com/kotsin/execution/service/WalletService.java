package com.kotsin.execution.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Service;

import java.util.UUID;

@Service
@Slf4j
public class WalletService {
    private final RedisTemplate<String, String> redis;
    private final ObjectMapper mapper = new ObjectMapper();

    public WalletService(RedisTemplate<String, String> executionStringRedisTemplate) {
        this.redis = executionStringRedisTemplate;
    }

    public String recordOrder(String scripCode, String side, int qty, Double price, String mode) {
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
            redis.opsForValue().set("wallet:orders:" + orderId, mapper.writeValueAsString(rec));
            return orderId;
        } catch (Exception e) {
            log.warn("Failed to record order: {}", e.getMessage());
            return null;
        }
    }

    /**
     * Update position with thread-safety using per-scripCode locking.
     * FIX: Added synchronization to prevent race condition on concurrent updates.
     * Uses a ConcurrentHashMap of locks to allow concurrent updates to different scripCodes.
     */
    private static final java.util.concurrent.ConcurrentHashMap<String, Object> scripLocks =
            new java.util.concurrent.ConcurrentHashMap<>();

    public Position updatePosition(String scripCode, String side, int qty, Double price) {
        // Get or create lock for this scripCode
        Object lock = scripLocks.computeIfAbsent(scripCode, k -> new Object());

        synchronized (lock) {
            try {
                String key = "wallet:positions:" + scripCode;
                String existing = redis.opsForValue().get(key);
                Position pos = existing != null ? mapper.readValue(existing, Position.class) : new Position();
                if (pos.getScripCode() == null) pos.setScripCode(scripCode);

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
                return pos;
            } catch (Exception e) {
                log.warn("Failed to update position: {}", e.getMessage());
                return null;
            }
        }
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
}
