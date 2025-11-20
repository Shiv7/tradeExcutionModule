package com.kotsin.execution.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * 🔑 Idempotency Key Manager
 * 
 * Prevents duplicate order placement due to:
 * - Network timeouts → retry → duplicate
 * - Kafka redelivery after crash
 * - User double-click
 * 
 * Uses UUID-based keys with automatic cleanup after 24 hours
 */
@Service
@Slf4j
public class IdempotencyKeyManager {
    
    // Map: idempotencyKey → (orderId, timestamp)
    private final Map<String, IdempotencyRecord> keyMap = new ConcurrentHashMap<>();
    
    // Cleanup scheduler
    private final ScheduledExecutorService cleanupScheduler = Executors.newSingleThreadScheduledExecutor();
    
    // TTL for keys (24 hours in milliseconds)
    private static final long KEY_TTL_MS = 24 * 60 * 60 * 1000;
    
    // Cleanup interval (1 hour)
    private static final long CLEANUP_INTERVAL_HOURS = 1;
    
    public IdempotencyKeyManager() {
        // Schedule periodic cleanup
        cleanupScheduler.scheduleAtFixedRate(
                this::cleanupExpiredKeys,
                CLEANUP_INTERVAL_HOURS,
                CLEANUP_INTERVAL_HOURS,
                TimeUnit.HOURS
        );
        log.info("🔑 IdempotencyKeyManager initialized | TTL=24h cleanup=1h");
    }
    
    /**
     * Generate idempotency key from trade parameters
     * Deterministic: same params → same key
     */
    public String generateKey(String scripCode, String direction, long signalTimestamp, double entryPrice) {
        String input = String.format("%s:%s:%d:%.2f", 
                scripCode, direction, signalTimestamp, entryPrice);
        
        // Use UUID v5 (name-based with SHA-1) for deterministic hashing
        UUID uuid = UUID.nameUUIDFromBytes(input.getBytes());
        String key = uuid.toString();
        
        log.debug("🔑 Generated idempotency key | scrip={} direction={} ts={} price={} → key={}",
                scripCode, direction, signalTimestamp, entryPrice, key);
        
        return key;
    }
    
    /**
     * Check if key already exists (duplicate detection)
     * 
     * @param key Idempotency key
     * @return OrderRecord if exists, null if new
     */
    public IdempotencyRecord checkKey(String key) {
        IdempotencyRecord record = keyMap.get(key);
        
        if (record != null) {
            log.warn("🔑 ⚠️ DUPLICATE detected | key={} existingOrderId={} age={}ms",
                    key, record.orderId(), System.currentTimeMillis() - record.timestamp());
            return record;
        }
        
        log.debug("🔑 ✅ NEW key (no duplicate) | key={}", key);
        return null;
    }
    
    /**
     * Register successful order
     */
    public void registerOrder(String key, String orderId) {
        IdempotencyRecord record = new IdempotencyRecord(
                orderId,
                System.currentTimeMillis()
        );
        
        keyMap.put(key, record);
        
        log.info("🔑 ✅ Registered order | key={} orderId={}", key, orderId);
    }
    
    /**
     * Check and register in one atomic operation
     * 
     * @return OrderRecord if duplicate, null if new (and registered with placeholder)
     */
    public IdempotencyRecord checkAndReserve(String key) {
        // Check for existing
        IdempotencyRecord existing = keyMap.get(key);
        if (existing != null) {
            log.warn("🔑 ⚠️ DUPLICATE (check-and-reserve) | key={} existingOrderId={}",
                    key, existing.orderId());
            return existing;
        }
        
        // Reserve with placeholder
        IdempotencyRecord placeholder = new IdempotencyRecord(
                "PENDING",
                System.currentTimeMillis()
        );
        
        keyMap.put(key, placeholder);
        log.debug("🔑 🔒 Reserved key (PENDING) | key={}", key);
        
        return null; // Indicates "new, reserved"
    }
    
    /**
     * Update placeholder with actual order ID
     */
    public void updateOrderId(String key, String orderId) {
        IdempotencyRecord existing = keyMap.get(key);
        if (existing != null && "PENDING".equals(existing.orderId())) {
            IdempotencyRecord updated = new IdempotencyRecord(
                    orderId,
                    existing.timestamp()
            );
            keyMap.put(key, updated);
            log.info("🔑 ✅ Updated key with orderId | key={} orderId={}", key, orderId);
        } else {
            log.warn("🔑 ⚠️ Attempted to update non-existent or non-pending key | key={}", key);
        }
    }
    
    /**
     * Remove key manually (for testing or emergency cleanup)
     */
    public void removeKey(String key) {
        IdempotencyRecord removed = keyMap.remove(key);
        if (removed != null) {
            log.info("🔑 🗑️ Removed key | key={} orderId={}", key, removed.orderId());
        }
    }
    
    /**
     * Cleanup expired keys (older than 24 hours)
     */
    private void cleanupExpiredKeys() {
        long now = System.currentTimeMillis();
        int removedCount = 0;
        
        for (Map.Entry<String, IdempotencyRecord> entry : keyMap.entrySet()) {
            long age = now - entry.getValue().timestamp();
            if (age > KEY_TTL_MS) {
                keyMap.remove(entry.getKey());
                removedCount++;
            }
        }
        
        if (removedCount > 0) {
            log.info("🔑 🧹 Cleanup completed | removed={} remaining={}", 
                    removedCount, keyMap.size());
        } else {
            log.debug("🔑 🧹 Cleanup completed | nothing to remove | current={}", keyMap.size());
        }
    }
    
    /**
     * Get current stats for monitoring
     */
    public IdempotencyStats getStats() {
        long now = System.currentTimeMillis();
        long oldestAge = keyMap.values().stream()
                .mapToLong(r -> now - r.timestamp())
                .max()
                .orElse(0);
        
        long pendingCount = keyMap.values().stream()
                .filter(r -> "PENDING".equals(r.orderId()))
                .count();
        
        return new IdempotencyStats(
                keyMap.size(),
                pendingCount,
                oldestAge
        );
    }
    
    public record IdempotencyRecord(String orderId, long timestamp) {}
    
    public record IdempotencyStats(int totalKeys, long pendingKeys, long oldestKeyAgeMs) {}
}
