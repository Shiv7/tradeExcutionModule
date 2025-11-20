package com.kotsin.execution.service;

import com.google.common.util.concurrent.RateLimiter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import jakarta.annotation.PostConstruct;
import java.util.concurrent.TimeUnit;

/**
 * 🚦 Broker API Rate Limiter
 * 
 * Prevents API abuse and account suspension by enforcing rate limits on:
 * - Order placement
 * - Quote fetching  
 * - Position updates
 * - Market data requests
 * 
 * Uses Google Guava RateLimiter for smooth, thread-safe rate limiting
 */
@Service
@Slf4j
public class BrokerRateLimiterService {
    
    // Rate limiters (permits per second)
    private RateLimiter orderLimiter;
    private RateLimiter quoteLimiter;
    private RateLimiter positionLimiter;
    private RateLimiter marketDataLimiter;
    
    // Configuration (adjust based on broker's actual limits)
    private static final double ORDERS_PER_SECOND = 50.0; // Conservative: 50/sec (brokers allow 100-200/sec)
    private static final double QUOTES_PER_SECOND = 500.0; // 500/sec  
    private static final double POSITIONS_PER_SECOND = 20.0; // 20/sec
    private static final double MARKET_DATA_PER_SECOND = 100.0; // 100/sec
    
    // Timeout for acquiring permits (prevent infinite blocking)
    private static final long ACQUIRE_TIMEOUT_SECONDS = 5;
    
    @PostConstruct
    public void init() {
        orderLimiter = RateLimiter.create(ORDERS_PER_SECOND);
        quoteLimiter = RateLimiter.create(QUOTES_PER_SECOND);
        positionLimiter = RateLimiter.create(POSITIONS_PER_SECOND);
        marketDataLimiter = RateLimiter.create(MARKET_DATA_PER_SECOND);
        
        log.info("🚦 Rate limiters initialized | orders={}/s quotes={}/s positions={}/s marketData={}/s",
                ORDERS_PER_SECOND, QUOTES_PER_SECOND, POSITIONS_PER_SECOND, MARKET_DATA_PER_SECOND);
    }
    
    /**
     * Acquire permit for order placement
     * Blocks until permit is available or timeout occurs
     * 
     * @return true if permit acquired, false if timeout
     */
    public boolean acquireOrderPermit() {
        try {
            boolean acquired = orderLimiter.tryAcquire(ACQUIRE_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            if (!acquired) {
                log.error("🚦 ❌ ORDER rate limit timeout after {}s | Current rate: {}/s",
                        ACQUIRE_TIMEOUT_SECONDS, orderLimiter.getRate());
            }
            return acquired;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.error("🚦 ❌ ORDER rate limit interrupted", e);
            return false;
        }
    }
    
    /**
     * Acquire permit for quote fetching
     */
    public boolean acquireQuotePermit() {
        try {
            boolean acquired = quoteLimiter.tryAcquire(ACQUIRE_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            if (!acquired) {
                log.error("🚦 ❌ QUOTE rate limit timeout after {}s | Current rate: {}/s",
                        ACQUIRE_TIMEOUT_SECONDS, quoteLimiter.getRate());
            }
            return acquired;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.error("🚦 ❌ QUOTE rate limit interrupted", e);
            return false;
        }
    }
    
    /**
     * Acquire permit for position updates
     */
    public boolean acquirePositionPermit() {
        try {
            boolean acquired = positionLimiter.tryAcquire(ACQUIRE_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            if (!acquired) {
                log.error("🚦 ❌ POSITION rate limit timeout after {}s | Current rate: {}/s",
                        ACQUIRE_TIMEOUT_SECONDS, positionLimiter.getRate());
            }
            return acquired;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.error("🚦 ❌ POSITION rate limit interrupted", e);
            return false;
        }
    }
    
    /**
     * Acquire permit for market data requests
     */
    public boolean acquireMarketDataPermit() {
        try {
            boolean acquired = marketDataLimiter.tryAcquire(ACQUIRE_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            if (!acquired) {
                log.error("🚦 ❌ MARKET_DATA rate limit timeout after {}s | Current rate: {}/s",
                        ACQUIRE_TIMEOUT_SECONDS, marketDataLimiter.getRate());
            }
            return acquired;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.error("🚦 ❌ MARKET_DATA rate limit interrupted", e);
            return false;
        }
    }
    
    /**
     * Acquire permit with custom timeout
     */
    public boolean acquireOrderPermitWithTimeout(long timeout, TimeUnit unit) {
        try {
            boolean acquired = orderLimiter.tryAcquire(timeout, unit);
            if (!acquired) {
                log.warn("🚦 ⚠️ ORDER rate limit timeout after {} {}",
                        timeout, unit);
            }
            return acquired;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.error("🚦 ❌ ORDER rate limit interrupted", e);
            return false;
        }
    }
    
    /**
     * Get current rate limiter stats for monitoring
     */
    public RateLimiterStats getStats() {
        return new RateLimiterStats(
                orderLimiter.getRate(),
                quoteLimiter.getRate(),
                positionLimiter.getRate(),
                marketDataLimiter.getRate()
        );
    }
    
    /**
     * Adjust order rate limit dynamically (for broker-specific limits)
     */
    public void setOrderRate(double permitsPerSecond) {
        orderLimiter.setRate(permitsPerSecond);
        log.info("🚦 Order rate limit adjusted to {}/s", permitsPerSecond);
    }
    
    public record RateLimiterStats(
            double orderRate,
            double quoteRate,
            double positionRate,
            double marketDataRate
    ) {}
}
