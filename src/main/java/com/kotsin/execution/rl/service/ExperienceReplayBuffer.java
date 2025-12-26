package com.kotsin.execution.rl.service;

import com.kotsin.execution.rl.model.RLExperience;
import com.kotsin.execution.rl.repository.RLExperienceRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

/**
 * Experience Replay Buffer for RL training
 * Implements prioritized sampling based on sample weight
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class ExperienceReplayBuffer {
    
    private final RLExperienceRepository repository;
    private final Random random = new Random();
    
    // In-memory cache for fast sampling
    private List<RLExperience> cache = new ArrayList<>();
    private static final int CACHE_SIZE = 10000;
    private static final int MIN_BUFFER_SIZE = 32;
    
    /**
     * Add experience to buffer
     */
    public void add(RLExperience experience) {
        // Persist to MongoDB
        repository.save(experience);
        
        // Add to cache
        cache.add(experience);
        
        // Trim cache if too large
        if (cache.size() > CACHE_SIZE) {
            cache.remove(0);
        }
        
        log.debug("Added experience: tradeId={}, reward={}, source={}", 
            experience.getTradeId(), experience.getReward(), experience.getSource());
    }
    
    /**
     * Sample a batch of experiences with priority weighting
     * Live trades (weight=2.0) are sampled more frequently than backtests (weight=1.0)
     */
    public List<RLExperience> sample(int batchSize) {
        if (cache.size() < MIN_BUFFER_SIZE) {
            // Load from DB if cache is empty
            refreshCache();
        }
        
        if (cache.isEmpty()) {
            return Collections.emptyList();
        }
        
        // Weighted sampling
        List<RLExperience> batch = new ArrayList<>();
        double totalWeight = cache.stream().mapToDouble(RLExperience::getSampleWeight).sum();
        
        for (int i = 0; i < batchSize && !cache.isEmpty(); i++) {
            double r = random.nextDouble() * totalWeight;
            double cumWeight = 0;
            
            for (RLExperience exp : cache) {
                cumWeight += exp.getSampleWeight();
                if (cumWeight >= r) {
                    batch.add(exp);
                    break;
                }
            }
        }
        
        // Fill remaining with random if needed
        while (batch.size() < batchSize && !cache.isEmpty()) {
            batch.add(cache.get(random.nextInt(cache.size())));
        }
        
        return batch;
    }
    
    /**
     * Sample uniformly (without priority)
     */
    public List<RLExperience> sampleUniform(int batchSize) {
        if (cache.size() < MIN_BUFFER_SIZE) {
            refreshCache();
        }
        
        if (cache.isEmpty()) {
            return Collections.emptyList();
        }
        
        List<RLExperience> shuffled = new ArrayList<>(cache);
        Collections.shuffle(shuffled);
        
        return shuffled.subList(0, Math.min(batchSize, shuffled.size()));
    }
    
    /**
     * Refresh cache from database
     */
    public void refreshCache() {
        log.info("Refreshing experience replay buffer cache...");
        cache = new ArrayList<>(repository.findTop1000ByOrderByCreatedAtDesc());
        log.info("Loaded {} experiences into cache", cache.size());
    }
    
    /**
     * Get buffer size
     */
    public int size() {
        return (int) repository.count();
    }
    
    /**
     * Get cache size
     */
    public int cacheSize() {
        return cache.size();
    }
    
    /**
     * Check if buffer has enough samples for training
     */
    public boolean isReady() {
        return size() >= MIN_BUFFER_SIZE;
    }
    
    /**
     * Get statistics
     */
    public BufferStats getStats() {
        long total = repository.count();
        long backtests = repository.countBySource(RLExperience.ExperienceSource.BACKTEST);
        long liveTrades = repository.countBySource(RLExperience.ExperienceSource.LIVE_TRADE);
        
        double avgReward = cache.stream()
            .mapToDouble(RLExperience::getReward)
            .average()
            .orElse(0.0);
        
        double winRate = (double) cache.stream()
            .filter(RLExperience::isWon)
            .count() / Math.max(1, cache.size());
        
        return new BufferStats(total, backtests, liveTrades, avgReward, winRate);
    }
    
    public record BufferStats(
        long totalExperiences,
        long backtestCount,
        long liveTradeCount,
        double avgReward,
        double winRate
    ) {}
}
