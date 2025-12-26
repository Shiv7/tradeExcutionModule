package com.kotsin.execution.rl.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

import java.time.LocalDateTime;
import java.util.Map;

/**
 * Saved RL Policy - Actor-Critic network weights and training stats
 * Persisted in MongoDB for recovery after restarts
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Document(collection = "rl_policies")
public class RLPolicy {
    
    @Id
    private String id;
    
    /**
     * Policy version (increments on each save)
     */
    private String version;
    
    /**
     * Actor network weights (serialized JSON)
     * Maps layer name to weight array
     */
    private Map<String, double[]> actorWeights;
    
    /**
     * Critic network weights (serialized JSON)
     */
    private Map<String, double[]> criticWeights;
    
    /**
     * Training statistics
     */
    private int totalExperiences;
    private int backtestExperiences;
    private int liveTradeExperiences;
    
    /**
     * Performance metrics
     */
    private double avgReward;
    private double cumulativeReward;
    private double recentWinRate;       // Last 100 trades
    private double recentAvgRMultiple;  // Last 100 trades
    
    /**
     * Training loss history (last N batches)
     */
    private double[] actorLossHistory;
    private double[] criticLossHistory;
    
    /**
     * Learned module importance (gradient-based)
     * Maps module name to importance weight
     */
    private Map<String, Double> moduleImportance;
    
    /**
     * Learned action tendencies
     */
    private double avgShouldTrade;      // Avg probability to trade
    private double avgEntryAdjustment;
    private double avgSlAdjustment;
    private double avgTargetAdjustment;
    
    /**
     * Current hyperparameters
     */
    private double learningRate;
    private double gamma;               // Discount factor
    private double explorationRate;     // For epsilon-greedy
    
    /**
     * Timestamps
     */
    private LocalDateTime createdAt;
    private LocalDateTime lastUpdatedAt;
    private LocalDateTime lastTrainedAt;
    
    /**
     * Is this the active policy?
     */
    private boolean active;
    
    /**
     * Create initial policy
     */
    public static RLPolicy createInitial() {
        return RLPolicy.builder()
            .version("v1")
            .totalExperiences(0)
            .backtestExperiences(0)
            .liveTradeExperiences(0)
            .avgReward(0.0)
            .cumulativeReward(0.0)
            .recentWinRate(0.0)
            .recentAvgRMultiple(0.0)
            .learningRate(0.001)
            .gamma(0.99)
            .explorationRate(0.1)
            .active(true)
            .createdAt(LocalDateTime.now())
            .lastUpdatedAt(LocalDateTime.now())
            .build();
    }
    
    /**
     * Increment version
     */
    public void incrementVersion() {
        int vNum = Integer.parseInt(version.substring(1));
        this.version = "v" + (vNum + 1);
        this.lastUpdatedAt = LocalDateTime.now();
    }
}
