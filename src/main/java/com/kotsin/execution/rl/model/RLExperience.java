package com.kotsin.execution.rl.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

import java.time.LocalDateTime;

/**
 * Experience tuple for RL experience replay buffer
 * Stored in MongoDB for persistence across restarts
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Document(collection = "rl_experiences")
public class RLExperience {
    
    @Id
    private String id;
    
    /**
     * Reference to the trade (BacktestTrade or ActiveTrade)
     */
    private String tradeId;
    
    /**
     * Scrip code for analytics
     */
    private String scripCode;
    
    /**
     * Signal type for grouping
     */
    private String signalType;
    
    /**
     * State at signal time (20 dimensions)
     */
    private double[] state;
    
    /**
     * Action taken (4 dimensions)
     * Initially from signal, later from policy
     */
    private double[] action;
    
    /**
     * Reward received (R-multiple based)
     */
    private double reward;
    
    /**
     * Source of this experience
     */
    private ExperienceSource source;
    
    /**
     * Sample weight for training
     * BACKTEST = 1.0, LIVE_TRADE = 2.0
     */
    private double sampleWeight;
    
    /**
     * Raw trade metrics
     */
    private double entryPrice;
    private double exitPrice;
    private double profitLoss;
    private double rMultiple;
    
    /**
     * Whether trade was a winner
     */
    private boolean won;
    
    /**
     * Exit reason
     */
    private String exitReason;
    
    /**
     * Timestamps
     */
    private LocalDateTime signalTime;
    private LocalDateTime entryTime;
    private LocalDateTime exitTime;
    private LocalDateTime createdAt;
    
    public enum ExperienceSource {
        BACKTEST,       // Historical backtest (1x weight)
        LIVE_TRADE,     // Real 5paisa execution (2x weight)
        VIRTUAL_TRADE   // Virtual trade (1.5x weight)
    }
    
    /**
     * Create experience from backtest
     */
    public static RLExperience fromBacktest(String tradeId, double[] state, double[] action, double reward,
                                             double rMultiple, boolean won, String exitReason) {
        return RLExperience.builder()
            .tradeId(tradeId)
            .state(state)
            .action(action)
            .reward(reward)
            .rMultiple(rMultiple)
            .won(won)
            .exitReason(exitReason)
            .source(ExperienceSource.BACKTEST)
            .sampleWeight(1.0)
            .createdAt(LocalDateTime.now())
            .build();
    }
    
    /**
     * Create experience from live trade
     */
    public static RLExperience fromLiveTrade(String tradeId, double[] state, double[] action, double reward,
                                              double rMultiple, boolean won, String exitReason) {
        return RLExperience.builder()
            .tradeId(tradeId)
            .state(state)
            .action(action)
            .reward(reward)
            .rMultiple(rMultiple)
            .won(won)
            .exitReason(exitReason)
            .source(ExperienceSource.LIVE_TRADE)
            .sampleWeight(2.0)  // Live trades weighted 2x
            .createdAt(LocalDateTime.now())
            .build();
    }
}
