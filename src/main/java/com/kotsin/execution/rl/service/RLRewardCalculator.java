package com.kotsin.execution.rl.service;

import com.kotsin.execution.model.BacktestTrade;
import com.kotsin.execution.rl.model.TradeAction;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

/**
 * Calculates reward from trade outcomes
 * Uses R-multiple with asymmetric penalties for losses
 */
@Slf4j
@Service
public class RLRewardCalculator {
    
    // Asymmetry factor: penalize losses more than reward wins
    private static final double LOSS_PENALTY_FACTOR = 1.5;
    
    // Bonus for hitting target (vs stop loss)
    private static final double TARGET_HIT_BONUS = 0.5;
    
    /**
     * Calculate reward from backtest trade
     * 
     * Reward = R-multiple (risk-adjusted return)
     * - If R >= 0: reward = R (winning trade)
     * - If R < 0: reward = R * 1.5 (losing trade, penalized)
     */
    public double calculate(BacktestTrade trade) {
        if (trade == null) return 0.0;
        
        // Cancelled trades get neutral reward
        if (trade.getStatus() == BacktestTrade.TradeStatus.CANCELLED ||
            trade.getStatus() == BacktestTrade.TradeStatus.PENDING) {
            return 0.0;
        }
        
        // Failed trades get slight negative
        if (trade.getStatus() == BacktestTrade.TradeStatus.FAILED) {
            return -0.1;
        }
        
        double rMultiple = trade.getRMultiple();
        
        // Base reward from R-multiple
        double reward;
        
        if (rMultiple >= 0) {
            // Winning trade: base reward = R-multiple
            reward = rMultiple;
            
            // Bonus for target hit (vs trailing stop/time exit)
            if ("TARGET1".equals(trade.getExitReason()) || 
                "TARGET2".equals(trade.getExitReason())) {
                reward += TARGET_HIT_BONUS;
            }
        } else {
            // Losing trade: penalize more heavily
            reward = rMultiple * LOSS_PENALTY_FACTOR;
        }
        
        log.debug("Reward for trade {}: R={}, reward={}", 
            trade.getScripCode(), rMultiple, reward);
        
        return reward;
    }
    
    /**
     * Calculate reward from trade outcome components
     */
    public double calculate(double entryPrice, double exitPrice, double stopLoss, 
                           double target, boolean isBullish) {
        if (entryPrice <= 0 || stopLoss <= 0) return 0.0;
        
        double pnl;
        double risk;
        
        if (isBullish) {
            pnl = exitPrice - entryPrice;
            risk = entryPrice - stopLoss;
        } else {
            pnl = entryPrice - exitPrice;
            risk = stopLoss - entryPrice;
        }
        
        if (risk == 0) return 0.0;
        
        double rMultiple = pnl / risk;
        
        if (rMultiple >= 0) {
            return rMultiple;
        } else {
            return rMultiple * LOSS_PENALTY_FACTOR;
        }
    }
    
    /**
     * Calculate reward with action adjustment penalty
     * Small penalty for large adjustments to encourage stable policy
     */
    public double calculateWithActionPenalty(BacktestTrade trade, TradeAction action) {
        double baseReward = calculate(trade);
        
        // Penalize large adjustments (encourage stable policy)
        double adjPenalty = 0.0;
        adjPenalty += Math.abs(action.getEntryAdjustment()) * 0.05;
        adjPenalty += Math.abs(action.getSlAdjustment()) * 0.05;
        adjPenalty += Math.abs(action.getTargetAdjustment()) * 0.05;
        
        return baseReward - adjPenalty;
    }
}
