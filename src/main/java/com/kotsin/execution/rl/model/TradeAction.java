package com.kotsin.execution.rl.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Action vector for RL agent (4 dimensions)
 * Continuous action space for trade decisions
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class TradeAction {
    
    /**
     * Probability of taking the trade (0-1)
     * > 0.5 = TRADE, <= 0.5 = SKIP
     */
    private double shouldTrade;
    
    /**
     * Entry price adjustment multiplier (-1 to +1)
     * Applied as: adjustedEntry = signalEntry * (1 + entryAdjustment * 0.005)
     * Range: ±0.5% adjustment
     */
    private double entryAdjustment;
    
    /**
     * Stop-loss adjustment multiplier (-1 to +1)
     * Negative = tighter stop, Positive = wider stop
     * Applied as: adjustedSL = signalSL * (1 + slAdjustment * 0.01)
     * Range: ±1% adjustment
     */
    private double slAdjustment;
    
    /**
     * Target adjustment multiplier (-1 to +1)
     * Negative = closer target, Positive = farther target
     * Applied as: adjustedTarget = signalTarget * (1 + targetAdjustment * 0.01)
     * Range: ±1% adjustment
     */
    private double targetAdjustment;
    
    /**
     * Convert to array for neural network output
     */
    public double[] toArray() {
        return new double[] {
            shouldTrade,
            entryAdjustment,
            slAdjustment,
            targetAdjustment
        };
    }
    
    /**
     * Create from array
     */
    public static TradeAction fromArray(double[] arr) {
        if (arr == null || arr.length != 4) {
            throw new IllegalArgumentException("Action array must have 4 elements");
        }
        return TradeAction.builder()
            .shouldTrade(arr[0])
            .entryAdjustment(arr[1])
            .slAdjustment(arr[2])
            .targetAdjustment(arr[3])
            .build();
    }
    
    /**
     * Create default action (take trade as signal suggests)
     */
    public static TradeAction defaultAction() {
        return TradeAction.builder()
            .shouldTrade(1.0)
            .entryAdjustment(0.0)
            .slAdjustment(0.0)
            .targetAdjustment(0.0)
            .build();
    }
    
    public static int getDimension() {
        return 4;
    }
    
    /**
     * Should we take this trade?
     */
    public boolean shouldExecuteTrade() {
        return shouldTrade > 0.5;
    }
}
