package com.kotsin.execution.rl.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * State vector for RL agent (20 dimensions)
 * Encodes the 16 module scores + trade context
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class TradeState {
    
    // === 16 MODULE SCORES (normalized 0-1) ===
    
    // VCP Component (5)
    private double supportScore;
    private double resistanceScore;
    private double structuralBias;      // Normalized from -1...+1 to 0...1
    private double runwayScore;
    private double vcpCombined;
    
    // IPU Component (7)
    private double instProxy;
    private double momentumContext;
    private double momentumState;       // FLAT=0.25, TRENDING=0.5, ACCEL=1.0, DECEL=0
    private double validatedMomentum;
    private double exhaustionScore;
    private double urgencyScore;
    private double urgencyLevel;        // PASSIVE=0.25, PATIENT=0.5, ELEVATED=0.75, AGGRESSIVE=1.0
    
    // Flow & Direction (4)
    private double direction;           // BEARISH=0, NEUTRAL=0.5, BULLISH=1
    private double directionalConviction;
    private double flowMomentumAgreement;
    private double xfactor;             // 0 or 1
    
    // === TRADE CONTEXT (4) ===
    private double signalRiskReward;    // R:R ratio normalized
    private double volatilityRank;      // ATR percentile
    private double timeOfDay;           // 0=open, 1=close
    private double confidence;          // Signal confidence 0-1
    
    /**
     * Convert to array for neural network input
     */
    public double[] toArray() {
        return new double[] {
            supportScore,
            resistanceScore,
            structuralBias,
            runwayScore,
            vcpCombined,
            instProxy,
            momentumContext,
            momentumState,
            validatedMomentum,
            exhaustionScore,
            urgencyScore,
            urgencyLevel,
            direction,
            directionalConviction,
            flowMomentumAgreement,
            xfactor,
            signalRiskReward,
            volatilityRank,
            timeOfDay,
            confidence
        };
    }
    
    /**
     * Create from array
     */
    public static TradeState fromArray(double[] arr) {
        if (arr == null || arr.length != 20) {
            throw new IllegalArgumentException("State array must have 20 elements");
        }
        return TradeState.builder()
            .supportScore(arr[0])
            .resistanceScore(arr[1])
            .structuralBias(arr[2])
            .runwayScore(arr[3])
            .vcpCombined(arr[4])
            .instProxy(arr[5])
            .momentumContext(arr[6])
            .momentumState(arr[7])
            .validatedMomentum(arr[8])
            .exhaustionScore(arr[9])
            .urgencyScore(arr[10])
            .urgencyLevel(arr[11])
            .direction(arr[12])
            .directionalConviction(arr[13])
            .flowMomentumAgreement(arr[14])
            .xfactor(arr[15])
            .signalRiskReward(arr[16])
            .volatilityRank(arr[17])
            .timeOfDay(arr[18])
            .confidence(arr[19])
            .build();
    }
    
    public static int getDimension() {
        return 20;
    }
}
