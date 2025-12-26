package com.kotsin.execution.rl.service;

import com.kotsin.execution.model.BacktestTrade;
import com.kotsin.execution.model.StrategySignal;
import com.kotsin.execution.rl.model.TradeState;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.time.LocalTime;

/**
 * Encodes trading signals and backtest trades into state vectors for RL
 */
@Slf4j
@Service
public class RLStateEncoder {
    
    private static final String[] MODULE_NAMES = {
        "supportScore", "resistanceScore", "structuralBias", "runwayScore", "vcpCombined",
        "instProxy", "momentumContext", "momentumState", "validatedMomentum", "exhaustionScore",
        "urgencyScore", "urgencyLevel", "direction", "directionalConviction", 
        "flowMomentumAgreement", "xfactor"
    };
    
    /**
     * Encode a StrategySignal into a TradeState
     */
    public TradeState encode(StrategySignal signal) {
        return TradeState.builder()
            // VCP Component
            .supportScore(normalize(signal.getSupportScore(), 0, 100))
            .resistanceScore(normalize(signal.getResistanceScore(), 0, 100))
            .structuralBias(normalizeSymmetric(signal.getStructuralBias(), -1, 1))
            .runwayScore(normalize(signal.getRunwayScore(), 0, 1))
            .vcpCombined(normalize(signal.getVcpCombinedScore(), 0, 100))
            
            // IPU Component
            .instProxy(normalize(signal.getIpuFinalScore(), 0, 100))
            .momentumContext(normalize(signal.getMomentumContext(), 0, 2))
            .momentumState(encodeMomentumState(signal.getMomentumState()))
            .validatedMomentum(normalize(signal.getValidatedMomentum(), 0, 1))
            .exhaustionScore(normalize(signal.getExhaustionScore(), 0, 1))
            .urgencyScore(normalize(signal.getUrgencyScore(), 0, 1))
            .urgencyLevel(encodeUrgencyLevel(signal.getUrgencyLevel()))
            
            // Flow & Direction
            .direction(encodeDirection(signal.getDirection()))
            .directionalConviction(normalize(signal.getDirectionalConviction(), 0, 1))
            .flowMomentumAgreement(normalize(signal.getFlowMomentumAgreement(), 0, 1))
            .xfactor(signal.isXfactorFlag() ? 1.0 : 0.0)
            
            // Trade Context
            .signalRiskReward(computeRiskReward(signal))
            .volatilityRank(0.5)  // TODO: compute from ATR
            .timeOfDay(encodeTimeOfDay(signal))
            .confidence(normalize(signal.getConfidence(), 0, 1))
            .build();
    }
    
    /**
     * Encode a BacktestTrade into a TradeState
     */
    public TradeState encode(BacktestTrade trade) {
        return TradeState.builder()
            // VCP Component
            .supportScore(normalize(trade.getSupportScore(), 0, 100))
            .resistanceScore(normalize(trade.getResistanceScore(), 0, 100))
            .structuralBias(normalizeSymmetric(trade.getStructuralBias(), -1, 1))
            .runwayScore(normalize(trade.getRunwayScore(), 0, 1))
            .vcpCombined(normalize(trade.getVcpScore(), 0, 100))
            
            // IPU Component
            .instProxy(normalize(trade.getIpuScore(), 0, 100))
            .momentumContext(normalize(trade.getMomentumContext(), 0, 2))
            .momentumState(encodeMomentumState(trade.getMomentumState()))
            .validatedMomentum(normalize(trade.getValidatedMomentum(), 0, 1))
            .exhaustionScore(normalize(trade.getExhaustionScore(), 0, 1))
            .urgencyScore(normalize(trade.getUrgencyScore(), 0, 1))
            .urgencyLevel(encodeUrgencyLevel(trade.getUrgencyLevel()))
            
            // Flow & Direction
            .direction(encodeDirection(trade.getDirection()))
            .directionalConviction(normalize(trade.getDirectionalConviction(), 0, 1))
            .flowMomentumAgreement(normalize(trade.getFlowMomentumAgreement(), 0, 1))
            .xfactor(trade.isXfactorFlag() ? 1.0 : 0.0)
            
            // Trade Context
            .signalRiskReward(computeRiskReward(trade))
            .volatilityRank(0.5)
            .timeOfDay(encodeTimeOfDay(trade.getSignalTime() != null ? 
                trade.getSignalTime().toLocalTime() : LocalTime.of(12, 0)))
            .confidence(normalize(trade.getConfidence(), 0, 1))
            .build();
    }
    
    /**
     * Normalize value to 0-1 range
     */
    private double normalize(double value, double min, double max) {
        if (max == min) return 0.5;
        double normalized = (value - min) / (max - min);
        return Math.max(0, Math.min(1, normalized));
    }
    
    /**
     * Normalize symmetric value (e.g., -1 to +1 → 0 to 1)
     */
    private double normalizeSymmetric(double value, double min, double max) {
        return (value - min) / (max - min);
    }
    
    /**
     * Encode momentum state to numerical value
     */
    private double encodeMomentumState(String state) {
        if (state == null) return 0.25;
        return switch (state.toUpperCase()) {
            case "ACCELERATING" -> 1.0;
            case "TRENDING" -> 0.75;
            case "DRIFTING" -> 0.5;
            case "FLAT" -> 0.25;
            case "DECELERATING" -> 0.0;
            default -> 0.25;
        };
    }
    
    /**
     * Encode urgency level
     */
    private double encodeUrgencyLevel(String level) {
        if (level == null) return 0.25;
        return switch (level.toUpperCase()) {
            case "AGGRESSIVE" -> 1.0;
            case "ELEVATED" -> 0.75;
            case "PATIENT" -> 0.5;
            case "PASSIVE" -> 0.25;
            default -> 0.25;
        };
    }
    
    /**
     * Encode direction
     */
    private double encodeDirection(String direction) {
        if (direction == null) return 0.5;
        return switch (direction.toUpperCase()) {
            case "BULLISH" -> 1.0;
            case "NEUTRAL" -> 0.5;
            case "BEARISH" -> 0.0;
            default -> 0.5;
        };
    }
    
    /**
     * Compute risk-reward ratio
     */
    private double computeRiskReward(StrategySignal signal) {
        double entry = signal.getEntryPrice();
        double sl = signal.getStopLoss();
        double target = signal.getTarget1();
        
        if (entry <= 0 || sl <= 0 || target <= 0) return 1.0;
        
        double risk = Math.abs(entry - sl);
        double reward = Math.abs(target - entry);
        
        if (risk == 0) return 1.0;
        
        double rr = reward / risk;
        return Math.min(rr / 5.0, 1.0);  // Normalize to 0-1, cap at R:R of 5
    }
    
    private double computeRiskReward(BacktestTrade trade) {
        double entry = trade.getSignalPrice();
        double sl = trade.getStopLoss();
        double target = trade.getTarget1();
        
        if (entry <= 0 || sl <= 0 || target <= 0) return 1.0;
        
        double risk = Math.abs(entry - sl);
        double reward = Math.abs(target - entry);
        
        if (risk == 0) return 1.0;
        
        double rr = reward / risk;
        return Math.min(rr / 5.0, 1.0);
    }
    
    /**
     * Encode time of day (0 = market open, 1 = market close)
     */
    private double encodeTimeOfDay(StrategySignal signal) {
        // TODO: extract time from signal
        return 0.5;
    }
    
    private double encodeTimeOfDay(LocalTime time) {
        // Market: 9:15 - 15:30 IST
        int minutesSinceOpen = time.getHour() * 60 + time.getMinute() - (9 * 60 + 15);
        int marketDuration = (15 * 60 + 30) - (9 * 60 + 15);  // 375 minutes
        
        return Math.max(0, Math.min(1, (double) minutesSinceOpen / marketDuration));
    }
    
    public String[] getModuleNames() {
        return MODULE_NAMES;
    }
}
