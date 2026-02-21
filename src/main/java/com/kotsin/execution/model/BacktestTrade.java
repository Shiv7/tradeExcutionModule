package com.kotsin.execution.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

import java.time.LocalDateTime;

/**
 * BacktestTrade - MongoDB entity for storing backtest results
 * 
 * Contains:
 * - Signal info (type, confidence, rationale)
 * - Why we traded (16-module analysis scores)
 * - Entry/Exit details
 * - P&L calculations
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Document(collection = "backtest_trades")
public class BacktestTrade {
    
    @Id
    private String id;
    
    // ========== Signal Info ==========
    private String scripCode;
    private String companyName;
    private String signalType;        // CONFIRMED_BOUNCE_LONG, CONFIRMED_BREAKDOWN_SHORT, etc.
    private double confidence;
    private String rationale;         // "Accelerating bullish momentum off support cluster"
    
    // ========== Why We Traded (16-Module Analysis) ==========
    private double vcpScore;          // Volume Cluster Profile score
    private double supportScore;
    private double resistanceScore;
    private double structuralBias;    // -1 (bearish) to +1 (bullish)
    private double ipuScore;          // Institutional Participation Unit score
    private double momentumContext;
    private String momentumState;     // ACCELERATING, DECELERATING, FLAT
    private String urgencyLevel;      // AGGRESSIVE, ELEVATED, PATIENT, PASSIVE
    private String direction;         // BULLISH, BEARISH, NEUTRAL
    private double flowMomentumAgreement;
    private boolean exhaustionWarning;
    private boolean xfactorFlag;      // Rare strong signal
    
    // ========== Additional RL Fields ==========
    private double runwayScore;       // For RL state encoding
    private double validatedMomentum;
    private double exhaustionScore;
    private double urgencyScore;
    private double directionalConviction;
    
    // ========== Entry Details ==========
    private LocalDateTime signalTime;
    private LocalDateTime entryTime;
    private double signalPrice;       // Price when signal was generated
    private double entryPrice;        // Actual entry price
    private double stopLoss;
    private double target1;
    private double target2;
    private double riskRewardRatio;
    private int positionSize;
    
    // ========== Exit Details ==========
    private LocalDateTime exitTime;
    private double exitPrice;
    private String exitReason;        // TARGET1, TARGET2, STOP_LOSS, TRAILING_STOP, END_OF_DAY
    
    // ========== P&L Calculations ==========
    private double profit;            // Absolute profit in points
    private double profitPercent;     // Profit as % of entry
    private double rMultiple;         // Profit in R units (profit / initial risk)
    
    // ========== Trade Status ==========
    private TradeStatus status;
    
    // ========== Metadata ==========
    private String exchange;          // N, B, M
    private String exchangeType;      // C, D
    private LocalDateTime createdAt;
    private LocalDateTime updatedAt;
    
    public enum TradeStatus {
        PENDING,      // Signal received, waiting for entry
        ACTIVE,       // Trade entered, monitoring
        COMPLETED,    // Trade exited with result
        CANCELLED,    // Signal expired without entry
        FAILED        // Error during execution
    }
    
    // ========== Factory Method ==========
    public static BacktestTrade fromSignal(StrategySignal signal, LocalDateTime signalTime) {
        return BacktestTrade.builder()
                .scripCode(signal.getScripCode())
                .companyName(signal.getCompanyName())
                .signalType(signal.getSignal())
                .confidence(signal.getConfidence())
                .rationale(signal.getRationale())
                .vcpScore(signal.getVcpCombinedScore())
                .supportScore(signal.getSupportScore())
                .resistanceScore(signal.getResistanceScore())
                .structuralBias(signal.getStructuralBias())
                .ipuScore(signal.getIpuFinalScore())
                .momentumContext(signal.getMomentumContext())
                .momentumState(signal.getMomentumState())
                .urgencyLevel(signal.getUrgencyLevel())
                .direction(signal.getDirection())
                .flowMomentumAgreement(signal.getFlowMomentumAgreement())
                .exhaustionWarning(signal.isExhaustionWarning())
                .xfactorFlag(signal.isXfactorFlag())
                .signalTime(signalTime)
                .signalPrice(signal.getEntryPrice())
                .entryPrice(signal.getEntryPrice())
                .stopLoss(signal.getStopLoss())
                .target1(signal.getTarget1())
                .target2(signal.getTarget2())
                .riskRewardRatio(signal.getRiskRewardRatio())
                .positionSize(1)
                .exchange(signal.getExchange())
                .exchangeType(signal.getExchangeType())
                .status(TradeStatus.PENDING)
                .createdAt(LocalDateTime.now())
                .build();
    }
    
    // ========== Helper Methods ==========
    public boolean isBullish() {
        return "BULLISH".equalsIgnoreCase(direction) || 
               (signalType != null && signalType.contains("LONG"));
    }
    
    public boolean isEntered() {
        return entryTime != null && entryPrice > 0;
    }
    
    public boolean isExited() {
        return exitTime != null && exitPrice > 0;
    }
    
    public void calculatePnL() {
        if (!isEntered() || !isExited()) return;
        
        if (isBullish()) {
            profit = (exitPrice - entryPrice) * positionSize;
        } else {
            profit = (entryPrice - exitPrice) * positionSize;
        }
        
        profitPercent = (profit / entryPrice) * 100;
        
        double initialRisk = Math.abs(entryPrice - stopLoss);
        if (initialRisk > 0) {
            rMultiple = profit / initialRisk;
        }
        
        updatedAt = LocalDateTime.now();
    }
}
