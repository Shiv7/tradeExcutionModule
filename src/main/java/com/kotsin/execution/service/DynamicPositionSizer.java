package com.kotsin.execution.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.Map;

/**
 * Dynamic Position Sizing Service
 *
 * Implements professional-grade position sizing that adjusts based on:
 * 1. ML Model Confidence (0.6-1.0)
 * 2. GARCH Volatility Regime (low/normal/high)
 * 3. VPIN Microstructure (toxic flow detection)
 * 4. Risk-Reward Ratio (1:1 to 5:1)
 *
 * Based on:
 * - AFML Chapter 10: Bet Sizing
 * - Kelly Criterion (conservative fractional Kelly)
 * - Volatility targeting (15% annual target)
 *
 * CRITICAL: This prevents the fixed position_size=1 bug that was losing money.
 *
 * NOTE: Copied from strategyModule to avoid cross-module dependency
 *
 * @author Kotsin Team
 * @version 2.0 - Production Grade
 */
@Service
@Slf4j
public class DynamicPositionSizer {

    // FIXED: Configuration now externalized to application.properties
    @Value("${position.sizing.base-risk-percent:0.02}")
    private double baseRiskPercent;

    @Value("${position.sizing.target-volatility:0.15}")
    private double targetAnnualVol;

    @Value("${position.sizing.min-ml-confidence:0.60}")
    private double minMlConfidence;

    @Value("${position.sizing.vpin-toxic-threshold:0.70}")
    private double vpinToxicThreshold;

    @Value("${position.sizing.min-risk-reward:1.0}")
    private double minRiskReward;

    @Value("${position.sizing.kelly-fraction:0.25}")
    private double kellyFraction;

    /**
     * Calculate optimal position size based on multiple factors
     *
     * @param accountValue Total account value (INR)
     * @param entryPrice Entry price for the trade
     * @param stopLoss Stop loss price
     * @param mlConfidence ML model confidence (0.0-1.0)
     * @param garchVolatility GARCH forecasted volatility (annualized)
     * @param vpinValue VPIN indicator value (0.0-1.0)
     * @param riskRewardRatio Risk:Reward ratio (e.g., 1:2.5)
     * @return Position size in quantity (0 if no trade should be taken)
     */
    public int calculatePositionSize(
            double accountValue,
            double entryPrice,
            double stopLoss,
            double mlConfidence,
            double garchVolatility,
            double vpinValue,
            double riskRewardRatio
    ) {
        log.debug("[POSITION-SIZING] Calculating position size:");
        log.debug("   Account: {}, Entry: {}, SL: {}", accountValue, entryPrice, stopLoss);
        log.debug("   ML Confidence: {}, GARCH Vol: {}, VPIN: {}, RR: {}",
                String.format("%.3f", mlConfidence), String.format("%.3f", garchVolatility), String.format("%.3f", vpinValue), String.format("%.2f", riskRewardRatio));

        // ========================================
        // FILTER 1: ML Confidence Check
        // ========================================
        if (mlConfidence < minMlConfidence) {
            log.info("[POSITION-SIZING] ML confidence too low: {} < {} -> SIZE = 0",
                    String.format("%.3f", mlConfidence), String.format("%.3f", minMlConfidence));
            return 0;
        }

        // ========================================
        // FILTER 2: VPIN Toxic Flow Check
        // ========================================
        if (vpinValue > vpinToxicThreshold) {
            log.info("[POSITION-SIZING] VPIN toxic flow detected: {} > {} -> SIZE = 0",
                    String.format("%.3f", vpinValue), String.format("%.3f", vpinToxicThreshold));
            return 0;
        }

        // ========================================
        // FILTER 3: Risk-Reward Check
        // ========================================
        if (riskRewardRatio < minRiskReward) {
            log.info("[POSITION-SIZING] Risk-Reward too low: {} < {} -> SIZE = 0",
                    String.format("%.2f", riskRewardRatio), String.format("%.2f", minRiskReward));
            return 0;
        }

        // ========================================
        // STEP 1: Calculate Base Risk Amount
        // ========================================
        double baseRiskAmount = accountValue * baseRiskPercent;
        log.debug("   Base risk (2% of account): {}", String.format("%.2f", baseRiskAmount));

        // ========================================
        // STEP 2: ML Confidence Adjustment
        // ========================================
        // Use quadratic scaling: confidence^2
        // This makes the system more conservative
        // 0.6 → 0.36x, 0.7 → 0.49x, 0.8 → 0.64x, 0.9 → 0.81x, 1.0 → 1.0x
        double mlAdjustment = Math.pow(mlConfidence, 2);
        log.debug("   ML adjustment (conf^2): {}", String.format("%.3f", mlAdjustment));

        // ========================================
        // STEP 3: GARCH Volatility Adjustment
        // ========================================
        // Target 15% annual volatility
        // If market vol = 20%, scale down by 15/20 = 0.75
        // If market vol = 10%, scale up by 15/10 = 1.5 (capped at 1.5)
        double volAdjustment = 1.0;
        if (garchVolatility > 0) {
            volAdjustment = Math.min(1.5, targetAnnualVol / garchVolatility);
        }
        log.debug("   Vol adjustment (targeting {}% vol): {}", targetAnnualVol * 100, String.format("%.3f", volAdjustment));

        // ========================================
        // STEP 4: Microstructure Adjustment (VPIN)
        // ========================================
        // Linear scaling based on VPIN
        // VPIN 0.0 → 1.0x, VPIN 0.5 → 0.71x, VPIN 0.7 → 0.43x
        double microAdjustment = 1.0 - (vpinValue / vpinToxicThreshold);
        microAdjustment = Math.max(0.0, Math.min(1.0, microAdjustment));
        log.debug("   Micro adjustment (VPIN-based): {}", String.format("%.3f", microAdjustment));

        // ========================================
        // STEP 5: Risk-Reward Bonus
        // ========================================
        // Reward higher RR ratios
        // RR 1:1 → 1.0x, RR 1:2 → 1.15x, RR 1:3 → 1.25x, RR 1:5 → 1.4x (capped)
        double rrBonus = 1.0 + Math.min(0.4, Math.log(riskRewardRatio) * 0.3);
        log.debug("   RR bonus (reward good setups): {}", String.format("%.3f", rrBonus));

        // ========================================
        // STEP 6: Calculate Stop Loss Distance
        // ========================================
        double stopLossDistance = Math.abs(entryPrice - stopLoss);
        if (stopLossDistance <= 0) {
            log.error("🚨 [POSITION-SIZING] Invalid stop loss distance: {}", stopLossDistance);
            return 0;
        }
        double stopLossPercent = stopLossDistance / entryPrice;
        log.debug("   Stop loss distance: {} ({}%)", String.format("%.2f", stopLossDistance), String.format("%.2f", stopLossPercent * 100));

        // ========================================
        // STEP 7: Combine All Adjustments
        // ========================================
        double adjustedRiskAmount = baseRiskAmount
                * mlAdjustment
                * volAdjustment
                * microAdjustment
                * rrBonus;

        log.debug("   Adjusted risk: {} (base x {} x {} x {} x {})",
                String.format("%.2f", adjustedRiskAmount), String.format("%.3f", mlAdjustment), String.format("%.3f", volAdjustment), String.format("%.3f", microAdjustment), String.format("%.3f", rrBonus));

        // ========================================
        // STEP 8: Convert to Quantity
        // ========================================
        // quantity = risk_amount / stop_loss_distance
        int quantity = (int) (adjustedRiskAmount / stopLossDistance);

        // Sanity checks
        if (quantity < 1) {
            log.info("⚠️ [POSITION-SIZING] Calculated quantity < 1 → SIZE = 0");
            return 0;
        }

        // Cap at reasonable maximum (10% of account)
        int maxQuantity = (int) (accountValue * 0.10 / entryPrice);
        if (quantity > maxQuantity) {
            log.warn("⚠️ [POSITION-SIZING] Quantity {} exceeds max {} → capping", quantity, maxQuantity);
            quantity = maxQuantity;
        }

        // ========================================
        // FINAL LOGGING
        // ========================================
        double positionValue = quantity * entryPrice;
        double portfolioPercent = (positionValue / accountValue) * 100;
        double riskPercent = (quantity * stopLossDistance / accountValue) * 100;

        log.info("[POSITION-SIZING] FINAL POSITION:");
        log.info("   Quantity: {} ({} position, {}% of portfolio)",
                quantity, String.format("%.2f", positionValue), String.format("%.2f", portfolioPercent));
        log.info("   Risk: {} ({}% of account)", String.format("%.2f", quantity * stopLossDistance), String.format("%.2f", riskPercent));
        log.info("   Factors: ML={}, Vol={}, VPIN={}, RR={}",
                String.format("%.3f", mlAdjustment), String.format("%.3f", volAdjustment), String.format("%.3f", microAdjustment), String.format("%.2f", rrBonus));

        return quantity;
    }

    /**
     * Simplified version when some data is missing
     * Uses conservative defaults
     */
    public int calculatePositionSizeSimplified(
            double accountValue,
            double entryPrice,
            double stopLoss,
            double mlConfidence
    ) {
        // Use conservative defaults
        double defaultVolatility = 0.20;  // 20% annual vol
        double defaultVpin = 0.30;        // Low VPIN (safe)
        double defaultRR = 2.0;           // Assume 1:2 RR

        return calculatePositionSize(
                accountValue,
                entryPrice,
                stopLoss,
                mlConfidence,
                defaultVolatility,
                defaultVpin,
                defaultRR
        );
    }

    /**
     * Calculate fractional Kelly position size (advanced)
     *
     * Kelly Criterion: f* = (p*b - q) / b
     * where:
     *   p = win probability
     *   q = 1 - p
     *   b = win/loss ratio (reward/risk)
     *
     * We use fractional Kelly (25%) for safety
     */
    public int calculateKellyPositionSize(
            double accountValue,
            double entryPrice,
            double stopLoss,
            double winProbability,
            double riskRewardRatio
    ) {
        if (winProbability <= 0.5 || winProbability >= 1.0) {
            log.warn("⚠️ [KELLY] Invalid win probability: {}", winProbability);
            return 0;
        }

        double q = 1.0 - winProbability;
        double b = riskRewardRatio;

        // Full Kelly fraction
        double kellyFraction = (winProbability * b - q) / b;

        if (kellyFraction <= 0) {
            log.info("[KELLY] Negative Kelly fraction: {} -> SIZE = 0", String.format("%.3f", kellyFraction));
            return 0;
        }

        // Use fractional Kelly (25% of full Kelly)
        double fractionalKelly = kellyFraction * this.kellyFraction;

        // Convert to quantity
        double stopLossDistance = Math.abs(entryPrice - stopLoss);
        int quantity = (int) ((accountValue * fractionalKelly) / stopLossDistance);

        log.info("[KELLY] Full Kelly: {}, Fractional: {}, Quantity: {}",
                String.format("%.3f", kellyFraction), String.format("%.3f", fractionalKelly), quantity);

        return Math.max(0, quantity);
    }

    /**
     * Get position sizing diagnostics for monitoring
     */
    public Map<String, Object> getPositionSizingDiagnostics(
            double accountValue,
            double entryPrice,
            double stopLoss,
            double mlConfidence,
            double garchVolatility,
            double vpinValue,
            double riskRewardRatio
    ) {
        Map<String, Object> diagnostics = new HashMap<>();

        // Calculate
        int quantity = calculatePositionSize(
                accountValue, entryPrice, stopLoss,
                mlConfidence, garchVolatility, vpinValue, riskRewardRatio
        );

        // Gather diagnostics
        double stopLossDistance = Math.abs(entryPrice - stopLoss);
        double positionValue = quantity * entryPrice;
        double riskAmount = quantity * stopLossDistance;

        diagnostics.put("quantity", quantity);
        diagnostics.put("positionValue", positionValue);
        diagnostics.put("portfolioPercent", (positionValue / accountValue) * 100);
        diagnostics.put("riskAmount", riskAmount);
        diagnostics.put("riskPercent", (riskAmount / accountValue) * 100);
        diagnostics.put("mlAdjustment", Math.pow(mlConfidence, 2));
        diagnostics.put("volAdjustment", Math.min(1.5, targetAnnualVol / garchVolatility));
        diagnostics.put("vpinAdjustment", 1.0 - (vpinValue / vpinToxicThreshold));
        diagnostics.put("rrBonus", 1.0 + Math.min(0.4, Math.log(riskRewardRatio) * 0.3));
        diagnostics.put("passedFilters", quantity > 0);

        return diagnostics;
    }
}

