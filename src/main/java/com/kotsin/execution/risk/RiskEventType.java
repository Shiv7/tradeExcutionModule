package com.kotsin.execution.risk;

/**
 * Types of risk events that can occur during trading.
 */
public enum RiskEventType {
    // Warning Events (thresholds approaching)
    DAILY_LOSS_WARNING,      // Approaching daily loss limit
    DRAWDOWN_WARNING,        // Approaching max drawdown
    POSITION_SIZE_WARNING,   // Position size getting large

    // Circuit Breaker Events
    CIRCUIT_BREAKER_TRIPPED, // Trading halted
    CIRCUIT_BREAKER_RESET,   // Trading resumed

    // Limit Events
    DAILY_LOSS_LIMIT_HIT,    // Daily loss limit reached
    DRAWDOWN_LIMIT_HIT,      // Max drawdown reached
    POSITION_LIMIT_HIT,      // Max positions reached
    MARGIN_LIMIT_HIT,        // Insufficient margin

    // Order Events
    ORDER_REJECTED_RISK,     // Order rejected due to risk
    ORDER_SIZE_REDUCED,      // Order size reduced for risk

    // Recovery Events
    PROFIT_TARGET_HIT,       // Daily/weekly profit target reached
    RECOVERY_DETECTED        // Recovering from drawdown
}
