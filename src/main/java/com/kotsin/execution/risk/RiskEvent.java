package com.kotsin.execution.risk;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;

/**
 * Represents a risk event for monitoring and alerts.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class RiskEvent {

    public enum Severity {
        INFO,
        WARNING,
        CRITICAL
    }

    private String eventId;
    private String walletId;
    private RiskEventType eventType;
    private Severity severity;
    private String message;

    // Current state
    private double currentValue;
    private double limitValue;
    private double thresholdPercent;

    // Context
    private String scripCode;
    private String orderId;
    private String positionId;

    // Wallet snapshot
    private double currentBalance;
    private double availableMargin;
    private double dayPnl;
    private double drawdown;

    private Instant timestamp;
    private boolean requiresAction;

    public static RiskEvent dailyLossWarning(String walletId, double currentLoss, double limit, double percent) {
        return RiskEvent.builder()
                .eventId(java.util.UUID.randomUUID().toString())
                .walletId(walletId)
                .eventType(RiskEventType.DAILY_LOSS_WARNING)
                .severity(percent >= 80 ? Severity.WARNING : Severity.INFO)
                .message(String.format("Daily loss at %.1f%% of limit (%.2f / %.2f)", percent, currentLoss, limit))
                .currentValue(currentLoss)
                .limitValue(limit)
                .thresholdPercent(percent)
                .timestamp(Instant.now())
                .requiresAction(percent >= 90)
                .build();
    }

    public static RiskEvent drawdownWarning(String walletId, double currentDrawdown, double limit, double percent) {
        return RiskEvent.builder()
                .eventId(java.util.UUID.randomUUID().toString())
                .walletId(walletId)
                .eventType(RiskEventType.DRAWDOWN_WARNING)
                .severity(percent >= 80 ? Severity.WARNING : Severity.INFO)
                .message(String.format("Drawdown at %.1f%% of limit (%.2f / %.2f)", percent, currentDrawdown, limit))
                .currentValue(currentDrawdown)
                .limitValue(limit)
                .thresholdPercent(percent)
                .timestamp(Instant.now())
                .requiresAction(percent >= 90)
                .build();
    }

    public static RiskEvent circuitBreakerTripped(String walletId, String reason) {
        return RiskEvent.builder()
                .eventId(java.util.UUID.randomUUID().toString())
                .walletId(walletId)
                .eventType(RiskEventType.CIRCUIT_BREAKER_TRIPPED)
                .severity(Severity.CRITICAL)
                .message("Circuit breaker tripped: " + reason)
                .timestamp(Instant.now())
                .requiresAction(true)
                .build();
    }

    public static RiskEvent circuitBreakerReset(String walletId) {
        return RiskEvent.builder()
                .eventId(java.util.UUID.randomUUID().toString())
                .walletId(walletId)
                .eventType(RiskEventType.CIRCUIT_BREAKER_RESET)
                .severity(Severity.INFO)
                .message("Circuit breaker reset - trading resumed")
                .timestamp(Instant.now())
                .requiresAction(false)
                .build();
    }

    public static RiskEvent orderRejected(String walletId, String orderId, String scripCode, String reason) {
        return RiskEvent.builder()
                .eventId(java.util.UUID.randomUUID().toString())
                .walletId(walletId)
                .eventType(RiskEventType.ORDER_REJECTED_RISK)
                .severity(Severity.WARNING)
                .message("Order rejected: " + reason)
                .orderId(orderId)
                .scripCode(scripCode)
                .timestamp(Instant.now())
                .requiresAction(false)
                .build();
    }

    public static RiskEvent marginLimitHit(String walletId, double required, double available) {
        return RiskEvent.builder()
                .eventId(java.util.UUID.randomUUID().toString())
                .walletId(walletId)
                .eventType(RiskEventType.MARGIN_LIMIT_HIT)
                .severity(Severity.WARNING)
                .message(String.format("Insufficient margin: required %.2f, available %.2f", required, available))
                .currentValue(required)
                .limitValue(available)
                .timestamp(Instant.now())
                .requiresAction(false)
                .build();
    }
}
