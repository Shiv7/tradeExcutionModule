package com.kotsin.execution.risk;

import com.kotsin.execution.virtual.VirtualWalletRepository;
import com.kotsin.execution.virtual.model.VirtualPosition;
import com.kotsin.execution.virtual.PriceProvider;
import com.kotsin.execution.wallet.model.WalletEntity;
import com.kotsin.execution.wallet.repository.WalletRepository;
import com.kotsin.execution.wallet.service.WalletTransactionService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Service that monitors risk metrics in real-time and publishes events.
 * Runs on a schedule to check unrealized P&L and risk thresholds.
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class RiskMonitorService {

    private static final String RISK_EVENTS_TOPIC = "risk-events";

    // Warning thresholds (percentage of limit)
    private static final double DAILY_LOSS_WARNING_THRESHOLD = 70.0;
    private static final double DRAWDOWN_WARNING_THRESHOLD = 70.0;

    private final WalletRepository walletRepository;
    private final WalletTransactionService walletTransactionService;
    private final VirtualWalletRepository positionRepository;
    private final PriceProvider priceProvider;
    private final KafkaTemplate<String, Object> kafkaTemplate;

    @Value("${wallet.id:virtual-wallet-1}")
    private String defaultWalletId;

    // Track last warning to avoid spam
    private final ConcurrentHashMap<String, Long> lastWarningTime = new ConcurrentHashMap<>();
    private static final long WARNING_COOLDOWN_MS = 60000; // 1 minute

    /**
     * Monitor unrealized P&L and update wallet state.
     * Runs every 5 seconds.
     */
    @Scheduled(fixedDelay = 5000)
    public void monitorUnrealizedPnl() {
        try {
            List<VirtualPosition> positions = positionRepository.listPositions();
            if (positions.isEmpty()) {
                return;
            }

            double totalUnrealizedPnl = 0;

            for (VirtualPosition position : positions) {
                if (position.getQtyOpen() <= 0) continue;

                Double ltp = priceProvider.getLtp(position.getScripCode());
                if (ltp == null) continue;

                double positionPnl;
                if (position.getSide() == VirtualPosition.Side.LONG) {
                    positionPnl = (ltp - position.getAvgEntry()) * position.getQtyOpen();
                } else {
                    positionPnl = (position.getAvgEntry() - ltp) * position.getQtyOpen();
                }

                totalUnrealizedPnl += positionPnl;
            }

            // Update wallet unrealized P&L
            walletTransactionService.updateUnrealizedPnl(defaultWalletId, totalUnrealizedPnl);

            // Check risk thresholds
            checkRiskThresholds(totalUnrealizedPnl);

        } catch (Exception e) {
            log.error("Error monitoring unrealized P&L: {}", e.getMessage());
        }
    }

    /**
     * Check risk thresholds and publish events.
     */
    private void checkRiskThresholds(double unrealizedPnl) {
        WalletEntity wallet = walletRepository.getWallet(defaultWalletId).orElse(null);
        if (wallet == null) return;

        // Calculate current loss (negative P&L = loss)
        double totalDayLoss = -wallet.getDayRealizedPnl() - unrealizedPnl;
        if (totalDayLoss < 0) totalDayLoss = 0; // Only track losses

        // Check daily loss threshold
        if (wallet.getMaxDailyLoss() > 0 && totalDayLoss > 0) {
            double lossPercent = (totalDayLoss / wallet.getMaxDailyLoss()) * 100;
            if (lossPercent >= DAILY_LOSS_WARNING_THRESHOLD) {
                publishWarning("daily_loss", RiskEvent.dailyLossWarning(
                        wallet.getWalletId(), totalDayLoss, wallet.getMaxDailyLoss(), lossPercent));
            }
        }

        // Check drawdown threshold
        double currentDrawdown = wallet.getPeakBalance() - wallet.getCurrentBalance() - unrealizedPnl;
        if (currentDrawdown < 0) currentDrawdown = 0;

        if (wallet.getMaxDrawdown() > 0 && currentDrawdown > 0) {
            double drawdownPercent = (currentDrawdown / wallet.getMaxDrawdown()) * 100;
            if (drawdownPercent >= DRAWDOWN_WARNING_THRESHOLD) {
                publishWarning("drawdown", RiskEvent.drawdownWarning(
                        wallet.getWalletId(), currentDrawdown, wallet.getMaxDrawdown(), drawdownPercent));
            }
        }
    }

    /**
     * Publish a warning with cooldown to prevent spam.
     */
    private void publishWarning(String warningKey, RiskEvent event) {
        long now = System.currentTimeMillis();
        Long lastTime = lastWarningTime.get(warningKey);

        if (lastTime == null || (now - lastTime) > WARNING_COOLDOWN_MS) {
            lastWarningTime.put(warningKey, now);
            publishRiskEvent(event);
            log.warn("RISK_WARNING type={} message={}", event.getEventType(), event.getMessage());
        }
    }

    /**
     * Publish risk event to Kafka.
     */
    public void publishRiskEvent(RiskEvent event) {
        try {
            kafkaTemplate.send(RISK_EVENTS_TOPIC, event.getWalletId(), event);
        } catch (Exception e) {
            log.error("Failed to publish risk event: {}", e.getMessage());
        }
    }

    /**
     * Get current risk status for API.
     */
    public RiskStatus getRiskStatus() {
        WalletEntity wallet = walletRepository.getWallet(defaultWalletId).orElse(null);
        if (wallet == null) {
            return RiskStatus.builder()
                    .healthy(true)
                    .message("No wallet found")
                    .build();
        }

        double totalDayLoss = -wallet.getDayPnl();
        if (totalDayLoss < 0) totalDayLoss = 0;

        double lossPercent = wallet.getMaxDailyLoss() > 0
                ? (totalDayLoss / wallet.getMaxDailyLoss()) * 100 : 0;

        double currentDrawdown = wallet.getPeakBalance() - wallet.getCurrentBalance();
        if (currentDrawdown < 0) currentDrawdown = 0;

        double drawdownPercent = wallet.getMaxDrawdown() > 0
                ? (currentDrawdown / wallet.getMaxDrawdown()) * 100 : 0;

        boolean isHealthy = !wallet.isCircuitBreakerTripped()
                && lossPercent < 90
                && drawdownPercent < 90;

        String status;
        if (wallet.isCircuitBreakerTripped()) {
            status = "HALTED";
        } else if (lossPercent >= 90 || drawdownPercent >= 90) {
            status = "CRITICAL";
        } else if (lossPercent >= 70 || drawdownPercent >= 70) {
            status = "WARNING";
        } else {
            status = "HEALTHY";
        }

        return RiskStatus.builder()
                .healthy(isHealthy)
                .status(status)
                .circuitBreakerTripped(wallet.isCircuitBreakerTripped())
                .circuitBreakerReason(wallet.getCircuitBreakerReason())
                .dailyLossPercent(lossPercent)
                .dailyLossAmount(totalDayLoss)
                .dailyLossLimit(wallet.getMaxDailyLoss())
                .drawdownPercent(drawdownPercent)
                .drawdownAmount(currentDrawdown)
                .drawdownLimit(wallet.getMaxDrawdown())
                .openPositions(countOpenPositions())
                .maxOpenPositions(wallet.getMaxOpenPositions())
                .currentBalance(wallet.getCurrentBalance())
                .availableMargin(wallet.getAvailableMargin())
                .message(wallet.isCircuitBreakerTripped()
                        ? "Trading halted: " + wallet.getCircuitBreakerReason()
                        : status + " - Risk metrics within limits")
                .build();
    }

    private int countOpenPositions() {
        return (int) positionRepository.listPositions().stream()
                .filter(p -> p.getQtyOpen() > 0)
                .count();
    }

    /**
     * Force trip circuit breaker (for manual intervention)
     */
    public void tripCircuitBreaker(String reason) {
        WalletEntity wallet = walletRepository.getWallet(defaultWalletId).orElse(null);
        if (wallet != null && !wallet.isCircuitBreakerTripped()) {
            publishRiskEvent(RiskEvent.circuitBreakerTripped(wallet.getWalletId(), reason));
        }
    }

    /**
     * Force reset circuit breaker
     */
    public void resetCircuitBreaker() {
        walletTransactionService.resetCircuitBreaker(defaultWalletId);
        publishRiskEvent(RiskEvent.circuitBreakerReset(defaultWalletId));
    }

    @lombok.Data
    @lombok.Builder
    public static class RiskStatus {
        private boolean healthy;
        private String status; // HEALTHY, WARNING, CRITICAL, HALTED
        private String message;

        private boolean circuitBreakerTripped;
        private String circuitBreakerReason;

        private double dailyLossPercent;
        private double dailyLossAmount;
        private double dailyLossLimit;

        private double drawdownPercent;
        private double drawdownAmount;
        private double drawdownLimit;

        private int openPositions;
        private int maxOpenPositions;

        private double currentBalance;
        private double availableMargin;
    }
}
