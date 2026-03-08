package com.kotsin.execution.wallet.service;

import com.kotsin.execution.virtual.VirtualEngineService;
import com.kotsin.execution.virtual.model.VirtualOrder;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Lazy;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Queues margin-insufficient orders for up to 2 minutes.
 * When funds are added, retries queued signals automatically.
 * Publishes wallet-events to Kafka for dashboard notifications.
 */
@Service
@Slf4j
public class SignalQueueService {

    private final ConcurrentHashMap<String, CopyOnWriteArrayList<QueuedSignal>> pendingByWallet = new ConcurrentHashMap<>();

    @Autowired
    @Lazy
    private VirtualEngineService virtualEngine;

    @Autowired
    private KafkaTemplate<String, Object> kafkaTemplate;

    @Value("${strategy.wallet.queue.timeout.ms:120000}")
    private long queueTimeoutMs;

    private static final String WALLET_EVENTS_TOPIC = "wallet-events";

    /**
     * Queue a signal when margin is insufficient.
     */
    public QueuedSignal queueSignal(VirtualOrder order, double requiredMargin, String walletId, String strategyKey) {
        QueuedSignal qs = new QueuedSignal(order, requiredMargin, walletId, strategyKey, System.currentTimeMillis());
        pendingByWallet.computeIfAbsent(walletId, k -> new CopyOnWriteArrayList<>()).add(qs);

        try {
            kafkaTemplate.send(WALLET_EVENTS_TOPIC, walletId, Map.of(
                    "eventType", "MARGIN_INSUFFICIENT",
                    "walletId", walletId,
                    "strategyKey", strategyKey,
                    "scripCode", order.getScripCode() != null ? order.getScripCode() : "",
                    "instrumentSymbol", order.getInstrumentSymbol() != null ? order.getInstrumentSymbol() : "",
                    "side", order.getSide() != null ? order.getSide().name() : "",
                    "qty", order.getQty(),
                    "requiredMargin", requiredMargin,
                    "expiresAt", qs.getCreatedAt() + queueTimeoutMs
            ));
        } catch (Exception e) {
            log.error("ERR [SIGNAL-QUEUE] Failed to publish MARGIN_INSUFFICIENT event: {}", e.getMessage());
        }

        log.warn("ERR [SIGNAL-QUEUE] Queued signal walletId={} scrip={} margin={} expiresIn={}s",
                walletId, order.getScripCode(), String.format("%.2f", requiredMargin), queueTimeoutMs / 1000);
        return qs;
    }

    /**
     * Called after fund top-up. Retries all queued signals for the wallet.
     * Each re-checks margin (another signal may have consumed funds first).
     */
    public int retryQueuedSignals(String walletId) {
        CopyOnWriteArrayList<QueuedSignal> list = pendingByWallet.get(walletId);
        if (list == null || list.isEmpty()) return 0;

        List<QueuedSignal> toRetry = new ArrayList<>(list);
        list.clear();
        int retried = 0;

        for (QueuedSignal qs : toRetry) {
            try {
                VirtualOrder result = virtualEngine.createOrder(qs.getOrder());
                retried++;
                log.info("[SIGNAL-QUEUE] Retried signal walletId={} scrip={} status={}",
                        walletId, qs.getOrder().getScripCode(), result.getStatus());
            } catch (Exception e) {
                log.error("ERR [SIGNAL-QUEUE] Failed to retry signal walletId={} scrip={}: {}",
                        walletId, qs.getOrder().getScripCode(), e.getMessage());
            }
        }
        return retried;
    }

    /**
     * Expire stale signals every 10 seconds.
     */
    @Scheduled(fixedDelay = 10000)
    public void expireStaleSignals() {
        long now = System.currentTimeMillis();
        pendingByWallet.forEach((walletId, list) -> {
            list.removeIf(qs -> {
                if (now - qs.getCreatedAt() > queueTimeoutMs) {
                    try {
                        kafkaTemplate.send(WALLET_EVENTS_TOPIC, walletId, Map.of(
                                "eventType", "SIGNAL_EXPIRED",
                                "walletId", walletId,
                                "scripCode", qs.getOrder().getScripCode() != null ? qs.getOrder().getScripCode() : "",
                                "strategyKey", qs.getStrategyKey() != null ? qs.getStrategyKey() : ""
                        ));
                    } catch (Exception e) {
                        log.error("ERR [SIGNAL-QUEUE] Failed to publish SIGNAL_EXPIRED: {}", e.getMessage());
                    }
                    log.warn("ERR [SIGNAL-QUEUE] Expired walletId={} scrip={}", walletId, qs.getOrder().getScripCode());
                    return true;
                }
                return false;
            });
        });
    }

    public List<QueuedSignal> getPendingSignals(String walletId) {
        CopyOnWriteArrayList<QueuedSignal> list = pendingByWallet.get(walletId);
        return list != null ? new ArrayList<>(list) : Collections.emptyList();
    }

    public int getPendingCount(String walletId) {
        CopyOnWriteArrayList<QueuedSignal> list = pendingByWallet.get(walletId);
        return list != null ? list.size() : 0;
    }

    @Data
    @AllArgsConstructor
    public static class QueuedSignal {
        private VirtualOrder order;
        private double requiredMargin;
        private String walletId;
        private String strategyKey;
        private long createdAt;
    }
}
