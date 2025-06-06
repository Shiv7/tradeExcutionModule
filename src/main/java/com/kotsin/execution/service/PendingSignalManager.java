package com.kotsin.execution.service;

import com.kotsin.execution.model.PendingSignal;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * Manages pending signals that are waiting for dynamic validation
 * with live market data from websocket
 */
@Service
@Slf4j
public class PendingSignalManager {
    
    // Store pending signals by signal ID
    private final Map<String, PendingSignal> pendingSignals = new ConcurrentHashMap<>();
    
    // Store signals by script code for quick lookup during price updates
    private final Map<String, Map<String, PendingSignal>> signalsByScript = new ConcurrentHashMap<>();
    
    /**
     * Add a new pending signal
     */
    public void addPendingSignal(PendingSignal signal) {
        pendingSignals.put(signal.getSignalId(), signal);
        
        // Index by script code for fast lookup during price updates
        signalsByScript.computeIfAbsent(signal.getScripCode(), k -> new ConcurrentHashMap<>())
                      .put(signal.getSignalId(), signal);
        
        log.info("📋 [PendingSignalManager] Added pending signal: {} (Total pending: {})", 
                signal.getSummary(), pendingSignals.size());
    }
    
    /**
     * Remove a pending signal (when validated or expired)
     */
    public void removePendingSignal(String signalId) {
        PendingSignal signal = pendingSignals.remove(signalId);
        
        if (signal != null) {
            // Remove from script index
            Map<String, PendingSignal> scriptSignals = signalsByScript.get(signal.getScripCode());
            if (scriptSignals != null) {
                scriptSignals.remove(signalId);
                
                // Clean up empty script entries
                if (scriptSignals.isEmpty()) {
                    signalsByScript.remove(signal.getScripCode());
                }
            }
            
            log.info("🗑️ [PendingSignalManager] Removed pending signal: {} (Remaining: {})", 
                    signal.getSummary(), pendingSignals.size());
        }
    }
    
    /**
     * Get all pending signals for a specific script
     */
    public Collection<PendingSignal> getPendingSignalsForScript(String scripCode) {
        Map<String, PendingSignal> scriptSignals = signalsByScript.get(scripCode);
        return scriptSignals != null ? scriptSignals.values() : java.util.Collections.emptyList();
    }
    
    /**
     * Get a specific pending signal by ID
     */
    public PendingSignal getPendingSignal(String signalId) {
        return pendingSignals.get(signalId);
    }
    
    /**
     * Get all pending signals
     */
    public Collection<PendingSignal> getAllPendingSignals() {
        return pendingSignals.values();
    }
    
    /**
     * Check if there's already a pending signal for a script+strategy combination
     */
    public boolean hasPendingSignal(String scripCode, String strategyName) {
        Collection<PendingSignal> scriptSignals = getPendingSignalsForScript(scripCode);
        
        return scriptSignals.stream()
                           .anyMatch(signal -> strategyName.equals(signal.getStrategyName()));
    }
    
    /**
     * Clean up expired signals
     */
    public int cleanupExpiredSignals() {
        LocalDateTime now = LocalDateTime.now();
        
        Collection<PendingSignal> expiredSignals = pendingSignals.values().stream()
                .filter(PendingSignal::isExpired)
                .collect(Collectors.toList());
        
        for (PendingSignal expired : expiredSignals) {
            removePendingSignal(expired.getSignalId());
            log.warn("⏰ [PendingSignalManager] Expired signal removed: {} (Age: {} minutes)", 
                    expired.getSummary(), 
                    java.time.Duration.between(expired.getSignalTime(), now).toMinutes());
        }
        
        if (!expiredSignals.isEmpty()) {
            log.info("🧹 [PendingSignalManager] Cleaned up {} expired signals", expiredSignals.size());
        }
        
        return expiredSignals.size();
    }
    
    /**
     * Get pending signals summary for monitoring
     */
    public Map<String, Object> getPendingSignalsSummary() {
        Map<String, Object> summary = new ConcurrentHashMap<>();
        
        summary.put("totalPendingSignals", pendingSignals.size());
        summary.put("uniqueScripts", signalsByScript.size());
        
        // Group by strategy
        Map<String, Long> byStrategy = pendingSignals.values().stream()
                .collect(Collectors.groupingBy(PendingSignal::getStrategyName, Collectors.counting()));
        summary.put("byStrategy", byStrategy);
        
        // Group by signal type
        Map<String, Long> byType = pendingSignals.values().stream()
                .collect(Collectors.groupingBy(PendingSignal::getSignalType, Collectors.counting()));
        summary.put("byType", byType);
        
        return summary;
    }
} 