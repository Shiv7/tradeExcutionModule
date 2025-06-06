package com.kotsin.execution.model;

import lombok.Builder;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;

/**
 * Represents a signal waiting for dynamic validation with live market data
 * Signals remain in pending state until all kotsinBackTestBE conditions are met
 */
@Data
@Builder
@Slf4j
public class PendingSignal {
    
    private String signalId;
    private String scripCode;
    private String companyName;
    private String exchange;
    private String exchangeType;
    private String strategyName;
    private String signalType; // BULLISH/BEARISH
    
    // Signal timing
    private LocalDateTime signalTime;
    private LocalDateTime expiryTime; // When signal expires if not validated
    
    // Price levels from Strategy Module (pivot-based)
    private Double stopLoss;
    private Double target1;
    private Double target2;
    private Double target3;
    
    // Validation tracking
    private int validationAttempts;
    private LocalDateTime lastValidationAttempt;
    private String lastRejectionReason;
    
    // Signal metadata
    @Builder.Default
    private Map<String, Object> originalSignalData = new HashMap<>();
    
    @Builder.Default
    private Map<String, Object> metadata = new HashMap<>();
    
    /**
     * Check if signal has expired
     */
    public boolean isExpired() {
        return LocalDateTime.now().isAfter(expiryTime);
    }
    
    /**
     * Check if signal is bullish
     */
    public boolean isBullish() {
        return "BULLISH".equalsIgnoreCase(signalType) || "BUY".equalsIgnoreCase(signalType);
    }
    
    /**
     * Update validation attempt
     */
    public void recordValidationAttempt(String rejectionReason) {
        this.validationAttempts++;
        this.lastValidationAttempt = LocalDateTime.now();
        this.lastRejectionReason = rejectionReason;
        
        log.debug("🔄 [PendingSignal] Validation attempt #{} for signal {}: {}", 
                validationAttempts, signalId, rejectionReason);
    }
    
    /**
     * Add metadata to signal
     */
    public void addMetadata(String key, Object value) {
        this.metadata.put(key, value);
    }
    
    /**
     * Get metadata value
     */
    public Object getMetadata(String key) {
        return this.metadata.get(key);
    }
    
    /**
     * Get signal summary for logging
     */
    public String getSummary() {
        return String.format("PendingSignal{id='%s', script='%s', type='%s', strategy='%s', attempts=%d}", 
                signalId, scripCode, signalType, strategyName, validationAttempts);
    }
} 