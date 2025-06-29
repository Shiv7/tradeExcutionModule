package com.kotsin.execution.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Fund Diversification Service - Manages portfolio allocation for simultaneous trades
 * Ensures proper risk distribution when multiple trades are active
 */
@Service
@Slf4j
@RequiredArgsConstructor
public class FundDiversificationService {
    
    @Value("${app.portfolio.initial-capital:1000000}")
    private double totalCapital;
    
    @Value("${app.portfolio.max-single-position:10.0}")
    private double maxSinglePositionPercent;
    
    @Value("${app.portfolio.max-sector-exposure:25.0}")
    private double maxSectorExposurePercent;
    
    @Value("${app.diversification.max-simultaneous-trades:2}")
    private int maxSimultaneousTrades;
    
    @Value("${app.diversification.capital-split-percent:45.0}")
    private double capitalSplitPercent; // Each trade gets 45% when 2 trades are active
    
    // Track active trades and their allocations
    private final Map<String, TradeAllocation> activeAllocations = new ConcurrentHashMap<>();
    
    /**
     * Trade allocation information
     */
    public static class TradeAllocation {
        public String tradeId;
        public String scripCode;
        public String sector;
        public double allocatedCapital;
        public double riskAmount;
        public double positionSize;
        public boolean isActive;
        
        public TradeAllocation(String tradeId, String scripCode, String sector, 
                             double allocatedCapital, double riskAmount, double positionSize) {
            this.tradeId = tradeId;
            this.scripCode = scripCode;
            this.sector = sector;
            this.allocatedCapital = allocatedCapital;
            this.riskAmount = riskAmount;
            this.positionSize = positionSize;
            this.isActive = true;
        }
    }
    
    /**
     * Calculate position size for new trade considering diversification
     */
    public double calculateDiversifiedPositionSize(String scripCode, double entryPrice, 
                                                  double stopLoss, double riskRewardRatio) {
        int activeTrades = activeAllocations.size();
        
        log.info("💰 [Diversification] Calculating position size for {} (Active trades: {})", 
                scripCode, activeTrades);
        
        // Check if we can add more trades
        if (activeTrades >= maxSimultaneousTrades) {
            log.warn("⚠️ [Diversification] Maximum simultaneous trades ({}) reached. Cannot add {}", 
                    maxSimultaneousTrades, scripCode);
            return 0.0;
        }
        
        // Calculate available capital
        double availableCapital = calculateAvailableCapital();
        
        // Calculate capital allocation for this trade
        double allocatedCapital = calculateCapitalAllocation(activeTrades);
        
        // Calculate risk amount
        double riskPerShare = Math.abs(entryPrice - stopLoss);
        double maxRiskAmount = allocatedCapital * (maxSinglePositionPercent / 100.0);
        
        // Calculate position size based on risk
        double positionSize = maxRiskAmount / riskPerShare;
        
        // Validate position size
        if (positionSize * entryPrice > allocatedCapital) {
            positionSize = allocatedCapital / entryPrice;
            log.warn("⚠️ [Diversification] Position size limited by allocated capital for {}", scripCode);
        }
        
        log.info("💰 [Diversification] {} allocation: Capital={}, Risk={}, Position={} shares", 
                scripCode, allocatedCapital, maxRiskAmount, positionSize);
        
        return positionSize;
    }
    
    /**
     * Reserve capital for a new trade
     */
    public boolean reserveCapitalForTrade(String tradeId, String scripCode, String sector,
                                        double entryPrice, double positionSize) {
        
        double allocatedCapital = calculateCapitalAllocation(activeAllocations.size());
        double riskAmount = positionSize * Math.abs(entryPrice * 0.01); // Assume 1% risk
        
        // Check sector exposure
        if (!checkSectorExposure(sector, allocatedCapital)) {
            log.warn("⚠️ [Diversification] Sector exposure limit exceeded for {} in sector {}", 
                    scripCode, sector);
            return false;
        }
        
        TradeAllocation allocation = new TradeAllocation(
            tradeId, scripCode, sector, allocatedCapital, riskAmount, positionSize);
        
        activeAllocations.put(tradeId, allocation);
        
        log.info("✅ [Diversification] Reserved capital for {}: {} (Sector: {}, Allocation: {})", 
                scripCode, allocatedCapital, sector, getAllocationPercentage(allocatedCapital));
        
        return true;
    }
    
    /**
     * Release capital when trade is closed
     */
    public void releaseCapitalForTrade(String tradeId) {
        TradeAllocation allocation = activeAllocations.remove(tradeId);
        if (allocation != null) {
            log.info("🔓 [Diversification] Released capital for {}: {} ({})", 
                    allocation.scripCode, allocation.allocatedCapital, 
                    getAllocationPercentage(allocation.allocatedCapital));
            
            // Rebalance remaining trades if needed
            rebalanceActiveTrades();
        }
    }
    
    /**
     * Calculate capital allocation per trade based on number of active trades
     */
    private double calculateCapitalAllocation(int activeTrades) {
        if (activeTrades == 0) {
            // First trade gets full capital allocation
            return totalCapital * (maxSinglePositionPercent / 100.0);
        } else if (activeTrades == 1) {
            // Second trade - split capital between two trades
            return totalCapital * (capitalSplitPercent / 100.0);
        } else {
            // More trades - divide equally
            return totalCapital / maxSimultaneousTrades * (capitalSplitPercent / 100.0);
        }
    }
    
    /**
     * Calculate available capital for new trades
     */
    private double calculateAvailableCapital() {
        double allocatedCapital = activeAllocations.values().stream()
                .mapToDouble(allocation -> allocation.allocatedCapital)
                .sum();
        
        return totalCapital - allocatedCapital;
    }
    
    /**
     * Check if sector exposure limits are respected
     */
    private boolean checkSectorExposure(String sector, double newAllocation) {
        if (sector == null || sector.isEmpty()) {
            return true; // No sector limit check if sector is unknown
        }
        
        double currentSectorExposure = activeAllocations.values().stream()
                .filter(allocation -> sector.equals(allocation.sector))
                .mapToDouble(allocation -> allocation.allocatedCapital)
                .sum();
        
        double totalSectorExposure = currentSectorExposure + newAllocation;
        double sectorExposurePercent = (totalSectorExposure / totalCapital) * 100.0;
        
        if (sectorExposurePercent > maxSectorExposurePercent) {
            log.warn("⚠️ [Diversification] Sector {} exposure would be {}% (limit: {}%)", 
                    sector, sectorExposurePercent, maxSectorExposurePercent);
            return false;
        }
        
        return true;
    }
    
    /**
     * Rebalance active trades when one trade is closed
     */
    private void rebalanceActiveTrades() {
        int remainingTrades = activeAllocations.size();
        
        if (remainingTrades == 1) {
            // Only one trade left - can increase its allocation
            TradeAllocation remaining = activeAllocations.values().iterator().next();
            double newAllocation = totalCapital * (maxSinglePositionPercent / 100.0);
            
            log.info("📊 [Diversification] Rebalancing: {} allocation increased from {} to {}", 
                    remaining.scripCode, remaining.allocatedCapital, newAllocation);
            
            remaining.allocatedCapital = newAllocation;
        }
    }
    
    /**
     * Get diversification statistics
     */
    public Map<String, Object> getDiversificationStats() {
        double totalAllocated = activeAllocations.values().stream()
                .mapToDouble(allocation -> allocation.allocatedCapital)
                .sum();
        
        double utilizationPercent = (totalAllocated / totalCapital) * 100.0;
        
        return Map.of(
            "totalCapital", totalCapital,
            "allocatedCapital", totalAllocated,
            "availableCapital", totalCapital - totalAllocated,
            "utilizationPercent", utilizationPercent,
            "activeTrades", activeAllocations.size(),
            "maxSimultaneousTrades", maxSimultaneousTrades,
            "capitalSplitPercent", capitalSplitPercent
        );
    }
    
    /**
     * Check if new trade can be accommodated
     */
    public boolean canAccommodateNewTrade(String sector) {
        if (activeAllocations.size() >= maxSimultaneousTrades) {
            return false;
        }
        
        double plannedAllocation = calculateCapitalAllocation(activeAllocations.size());
        return checkSectorExposure(sector, plannedAllocation);
    }
    
    /**
     * Get allocation percentage string for logging
     */
    private String getAllocationPercentage(double allocation) {
        double percentage = (allocation / totalCapital) * 100.0;
        return String.format("%.1f%%", percentage);
    }
    
    /**
     * Force close all allocations (emergency use)
     */
    public void emergencyReset() {
        log.warn("🚨 [Diversification] Emergency reset - clearing all allocations");
        activeAllocations.clear();
    }
} 