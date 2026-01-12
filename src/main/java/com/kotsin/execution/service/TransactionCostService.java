package com.kotsin.execution.service;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 💰 Transaction Cost Calculator for Indian Markets
 * 
 * Calculates all-inclusive transaction costs including:
 * - Brokerage (flat or percentage-based)
 * - STT (Securities Transaction Tax)
 * - Exchange charges
 * - GST (on brokerage + exchange fees)
 * - SEBI charges
 * - Stamp duty
 * 
 * Critical for accurate P&L calculation and strategy backtesting
 */
@Service
@Slf4j
public class TransactionCostService {
    
    // Configuration (can be externalized to properties)
    private static final double BROKERAGE_FLAT_PER_ORDER = 20.0; // ₹20 per order
    private static final double BROKERAGE_PERCENTAGE = 0.05; // 0.05% for F&O
    private static final boolean USE_FLAT_BROKERAGE = true; // true for discount brokers
    
    // STT rates (as of 2024)
    private static final double STT_EQUITY_DELIVERY = 0.1 / 100; // 0.1% both sides
    private static final double STT_EQUITY_INTRADAY = 0.025 / 100; // 0.025% sell side only
    private static final double STT_FUTURES_SELL = 0.0125 / 100; // 0.0125% sell side only
    private static final double STT_OPTIONS_SELL = 0.05 / 100; // 0.05% sell side (on premium)
    
    // Other charges
    private static final double EXCHANGE_CHARGES_NSE_FO = 0.00325 / 100; // 0.00325%
    private static final double EXCHANGE_CHARGES_NSE_EQ = 0.00345 / 100; // 0.00345%
    private static final double GST_RATE = 0.18; // 18% on brokerage + exchange fees
    private static final double SEBI_CHARGES_PER_CRORE = 10.0; // ₹10 per crore turnover
    private static final double STAMP_DUTY = 0.003 / 100; // 0.003% (buy side only)
    
    /**
     * Calculate total transaction cost for a trade
     * 
     * @param tradeType Type of trade (EQUITY_DELIVERY, EQUITY_INTRADAY, FUTURES, OPTIONS)
     * @param tradeValue Total trade value in INR
     * @param side BUY or SELL
     * @param quantity Quantity traded
     * @param price Price per unit
     * @return Complete breakdown of all costs
     */
    public TransactionCost calculateCost(TradeType tradeType, double tradeValue, TradeSide side, int quantity, double price) {
        
        log.debug("💰 Calculating transaction cost | type={} value={} side={} qty={} price={}",
                tradeType, tradeValue, side, quantity, price);
        
        // 1. Brokerage
        double brokerage = calculateBrokerage(tradeValue);
        
        // 2. STT (Securities Transaction Tax)
        double stt = calculateSTT(tradeType, tradeValue, side);
        
        // 3. Exchange charges
        double exchangeCharges = calculateExchangeCharges(tradeType, tradeValue);
        
        // 4. GST (18% on brokerage + exchange charges)
        double gst = (brokerage + exchangeCharges) * GST_RATE;
        
        // 5. SEBI charges (₹10 per crore)
        double sebiCharges = (tradeValue / 10000000.0) * SEBI_CHARGES_PER_CRORE;
        
        // 6. Stamp duty (only on buy side)
        double stampDuty = 0.0;
        if (side == TradeSide.BUY) {
            stampDuty = tradeValue * STAMP_DUTY;
        }
        
        // Total
        double total = brokerage + stt + exchangeCharges + gst + sebiCharges + stampDuty;
        
        // Breakdown map (ordered)
        Map<String, Double> breakdown = new LinkedHashMap<>();
        breakdown.put("brokerage", brokerage);
        breakdown.put("stt", stt);
        breakdown.put("exchangeCharges", exchangeCharges);
        breakdown.put("gst", gst);
        breakdown.put("sebiCharges", sebiCharges);
        breakdown.put("stampDuty", stampDuty);
        
        TransactionCost cost = TransactionCost.builder()
                .total(total)
                .brokerage(brokerage)
                .stt(stt)
                .exchangeCharges(exchangeCharges)
                .gst(gst)
                .sebiCharges(sebiCharges)
                .stampDuty(stampDuty)
                .breakdown(breakdown)
                .tradeValue(tradeValue)
                .costPercentage((total / tradeValue) * 100)
                .build();
        
        log.info("Transaction cost calculated | type={} value={} side={} | " +
                "total={} ({}%) | breakdown: brokerage={} stt={} exchange={} gst={} sebi={} stamp={}",
                tradeType, tradeValue, side,
                String.format("%.2f", total), String.format("%.3f", cost.getCostPercentage()),
                String.format("%.2f", brokerage), String.format("%.2f", stt), String.format("%.2f", exchangeCharges), String.format("%.2f", gst), String.format("%.2f", sebiCharges), String.format("%.2f", stampDuty));
        
        return cost;
    }
    
    /**
     * Calculate brokerage (flat or percentage-based)
     */
    private double calculateBrokerage(double tradeValue) {
        if (USE_FLAT_BROKERAGE) {
            return BROKERAGE_FLAT_PER_ORDER;
        } else {
            return tradeValue * (BROKERAGE_PERCENTAGE / 100);
        }
    }
    
    /**
     * Calculate STT based on trade type and side
     */
    private double calculateSTT(TradeType tradeType, double tradeValue, TradeSide side) {
        switch (tradeType) {
            case EQUITY_DELIVERY:
                // 0.1% on both buy and sell
                return tradeValue * STT_EQUITY_DELIVERY;
                
            case EQUITY_INTRADAY:
                // 0.025% only on sell side
                return side == TradeSide.SELL ? tradeValue * STT_EQUITY_INTRADAY : 0.0;
                
            case FUTURES:
                // 0.0125% only on sell side
                return side == TradeSide.SELL ? tradeValue * STT_FUTURES_SELL : 0.0;
                
            case OPTIONS:
                // 0.05% on sell side (on premium, not notional)
                return side == TradeSide.SELL ? tradeValue * STT_OPTIONS_SELL : 0.0;
                
            default:
                log.warn("⚠️ Unknown trade type for STT calculation: {}", tradeType);
                return 0.0;
        }
    }
    
    /**
     * Calculate exchange charges (NSE F&O vs Equity)
     */
    private double calculateExchangeCharges(TradeType tradeType, double tradeValue) {
        if (tradeType == TradeType.EQUITY_DELIVERY || tradeType == TradeType.EQUITY_INTRADAY) {
            return tradeValue * EXCHANGE_CHARGES_NSE_EQ;
        } else {
            return tradeValue * EXCHANGE_CHARGES_NSE_FO;
        }
    }
    
    /**
     * Calculate round-trip cost (entry + exit)
     */
    public RoundTripCost calculateRoundTripCost(TradeType tradeType, double entryValue, double exitValue, int quantity, double entryPrice, double exitPrice) {
        
        TransactionCost entryCost = calculateCost(tradeType, entryValue, TradeSide.BUY, quantity, entryPrice);
        TransactionCost exitCost = calculateCost(tradeType, exitValue, TradeSide.SELL, quantity, exitPrice);
        
        double totalCost = entryCost.getTotal() + exitCost.getTotal();
        double grossPnL = exitValue - entryValue;
        double netPnL = grossPnL - totalCost;
        double costImpactPercent = (totalCost / entryValue) * 100;
        
        log.info("Round-trip cost | entry={} exit={} | grossPnL={} costs={} netPnL={} | costImpact={}%",
                entryValue, exitValue, String.format("%.2f", grossPnL), String.format("%.2f", totalCost), String.format("%.2f", netPnL), String.format("%.3f", costImpactPercent));
        
        return RoundTripCost.builder()
                .entryCost(entryCost)
                .exitCost(exitCost)
                .totalCost(totalCost)
                .grossPnL(grossPnL)
                .netPnL(netPnL)
                .costImpactPercent(costImpactPercent)
                .build();
    }
    
    // ==================== DTOs ====================
    
    @Data
    @Builder
    @AllArgsConstructor
    public static class TransactionCost {
        private double total;
        private double brokerage;
        private double stt;
        private double exchangeCharges;
        private double gst;
        private double sebiCharges;
        private double stampDuty;
        private Map<String, Double> breakdown;
        private double tradeValue;
        private double costPercentage; // % of trade value
    }
    
    @Data
    @Builder
    @AllArgsConstructor
    public static class RoundTripCost {
        private TransactionCost entryCost;
        private TransactionCost exitCost;
        private double totalCost;
        private double grossPnL;
        private double netPnL;
        private double costImpactPercent;
    }
    
    public enum TradeType {
        EQUITY_DELIVERY,
        EQUITY_INTRADAY,
        FUTURES,
        OPTIONS
    }
    
    public enum TradeSide {
        BUY,
        SELL
    }
}
