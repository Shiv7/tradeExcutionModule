package com.kotsin.execution.service;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Transaction Cost Calculator — Zerodha Rate Card (2026)
 *
 * Covers all segments: Equity (NSE/BSE), Currency, Commodity (MCX).
 * Calculates brokerage, STT/CTT, exchange/transaction charges, GST, SEBI, stamp duty.
 */
@Service
@Slf4j
public class TransactionCostService {

    // ── Brokerage ──
    private static final double BROKERAGE_FLAT = 20.0;           // Rs 20 per order
    private static final double BROKERAGE_PCT = 0.03 / 100;      // 0.03%  (whichever is lower)

    // ── STT / CTT ──
    // Equity
    private static final double STT_EQ_DELIVERY_RATE = 0.1 / 100;       // 0.1% buy+sell
    private static final double STT_EQ_INTRADAY_SELL = 0.025 / 100;     // 0.025% sell side
    private static final double STT_EQ_FUTURES_SELL  = 0.02 / 100;      // 0.02% sell side
    private static final double STT_EQ_OPTIONS_SELL  = 0.1 / 100;       // 0.1% sell (on premium)
    // Currency — NIL
    // Commodity (CTT)
    private static final double CTT_COMM_FUTURES_SELL = 0.01 / 100;     // 0.01% sell (Non-Agri)
    private static final double CTT_COMM_OPTIONS_SELL = 0.05 / 100;     // 0.05% sell

    // ── Exchange / Transaction Charges ──
    // Equity
    private static final double TXN_NSE_EQ      = 0.00307 / 100;   // NSE equity
    private static final double TXN_BSE_EQ      = 0.00375 / 100;   // BSE equity
    private static final double TXN_NSE_FUTURES  = 0.00183 / 100;  // NSE futures
    private static final double TXN_NSE_OPTIONS  = 0.03553 / 100;  // NSE options (on premium)
    // Currency
    private static final double TXN_CUR_FUTURES_NSE = 0.00035 / 100;
    private static final double TXN_CUR_FUTURES_BSE = 0.00045 / 100;
    private static final double TXN_CUR_OPTIONS_NSE = 0.0311 / 100;
    private static final double TXN_CUR_OPTIONS_BSE = 0.001 / 100;
    // Commodity
    private static final double TXN_COMM_FUTURES_MCX = 0.0021 / 100;
    private static final double TXN_COMM_FUTURES_NSE = 0.0001 / 100;
    private static final double TXN_COMM_OPTIONS_MCX = 0.0418 / 100;
    private static final double TXN_COMM_OPTIONS_NSE = 0.001 / 100;

    // ── GST ──
    private static final double GST_RATE = 0.18;   // 18% on (brokerage + SEBI + txn)

    // ── SEBI ──
    private static final double SEBI_PER_CRORE = 10.0;  // Rs 10 per crore
    private static final double CRORE = 1_00_00_000.0;

    // ── Stamp Duty (buy side only) ──
    private static final double STAMP_EQ_DELIVERY  = 0.015 / 100;
    private static final double STAMP_EQ_INTRADAY  = 0.003 / 100;
    private static final double STAMP_EQ_FUTURES   = 0.002 / 100;
    private static final double STAMP_EQ_OPTIONS   = 0.003 / 100;
    private static final double STAMP_CUR_FUTURES  = 0.0001 / 100;
    private static final double STAMP_CUR_OPTIONS  = 0.0001 / 100;
    private static final double STAMP_COMM_FUTURES = 0.002 / 100;
    private static final double STAMP_COMM_OPTIONS = 0.003 / 100;

    // ═══════════════════════════════════════════════════════════════
    //  PUBLIC API
    // ═══════════════════════════════════════════════════════════════

    /**
     * Calculate cost for one leg (BUY or SELL) of a trade.
     */
    public TransactionCost calculateCost(TradeType tradeType, double tradeValue,
                                          TradeSide side, String exchange) {

        double brokerage = calculateBrokerage(tradeType, tradeValue);
        double stt       = calculateSTT(tradeType, tradeValue, side);
        double txnCharge = calculateTransactionCharges(tradeType, tradeValue, exchange);
        double sebi      = (tradeValue / CRORE) * SEBI_PER_CRORE;
        double gst       = (brokerage + sebi + txnCharge) * GST_RATE;
        double stamp     = (side == TradeSide.BUY) ? calculateStampDuty(tradeType, tradeValue) : 0.0;

        double total = brokerage + stt + txnCharge + gst + sebi + stamp;

        Map<String, Double> breakdown = new LinkedHashMap<>();
        breakdown.put("brokerage", brokerage);
        breakdown.put("stt", stt);
        breakdown.put("exchangeCharges", txnCharge);
        breakdown.put("gst", gst);
        breakdown.put("sebiCharges", sebi);
        breakdown.put("stampDuty", stamp);

        return TransactionCost.builder()
                .total(total)
                .brokerage(brokerage)
                .stt(stt)
                .exchangeCharges(txnCharge)
                .gst(gst)
                .sebiCharges(sebi)
                .stampDuty(stamp)
                .breakdown(breakdown)
                .tradeValue(tradeValue)
                .costPercentage(tradeValue > 0 ? (total / tradeValue) * 100 : 0)
                .build();
    }

    /**
     * Calculate round-trip (entry + exit) cost and derive net P&L.
     */
    public RoundTripCost calculateRoundTripCost(TradeType tradeType,
                                                 double entryPrice, double exitPrice,
                                                 int qty, String exchange) {

        double entryValue = entryPrice * qty;
        double exitValue  = exitPrice * qty;

        TransactionCost entryCost = calculateCost(tradeType, entryValue, TradeSide.BUY, exchange);
        TransactionCost exitCost  = calculateCost(tradeType, exitValue, TradeSide.SELL, exchange);

        double totalCost = entryCost.getTotal() + exitCost.getTotal();
        double grossPnL  = exitValue - entryValue;       // always LONG perspective; caller adjusts for SHORT
        double netPnL    = grossPnL - totalCost;
        double costPct   = entryValue > 0 ? (totalCost / entryValue) * 100 : 0;

        log.info("ROUND_TRIP_COST type={} entry={} exit={} qty={} exch={} | " +
                        "grossPnL={} charges={} netPnL={} costPct={}%",
                tradeType, entryPrice, exitPrice, qty, exchange,
                fmt(grossPnL), fmt(totalCost), fmt(netPnL), fmt(costPct));

        return RoundTripCost.builder()
                .entryCost(entryCost)
                .exitCost(exitCost)
                .totalCost(totalCost)
                .grossPnL(grossPnL)
                .netPnL(netPnL)
                .costImpactPercent(costPct)
                .build();
    }

    // ═══════════════════════════════════════════════════════════════
    //  HELPERS — resolve TradeType from position metadata
    // ═══════════════════════════════════════════════════════════════

    /**
     * Resolve TradeType from position exchange code and instrumentType.
     *
     * @param exchange       N=NSE, B=BSE, M=MCX, C=Currency (nullable, defaults to N)
     * @param instrumentType OPTION / FUTURES / null (equity intraday)
     */
    public static TradeType resolveTradeType(String exchange, String instrumentType) {
        String ex  = (exchange == null || exchange.isEmpty()) ? "N" : exchange.toUpperCase();
        String ins = (instrumentType == null) ? "" : instrumentType.toUpperCase();

        if ("M".equals(ex)) {
            // Commodity (MCX)
            if (ins.contains("OPTION")) return TradeType.COMMODITY_OPTIONS;
            return TradeType.COMMODITY_FUTURES;
        }
        if ("C".equals(ex)) {
            // Currency
            if (ins.contains("OPTION")) return TradeType.CURRENCY_OPTIONS;
            return TradeType.CURRENCY_FUTURES;
        }
        // Default: Equity (NSE/BSE)
        if (ins.contains("OPTION")) return TradeType.EQUITY_OPTIONS;
        if (ins.contains("FUTURE")) return TradeType.EQUITY_FUTURES;
        return TradeType.EQUITY_INTRADAY;
    }

    /** Normalize exchange code to NSE/BSE/MCX for rate lookup. */
    public static String normalizeExchange(String exchange) {
        if (exchange == null || exchange.isEmpty()) return "NSE";
        switch (exchange.toUpperCase()) {
            case "N": return "NSE";
            case "B": return "BSE";
            case "M": return "MCX";
            case "C": return "NSE";  // Currency on NSE by default
            default:  return "NSE";
        }
    }

    // ═══════════════════════════════════════════════════════════════
    //  PRIVATE CALCULATION METHODS
    // ═══════════════════════════════════════════════════════════════

    private double calculateBrokerage(TradeType tradeType, double tradeValue) {
        if (tradeType == TradeType.EQUITY_DELIVERY) {
            return 0.0;  // Zero brokerage on delivery
        }
        if (tradeType == TradeType.EQUITY_OPTIONS ||
            tradeType == TradeType.CURRENCY_OPTIONS ||
            tradeType == TradeType.COMMODITY_OPTIONS) {
            return BROKERAGE_FLAT;  // Flat Rs 20 for options
        }
        // Intraday / Futures / Currency Futures / Commodity Futures: lower of 0.03% or Rs 20
        return Math.min(tradeValue * BROKERAGE_PCT, BROKERAGE_FLAT);
    }

    private double calculateSTT(TradeType tradeType, double tradeValue, TradeSide side) {
        switch (tradeType) {
            case EQUITY_DELIVERY:
                return tradeValue * STT_EQ_DELIVERY_RATE;           // both sides
            case EQUITY_INTRADAY:
                return side == TradeSide.SELL ? tradeValue * STT_EQ_INTRADAY_SELL : 0.0;
            case EQUITY_FUTURES:
                return side == TradeSide.SELL ? tradeValue * STT_EQ_FUTURES_SELL : 0.0;
            case EQUITY_OPTIONS:
                return side == TradeSide.SELL ? tradeValue * STT_EQ_OPTIONS_SELL : 0.0;
            case CURRENCY_FUTURES:
            case CURRENCY_OPTIONS:
                return 0.0;  // Currency — NIL STT
            case COMMODITY_FUTURES:
                return side == TradeSide.SELL ? tradeValue * CTT_COMM_FUTURES_SELL : 0.0;
            case COMMODITY_OPTIONS:
                return side == TradeSide.SELL ? tradeValue * CTT_COMM_OPTIONS_SELL : 0.0;
            default:
                return 0.0;
        }
    }

    private double calculateTransactionCharges(TradeType tradeType, double tradeValue, String exchange) {
        String exch = (exchange == null) ? "NSE" : exchange.toUpperCase();

        switch (tradeType) {
            case EQUITY_DELIVERY:
            case EQUITY_INTRADAY:
                return tradeValue * ("BSE".equals(exch) ? TXN_BSE_EQ : TXN_NSE_EQ);
            case EQUITY_FUTURES:
                return tradeValue * TXN_NSE_FUTURES;
            case EQUITY_OPTIONS:
                return tradeValue * TXN_NSE_OPTIONS;
            case CURRENCY_FUTURES:
                return tradeValue * ("BSE".equals(exch) ? TXN_CUR_FUTURES_BSE : TXN_CUR_FUTURES_NSE);
            case CURRENCY_OPTIONS:
                return tradeValue * ("BSE".equals(exch) ? TXN_CUR_OPTIONS_BSE : TXN_CUR_OPTIONS_NSE);
            case COMMODITY_FUTURES:
                return tradeValue * ("MCX".equals(exch) ? TXN_COMM_FUTURES_MCX : TXN_COMM_FUTURES_NSE);
            case COMMODITY_OPTIONS:
                return tradeValue * ("MCX".equals(exch) ? TXN_COMM_OPTIONS_MCX : TXN_COMM_OPTIONS_NSE);
            default:
                return tradeValue * TXN_NSE_EQ;
        }
    }

    private double calculateStampDuty(TradeType tradeType, double tradeValue) {
        switch (tradeType) {
            case EQUITY_DELIVERY:   return tradeValue * STAMP_EQ_DELIVERY;
            case EQUITY_INTRADAY:   return tradeValue * STAMP_EQ_INTRADAY;
            case EQUITY_FUTURES:    return tradeValue * STAMP_EQ_FUTURES;
            case EQUITY_OPTIONS:    return tradeValue * STAMP_EQ_OPTIONS;
            case CURRENCY_FUTURES:  return tradeValue * STAMP_CUR_FUTURES;
            case CURRENCY_OPTIONS:  return tradeValue * STAMP_CUR_OPTIONS;
            case COMMODITY_FUTURES: return tradeValue * STAMP_COMM_FUTURES;
            case COMMODITY_OPTIONS: return tradeValue * STAMP_COMM_OPTIONS;
            default:                return tradeValue * STAMP_EQ_INTRADAY;
        }
    }

    private static String fmt(double v) {
        return String.format("%.2f", v);
    }

    // ═══════════════════════════════════════════════════════════════
    //  DTOs
    // ═══════════════════════════════════════════════════════════════

    @Data @Builder @AllArgsConstructor
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
        private double costPercentage;
    }

    @Data @Builder @AllArgsConstructor
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
        EQUITY_FUTURES,
        EQUITY_OPTIONS,
        CURRENCY_FUTURES,
        CURRENCY_OPTIONS,
        COMMODITY_FUTURES,
        COMMODITY_OPTIONS
    }

    public enum TradeSide {
        BUY, SELL
    }
}
