package com.kotsin.execution.model;

/**
 * Centralized enums used by the execution module.
 * (Use as needed; current code still uses String flags for compatibility.)
 */
public final class TradingTypes {
    private TradingTypes() {}

    public enum Side { BUY, SELL }

    public enum SignalSide { BULLISH, BEARISH }

    public enum ExitReason { TARGET1, STOP_LOSS, END_OF_SIM }

    public enum Exchange { N, B }

    public enum ExchangeType { C, D }

    public enum TradeStatus { WAITING_FOR_ENTRY, ACTIVE, COMPLETED, FAILED }
}
