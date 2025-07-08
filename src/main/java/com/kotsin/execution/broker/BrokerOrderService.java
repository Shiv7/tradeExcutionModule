package com.kotsin.execution.broker;

public interface BrokerOrderService {

    enum Side { BUY, SELL }

    /**
     * Places a market order and returns broker order-id.
     */
    String placeMarketOrder(String scripCode,
                            String exch,
                            String exchType,
                            Side side,
                            int quantity) throws BrokerException;

    /**
     * Places a limit order and returns broker order-id.
     */
    String placeLimitOrder(String scripCode,
                           String exch,
                           String exchType,
                           Side side,
                           int quantity,
                           double price) throws BrokerException;

    /**
     * Close an existing position with opposite side.
     */
    void squareOffPosition(String scripCode,
                           String exch,
                           String exchType,
                           Side currentSide,
                           int remainingQty) throws BrokerException;

    /**
     * Square-off all open positions held by this client.
     */
    void squareOffAll() throws BrokerException;
} 