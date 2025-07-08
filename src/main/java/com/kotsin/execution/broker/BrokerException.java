package com.kotsin.execution.broker;

/**
 * Generic wrapper for any error that occurs while communicating with the broker.
 */
public class BrokerException extends RuntimeException {

    public BrokerException(String message) {
        super(message);
    }

    public BrokerException(String message, Throwable cause) {
        super(message, cause);
    }
} 