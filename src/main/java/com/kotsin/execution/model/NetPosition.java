package com.kotsin.execution.model;

/**
 * Lightweight DTO representing a row from 5Paisa NetPositionNetWise API.
 */
public record NetPosition(String scripCode,
                          String exch,
                          String exchType,
                          long netQty,
                          double buyAvgRate,
                          double sellAvgRate,
                          double mtm) {
} 