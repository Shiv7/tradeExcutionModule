package com.kotsin.execution.virtual.model;

import lombok.Data;

@Data
public class VirtualSettings {
    private double feesPerOrder = 0.0;   // absolute
    private double slippageBps = 0.0;    // basis points
    private double tickSize = 0.05;      // default tick
    private double accountValue = 1000000.0; // ₹10L default
}

