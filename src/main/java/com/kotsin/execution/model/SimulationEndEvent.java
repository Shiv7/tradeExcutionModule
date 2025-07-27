package com.kotsin.execution.model;

import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public class SimulationEndEvent {
    private final Candlestick lastCandle;
}
