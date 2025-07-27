package com.kotsin.execution.model;

import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
public class Candlestick {
    private String companyName;
    private double open;
    private double high;
    private double low;
    private double close;
    private long volume;
    private long windowStartMillis;
    private long windowEndMillis;
}
