package com.kotsin.execution.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "trade")
public record TradeProps(
        int maxSkewSeconds // e.g., 900 (15 minutes)
) {
}
