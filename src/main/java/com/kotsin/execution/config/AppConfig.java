package com.kotsin.execution.config;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.time.Duration;

@Configuration
@EnableConfigurationProperties(TradeProps.class)
public class AppConfig {

    @Bean
    public Cache<String, Boolean> processedSignalsCache(
            @Value("${trade.idempotency.max-entries:100000}") long maxEntries) {
        return Caffeine.newBuilder()
                .maximumSize(maxEntries)
                .build();
    }
}
