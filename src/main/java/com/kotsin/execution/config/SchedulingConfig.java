package com.kotsin.execution.config;

import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * Configuration class to enable Spring's scheduled tasks
 * Required for the trade timeout mechanism to work
 */
@Configuration
@EnableScheduling
public class SchedulingConfig {
    // This class enables the @Scheduled annotations to work
    // No additional configuration needed for basic scheduling
} 