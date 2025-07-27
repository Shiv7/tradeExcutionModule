package com.kotsin.execution;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * Main Spring Boot application for the Real-Time Trade Execution Module.
 * 
 * This module consumes strategy signals, monitors live market data,
 * and publishes trade results with profit/loss to Kafka topics.
 * 
 * 🛡️ BULLETPROOF ERROR HANDLING: Malformed messages are gracefully handled and discarded
 * 📊 COMPREHENSIVE MONITORING: Error statistics and patterns are tracked for analysis
 */
@SpringBootApplication
// ComponentScan removed; rely on default scanning within this module
@EnableKafka
@EnableScheduling
@EnableCaching
public class TradeExecutionApplication {
    
    public static void main(String[] args) {
        SpringApplication.run(TradeExecutionApplication.class, args);
        System.out.println("🚀 Kotsin Trade Execution Module Started Successfully!");
        System.out.println("📊 Listening for strategy signals and market data...");
        System.out.println("🛡️ Bulletproof error handling enabled - malformed messages will be discarded gracefully");
    }
}
