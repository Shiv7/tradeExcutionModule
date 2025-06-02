package com.kotsin.execution;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.EnableKafka;

/**
 * Main Spring Boot application for the Real-Time Trade Execution Module.
 * 
 * This module consumes strategy signals, monitors live market data,
 * and publishes trade results with profit/loss to Kafka topics.
 */
@SpringBootApplication
@EnableKafka
public class TradeExecutionApplication {
    
    public static void main(String[] args) {
        SpringApplication.run(TradeExecutionApplication.class, args);
        System.out.println("🚀 Kotsin Trade Execution Module Started Successfully!");
        System.out.println("📊 Listening for strategy signals and market data...");
    }
} 