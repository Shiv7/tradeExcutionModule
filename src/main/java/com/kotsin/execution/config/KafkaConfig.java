package com.kotsin.execution.config;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.support.serializer.JsonDeserializer;

import java.util.HashMap;
import java.util.Map;

/**
 * 🛡️ BULLETPROOF Kafka Configuration with Centralized Consumer Groups
 * Uses application.properties for all consumer group IDs
 */
@Configuration
@EnableKafka
public class KafkaConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    // 🎯 CENTRALIZED CONSUMER GROUP CONFIGURATION
    @Value("${app.kafka.consumer.default-group-id}")
    private String defaultGroupId;
    
    @Value("${app.kafka.consumer.bulletproof-signal-group-id}")
    private String bulletproofSignalGroupId;
    
    @Value("${app.kafka.consumer.market-data-group-id}")
    private String marketDataGroupId;

    @Bean
    public ConsumerFactory<String, Object> consumerFactory() {
        Map<String, Object> configProps = new HashMap<>();
        configProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        configProps.put(ConsumerConfig.GROUP_ID_CONFIG, defaultGroupId);
        configProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        
        // 🔧 FIXED: Use JsonDeserializer for proper JSON to POJO conversion
        configProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
        
        // 🔧 FIXED: Configure JsonDeserializer to trust all packages and use HashMap for signals
        configProps.put(JsonDeserializer.TRUSTED_PACKAGES, "*");
        configProps.put(JsonDeserializer.VALUE_DEFAULT_TYPE, "java.util.HashMap");
        
        configProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        configProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        configProps.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 10);
        configProps.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 30000);
        configProps.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 10000);
        
        return new DefaultKafkaConsumerFactory<>(configProps);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, Object> kafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, Object> factory = 
            new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory());
        
        // 🔧 CRITICAL FIX: Enable manual acknowledgment mode
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
        
        return factory;
    }
    
    /**
     * 🎯 BULLETPROOF SIGNAL CONSUMER FACTORY
     * Uses centralized group ID from application.properties
     */
    @Bean("strategySignalConsumerFactory")
    public ConsumerFactory<String, com.kotsin.execution.model.StrategySignal> strategySignalConsumerFactory() {
        Map<String, Object> configProps = new HashMap<>();
        configProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        configProps.put(ConsumerConfig.GROUP_ID_CONFIG, bulletproofSignalGroupId); // 🎯 From properties
        configProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        configProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
        
        // 🔧 FIXED: Configure JsonDeserializer specifically for StrategySignal
        configProps.put(JsonDeserializer.TRUSTED_PACKAGES, "*");
        configProps.put(JsonDeserializer.VALUE_DEFAULT_TYPE, "com.kotsin.execution.model.StrategySignal");
        configProps.put(JsonDeserializer.USE_TYPE_INFO_HEADERS, false);
        
        configProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        configProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        configProps.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 10);
        configProps.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 30000);
        configProps.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 10000);
        
        return new DefaultKafkaConsumerFactory<>(configProps);
    }

    @Bean("strategySignalKafkaListenerContainerFactory")
    public ConcurrentKafkaListenerContainerFactory<String, com.kotsin.execution.model.StrategySignal> strategySignalKafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, com.kotsin.execution.model.StrategySignal> factory = 
            new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(strategySignalConsumerFactory());
        
        // 🔧 CRITICAL FIX: Enable manual acknowledgment mode
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
        
        return factory;
    }

    /**
     * 🎯 MARKET DATA CONSUMER FACTORY
     * Uses centralized group ID from application.properties
     */
    @Bean("marketDataConsumerFactory")
    public ConsumerFactory<String, com.kotsin.execution.model.MarketData> marketDataConsumerFactory() {
        Map<String, Object> configProps = new HashMap<>();
        configProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        configProps.put(ConsumerConfig.GROUP_ID_CONFIG, marketDataGroupId); // 🎯 From properties
        configProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        configProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
        
        // 🔧 FIXED: Configure JsonDeserializer specifically for MarketData
        configProps.put(JsonDeserializer.TRUSTED_PACKAGES, "*");
        configProps.put(JsonDeserializer.VALUE_DEFAULT_TYPE, "com.kotsin.execution.model.MarketData");
        configProps.put(JsonDeserializer.USE_TYPE_INFO_HEADERS, false);
        
        configProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        configProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        configProps.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 50);
        configProps.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 30000);
        configProps.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 10000);
        
        return new DefaultKafkaConsumerFactory<>(configProps);
    }

    @Bean("marketDataKafkaListenerContainerFactory")
    public ConcurrentKafkaListenerContainerFactory<String, com.kotsin.execution.model.MarketData> marketDataKafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, com.kotsin.execution.model.MarketData> factory = 
            new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(marketDataConsumerFactory());
        
        // 🔧 CRITICAL FIX: Enable manual acknowledgment mode
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
        
        return factory;
    }
} 