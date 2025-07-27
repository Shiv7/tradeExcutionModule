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
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.util.backoff.FixedBackOff;

import java.util.HashMap;
import java.util.Map;

/**
 * 🛡️ BULLETPROOF Kafka Configuration with ROBUST Error Handling
 * Uses ErrorHandlingDeserializer to gracefully handle malformed JSON messages
 */
@Configuration
@EnableKafka
public class KafkaConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    // 🎯 ONLY THE 2 CONSUMER GROUP IDs WE ACTUALLY USE
    @Value("${app.kafka.consumer.bulletproof-signal-group-id}")
    private String bulletproofSignalGroupId;
    
    @Value("${app.kafka.consumer.market-data-group-id}")
    private String marketDataGroupId;

    @Value("${app.kafka.consumer.candlestick-group-id}")
    private String candlestickGroupId;

    
    /**
     * 🛡️ BULLETPROOF SIGNAL CONSUMER FACTORY with ERROR HANDLING
     * Uses ErrorHandlingDeserializer to gracefully handle malformed JSON
     */
    @Bean("strategySignalConsumerFactory")
    public ConsumerFactory<String, com.kotsin.execution.model.StrategySignal> strategySignalConsumerFactory() {
        Map<String, Object> configProps = new HashMap<>();
        configProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        configProps.put(ConsumerConfig.GROUP_ID_CONFIG, bulletproofSignalGroupId);
        
        // 🛡️ BULLETPROOF: Use ErrorHandlingDeserializer for keys and values
        configProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
        configProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
        
        // Configure the actual deserializers that ErrorHandlingDeserializer will use
        configProps.put(ErrorHandlingDeserializer.KEY_DESERIALIZER_CLASS, StringDeserializer.class);
        configProps.put(ErrorHandlingDeserializer.VALUE_DESERIALIZER_CLASS, JsonDeserializer.class);
        
        // 🔧 Configure JsonDeserializer specifically for StrategySignal
        configProps.put(JsonDeserializer.TRUSTED_PACKAGES, "*");
        configProps.put(JsonDeserializer.VALUE_DEFAULT_TYPE, "com.kotsin.execution.model.StrategySignal");
        configProps.put(JsonDeserializer.USE_TYPE_INFO_HEADERS, false);
        
        // 🛡️ BULLETPROOF: Configure error handling behavior
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
        
        // 🔧 Enable manual acknowledgment mode
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
        
        // 🛡️ BULLETPROOF: Configure error handler for deserialization failures
        DefaultErrorHandler errorHandler = new DefaultErrorHandler(new FixedBackOff(1000L, 3L));
        errorHandler.addNotRetryableExceptions(
            org.springframework.kafka.support.serializer.DeserializationException.class,
            org.apache.kafka.common.errors.SerializationException.class
        );
        factory.setCommonErrorHandler(errorHandler);
        
        return factory;
    }

    /**
     * 🛡️ BULLETPROOF MARKET DATA CONSUMER FACTORY with ERROR HANDLING
     * Uses ErrorHandlingDeserializer to gracefully handle malformed JSON
     */
    @Bean("marketDataConsumerFactory")
    public ConsumerFactory<String, com.kotsin.execution.model.MarketData> marketDataConsumerFactory() {
        Map<String, Object> configProps = new HashMap<>();
        configProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        configProps.put(ConsumerConfig.GROUP_ID_CONFIG, marketDataGroupId);
        
        // 🛡️ BULLETPROOF: Use ErrorHandlingDeserializer for keys and values
        configProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
        configProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
        
        // Configure the actual deserializers that ErrorHandlingDeserializer will use
        configProps.put(ErrorHandlingDeserializer.KEY_DESERIALIZER_CLASS, StringDeserializer.class);
        configProps.put(ErrorHandlingDeserializer.VALUE_DESERIALIZER_CLASS, JsonDeserializer.class);
        
        // 🔧 Configure JsonDeserializer specifically for MarketData
        configProps.put(JsonDeserializer.TRUSTED_PACKAGES, "*");
        configProps.put(JsonDeserializer.VALUE_DEFAULT_TYPE, "com.kotsin.execution.model.MarketData");
        configProps.put(JsonDeserializer.USE_TYPE_INFO_HEADERS, false);
        
        // 🛡️ BULLETPROOF: Configure error handling behavior
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
        
        // 🔧 Enable manual acknowledgment mode
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
        
        // 🛡️ BULLETPROOF: Configure error handler for deserialization failures
        DefaultErrorHandler errorHandler = new DefaultErrorHandler(new FixedBackOff(1000L, 3L));
        errorHandler.addNotRetryableExceptions(
            org.springframework.kafka.support.serializer.DeserializationException.class,
            org.apache.kafka.common.errors.SerializationException.class
        );
        factory.setCommonErrorHandler(errorHandler);
        
        return factory;
    }

    @Bean("candlestickConsumerFactory")
    public ConsumerFactory<String, com.kotsin.execution.model.Candlestick> candlestickConsumerFactory() {
        Map<String, Object> configProps = new HashMap<>();
        configProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        configProps.put(ConsumerConfig.GROUP_ID_CONFIG, candlestickGroupId);
        configProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
        configProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
        configProps.put(ErrorHandlingDeserializer.KEY_DESERIALIZER_CLASS, StringDeserializer.class);
        configProps.put(ErrorHandlingDeserializer.VALUE_DESERIALIZER_CLASS, JsonDeserializer.class);
        configProps.put(JsonDeserializer.TRUSTED_PACKAGES, "*");
        configProps.put(JsonDeserializer.VALUE_DEFAULT_TYPE, "com.kotsin.execution.model.Candlestick");
        configProps.put(JsonDeserializer.USE_TYPE_INFO_HEADERS, false);
        configProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        configProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        return new DefaultKafkaConsumerFactory<>(configProps);
    }

    @Bean("candlestickKafkaListenerContainerFactory")
    public ConcurrentKafkaListenerContainerFactory<String, com.kotsin.execution.model.Candlestick> candlestickKafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, com.kotsin.execution.model.Candlestick> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(candlestickConsumerFactory());
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
        DefaultErrorHandler errorHandler = new DefaultErrorHandler(new FixedBackOff(1000L, 3L));
        errorHandler.addNotRetryableExceptions(
                org.springframework.kafka.support.serializer.DeserializationException.class,
                org.apache.kafka.common.errors.SerializationException.class
        );
        factory.setCommonErrorHandler(errorHandler);
        return factory;
    }
}
