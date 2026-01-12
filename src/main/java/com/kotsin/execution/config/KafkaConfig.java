package com.kotsin.execution.config;

import com.kotsin.execution.paper.model.PaperTradeOutcome;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.*;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;
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
        configProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
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

    // ========== PAPER TRADE OUTCOME PRODUCER ==========
    
    @Bean
    public ProducerFactory<String, PaperTradeOutcome> outcomeProducerFactory() {
        Map<String, Object> configProps = new HashMap<>();
        configProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        configProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        configProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
        configProps.put(ProducerConfig.ACKS_CONFIG, "all");
        configProps.put(ProducerConfig.RETRIES_CONFIG, 3);
        configProps.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        return new DefaultKafkaProducerFactory<>(configProps);
    }

    @Bean
    public KafkaTemplate<String, PaperTradeOutcome> outcomeKafkaTemplate() {
        return new KafkaTemplate<>(outcomeProducerFactory());
    }

    // ========== CURATED SIGNAL CONSUMER (String payload for flexible parsing) ==========

    /**
     * FIX: Consumer group ID for QuantSignalConsumer.
     * Previously hardcoded as "paper-trade-executor" which ignored the @KafkaListener groupId.
     * Now using a configurable property with sensible default.
     */
    @Value("${quant.signals.group-id:quant-signal-executor-v2}")
    private String quantSignalGroupId;

    @Bean("curatedSignalConsumerFactory")
    public ConsumerFactory<String, String> curatedSignalConsumerFactory() {
        Map<String, Object> configProps = new HashMap<>();
        configProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

        // FIX: Use configurable group ID instead of hardcoded value
        // Previously: configProps.put(ConsumerConfig.GROUP_ID_CONFIG, "paper-trade-executor");
        // This was ignoring the @KafkaListener groupId annotation in QuantSignalConsumer
        configProps.put(ConsumerConfig.GROUP_ID_CONFIG, quantSignalGroupId);

        configProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        configProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        // FIX: Changed from "latest" to "earliest" to process signals published before consumer started
        // Previously: configProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        // This was causing all historical signals to be permanently skipped on first consumer start
        configProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // FIX: Disable auto-commit for manual acknowledgment
        // Previously: configProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
        // Auto-commit can cause message loss if processing fails after commit
        configProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        // Additional robustness settings
        configProps.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 10);
        configProps.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 30000);
        configProps.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 10000);

        return new DefaultKafkaConsumerFactory<>(configProps);
    }

    @Bean("curatedSignalKafkaListenerContainerFactory")
    public ConcurrentKafkaListenerContainerFactory<String, String> curatedSignalKafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, String> factory =
            new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(curatedSignalConsumerFactory());

        // FIX: Enable manual acknowledgment mode (consistent with other factories)
        // This ensures messages are only committed after successful processing
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);

        // FIX: Add error handler for robustness
        DefaultErrorHandler errorHandler = new DefaultErrorHandler(new FixedBackOff(1000L, 3L));
        errorHandler.addNotRetryableExceptions(
            org.springframework.kafka.support.serializer.DeserializationException.class,
            org.apache.kafka.common.errors.SerializationException.class
        );
        factory.setCommonErrorHandler(errorHandler);

        return factory;
    }
    
    // ========== GENERIC KAFKA TEMPLATE (for TradeResultProducer) ==========
    
    @Bean
    public ProducerFactory<String, Object> genericProducerFactory() {
        Map<String, Object> configProps = new HashMap<>();
        configProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        configProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        configProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
        configProps.put(ProducerConfig.ACKS_CONFIG, "all");
        configProps.put(ProducerConfig.RETRIES_CONFIG, 3);
        configProps.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        return new DefaultKafkaProducerFactory<>(configProps);
    }

    @Bean
    public KafkaTemplate<String, Object> kafkaTemplate() {
        return new KafkaTemplate<>(genericProducerFactory());
    }
    
    // ========== STRING KAFKA TEMPLATE (for ProfitLossProducer) ==========
    
    @Bean
    public ProducerFactory<String, String> stringProducerFactory() {
        Map<String, Object> configProps = new HashMap<>();
        configProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        configProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        configProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        configProps.put(ProducerConfig.ACKS_CONFIG, "all");
        configProps.put(ProducerConfig.RETRIES_CONFIG, 3);
        configProps.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        return new DefaultKafkaProducerFactory<>(configProps);
    }

    @Bean
    public KafkaTemplate<String, String> stringKafkaTemplate() {
        return new KafkaTemplate<>(stringProducerFactory());
    }
}
