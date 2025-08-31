package com.kotsin.execution.config;

import com.kotsin.execution.service.ErrorMonitoringService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.TopicPartition;
import org.springframework.kafka.listener.ConsumerAwareListenerErrorHandler;
import org.springframework.kafka.listener.ListenerExecutionFailedException;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.support.serializer.DeserializationException;
import org.springframework.messaging.Message;
import org.springframework.stereotype.Component;

@Component
@Slf4j
@RequiredArgsConstructor
public class BulletproofErrorHandler implements ConsumerAwareListenerErrorHandler {

    private final ErrorMonitoringService errorMonitoringService;

    @Override
    public Object handleError(Message<?> message, ListenerExecutionFailedException exception, Consumer<?, ?> consumer) {

        // Extract topic/partition/offset from headers (use Spring Kafka constants)
        String topic = safeHeader(message, KafkaHeaders.RECEIVED_TOPIC);
        Integer partition = (Integer) message.getHeaders().get(KafkaHeaders.RECEIVED_PARTITION);
        Long offset = (Long) message.getHeaders().get(KafkaHeaders.OFFSET);

        Throwable root = getRootCause(exception);

        if (root instanceof DeserializationException de) {
            log.error("Deserialization error at topic={} partition={} offset={}", topic, partition, offset);
            byte[] bad = de.getData();
            if (bad != null && bad.length > 0) {
                int len = Math.min(500, bad.length);
                String snippet = new String(bad, 0, len);
                log.error("Malformed payload (first {} chars): {}", len, snippet);
            }
            errorMonitoringService.recordError("kafka-deserialization", root.getMessage(), root);
        } else {
            log.error("Listener processing error at topic={} partition={} offset={} err={}", topic, partition, offset, root.toString(), root);
            errorMonitoringService.recordError("kafka-processing", root.getMessage(), root);
        }

        // Skip the bad record by seeking to the next offset (best-effort if we have metadata)
        try {
            if (topic != null && partition != null && offset != null) {
                TopicPartition tp = new TopicPartition(topic, partition);
                consumer.seek(tp, offset + 1);
                log.warn("Skipped record via seek: {}-{}@{}", topic, partition, offset);
            }
        } catch (Exception seekEx) {
            log.warn("Failed to seek past bad record: {}", seekEx.toString(), seekEx);
        }

        // Returning null tells the container we're done handling; it will continue with the next record
        return null;
    }

    private String safeHeader(Message<?> message, String key) {
        Object v = message.getHeaders().get(key);
        return v != null ? v.toString() : null;
    }

    private Throwable getRootCause(Throwable t) {
        Throwable r = t;
        while (r.getCause() != null && r.getCause() != r) {
            r = r.getCause();
        }
        return r;
    }
}
