package com.example.busquedashoteleras.infrastructure.kafka;

import java.util.Map;
import java.util.Objects;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

public final class KafkaProducerSettings {
    private KafkaProducerSettings() { }

    public static Map<String, Object> properties(String bootstrapServers) {
        Objects.requireNonNull(bootstrapServers, "bootstrapServers must not be null");
        if (bootstrapServers.isBlank()) {
            throw new IllegalArgumentException("bootstrapServers must not be blank");
        }
        return Map.ofEntries(
                Map.entry(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers),
                Map.entry(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class),
                Map.entry(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class),
                Map.entry(ProducerConfig.ACKS_CONFIG, "all"),
                Map.entry(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true),
                Map.entry(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 10_000),
                Map.entry(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 5_000),
                Map.entry(ProducerConfig.MAX_BLOCK_MS_CONFIG, 5_000),
                Map.entry(ProducerConfig.LINGER_MS_CONFIG, 0));
    }
}
