package com.example.busquedashoteleras.infrastructure.kafka;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullSource;
import org.junit.jupiter.params.provider.ValueSource;

class KafkaProducerSettingsTest {
    @Test
    void configuresAcknowledgedIdempotentSendsWithBoundedWaits() {
        var config = KafkaProducerSettings.properties("kafka:9092");
        assertAll(
                () -> assertEquals("all", config.get(ProducerConfig.ACKS_CONFIG), "Debe esperar a las replicas ISR"),
                () -> assertEquals(true, config.get(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG), "Idempotencia del productor"),
                () -> assertEquals(10_000, config.get(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG), "Entrega acotada"),
                () -> assertEquals(5_000, config.get(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG), "Peticion acotada"),
                () -> assertEquals(5_000, config.get(ProducerConfig.MAX_BLOCK_MS_CONFIG), "Metadatos y buffer acotados"),
                () -> assertEquals(StringSerializer.class, config.get(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG), "Clave UTF-8"),
                () -> assertEquals(StringSerializer.class, config.get(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG), "Payload UTF-8"));
    }

    @ParameterizedTest
    @ValueSource(strings = {"", "  "})
    void rejectsEmptyBootstrapServers(String servers) {
        assertThrows(IllegalArgumentException.class, () -> KafkaProducerSettings.properties(servers));
    }

    @ParameterizedTest
    @NullSource
    void requiresBootstrapServers(String servers) {
        assertThrows(NullPointerException.class, () -> KafkaProducerSettings.properties(servers));
    }
}
