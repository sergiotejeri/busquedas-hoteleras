package com.example.busquedashoteleras.infrastructure.config;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import com.example.busquedashoteleras.application.port.ProcessingResult;
import com.example.busquedashoteleras.infrastructure.kafka.KafkaSearchPublisher;
import com.example.busquedashoteleras.infrastructure.oracle.OracleFailureAdapter;
import com.example.busquedashoteleras.infrastructure.oracle.OracleProcessingAdapter;
import java.time.Duration;
import javax.sql.DataSource;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.listener.ContainerProperties;

class ConsumptionConfigurationTest {
    @Test
    void wiresDurableAdaptersAndManualConsumerWithBoundedConcurrency() {
        var configuration = new ConsumptionConfiguration();
        var source = mock(DataSource.class);
        try (var executor = configuration.persistenceExecutor(search -> ProcessingResult.PROCESSED)) {
            var listener = configuration.searchConsumer(executor, configuration.failurePort(source));
            var container = configuration.searchListener(listener, "127.0.0.1:9092", "test-group", 3);
            var properties = container.getContainerProperties();
            var settings = ConsumptionConfiguration.consumerSettings("127.0.0.1:9092", "test-group");
            assertAll(
                    () -> assertInstanceOf(OracleProcessingAdapter.class, configuration.processingPort(source), "Persistencia real"),
                    () -> assertInstanceOf(OracleFailureAdapter.class, configuration.failurePort(source), "Cuarentena real"),
                    () -> assertEquals(KafkaSearchPublisher.TOPIC, properties.getTopics()[0], "Topic del productor"),
                    () -> assertEquals(ContainerProperties.AckMode.MANUAL_IMMEDIATE, properties.getAckMode(), "Ack explicito"),
                    () -> assertTrue(properties.isSyncCommits(), "Espera la confirmacion de offsets"),
                    () -> assertEquals(Duration.ofSeconds(10), properties.getSyncCommitTimeout(), "Espera acotada"),
                    () -> assertEquals(3, container.getConcurrency(), "Concurrencia acotada"),
                    () -> assertEquals(false, settings.get(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG), "Sin confirmacion automatica"),
                    () -> assertEquals(1, settings.get(ConsumerConfig.MAX_POLL_RECORDS_CONFIG), "Una tarea por consumidor"),
                    () -> assertFalse(container.getCommonErrorHandler().isAckAfterHandle(), "Un error no confirma consumo"));
        }
    }
}
