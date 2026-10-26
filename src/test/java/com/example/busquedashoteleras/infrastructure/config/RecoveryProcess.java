package com.example.busquedashoteleras.infrastructure.config;

import com.example.busquedashoteleras.application.port.PublicationResult;
import com.example.busquedashoteleras.application.service.PublishOutboxService;
import com.example.busquedashoteleras.domain.SearchCriteria;
import com.example.busquedashoteleras.domain.SearchSubmission;
import com.example.busquedashoteleras.infrastructure.kafka.KafkaProducerSettings;
import com.example.busquedashoteleras.infrastructure.kafka.KafkaSearchPublisher;
import com.example.busquedashoteleras.infrastructure.oracle.OracleAcceptanceAdapter;
import com.example.busquedashoteleras.infrastructure.oracle.OracleOutboxAdapter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.LocalDate;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.TopicPartition;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

/** Proceso auxiliar de pruebas: el padre lo mata al alcanzar una frontera durable. */
public final class RecoveryProcess {
    enum Phase { ACCEPTED, PUBLISHED, PROCESSED }
    private static final int BATCH_SIZE = 1;
    private RecoveryProcess() { }

    public static void main(String[] args) {
        Phase phase = Phase.valueOf(args[0]);
        UUID id = UUID.fromString(args[1]);
        Path marker = Path.of(args[3]);
        String servers = required("TEST_KAFKA_BOOTSTRAP_SERVERS");
        var source = new DriverManagerDataSource(required("TEST_DB_URL"), required("TEST_DB_USER"), required("TEST_DB_PASSWORD"));
        var search = new SearchSubmission(id, new SearchCriteria(args[2], LocalDate.of(2026, 12, 1),
                LocalDate.of(2026, 12, 3), List.of(30, 1)));
        new OracleAcceptanceAdapter(source).accept(search);
        if (phase == Phase.ACCEPTED) {
            waitForCut(marker);
        }
        try (var producer = new KafkaProducer<String, String>(KafkaProducerSettings.properties(servers))) {
            var publisher = new KafkaSearchPublisher(producer);
            var relay = new PublishOutboxService(new OracleOutboxAdapter(source), (searchId, json) -> {
                PublicationResult result = publisher.publish(searchId, json);
                if (phase == Phase.PUBLISHED && result == PublicationResult.CONFIRMED) {
                    waitForCut(marker);
                }
                return result;
            });
            if (relay.publishBatch(BATCH_SIZE) != BATCH_SIZE) {
                throw new IllegalStateException("No se confirmo la publicacion de la prueba");
            }
            var configuration = new ConsumptionConfiguration();
            try (var executor = configuration.persistenceExecutor(configuration.processingPort(source));
                    var consumer = new KafkaConsumer<String, String>(ConsumptionConfiguration.consumerSettings(servers, args[4]))) {
                var listener = configuration.searchConsumer(executor, configuration.failurePort(source));
                List<TopicPartition> partitions = consumer.partitionsFor(KafkaSearchPublisher.TOPIC).stream()
                        .map(partition -> new TopicPartition(partition.topic(), partition.partition())).toList();
                consumer.assign(partitions);
                long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
                while (System.nanoTime() < deadline) {
                    for (var record : consumer.poll(Duration.ofMillis(250))) {
                        if (id.toString().equals(record.key())) {
                            listener.onMessage(record, () -> waitForCut(marker));
                        }
                    }
                }
                throw new IllegalStateException("No se recibio el evento antes del corte");
            }
        }
    }

    private static void waitForCut(Path marker) {
        try {
            Files.createFile(marker);
            if (!new CountDownLatch(1).await(120, TimeUnit.SECONDS)) {
                throw new IllegalStateException("El padre no interrumpio el proceso");
            }
        } catch (IOException exception) {
            throw new IllegalStateException("No se pudo crear el marcador del corte", exception);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Espera del corte interrumpida", interrupted);
        }
    }

    private static String required(String name) {
        return Objects.requireNonNull(System.getenv(name), name);
    }
}
