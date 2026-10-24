package com.example.busquedashoteleras.infrastructure.kafka;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.example.busquedashoteleras.application.service.PublishOutboxService;
import com.example.busquedashoteleras.domain.SearchCriteria;
import com.example.busquedashoteleras.domain.SearchSubmission;
import com.example.busquedashoteleras.infrastructure.oracle.OracleAcceptanceAdapter;
import com.example.busquedashoteleras.infrastructure.oracle.OracleOutboxAdapter;
import com.example.busquedashoteleras.infrastructure.serialization.SearchEventCodec;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.jdbc.support.JdbcTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

class KafkaPublicationIT {
    private static final DriverManagerDataSource SOURCE = new DriverManagerDataSource(
            required("TEST_DB_URL"), required("TEST_DB_USER"), required("TEST_DB_PASSWORD"));
    private static final String BOOTSTRAP = required("TEST_KAFKA_BOOTSTRAP_SERVERS");
    private final JdbcTemplate jdbc = new JdbcTemplate(SOURCE);
    private final List<UUID> ids = new ArrayList<>();

    @BeforeAll
    static void migrate() {
        Flyway.configure().dataSource(SOURCE).cleanDisabled(true).load().migrate();
    }

    @BeforeEach
    void requireEmptyQueue() {
        Integer count = jdbc.queryForObject("SELECT COUNT(*) FROM search_outbox WHERE state = 'READY'", Integer.class);
        if (!Integer.valueOf(0).equals(count)) {
            throw new IllegalStateException("El esquema de pruebas contiene trabajo previo");
        }
    }

    @AfterEach
    void removeOwnRows() {
        new TransactionTemplate(new JdbcTransactionManager(SOURCE)).executeWithoutResult(status -> {
            for (UUID id : ids) {
                jdbc.update("DELETE FROM search_outbox WHERE search_id = HEXTORAW(?)", hex(id));
                jdbc.update("DELETE FROM search_request WHERE search_id = HEXTORAW(?)", hex(id));
            }
        });
    }

    @Test
    void publishesOriginalEventAndMarksSentAfterBrokerConfirmation() {
        SearchSubmission submission = accept("hotel-ñ");
        String expectedJson = new String(new SearchEventCodec().encode(submission), StandardCharsets.UTF_8);
        try (var producer = new KafkaProducer<String, String>(KafkaProducerSettings.properties(BOOTSTRAP));
                var consumer = consumer()) {
            int sent = new PublishOutboxService(new OracleOutboxAdapter(SOURCE), new KafkaSearchPublisher(producer)).publishBatch(100);
            ConsumerRecord<String, String> received = awaitRecord(consumer, submission.searchId());
            assertAll(
                    () -> assertEquals(1, sent, "Debe registrar la confirmacion"),
                    () -> assertEquals("hotel_availability_searches", received.topic(), "Topic del ejercicio"),
                    () -> assertEquals(submission.searchId().toString(), received.key(), "Key estable por solicitud"),
                    () -> assertEquals(expectedJson, received.value(), "Debe conservar el JSON original"),
                    () -> assertEquals("SENT/PENDING", states(submission.searchId()), "Publicar no es procesar"));
        }
    }

    @Test
    void publishesEventAtAcceptanceSizeLimit() {
        UUID id = UUID.randomUUID();
        SearchSubmission small = submission(id, "x");
        int padding = 262_144 - new SearchEventCodec().encode(small).length;
        SearchSubmission large = submission(id, "x".repeat(padding + 1));
        ids.add(id);
        new OracleAcceptanceAdapter(SOURCE).accept(large);
        try (var producer = new KafkaProducer<String, String>(KafkaProducerSettings.properties(BOOTSTRAP));
                var consumer = consumer()) {
            int sent = new PublishOutboxService(new OracleOutboxAdapter(SOURCE), new KafkaSearchPublisher(producer)).publishBatch(1);
            ConsumerRecord<String, String> received = awaitRecord(consumer, id);
            assertAll(
                    () -> assertEquals(1, sent, "El limite aceptado debe poder publicarse"),
                    () -> assertEquals(262_144, received.value().getBytes(StandardCharsets.UTF_8).length, "Tamano exacto"),
                    () -> assertEquals(large, new SearchEventCodec().decode(received.value()), "Sin truncamiento"));
        }
    }

    @Test
    void unavailableBrokerKeepsOutboxRecoverableAndLaterPublicationSucceeds() {
        SearchSubmission submission = accept("recoverable");
        int first;
        try (var unavailable = new KafkaProducer<String, String>(KafkaProducerSettings.properties("127.0.0.1:1"))) {
            first = new PublishOutboxService(new OracleOutboxAdapter(SOURCE), new KafkaSearchPublisher(unavailable)).publishBatch(100);
        }
        String pending = states(submission.searchId());
        Integer attempts = jdbc.queryForObject("SELECT retry_attempts FROM search_outbox WHERE search_id = HEXTORAW(?)",
                Integer.class, hex(submission.searchId()));
        jdbc.update("UPDATE search_outbox SET available_at = SYSTIMESTAMP - INTERVAL '1' SECOND WHERE search_id = HEXTORAW(?)",
                hex(submission.searchId()));
        try (var producer = new KafkaProducer<String, String>(KafkaProducerSettings.properties(BOOTSTRAP));
                var consumer = consumer()) {
            int recovered = new PublishOutboxService(new OracleOutboxAdapter(SOURCE), new KafkaSearchPublisher(producer)).publishBatch(100);
            ConsumerRecord<String, String> received = awaitRecord(consumer, submission.searchId());
            assertAll(
                    () -> assertEquals(0, first, "La indisponibilidad no confirma envios"),
                    () -> assertEquals("READY/PENDING", pending, "Debe conservar el trabajo"),
                    () -> assertEquals(1, attempts, "Debe registrar el reintento"),
                    () -> assertEquals(1, recovered, "Debe recuperarse con el broker disponible"),
                    () -> assertEquals(submission.searchId().toString(), received.key(), "Debe mantener la identidad"),
                    () -> assertEquals("SENT/PENDING", states(submission.searchId()), "Solo cambia la publicacion"));
        }
    }

    private SearchSubmission accept(String hotel) {
        SearchSubmission submission = submission(UUID.randomUUID(), hotel);
        ids.add(submission.searchId());
        new OracleAcceptanceAdapter(SOURCE).accept(submission);
        return submission;
    }

    private static SearchSubmission submission(UUID id, String hotel) {
        return new SearchSubmission(id, new SearchCriteria(hotel, LocalDate.of(2026, 12, 1),
                LocalDate.of(2026, 12, 3), List.of(30, 1)));
    }

    private static KafkaConsumer<String, String> consumer() {
        return new KafkaConsumer<>(Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP,
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false,
                ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, 10_000));
    }

    private static ConsumerRecord<String, String> awaitRecord(KafkaConsumer<String, String> consumer, UUID id) {
        List<TopicPartition> partitions = consumer.partitionsFor(KafkaSearchPublisher.TOPIC).stream()
                .map(partition -> new TopicPartition(partition.topic(), partition.partition())).toList();
        consumer.assign(partitions);
        consumer.seekToBeginning(partitions);
        long deadline = System.nanoTime() + Duration.ofSeconds(15).toNanos();
        while (System.nanoTime() < deadline) {
            for (ConsumerRecord<String, String> record : consumer.poll(Duration.ofMillis(250))) {
                if (id.toString().equals(record.key())) {
                    return record;
                }
            }
        }
        throw new AssertionError("El evento confirmado no llego al topic dentro del plazo");
    }

    private String states(UUID id) {
        return jdbc.queryForObject("SELECT o.state || '/' || r.status FROM search_outbox o"
                + " JOIN search_request r ON r.search_id = o.search_id WHERE o.search_id = HEXTORAW(?)",
                String.class, hex(id));
    }

    private static String hex(UUID id) {
        return id.toString().replace("-", "").toUpperCase(java.util.Locale.ROOT);
    }

    private static String required(String key) {
        return Objects.requireNonNull(System.getenv(key), "Missing integration environment: " + key);
    }
}
