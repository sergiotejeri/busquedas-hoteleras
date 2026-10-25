package com.example.busquedashoteleras.infrastructure.kafka;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.example.busquedashoteleras.application.service.PublishOutboxService;
import com.example.busquedashoteleras.BusquedasHotelerasApplication;
import com.example.busquedashoteleras.domain.SearchCriteria;
import com.example.busquedashoteleras.domain.SearchSubmission;
import com.example.busquedashoteleras.infrastructure.oracle.OracleAcceptanceAdapter;
import com.example.busquedashoteleras.infrastructure.oracle.OracleOutboxAdapter;
import com.example.busquedashoteleras.infrastructure.serialization.SearchEventCodec;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.URI;
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
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.TopicPartition;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.WebApplicationType;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.jdbc.support.JdbcTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import tools.jackson.databind.json.JsonMapper;

class KafkaPublicationIT {
    private static final String PUBLISHED_STATE = "SENT/PENDING";
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
    void applicationPublishesAcceptedRequestWithoutManualInvocation() throws java.io.IOException, InterruptedException {
        try (var context = application(BOOTSTRAP); var consumer = consumer()) {
            UUID accepted = postSearch(context);
            ConsumerRecord<String, String> received = awaitRecord(consumer, accepted);
            await().atMost(Duration.ofSeconds(5)).untilAsserted(() ->
                    assertEquals(PUBLISHED_STATE, states(accepted), "El scheduler debe confirmar el envio"));
            assertEquals(accepted, new SearchEventCodec().decode(received.value()).searchId(), "Identidad aceptada");
        }
    }

    @Test
    void scheduledFailureSurvivesApplicationRestartAndPublishesAutomatically() throws java.io.IOException, InterruptedException {
        UUID accepted;
        String pending;
        try (var unavailable = application("127.0.0.1:1")) {
            accepted = postSearch(unavailable);
            await().atMost(Duration.ofSeconds(15)).untilAsserted(() -> {
                Integer attempts = jdbc.queryForObject(
                        "SELECT retry_attempts FROM search_outbox WHERE search_id = HEXTORAW(?)",
                        Integer.class, hex(accepted));
                assertTrue(attempts != null && attempts > 0,
                        "El scheduler debe registrar el fallo transitorio");
            });
            pending = states(accepted);
        }
        try (var recovered = application(BOOTSTRAP); var consumer = consumer()) {
            var received = awaitRecord(consumer, accepted);
            await().atMost(Duration.ofSeconds(5)).untilAsserted(() ->
                    assertEquals(PUBLISHED_STATE, states(accepted), "Confirma tras recuperar Kafka"));
            assertAll(
                    () -> assertEquals("READY/PENDING", pending, "Conserva la solicitud durante el fallo"),
                    () -> assertEquals(accepted.toString(), received.key(), "Recupera la misma solicitud"),
                    () -> assertTrue(recovered.isActive(), "La aplicacion recuperada sigue activa"));
        }
    }

    private static ConfigurableApplicationContext application(String servers) {
        return new SpringApplicationBuilder(BusquedasHotelerasApplication.class)
                .web(WebApplicationType.SERVLET).run(
                        "--server.port=0",
                        "--app.consumer.enabled=false",
                        "--spring.datasource.url=" + required("TEST_DB_URL"),
                        "--spring.datasource.username=" + required("TEST_DB_USER"),
                        "--spring.datasource.password=" + required("TEST_DB_PASSWORD"),
                        "--app.kafka.bootstrap-servers=" + servers,
                        "--app.outbox.fixed-delay=100");
    }

    private UUID postSearch(ConfigurableApplicationContext context) throws java.io.IOException, InterruptedException {
        String body = "{\"hotelId\":\"scheduled\",\"checkIn\":\"01/12/2026\",\"checkOut\":\"03/12/2026\",\"ages\":[30,1]}";
        var uri = URI.create("http://localhost:" + context.getEnvironment().getRequiredProperty("local.server.port") + "/search");
        try (var client = HttpClient.newHttpClient()) {
            var request = HttpRequest.newBuilder(uri).timeout(Duration.ofSeconds(5))
                    .header("Content-Type", "application/json").POST(HttpRequest.BodyPublishers.ofString(body)).build();
            var response = client.send(request, HttpResponse.BodyHandlers.ofString());
            assertEquals(202, response.statusCode(), "Debe aceptar sin depender del envio a Kafka");
            UUID id = UUID.fromString(JsonMapper.builder().build()
                    .readTree(response.body()).get("searchId").asString());
            ids.add(id);
            return id;
        }
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
                    () -> assertEquals(PUBLISHED_STATE, states(submission.searchId()), "Publicar no es procesar"));
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
                    () -> assertEquals(PUBLISHED_STATE, states(submission.searchId()), "Solo cambia la publicacion"));
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
