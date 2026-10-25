package com.example.busquedashoteleras.infrastructure.config;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.example.busquedashoteleras.BusquedasHotelerasApplication;
import com.example.busquedashoteleras.application.port.FailurePort;
import com.example.busquedashoteleras.application.port.ProcessSearchPort;
import com.example.busquedashoteleras.domain.SearchCriteria;
import com.example.busquedashoteleras.domain.SearchSubmission;
import com.example.busquedashoteleras.infrastructure.kafka.KafkaProducerSettings;
import com.example.busquedashoteleras.infrastructure.kafka.KafkaSearchPublisher;
import com.example.busquedashoteleras.infrastructure.kafka.VirtualPersistenceExecutor;
import com.example.busquedashoteleras.infrastructure.oracle.OracleAcceptanceAdapter;
import com.example.busquedashoteleras.infrastructure.serialization.SearchEventCodec;
import java.nio.charset.StandardCharsets;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import tools.jackson.databind.json.JsonMapper;

class KafkaConsumptionIT {
    private static final String TOPIC = KafkaSearchPublisher.TOPIC;
    private static final String SERVERS = required("TEST_KAFKA_BOOTSTRAP_SERVERS");
    private static final String PROCESSED = "PROCESSED";
    private static final DriverManagerDataSource SOURCE = new DriverManagerDataSource(
            required("TEST_DB_URL"), required("TEST_DB_USER"), required("TEST_DB_PASSWORD"));
    private final JdbcTemplate jdbc = new JdbcTemplate(SOURCE);
    private final ConsumptionConfiguration configuration = new ConsumptionConfiguration();
    private final String group = "consumption-" + UUID.randomUUID();
    private final String hotel = "consumer-" + UUID.randomUUID();
    private final List<SearchSubmission> searches = new ArrayList<>();
    private final List<RecordMetadata> sent = new ArrayList<>();
    private Admin admin;

    @BeforeAll
    static void migrate() {
        Flyway.configure().dataSource(SOURCE).cleanDisabled(true).load().migrate();
    }

    @BeforeEach
    void isolateConsumerGroupAtCurrentOffsets() throws InterruptedException, ExecutionException, TimeoutException {
        admin = Admin.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, SERVERS));
        Map<TopicPartition, OffsetSpec> requests = new HashMap<>();
        IntStream.range(0, 3).mapToObj(partition -> new TopicPartition(TOPIC, partition))
                .forEach(partition -> requests.put(partition, OffsetSpec.latest()));
        Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
        admin.listOffsets(requests).all().get(10, TimeUnit.SECONDS)
                .forEach((partition, offset) -> offsets.put(partition, new OffsetAndMetadata(offset.offset())));
        admin.alterConsumerGroupOffsets(group, offsets).all().get(10, TimeUnit.SECONDS);
    }

    @AfterEach
    void removeOwnedData() throws InterruptedException, ExecutionException, TimeoutException {
        try (Admin cleanupAdmin = admin) {
            for (RecordMetadata record : sent) {
                Object[] coordinates = {record.topic(), record.partition(), record.offset()};
                jdbc.update("DELETE FROM rejected_message WHERE topic_name = ? AND partition_no = ? AND offset_no = ?", coordinates);
                jdbc.update("DELETE FROM rejected_message_receipt WHERE topic_name = ? AND partition_no = ? AND offset_no = ?", coordinates);
            }
            List<byte[]> groups = jdbc.query("SELECT group_id FROM criteria_group WHERE JSON_VALUE(criteria_json, '$.hotelId') = ?",
                    (row, index) -> row.getBytes(1), hotel);
            for (SearchSubmission search : searches) {
                for (String table : List.of("processed_search", "search_outbox", "search_request")) {
                    jdbc.update("DELETE FROM " + table + " WHERE search_id = HEXTORAW(?)", hex(search));
                }
            }
            for (byte[] id : groups) {
                byte[] hash = jdbc.queryForObject("SELECT criteria_hash FROM criteria_group WHERE group_id = ?", byte[].class, id);
                jdbc.update("DELETE FROM criteria_group WHERE group_id = ?", id);
                jdbc.update("DELETE FROM criteria_group_lock b WHERE criteria_hash = ? "
                        + "AND NOT EXISTS (SELECT 1 FROM criteria_group g WHERE g.criteria_hash = b.criteria_hash)", hash);
            }
            cleanupAdmin.deleteConsumerGroups(List.of(group)).all().get(10, TimeUnit.SECONDS);
        }
    }

    @Test
    void applicationWiringProcessesDuplicatesOnce() throws InterruptedException, ExecutionException, TimeoutException {
        try (var context = application(false)) {
            SearchSubmission search = accept();
            send(search);
            RecordMetadata duplicate = send(search);
            awaitCommit(duplicate);
            assertAll(
                    () -> assertTrue(context.isActive(), "El consumidor arranca con la aplicacion"),
                    () -> assertEquals(PROCESSED, state(search), "Actualiza el estado"),
                    () -> assertEquals(1, projectionCount(search), "La reentrega no duplica la proyeccion"));
        }
    }

    @Test
    void twoEqualHttpRequestsConvergeToCountTwoThroughKafka() throws IOException, InterruptedException {
        try (var context = application(true); var client = HttpClient.newHttpClient()) {
            String base = "http://localhost:" + context.getEnvironment().getRequiredProperty("local.server.port");
            UUID first = submitHttp(client, base);
            UUID second = submitHttp(client, base);
            await().atMost(Duration.ofSeconds(20)).untilAsserted(() -> assertAll(
                    () -> assertNotEquals(first, second, "POST iguales reciben identificadores distintos"),
                    () -> checkHttpCount(client, base, first),
                    () -> checkHttpCount(client, base, second)));
        }
    }

    private ConfigurableApplicationContext application(boolean publish) {
        if (publish) {
            requireEmptyQueue();
        }
        return new SpringApplicationBuilder(BusquedasHotelerasApplication.class).web(WebApplicationType.SERVLET).run(
                "--server.port=0",
                "--spring.datasource.url=" + required("TEST_DB_URL"),
                "--spring.datasource.username=" + required("TEST_DB_USER"),
                "--spring.datasource.password=" + required("TEST_DB_PASSWORD"),
                "--app.kafka.bootstrap-servers=" + SERVERS,
                "--app.kafka.consumer-group=" + group,
                "--app.outbox.enabled=" + publish);
    }

    @Test
    void refusesPublisherStartupWhenPreviousWorkExists() {
        requireEmptyQueue();
        SearchSubmission search = accept();
        assertAll(
                () -> assertThrows(IllegalStateException.class, () -> {
                    try (var unexpected = application(true)) {
                        throw new AssertionError("Publisher should not start: " + unexpected.isActive());
                    }
                }, "Debe conservar el trabajo previo sin arrancar el publicador"),
                () -> assertEquals("READY", jdbc.queryForObject(
                        "SELECT state FROM search_outbox WHERE search_id = HEXTORAW(?)", String.class, hex(search)),
                        "No modifica ni publica trabajo previo"));
    }

    private void requireEmptyQueue() {
        Integer count = jdbc.queryForObject("SELECT COUNT(*) FROM search_outbox WHERE state = 'READY'", Integer.class);
        if (!Integer.valueOf(0).equals(count)) {
            throw new IllegalStateException("El esquema de pruebas contiene trabajo previo; no se publica automaticamente");
        }
    }

    private UUID submitHttp(HttpClient client, String base) throws IOException, InterruptedException {
        String body = "{\"hotelId\":\"" + hotel + "\",\"checkIn\":\"01/12/2026\",\"checkOut\":\"03/12/2026\",\"ages\":[30,1]}";
        var response = client.send(HttpRequest.newBuilder(URI.create(base + "/search"))
                .timeout(Duration.ofSeconds(5)).header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(body)).build(), HttpResponse.BodyHandlers.ofString());
        assertEquals(202, response.statusCode(), response.body());
        UUID id = UUID.fromString(JsonMapper.builder().build().readTree(response.body()).get("searchId").asString());
        searches.add(new SearchSubmission(id, new SearchCriteria(hotel,
                LocalDate.of(2026, 12, 1), LocalDate.of(2026, 12, 3), List.of(30, 1))));
        return id;
    }

    private void checkHttpCount(HttpClient client, String base, UUID id) throws IOException, InterruptedException {
        var response = client.send(HttpRequest.newBuilder(URI.create(base + "/count?searchId=" + id))
                .timeout(Duration.ofSeconds(5)).GET().build(), HttpResponse.BodyHandlers.ofString());
        var body = JsonMapper.builder().build().readTree(response.body());
        assertAll(
                () -> assertEquals(200, response.statusCode(), "El consumidor debe terminar el procesamiento"),
                () -> assertEquals(id.toString(), body.path("searchId").asString(), "Devuelve el ID consultado"),
                () -> assertEquals(2L, body.path("count").asLong(), "Cuenta ambas solicitudes, incluida la propia"),
                () -> assertEquals(hotel, body.at("/search/hotelId").asString(), "Conserva el hotel"),
                () -> assertEquals("[30,1]", body.at("/search/ages").toString(), "Conserva orden y edades"));
    }

    @Test
    void offsetWaitsForPersistenceCompletion() throws InterruptedException, ExecutionException, TimeoutException {
        var persisted = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        ProcessSearchPort real = configuration.processingPort(SOURCE);
        try (var running = start(search -> {
            var result = real.process(search);
            persisted.countDown();
            try {
                if (!release.await(15, TimeUnit.SECONDS)) {
                    throw new IllegalStateException("Test did not release persistence");
                }
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException(interrupted);
            }
            return result;
        }, configuration.failurePort(SOURCE))) {
            SearchSubmission search = accept();
            RecordMetadata record = send(search);
            try {
                assertAll(
                        () -> assertTrue(persisted.await(10, TimeUnit.SECONDS), "Oracle debe haber confirmado la transaccion"),
                        () -> assertEquals(PROCESSED, state(search), "Otra conexion ve el commit de Oracle"),
                        () -> assertEquals(record.offset(), committed(record), "Kafka aun no confirma el mensaje"),
                        () -> assertTrue(running.container().isRunning(), "El listener espera la tarea"));
            } finally {
                release.countDown();
            }
            awaitCommit(record);
        }
    }

    @Test
    void storageFailureKeepsOffsetAndRetriesBeyondDefaultLimit() throws InterruptedException, ExecutionException, TimeoutException {
        var unavailable = new AtomicBoolean(true);
        var attempts = new AtomicInteger();
        ProcessSearchPort real = configuration.processingPort(SOURCE);
        try (var running = start(search -> {
            attempts.incrementAndGet();
            if (unavailable.get()) {
                throw new IllegalStateException("simulated storage outage");
            }
            return real.process(search);
        }, configuration.failurePort(SOURCE))) {
            SearchSubmission search = accept();
            RecordMetadata record = send(search);
            await().atMost(Duration.ofSeconds(25)).until(() -> attempts.get() >= 12);
            long offsetDuringFailure = committed(record);
            int countDuringFailure = projectionCount(search);
            boolean runningDuringFailure = running.container().isRunning();
            unavailable.set(false);
            awaitCommit(record);
            assertAll(
                    () -> assertEquals(record.offset(), offsetDuringFailure, "No descarta tras diez fallos"),
                    () -> assertEquals(0, countDuringFailure, "No confirma una busqueda ausente"),
                    () -> assertTrue(runningDuringFailure, "El consumidor sigue disponible"),
                    () -> assertEquals(PROCESSED, state(search), "Se recupera sin reenviar manualmente"));
        }
    }

    @Test
    void quarantineFailureRetriesBeforeCommittingInvalidMessage() throws InterruptedException, ExecutionException, TimeoutException {
        var unavailable = new AtomicBoolean(true);
        var attempts = new AtomicInteger();
        FailurePort real = configuration.failurePort(SOURCE);
        try (var running = start(configuration.processingPort(SOURCE), rejection -> {
            attempts.incrementAndGet();
            if (unavailable.get()) {
                throw new IllegalStateException("simulated quarantine outage");
            }
            real.quarantine(rejection);
        })) {
            RecordMetadata record = send("invalid-key", "{");
            await().atMost(Duration.ofSeconds(10)).until(() -> attempts.get() >= 2);
            long offsetDuringFailure = committed(record);
            boolean runningDuringFailure = running.container().isRunning();
            unavailable.set(false);
            awaitCommit(record);
            assertAll(
                    () -> assertEquals(record.offset(), offsetDuringFailure, "Un rechazo sin guardar no confirma Kafka"),
                    () -> assertTrue(runningDuringFailure, "Sigue reintentando"),
                    () -> assertEquals("INVALID_EVENT", jdbc.queryForObject(
                            "SELECT reason_code FROM rejected_message WHERE topic_name = ? AND partition_no = ? AND offset_no = ?",
                            String.class, record.topic(), record.partition(), record.offset()), "El rechazo queda guardado"));
        }
    }

    private RunningConsumer start(ProcessSearchPort port, FailurePort failurePort) {
        var executor = configuration.persistenceExecutor(port);
        var listener = configuration.searchConsumer(executor, failurePort);
        var container = configuration.searchListener(listener, SERVERS, group, 3);
        container.start();
        return new RunningConsumer(container, executor);
    }

    private SearchSubmission accept() {
        var search = new SearchSubmission(UUID.randomUUID(), new SearchCriteria(hotel,
                LocalDate.of(2026, 12, 1), LocalDate.of(2026, 12, 3), List.of(30, 1)));
        searches.add(search);
        new OracleAcceptanceAdapter(SOURCE).accept(search);
        return search;
    }

    private RecordMetadata send(SearchSubmission search) throws InterruptedException, ExecutionException, TimeoutException {
        return send(search.searchId().toString(), new String(new SearchEventCodec().encode(search), StandardCharsets.UTF_8));
    }

    private RecordMetadata send(String key, String payload) throws InterruptedException, ExecutionException, TimeoutException {
        try (var producer = new KafkaProducer<String, String>(KafkaProducerSettings.properties(SERVERS))) {
            RecordMetadata record = producer.send(new ProducerRecord<>(TOPIC, 0, key, payload)).get(10, TimeUnit.SECONDS);
            sent.add(record);
            return record;
        }
    }

    private long committed(RecordMetadata record) throws InterruptedException, ExecutionException, TimeoutException {
        return admin.listConsumerGroupOffsets(group).partitionsToOffsetAndMetadata().get(10, TimeUnit.SECONDS)
                .get(new TopicPartition(record.topic(), record.partition())).offset();
    }

    private void awaitCommit(RecordMetadata record) {
        await().atMost(Duration.ofSeconds(20)).untilAsserted(() ->
                assertEquals(record.offset() + 1, committed(record), "Confirma exactamente hasta el mensaje guardado"));
    }

    private int projectionCount(SearchSubmission search) {
        return Objects.requireNonNull(jdbc.queryForObject("SELECT COUNT(*) FROM processed_search WHERE search_id = HEXTORAW(?)",
                Integer.class, hex(search)));
    }

    private String state(SearchSubmission search) {
        return jdbc.queryForObject("SELECT status FROM search_request WHERE search_id = HEXTORAW(?)", String.class, hex(search));
    }

    private static String hex(SearchSubmission search) {
        return search.searchId().toString().replace("-", "");
    }

    private static String required(String name) {
        return Objects.requireNonNull(System.getenv(name), name);
    }

    private record RunningConsumer(ConcurrentMessageListenerContainer<String, String> container,
            VirtualPersistenceExecutor executor) implements AutoCloseable {
        @Override
        public void close() {
            container.stop();
            executor.close();
        }
    }
}
