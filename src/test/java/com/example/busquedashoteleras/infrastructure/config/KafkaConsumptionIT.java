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
import com.example.busquedashoteleras.application.port.PublicationResult;
import com.example.busquedashoteleras.application.service.PublishOutboxService;
import com.example.busquedashoteleras.domain.SearchCriteria;
import com.example.busquedashoteleras.domain.SearchSubmission;
import com.example.busquedashoteleras.infrastructure.kafka.KafkaProducerSettings;
import com.example.busquedashoteleras.infrastructure.kafka.KafkaSearchPublisher;
import com.example.busquedashoteleras.infrastructure.kafka.KafkaSearchConsumer;
import com.example.busquedashoteleras.infrastructure.kafka.VirtualPersistenceExecutor;
import com.example.busquedashoteleras.infrastructure.oracle.OracleAcceptanceAdapter;
import com.example.busquedashoteleras.infrastructure.oracle.OracleOutboxAdapter;
import com.example.busquedashoteleras.infrastructure.serialization.SearchEventCodec;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigResource;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.junit.jupiter.params.provider.EnumSource;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.jdbc.core.JdbcTemplate;
import com.zaxxer.hikari.HikariDataSource;
import com.example.busquedashoteleras.infrastructure.oracle.OracleTestDataSources;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import tools.jackson.databind.json.JsonMapper;

class KafkaConsumptionIT {
    private static final String TOPIC = KafkaSearchPublisher.TOPIC;
    private static final String SERVERS = required("TEST_KAFKA_BOOTSTRAP_SERVERS");
    private static final String PROCESSED = "PROCESSED";
    private static final HikariDataSource SOURCE = OracleTestDataSources.create();
    private final JdbcTemplate jdbc = new JdbcTemplate(SOURCE);
    private final ConsumptionConfiguration configuration = new ConsumptionConfiguration();
    private final String group = "consumption-" + UUID.randomUUID();
    private final String hotel = "consumer-" + UUID.randomUUID();
    private final List<SearchSubmission> searches = Collections.synchronizedList(new ArrayList<>());
    private final List<RecordMetadata> sent = new ArrayList<>();
    private Admin admin;
    @TempDir
    private Path temporaryDirectory;

    @AfterAll
    static void closeDataSource() {
        SOURCE.close();
    }

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

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void concurrentHttpRequestsKeepIndependentIdsAndExactCounts(boolean mixedGroups)
            throws InterruptedException, ExecutionException, TimeoutException {
        List<List<Integer>> criteria = mixedGroups
                ? List.of(List.of(30, 1), List.of(1, 30), List.of(30, 1, 1))
                : List.of(List.of(30, 1));
        List<Integer> totals = mixedGroups ? List.of(9, 6, 3) : List.of(18);
        try (var context = application(true); var client = HttpClient.newHttpClient()) {
            String base = "http://localhost:" + context.getEnvironment().getRequiredProperty("local.server.port");
            List<UUID> acceptedIds = submitConcurrentBatch(client, base, criteria, totals);
            assertAll(
                    () -> assertEquals(18L, acceptedIds.stream().distinct().count(), "Cada POST tiene un ID propio"),
                    () -> checkGroups(client, base, criteria, totals));
        }
    }

    private List<UUID> submitConcurrentBatch(HttpClient client, String base, List<List<Integer>> criteria,
            List<Integer> totals) throws InterruptedException, ExecutionException, TimeoutException {
        var ready = new CountDownLatch(18);
        var release = new CountDownLatch(1);
        List<Future<UUID>> pending = new ArrayList<>();
        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            try {
                for (int groupIndex = 0; groupIndex < criteria.size(); groupIndex++) {
                    List<Integer> ages = criteria.get(groupIndex);
                    for (int request = 0; request < totals.get(groupIndex); request++) {
                        pending.add(executor.submit(() -> {
                            ready.countDown();
                            assertTrue(release.await(10, TimeUnit.SECONDS), "Se libera el lote concurrente");
                            return submitHttp(client, base, ages);
                        }));
                    }
                }
                assertTrue(ready.await(10, TimeUnit.SECONDS), "Todas las peticiones estan listas antes de enviarlas");
            } finally {
                release.countDown();
            }
            List<UUID> acceptedIds = new ArrayList<>();
            for (Future<UUID> response : pending) {
                acceptedIds.add(response.get(15, TimeUnit.SECONDS));
            }
            return acceptedIds;
        }
    }

    private void checkGroups(HttpClient client, String base, List<List<Integer>> criteria, List<Integer> totals) {
        for (int groupIndex = 0; groupIndex < criteria.size(); groupIndex++) {
            List<Integer> ages = criteria.get(groupIndex);
            int total = totals.get(groupIndex);
            List<SearchSubmission> members = searches.stream().filter(search -> search.criteria().ages().equals(ages)).toList();
            assertEquals(total, members.size(), "Se aceptan todas las solicitudes del grupo");
            await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> assertAll(members.stream()
                    .map(search -> () -> checkHttpCount(client, base, search.searchId(), ages, total))));
        }
    }

    @ParameterizedTest
    @EnumSource(RecoveryProcess.Phase.class)
    void abruptProcessDeathPreservesAcceptedSearchAndDoesNotDoubleCount(RecoveryProcess.Phase phase)
            throws IOException, InterruptedException, ExecutionException, TimeoutException {
        requireEmptyQueue();
        var search = new SearchSubmission(UUID.randomUUID(), new SearchCriteria(hotel,
                LocalDate.of(2026, 12, 1), LocalDate.of(2026, 12, 3), List.of(30, 1)));
        searches.add(search);
        var offsetsBefore = admin.listConsumerGroupOffsets(group).partitionsToOffsetAndMetadata().get(10, TimeUnit.SECONDS);
        killAtBoundary(phase, search, offsetsBefore);
        try (var context = application(false);
                var producer = new KafkaProducer<String, String>(KafkaProducerSettings.properties(SERVERS))) {
            var relay = new PublishOutboxService(new OracleOutboxAdapter(SOURCE), new KafkaSearchPublisher(producer));
            awaitRecovery(relay, search);
            awaitCommit(send(search));
            assertAll(
                    () -> assertTrue(context.isActive(), "La instancia recuperada sigue activa"),
                    () -> assertEquals(1, projectionCount(search), "La recuperacion y la reentrega cuentan una sola vez"));
        }
    }

    private void checkCutBoundary(RecoveryProcess.Phase phase, SearchSubmission search,
            Map<TopicPartition, OffsetAndMetadata> offsetsBefore) {
        boolean processed = phase == RecoveryProcess.Phase.PROCESSED;
        assertAll(
                () -> assertEquals(processed ? PROCESSED : "PENDING", state(search), "El commit sobrevive al corte"),
                () -> assertEquals(processed ? 1 : 0, projectionCount(search), "Solo persiste en la frontera prevista"),
                () -> assertEquals(processed ? "SENT" : "READY", jdbc.queryForObject(
                        "SELECT state FROM search_outbox WHERE search_id = HEXTORAW(?)", String.class, hex(search)),
                        "La outbox conserva la frontera de publicacion"),
                () -> assertEquals(phase == RecoveryProcess.Phase.PUBLISHED ? 1 : 0, jdbc.queryForObject(
                        "SELECT COUNT(*) FROM search_outbox WHERE search_id = HEXTORAW(?) AND lease_token IS NOT NULL",
                        Integer.class, hex(search)), "El corte tras publicar deja la reserva sin confirmar"),
                () -> assertEquals(offsetsBefore, admin.listConsumerGroupOffsets(group)
                        .partitionsToOffsetAndMetadata().get(10, TimeUnit.SECONDS), "El hijo no confirma Kafka antes del corte"));
    }

    private void awaitRecovery(PublishOutboxService relay, SearchSubmission search) {
        await().atMost(Duration.ofSeconds(90)).pollInterval(Duration.ofMillis(500)).untilAsserted(() -> {
            relay.publishBatch(1);
            assertAll(
                    () -> assertEquals("SENT", jdbc.queryForObject(
                            "SELECT state FROM search_outbox WHERE search_id = HEXTORAW(?)", String.class, hex(search)),
                            "La reserva vencida vuelve a publicarse sin cambiar el reloj de Oracle"),
                    () -> assertEquals(PROCESSED, state(search), "La nueva instancia termina el procesamiento"));
        });
    }

    private void killAtBoundary(RecoveryProcess.Phase phase, SearchSubmission search,
            Map<TopicPartition, OffsetAndMetadata> offsetsBefore) throws IOException, InterruptedException {
        Path marker = temporaryDirectory.resolve("ready");
        Path log = temporaryDirectory.resolve("child.log");
        String classpath = System.getProperty("surefire.test.class.path", System.getProperty("java.class.path"));
        var builder = new ProcessBuilder(Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                "-cp", classpath, RecoveryProcess.class.getName(), phase.name(), search.searchId().toString(), hotel,
                marker.toString(), group).redirectErrorStream(true).redirectOutput(log.toFile());
        try (var running = new RunningProcess(builder.start())) {
            Process child = running.process();
            await().atMost(Duration.ofSeconds(40)).until(() -> Files.exists(marker) || !child.isAlive());
            assertAll(
                    () -> assertTrue(Files.exists(marker), Files.readString(log)),
                    () -> assertTrue(child.isAlive(), "El hijo espera el corte en la frontera indicada"));
            child.destroyForcibly();
            assertTrue(child.waitFor(10, TimeUnit.SECONDS), "El proceso debe terminar tras SIGKILL");
            assertEquals(137, child.exitValue(), "Corte brusco en el ejecutor Linux, sin cierre graceful");
        }
        checkCutBoundary(phase, search, offsetsBefore);
    }

    private record RunningProcess(Process process) implements AutoCloseable {
        @Override
        public void close() throws InterruptedException {
            if (process.isAlive()) {
                process.destroyForcibly();
                if (!process.waitFor(10, TimeUnit.SECONDS)) {
                    throw new IllegalStateException("No se pudo detener el proceso auxiliar: " + process.pid());
                }
            }
        }
    }

    @Test
    void expiredKafkaRecordIsRepublishedFromDurableOutbox()
            throws InterruptedException, ExecutionException, TimeoutException {
        requireEmptyQueue();
        String topic = "retention-" + UUID.randomUUID();
        var partition = new TopicPartition(topic, 0);
        admin.createTopics(List.of(new NewTopic(topic, 1, (short) 1).configs(Map.of(
                "cleanup.policy", "delete", "retention.ms", "1000", "segment.ms", "1000",
                "file.delete.delay.ms", "1000")))).all().get(10, TimeUnit.SECONDS);
        try (var producer = new KafkaProducer<String, String>(KafkaProducerSettings.properties(SERVERS));
                var consumer = new KafkaConsumer<String, String>(ConsumptionConfiguration.consumerSettings(SERVERS, group));
                var executor = configuration.persistenceExecutor(configuration.processingPort(SOURCE))) {
            SearchSubmission search = accept();
            var relay = new PublishOutboxService(new OracleOutboxAdapter(SOURCE),
                    (id, json) -> publishToTestTopic(producer, topic, id, json));
            confirmOriginalPublication(relay);
            long originalEnd = offset(partition, OffsetSpec.latest());
            // Solo este topic efimero usa retencion corta; no se altera el topic del servicio.
            await().atMost(Duration.ofMinutes(7)).pollInterval(Duration.ofSeconds(2)).until(() -> {
                producer.send(new ProducerRecord<>(topic, "roll", "roll")).get(10, TimeUnit.SECONDS);
                return offset(partition, OffsetSpec.earliest()) >= originalEnd;
            });
            admin.incrementalAlterConfigs(Map.of(new ConfigResource(ConfigResource.Type.TOPIC, topic), List.of(
                    new AlterConfigOp(new ConfigEntry("retention.ms", "86400000"), AlterConfigOp.OpType.SET))))
                    .all().get(10, TimeUnit.SECONDS);
            long beforeRecovery = offset(partition, OffsetSpec.latest());
            consumer.assign(List.of(partition));
            consumer.seek(partition, beforeRecovery);
            recoverExpiredRecord(relay, search);
            var listener = configuration.searchConsumer(executor, configuration.failurePort(SOURCE));
            consumeRecoveredTwice(consumer, listener, search);
            assertAll(
                    () -> assertEquals(PROCESSED, state(search), "El evento recuperado termina el procesamiento"),
                    () -> assertEquals(1, projectionCount(search), "Reentregar el evento recuperado no duplica el conteo"));
        } finally {
            admin.deleteTopics(List.of(topic)).all().get(10, TimeUnit.SECONDS);
        }
    }

    private static void confirmOriginalPublication(PublishOutboxService relay) {
        int first = relay.publishBatch(1);
        int immediateRetry = relay.publishBatch(1);
        assertAll(
                () -> assertEquals(1, first, "El broker confirma el evento original"),
                () -> assertEquals(0, immediateRetry, "No recupera antes de la antiguedad minima"));
    }

    private void recoverExpiredRecord(PublishOutboxService relay, SearchSubmission search) {
        assertAll(
                () -> assertEquals("PENDING", state(search), "La perdida en Kafka no elimina la aceptacion"),
                () -> assertEquals(0, projectionCount(search), "El original nunca se proceso"));
        jdbc.update("UPDATE search_outbox SET available_at = SYSTIMESTAMP - INTERVAL '10' MINUTE "
                + "WHERE search_id = HEXTORAW(?)", hex(search));
        assertEquals(1, relay.publishBatch(1), "Recupera el JSON durable despues de perder el original");
    }

    private static void consumeRecoveredTwice(KafkaConsumer<String, String> consumer,
            KafkaSearchConsumer listener, SearchSubmission search) {
        var acknowledgments = new AtomicInteger();
        var codec = new SearchEventCodec();
        await().atMost(Duration.ofSeconds(20)).until(() -> {
            for (var record : consumer.poll(Duration.ofMillis(250))) {
                assertEquals(search, codec.decode(record.value()), "Recupera el mismo ID y criterios");
                listener.onMessage(record, acknowledgments::incrementAndGet);
                listener.onMessage(record, acknowledgments::incrementAndGet);
            }
            return acknowledgments.get() == 2;
        });
    }

    private long offset(TopicPartition partition, OffsetSpec position)
            throws InterruptedException, ExecutionException, TimeoutException {
        return admin.listOffsets(Map.of(partition, position)).all().get(10, TimeUnit.SECONDS).get(partition).offset();
    }

    private static PublicationResult publishToTestTopic(KafkaProducer<String, String> producer, String topic, UUID id, String json) {
        try {
            producer.send(new ProducerRecord<>(topic, id.toString(), json)).get(10, TimeUnit.SECONDS);
            return PublicationResult.CONFIRMED;
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Publicacion de prueba interrumpida", interrupted);
        } catch (ExecutionException | TimeoutException failure) {
            throw new IllegalStateException("No se pudo publicar en el topic de prueba", failure);
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

    @ParameterizedTest
    @ValueSource(strings = {"READY", "SENT"})
    void refusesPublisherStartupWhenPreviousWorkExists(String eventState) {
        requireEmptyQueue();
        SearchSubmission search = accept();
        jdbc.update("UPDATE search_outbox SET state = ? WHERE search_id = HEXTORAW(?)", eventState, hex(search));
        assertAll(
                () -> assertThrows(IllegalStateException.class, () -> {
                    try (var unexpected = application(true)) {
                        throw new AssertionError("Publisher should not start: " + unexpected.isActive());
                    }
                }, "Debe conservar el trabajo previo sin arrancar el publicador"),
                () -> assertEquals(eventState, jdbc.queryForObject(
                        "SELECT state FROM search_outbox WHERE search_id = HEXTORAW(?)", String.class, hex(search)),
                        "No modifica ni publica trabajo previo"));
    }

    @Test
    void republishesOldPendingSearchAndProcessesItOnceThroughKafka()
            throws InterruptedException, ExecutionException, TimeoutException {
        requireEmptyQueue();
        try (var context = application(false);
                var producer = new KafkaProducer<String, String>(KafkaProducerSettings.properties(SERVERS))) {
            SearchSubmission search = accept();
            jdbc.update("UPDATE search_outbox SET state = 'SENT', available_at = SYSTIMESTAMP - INTERVAL '10' MINUTE "
                    + "WHERE search_id = HEXTORAW(?)", hex(search));
            int published = new PublishOutboxService(new OracleOutboxAdapter(SOURCE), new KafkaSearchPublisher(producer))
                    .publishBatch(1);
            assertAll(
                    () -> {
                        assertEquals(1, published, "El evento antiguo debe volver a publicarse en Kafka");
                        await().atMost(Duration.ofSeconds(20)).until(() -> PROCESSED.equals(state(search)));
                        awaitCommit(send(search));
                    },
                    () -> assertTrue(context.isActive(), "El consumidor sigue activo"),
                    () -> assertEquals(1, projectionCount(search), "La recuperacion y reentrega no duplican la busqueda"),
                    () -> assertEquals(0, new OracleOutboxAdapter(SOURCE).recoverPending(100, Duration.ofMinutes(5)),
                            "La solicitud procesada no vuelve a recuperarse"));
        }
    }

    private void requireEmptyQueue() {
        Integer count = jdbc.queryForObject("SELECT COUNT(*) FROM search_outbox o JOIN search_request r "
                + "ON r.search_id = o.search_id WHERE o.state = 'READY' OR (o.state = 'SENT' AND r.status = 'PENDING')", Integer.class);
        if (!Integer.valueOf(0).equals(count)) {
            throw new IllegalStateException("El esquema de pruebas contiene trabajo previo; no se publica automaticamente");
        }
    }

    private UUID submitHttp(HttpClient client, String base) throws IOException, InterruptedException {
        return submitHttp(client, base, List.of(30, 1));
    }

    private UUID submitHttp(HttpClient client, String base, List<Integer> ages) throws IOException, InterruptedException {
        String body = JsonMapper.builder().build().writeValueAsString(Map.of("hotelId", hotel,
                "checkIn", "01/12/2026", "checkOut", "03/12/2026", "ages", ages));
        var response = client.send(HttpRequest.newBuilder(URI.create(base + "/search"))
                .timeout(Duration.ofSeconds(5)).header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(body)).build(), HttpResponse.BodyHandlers.ofString());
        assertEquals(202, response.statusCode(), response.body());
        UUID id = UUID.fromString(JsonMapper.builder().build().readTree(response.body()).get("searchId").asString());
        searches.add(new SearchSubmission(id, new SearchCriteria(hotel,
                LocalDate.of(2026, 12, 1), LocalDate.of(2026, 12, 3), ages)));
        return id;
    }

    private void checkHttpCount(HttpClient client, String base, UUID id) throws IOException, InterruptedException {
        checkHttpCount(client, base, id, List.of(30, 1), 2);
    }

    private void checkHttpCount(HttpClient client, String base, UUID id, List<Integer> ages, long count)
            throws IOException, InterruptedException {
        var response = client.send(HttpRequest.newBuilder(URI.create(base + "/count?searchId=" + id))
                .timeout(Duration.ofSeconds(5)).GET().build(), HttpResponse.BodyHandlers.ofString());
        var body = JsonMapper.builder().build().readTree(response.body());
        assertAll(
                () -> assertEquals(200, response.statusCode(), "El consumidor debe terminar el procesamiento"),
                () -> assertEquals(id.toString(), body.path("searchId").asString(), "Devuelve el ID consultado"),
                () -> assertEquals(count, body.path("count").asLong(), "Cuenta solo las solicitudes del mismo grupo"),
                () -> assertEquals(hotel, body.at("/search/hotelId").asString(), "Conserva el hotel"),
                () -> assertEquals(JsonMapper.builder().build().valueToTree(ages), body.at("/search/ages"), "Conserva orden y edades"),
                () -> assertEquals("01/12/2026", body.at("/search/checkIn").asString(), "Conserva la entrada"),
                () -> assertEquals("03/12/2026", body.at("/search/checkOut").asString(), "Conserva la salida"));
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
