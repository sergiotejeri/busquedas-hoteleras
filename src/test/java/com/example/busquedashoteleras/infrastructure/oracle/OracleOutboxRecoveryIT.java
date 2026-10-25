package com.example.busquedashoteleras.infrastructure.oracle;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.example.busquedashoteleras.application.port.ProcessingResult;
import com.example.busquedashoteleras.application.port.OutboxClaim;
import com.example.busquedashoteleras.domain.SearchSubmission;
import com.example.busquedashoteleras.domain.SearchStatus;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

class OracleOutboxRecoveryIT {
    private static final DriverManagerDataSource SOURCE = new DriverManagerDataSource(
            required("TEST_DB_URL"), required("TEST_DB_USER"), required("TEST_DB_PASSWORD"));
    private static final Duration MINIMUM_AGE = Duration.ofMinutes(5);
    private static final String SENT = "SENT";
    private static final String READY = "READY";
    private final JdbcTemplate jdbc = new JdbcTemplate(SOURCE);
    private final OracleOutboxAdapter outbox = new OracleOutboxAdapter(SOURCE);
    private final List<SearchSubmission> owned = new ArrayList<>();
    private final String hotel = "recovery-" + UUID.randomUUID();

    @BeforeAll
    static void migrate() {
        Flyway.configure().dataSource(SOURCE).cleanDisabled(true).load().migrate();
    }

    @BeforeEach
    void requireNoPreviousWork() {
        Integer count = jdbc.queryForObject("SELECT COUNT(*) FROM search_outbox o JOIN search_request r "
                + "ON r.search_id = o.search_id WHERE o.state = 'READY' OR (o.state = 'SENT' AND r.status = 'PENDING')",
                Integer.class);
        if (!Integer.valueOf(0).equals(count)) {
            throw new IllegalStateException("El esquema contiene trabajo previo; no se modifica automaticamente");
        }
    }

    @AfterEach
    void removeOwnedData() {
        List<byte[]> groups = jdbc.query("SELECT group_id FROM criteria_group WHERE JSON_VALUE(criteria_json, '$.hotelId') = ?",
                (row, index) -> row.getBytes(1), hotel);
        List<byte[]> hashes = jdbc.query("SELECT criteria_hash FROM criteria_group WHERE JSON_VALUE(criteria_json, '$.hotelId') = ?",
                (row, index) -> row.getBytes(1), hotel);
        for (SearchSubmission search : owned) {
            jdbc.update("DELETE FROM processed_search WHERE search_id = HEXTORAW(?)", hex(search));
            jdbc.update("DELETE FROM search_outbox WHERE search_id = HEXTORAW(?)", hex(search));
            jdbc.update("DELETE FROM search_request WHERE search_id = HEXTORAW(?)", hex(search));
        }
        for (byte[] group : groups) {
            jdbc.update("DELETE FROM criteria_group WHERE group_id = ?", group);
        }
        for (byte[] hash : hashes) {
            jdbc.update("DELETE FROM criteria_group_lock b WHERE criteria_hash = ? "
                    + "AND NOT EXISTS (SELECT 1 FROM criteria_group g WHERE g.criteria_hash = b.criteria_hash)", hash);
        }
    }

    @ParameterizedTest
    @CsvSource({"SENT,PENDING,600,1", "SENT,PENDING,0,0", "READY,PENDING,600,0",
            "SENT,PROCESSED,600,0", "SENT,FAILED,600,0", "FAILED,PENDING,600,0"})
    void recoversOnlyOldSentEventsWithPendingRequests(String eventState, String requestState, int age, int expected) {
        SearchSubmission search = accept();
        if (SearchStatus.PROCESSED.name().equals(requestState)) {
            new OracleProcessingAdapter(SOURCE).process(search);
        } else {
            jdbc.update("UPDATE search_request SET status = ? WHERE search_id = HEXTORAW(?)", requestState, hex(search));
        }
        setEvent(search, eventState, age);
        int recovered = outbox.recoverPending(100, MINIMUM_AGE);
        assertAll(
                () -> assertEquals(expected, recovered, "Solo SENT antiguo y PENDING se recupera"),
                () -> assertEquals(expected == 1 ? READY : eventState, state(search), "Conserva los demas eventos"),
                () -> assertEquals(requestState, jdbc.queryForObject(
                        "SELECT status FROM search_request WHERE search_id = HEXTORAW(?)", String.class, hex(search)),
                        "Recuperar no cambia el estado de procesamiento"));
    }

    @Test
    void boundsBatchAndPreservesOriginalEvent() {
        SearchSubmission oldest = accept();
        SearchSubmission second = accept();
        SearchSubmission newest = accept();
        setEvent(oldest, SENT, 900);
        setEvent(second, SENT, 800);
        setEvent(newest, SENT, 700);
        String original = jdbc.queryForObject("SELECT event_json FROM search_outbox WHERE search_id = HEXTORAW(?)",
                String.class, hex(oldest));
        int recovered = outbox.recoverPending(2, MINIMUM_AGE);
        List<OutboxClaim> claims = java.util.stream.Stream.of(
                outbox.claimNext(Duration.ofMinutes(1)), outbox.claimNext(Duration.ofMinutes(1)))
                .flatMap(java.util.Optional::stream).toList();
        assertAll(
                () -> assertEquals(2, recovered, "El lote debe estar acotado"),
                () -> assertEquals(READY, state(oldest), "Recupera el mas antiguo"),
                () -> assertEquals(READY, state(second), "Recupera el segundo"),
                () -> assertEquals(SENT, state(newest), "No recupera una fila extra"),
                () -> assertEquals(Set.of(oldest.searchId(), second.searchId()),
                        claims.stream().map(OutboxClaim::searchId).collect(java.util.stream.Collectors.toSet()),
                        "Se pueden reservar los dos eventos recuperados"),
                () -> assertEquals(original, claims.stream().filter(item -> oldest.searchId().equals(item.searchId()))
                        .map(OutboxClaim::eventJson).findFirst().orElse(null), "No reconstruye el evento"),
                () -> assertEquals(1, outbox.recoverPending(100, MINIMUM_AGE), "El siguiente lote recoge el resto"));
    }

    @Test
    void startsRecoveryAgeAtBrokerConfirmation() {
        SearchSubmission search = accept();
        var claim = outbox.claimNext(Duration.ofHours(1)).orElseThrow();
        OffsetDateTime before = now();
        boolean confirmed = outbox.markSent(search.searchId(), claim.token());
        OffsetDateTime after = now();
        OffsetDateTime sentAt = jdbc.queryForObject("SELECT available_at FROM search_outbox WHERE search_id = HEXTORAW(?)",
                (row, index) -> row.getObject(1, OffsetDateTime.class), hex(search));
        assertAll(
                () -> assertTrue(confirmed, "Debe confirmar el envio"),
                () -> assertTrue(!Objects.requireNonNull(sentAt).isBefore(before) && !sentAt.isAfter(after),
                        "La antiguedad empieza al confirmar, no al vencer la reserva"),
                () -> assertEquals(0, outbox.recoverPending(1, MINIMUM_AGE), "No recupera un envio reciente"),
                () -> assertNull(jdbc.queryForObject("SELECT lease_token FROM search_outbox WHERE search_id = HEXTORAW(?)",
                        byte[].class, hex(search)), "La confirmacion libera el token"));
    }

    @Test
    void twoRecoverersDoNotCountTheSameTransitionTwice()
            throws InterruptedException, ExecutionException, TimeoutException {
        SearchSubmission search = accept();
        setEvent(search, SENT, 600);
        CountDownLatch start = new CountDownLatch(1);
        try (var workers = Executors.newFixedThreadPool(2)) {
            var first = workers.submit(() -> { start.await(); return outbox.recoverPending(1, MINIMUM_AGE); });
            var second = workers.submit(() -> { start.await(); return outbox.recoverPending(1, MINIMUM_AGE); });
            start.countDown();
            int total = first.get(10, TimeUnit.SECONDS) + second.get(10, TimeUnit.SECONDS);
            assertAll(
                    () -> assertEquals(1, total, "Una sola transicion aunque compitan dos recuperadores"),
                    () -> assertEquals(READY, state(search), "El evento vuelve a estar disponible"));
        }
    }

    @Test
    void skipsRequestBeingProcessedAndRecoversOtherWork()
            throws SQLException, InterruptedException, ExecutionException, TimeoutException {
        SearchSubmission locked = accept();
        SearchSubmission free = accept();
        setEvent(locked, SENT, 900);
        setEvent(free, SENT, 600);
        int recovered;
        try (Connection connection = SOURCE.getConnection();
                PreparedStatement statement = connection.prepareStatement(
                        "SELECT status FROM search_request WHERE search_id = HEXTORAW(?) FOR UPDATE");
                var worker = Executors.newSingleThreadExecutor()) {
            connection.setAutoCommit(false);
            statement.setString(1, hex(locked));
            try (ResultSet rows = statement.executeQuery()) {
                if (!rows.next()) {
                    throw new SQLException("Missing request to lock");
                }
                recovered = worker.submit(() -> outbox.recoverPending(1, MINIMUM_AGE)).get(10, TimeUnit.SECONDS);
            } finally {
                connection.rollback();
            }
        }
        new OracleProcessingAdapter(SOURCE).process(locked);
        assertAll(
                () -> assertEquals(1, recovered, "No debe esperar por la solicitud bloqueada"),
                () -> assertEquals(READY, state(free), "Recupera la siguiente solicitud"),
                () -> assertEquals(0, outbox.recoverPending(100, MINIMUM_AGE), "No reabre el trabajo ya procesado"),
                () -> assertEquals(SENT, state(locked), "Conserva el evento del trabajo procesado"));
    }

    @Test
    void recoveryRacingWithProcessingCannotDuplicateProjection()
            throws InterruptedException, ExecutionException, TimeoutException {
        SearchSubmission search = accept();
        setEvent(search, SENT, 600);
        CountDownLatch start = new CountDownLatch(1);
        try (var workers = Executors.newFixedThreadPool(2)) {
            var recovery = workers.submit(() -> { start.await(); return outbox.recoverPending(1, MINIMUM_AGE); });
            var processing = workers.submit(() -> { start.await(); return new OracleProcessingAdapter(SOURCE).process(search); });
            start.countDown();
            int recovered = recovery.get(10, TimeUnit.SECONDS);
            ProcessingResult processed = processing.get(10, TimeUnit.SECONDS);
            assertAll(
                    () -> assertTrue(recovered == 0 || recovered == 1, "Ambos ordenes de ejecucion son validos"),
                    () -> assertEquals(ProcessingResult.PROCESSED, processed, "La solicitud termina procesada"),
                    () -> assertEquals(ProcessingResult.ALREADY_PROCESSED, new OracleProcessingAdapter(SOURCE).process(search),
                            "Una reentrega recuperada es inocua"),
                    () -> assertEquals(1, jdbc.queryForObject("SELECT COUNT(*) FROM processed_search WHERE search_id = HEXTORAW(?)",
                            Integer.class, hex(search)), "No duplica la proyeccion"),
                    () -> assertEquals(0, outbox.recoverPending(1, MINIMUM_AGE), "No vuelve a recuperar una procesada"));
        }
    }

    @Test
    void failureRollsBackWholeRecoveryBatch() {
        SearchSubmission first = accept();
        SearchSubmission second = accept();
        setEvent(first, SENT, 900);
        setEvent(second, SENT, 600);
        String constraint = "reject_" + hex(second).substring(0, 20);
        jdbc.execute("ALTER TABLE search_outbox ADD CONSTRAINT " + constraint
                + " CHECK(search_id <> HEXTORAW('" + hex(second) + "') OR state <> 'READY')");
        try {
            assertAll(
                    () -> assertThrows(DataIntegrityViolationException.class, () -> outbox.recoverPending(2, MINIMUM_AGE)),
                    () -> assertEquals(SENT, state(first), "La primera escritura tambien se revierte"),
                    () -> assertEquals(SENT, state(second), "El evento que falla se conserva"));
        } finally {
            jdbc.execute("ALTER TABLE search_outbox DROP CONSTRAINT " + constraint);
        }
    }

    private SearchSubmission accept() {
        SearchSubmission search = AcceptanceFixtures.submission(UUID.randomUUID(), hotel);
        owned.add(search);
        new OracleAcceptanceAdapter(SOURCE).accept(search);
        return search;
    }

    private void setEvent(SearchSubmission search, String state, int ageSeconds) {
        jdbc.update("UPDATE search_outbox SET state = ?, available_at = SYSTIMESTAMP - NUMTODSINTERVAL(?, 'SECOND') "
                + "WHERE search_id = HEXTORAW(?)", state, ageSeconds, hex(search));
    }

    private String state(SearchSubmission search) {
        return jdbc.queryForObject("SELECT state FROM search_outbox WHERE search_id = HEXTORAW(?)", String.class, hex(search));
    }

    private OffsetDateTime now() {
        return Objects.requireNonNull(jdbc.queryForObject("SELECT SYSTIMESTAMP FROM DUAL",
                (row, index) -> row.getObject(1, OffsetDateTime.class)));
    }

    private static String hex(SearchSubmission search) {
        return search.searchId().toString().replace("-", "");
    }

    private static String required(String name) {
        return Objects.requireNonNull(System.getenv(name), name);
    }
}
