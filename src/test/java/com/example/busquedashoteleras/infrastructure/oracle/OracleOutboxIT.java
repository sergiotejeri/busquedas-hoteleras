package com.example.busquedashoteleras.infrastructure.oracle;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.example.busquedashoteleras.application.port.OutboxClaim;
import com.example.busquedashoteleras.application.port.PublicationResult;
import com.example.busquedashoteleras.application.service.PublishOutboxService;
import com.example.busquedashoteleras.infrastructure.serialization.SearchEventCodec;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
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
import org.springframework.jdbc.support.JdbcTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

class OracleOutboxIT {
    private static final DriverManagerDataSource SOURCE = new DriverManagerDataSource(
            required("TEST_DB_URL"), required("TEST_DB_USER"), required("TEST_DB_PASSWORD"));
    private static final Duration LEASE = Duration.ofMinutes(1);
    private static final String READY_PENDING = "READY/PENDING";
    private final JdbcTemplate jdbc = new JdbcTemplate(SOURCE);
    private final OracleOutboxAdapter adapter = new OracleOutboxAdapter(SOURCE);
    private final List<UUID> ids = new ArrayList<>();

    @BeforeAll
    static void migrate() {
        Flyway.configure().dataSource(SOURCE).cleanDisabled(true).load().migrate();
    }

    @BeforeEach
    void requireEmptyQueue() {
        Integer count = jdbc.queryForObject("SELECT COUNT(*) FROM search_outbox WHERE state = 'READY'", Integer.class);
        if (!Integer.valueOf(0).equals(count)) {
            throw new IllegalStateException("El esquema de pruebas contiene trabajo previo; no se elimina automaticamente");
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
    void reservesOneEventAndCommitsOwnershipBeforeReturning() {
        UUID id = accept();
        OutboxClaim claim = adapter.claimNext(LEASE).orElseThrow();
        assertAll(
                () -> assertEquals(id, claim.searchId(), "Debe reservar el evento aceptado"),
                () -> assertEquals(id, new SearchEventCodec().decode(claim.eventJson()).searchId(),
                        "Debe conservar el payload completo"),
                () -> assertEquals(hex(claim.token()), storedToken(id), "El token debe estar confirmado en otra conexion"),
                () -> assertTrue(adapter.claimNext(LEASE).isEmpty(), "No debe reservar de nuevo antes del vencimiento"),
                () -> assertEquals("PENDING", jdbc.queryForObject(
                        "SELECT status FROM search_request WHERE search_id = HEXTORAW(?)", String.class, hex(id)),
                        "Reservar no significa procesar"));
    }

    @Test
    void preservesSubsecondLeaseInOracle() {
        UUID id = accept();
        OffsetDateTime before = jdbc.queryForObject("SELECT SYSTIMESTAMP FROM DUAL",
                (row, index) -> row.getObject(1, OffsetDateTime.class));
        adapter.claimNext(Duration.ofMillis(500)).orElseThrow();
        OffsetDateTime due = jdbc.queryForObject("SELECT available_at FROM search_outbox WHERE search_id = HEXTORAW(?)",
                (row, index) -> row.getObject(1, OffsetDateTime.class), hex(id));
        assertTrue(Objects.requireNonNull(due).toInstant().compareTo(
                Objects.requireNonNull(before).toInstant().plusMillis(500)) >= 0,
                "La reserva no debe truncar 500 ms a cero segundos");
    }

    @Test
    void rejectsForeignExpiredAndReplacedTokens() {
        UUID id = accept();
        OutboxClaim first = adapter.claimNext(LEASE).orElseThrow();
        boolean foreignConfirmed = adapter.markSent(id, UUID.randomUUID());
        expire(id);
        boolean expiredConfirmed = adapter.markSent(id, first.token());
        OutboxClaim second = adapter.claimNext(LEASE).orElseThrow();
        boolean replacedConfirmed = adapter.markSent(id, first.token());
        String tokenAfterStaleAttempt = storedToken(id);
        boolean currentConfirmed = adapter.markSent(id, second.token());
        assertAll(
                () -> assertFalse(foreignConfirmed, "Un token ajeno no debe confirmar"),
                () -> assertFalse(expiredConfirmed, "Una reserva vencida no debe confirmar"),
                () -> assertNotEquals(first.token(), second.token(), "Cada reserva debe renovar el token"),
                () -> assertFalse(replacedConfirmed, "El propietario anterior no debe confirmar"),
                () -> assertEquals(hex(second.token()), tokenAfterStaleAttempt, "El token vigente debe conservarse"),
                () -> assertTrue(currentConfirmed, "Solo la reserva vigente puede marcar SENT"),
                () -> assertFalse(adapter.markSent(id, second.token()), "Una confirmacion repetida no debe cambiar nada"),
                () -> assertTrue(adapter.claimNext(LEASE).isEmpty(), "Un evento enviado no debe volver a reservarse"),
                () -> assertEquals("SENT", jdbc.queryForObject(
                        "SELECT state FROM search_outbox WHERE search_id = HEXTORAW(?)", String.class, hex(id)),
                        "Debe registrar la publicacion"),
                () -> assertNull(storedToken(id), "Al confirmar debe liberar el token"));
    }

    @Test
    void cannotUseOneEventsTokenToConfirmAnotherEvent() {
        accept();
        accept();
        OutboxClaim first = adapter.claimNext(LEASE).orElseThrow();
        OutboxClaim second = adapter.claimNext(LEASE).orElseThrow();
        assertAll(
                () -> assertFalse(adapter.markSent(second.searchId(), first.token()),
                        "El token no debe autorizar otra identidad"),
                () -> assertFalse(adapter.markSent(first.searchId(), second.token()),
                        "Intercambiar tokens no debe confirmar ninguna reserva"),
                () -> assertEquals(hex(first.token()), storedToken(first.searchId()), "Debe conservar la primera reserva"),
                () -> assertEquals(hex(second.token()), storedToken(second.searchId()), "Debe conservar la segunda reserva"));
    }

    @Test
    void ignoresFutureAndTerminalMessages() {
        UUID future = accept();
        UUID sent = accept();
        UUID failed = accept();
        jdbc.update("UPDATE search_outbox SET available_at = SYSTIMESTAMP + INTERVAL '1' HOUR WHERE search_id = HEXTORAW(?)",
                hex(future));
        jdbc.update("UPDATE search_outbox SET state = 'SENT' WHERE search_id = HEXTORAW(?)", hex(sent));
        jdbc.update("UPDATE search_outbox SET state = 'FAILED' WHERE search_id = HEXTORAW(?)", hex(failed));
        assertTrue(adapter.claimNext(LEASE).isEmpty(), "Solo debe reclamar mensajes READY que ya esten disponibles");
    }

    @Test
    void twoConcurrentWorkersCannotOwnTheSameEvent() throws InterruptedException, ExecutionException, TimeoutException {
        UUID id = accept();
        CountDownLatch start = new CountDownLatch(1);
        try (var workers = Executors.newFixedThreadPool(2)) {
            Future<Optional<OutboxClaim>> first = workers.submit(() -> {
                start.await();
                return new OracleOutboxAdapter(SOURCE).claimNext(LEASE);
            });
            Future<Optional<OutboxClaim>> second = workers.submit(() -> {
                start.await();
                return new OracleOutboxAdapter(SOURCE).claimNext(LEASE);
            });
            start.countDown();
            List<OutboxClaim> claims = java.util.stream.Stream.of(
                    first.get(10, TimeUnit.SECONDS), second.get(10, TimeUnit.SECONDS)).flatMap(Optional::stream).toList();
            assertAll(
                    () -> assertEquals(1, claims.size(), "Solo un trabajador debe obtener la reserva"),
                    () -> assertEquals(id, claims.getFirst().searchId(), "La reserva debe corresponder al evento"));
        }
    }

    @Test
    void skipsLockedOldestRowWithoutWaitingOrLosingLaterWork()
            throws SQLException, InterruptedException, ExecutionException, TimeoutException {
        UUID locked = accept();
        UUID free = accept();
        jdbc.update("UPDATE search_outbox SET available_at = SYSTIMESTAMP - INTERVAL '1' HOUR WHERE search_id = HEXTORAW(?)",
                hex(locked));
        OutboxClaim result;
        try (Connection connection = SOURCE.getConnection();
                PreparedStatement lock = connection.prepareStatement(
                        "SELECT search_id FROM search_outbox WHERE search_id = HEXTORAW(?) FOR UPDATE");
                var worker = Executors.newSingleThreadExecutor()) {
            connection.setAutoCommit(false);
            lock.setString(1, hex(locked));
            try (ResultSet row = lock.executeQuery()) {
                if (!row.next()) {
                    throw new SQLException("La fila que debe bloquearse no existe");
                }
                Future<OutboxClaim> claimed = worker.submit(() -> adapter.claimNext(LEASE).orElseThrow());
                result = claimed.get(10, TimeUnit.SECONDS);
            } finally {
                connection.rollback();
            }
        }
        assertAll(
                () -> assertEquals(free, result.searchId(), "Debe saltar la fila bloqueada y reservar la siguiente"),
                () -> assertEquals(locked, adapter.claimNext(LEASE).orElseThrow().searchId(),
                        "La fila desbloqueada debe seguir disponible"));
    }

    @Test
    // Se comprueba el rollback antes de retirar la restriccion y la recuperacion despues.
    @SuppressWarnings("PMD.UnitTestContainsTooManyAsserts")
    void failedReservationRollsBackAndReleasesTheRow() {
        UUID id = accept();
        String constraint = "reject_" + hex(id).substring(0, 20);
        jdbc.execute("ALTER TABLE search_outbox ADD CONSTRAINT " + constraint
                + " CHECK (search_id <> HEXTORAW('" + hex(id) + "') OR lease_token IS NULL)");
        try {
            assertAll(
                    () -> assertThrows(DataIntegrityViolationException.class, () -> adapter.claimNext(LEASE),
                            "El fallo de escritura debe propagarse"),
                    () -> assertNull(storedToken(id), "El fallo no debe dejar una reserva parcial"));
        } finally {
            jdbc.execute("ALTER TABLE search_outbox DROP CONSTRAINT " + constraint);
        }
        assertEquals(id, adapter.claimNext(LEASE).orElseThrow().searchId(), "Debe poder reservarse tras el rollback");
    }

    @Test
    void retryRetainsEventAndPendingRequestUntilDue() {
        UUID id = accept();
        OutboxClaim claim = adapter.claimNext(LEASE).orElseThrow();
        boolean retried = adapter.retry(id, claim.token(), Duration.ofSeconds(30));
        assertAll(
                () -> assertTrue(retried, "Debe aplazar la reserva vigente"),
                () -> assertTrue(adapter.claimNext(LEASE).isEmpty(), "No debe reintentarse antes de tiempo"),
                () -> assertEquals(READY_PENDING, states(id), "Un fallo transitorio no es terminal"),
                () -> assertNull(storedToken(id), "Debe liberar la reserva"),
                () -> assertEquals(1, retryAttempts(id), "Debe conservar el nivel de backoff"),
                () -> assertEquals(claim.eventJson(), jdbc.queryForObject(
                        "SELECT event_json FROM search_outbox WHERE search_id = HEXTORAW(?)", String.class, hex(id)),
                        "No debe regenerar ni perder el evento"));
    }

    @Test
    void retryLevelSurvivesNewAdaptersAndSaturatesWithoutDiscardingWork() {
        UUID id = accept();
        OracleOutboxAdapter retryAdapter = new OracleOutboxAdapter(SOURCE);
        for (int attempt = 0; attempt < 8; attempt++) {
            OutboxClaim claim = retryAdapter.claimNext(LEASE).orElseThrow();
            if (!adapter.retry(id, claim.token(), Duration.ofSeconds(1))) {
                throw new IllegalStateException("La reserva vigente debe admitir reintento");
            }
            expire(id);
        }
        OutboxClaim recovered = new OracleOutboxAdapter(SOURCE).claimNext(LEASE).orElseThrow();
        assertAll(
                () -> assertEquals(6, recovered.retryAttempts(), "El contador se satura, no desborda"),
                () -> assertEquals(READY_PENDING, states(id), "No se descarta por agotar reintentos"));
    }

    @Test
    void staleAndExpiredTokensCannotRetryOrFail() {
        UUID id = accept();
        OutboxClaim first = adapter.claimNext(LEASE).orElseThrow();
        boolean foreignRetry = adapter.retry(id, UUID.randomUUID(), Duration.ofSeconds(1));
        boolean foreignFail = adapter.markFailed(id, UUID.randomUUID());
        expire(id);
        boolean expiredRetry = adapter.retry(id, first.token(), Duration.ofSeconds(1));
        boolean expiredFail = adapter.markFailed(id, first.token());
        OutboxClaim current = adapter.claimNext(LEASE).orElseThrow();
        boolean staleRetry = adapter.retry(id, first.token(), Duration.ofSeconds(1));
        boolean staleFail = adapter.markFailed(id, first.token());
        assertAll(
                () -> assertFalse(foreignRetry || foreignFail || expiredRetry || expiredFail || staleRetry || staleFail,
                        "Solo el token vigente y no vencido puede modificar"),
                () -> assertEquals(hex(current.token()), storedToken(id), "Debe conservar el propietario actual"),
                () -> assertEquals(0, retryAttempts(id), "No debe aumentar el contador"),
                () -> assertEquals(READY_PENDING, states(id), "No debe alterar ninguno de los estados"));
    }

    @ParameterizedTest
    @CsvSource({"PENDING,FAILED/FAILED", "PROCESSED,SENT/PROCESSED", "FAILED,FAILED/FAILED"})
    void permanentFailurePreservesProcessingState(String requestState, String expected) {
        UUID id = accept();
        jdbc.update("UPDATE search_request SET status = ? WHERE search_id = HEXTORAW(?)", requestState, hex(id));
        OutboxClaim claim = adapter.claimNext(LEASE).orElseThrow();
        boolean failed = adapter.markFailed(id, claim.token());
        assertAll(
                () -> assertTrue(failed, "Debe cerrar la reserva vigente"),
                () -> assertEquals(expected, states(id), "No debe degradar una solicitud procesada"),
                () -> assertNull(storedToken(id), "La reserva debe quedar liberada"),
                () -> assertFalse(adapter.markFailed(id, claim.token()), "Repetir el fallo no cambia nada"),
                () -> assertTrue(adapter.claimNext(LEASE).isEmpty(), "No debe reclamar un estado terminal"));
    }

    @Test
    void failureOfRequestUpdateRollsBackOutboxTransition() {
        UUID id = accept();
        OutboxClaim claim = adapter.claimNext(LEASE).orElseThrow();
        String constraint = "reject_" + hex(id).substring(0, 20);
        jdbc.execute("ALTER TABLE search_request ADD CONSTRAINT " + constraint
                + " CHECK (search_id <> HEXTORAW('" + hex(id) + "') OR status <> 'FAILED')");
        try {
            assertAll(
                    () -> assertThrows(DataIntegrityViolationException.class, () -> adapter.markFailed(id, claim.token()),
                            "Debe propagar el fallo de la segunda escritura"),
                    () -> assertEquals(READY_PENDING, states(id), "Ambas escrituras deben revertirse"),
                    () -> assertEquals(hex(claim.token()), storedToken(id), "Debe conservar la reserva recuperable"));
        } finally {
            jdbc.execute("ALTER TABLE search_request DROP CONSTRAINT " + constraint);
        }
    }

    @Test
    void failedRetryUpdateDoesNotReleaseOwnershipOrIncrementAttempts() {
        UUID id = accept();
        OutboxClaim claim = adapter.claimNext(LEASE).orElseThrow();
        String constraint = "reject_" + hex(id).substring(0, 20);
        jdbc.execute("ALTER TABLE search_outbox ADD CONSTRAINT " + constraint
                + " CHECK (search_id <> HEXTORAW('" + hex(id) + "') OR retry_attempts = 0)");
        try {
            assertAll(
                    () -> assertThrows(DataIntegrityViolationException.class,
                            () -> adapter.retry(id, claim.token(), Duration.ofSeconds(1)), "Debe propagar el fallo"),
                    () -> assertEquals(0, retryAttempts(id), "No debe dejar un contador parcial"),
                    () -> assertEquals(hex(claim.token()), storedToken(id), "No debe perder el propietario"));
        } finally {
            jdbc.execute("ALTER TABLE search_outbox DROP CONSTRAINT " + constraint);
        }
    }

    @Test
    void serviceRecoversDeferredPublicationUsingRealPersistence() {
        UUID id = accept();
        int first = new PublishOutboxService(adapter, (key, payload) -> PublicationResult.RETRYABLE_FAILURE).publishBatch(100);
        String pending = states(id);
        expire(id);
        List<UUID> published = new ArrayList<>();
        int second = new PublishOutboxService(new OracleOutboxAdapter(SOURCE), (key, payload) -> {
            published.add(new SearchEventCodec().decode(payload).searchId());
            return PublicationResult.CONFIRMED;
        }).publishBatch(100);
        assertAll(
                () -> assertEquals(0, first, "El fallo no es una publicacion"),
                () -> assertEquals(READY_PENDING, pending, "Debe mantener trabajo recuperable"),
                () -> assertEquals(1, second, "El siguiente ciclo debe confirmar"),
                () -> assertEquals(List.of(id), published, "Debe publicar el evento original"),
                () -> assertEquals("SENT/PENDING", states(id), "Publicar no significa procesar"));
    }

    private String states(UUID id) {
        return jdbc.queryForObject("SELECT o.state || '/' || r.status FROM search_outbox o"
                + " JOIN search_request r ON r.search_id = o.search_id WHERE o.search_id = HEXTORAW(?)",
                String.class, hex(id));
    }

    private int retryAttempts(UUID id) {
        return Objects.requireNonNull(jdbc.queryForObject(
                "SELECT retry_attempts FROM search_outbox WHERE search_id = HEXTORAW(?)", Integer.class, hex(id)));
    }

    private UUID accept() {
        UUID id = UUID.randomUUID();
        ids.add(id);
        new OracleAcceptanceAdapter(SOURCE).accept(AcceptanceFixtures.submission(id, "outbox-test"));
        return id;
    }

    private void expire(UUID id) {
        jdbc.update("UPDATE search_outbox SET available_at = SYSTIMESTAMP - INTERVAL '1' SECOND WHERE search_id = HEXTORAW(?)",
                hex(id));
    }

    private String storedToken(UUID id) {
        return jdbc.queryForObject("SELECT RAWTOHEX(lease_token) FROM search_outbox WHERE search_id = HEXTORAW(?)",
                String.class, hex(id));
    }

    private static String hex(UUID id) {
        return id.toString().replace("-", "").toUpperCase(java.util.Locale.ROOT);
    }

    private static String required(String name) {
        return Objects.requireNonNull(System.getenv(name), "Missing integration environment: " + name);
    }
}
