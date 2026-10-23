package com.example.busquedashoteleras.infrastructure.oracle;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.example.busquedashoteleras.application.port.OutboxClaim;
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
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.jdbc.support.JdbcTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

class OracleOutboxIT {
    private static final DriverManagerDataSource SOURCE = new DriverManagerDataSource(
            required("TEST_DB_URL"), required("TEST_DB_USER"), required("TEST_DB_PASSWORD"));
    private static final Duration LEASE = Duration.ofMinutes(1);
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
