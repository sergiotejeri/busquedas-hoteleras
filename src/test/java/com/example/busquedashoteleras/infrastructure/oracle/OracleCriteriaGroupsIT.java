package com.example.busquedashoteleras.infrastructure.oracle;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.example.busquedashoteleras.domain.SearchCriteria;
import com.example.busquedashoteleras.infrastructure.serialization.CanonicalCriteriaCodec;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.TimeUnit;
import java.util.HexFormat;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.jdbc.support.JdbcTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

class OracleCriteriaGroupsIT {
    private static final DriverManagerDataSource SOURCE = new DriverManagerDataSource(
            required("TEST_DB_URL"), required("TEST_DB_USER"), required("TEST_DB_PASSWORD"));
    private final JdbcTemplate jdbc = new JdbcTemplate(SOURCE);
    private final OracleCriteriaGroups groups = new OracleCriteriaGroups(SOURCE);
    private final byte[] bucket = UUID.randomUUID().toString().replace("-", "").getBytes(StandardCharsets.US_ASCII);

    @BeforeAll
    static void migrateGroups() {
        Flyway.configure().dataSource(SOURCE).cleanDisabled(true).load().migrate();
    }

    @AfterEach
    void deleteOwnBucket() {
        transaction().executeWithoutResult(status -> {
            jdbc.update("DELETE FROM criteria_group WHERE criteria_hash = ?", bucket);
            jdbc.update("DELETE FROM criteria_group_lock WHERE criteria_hash = ?", bucket);
        });
    }

    @Test
    void reusesExactCriteriaButSeparatesCollisionsAndAgeSequences() {
        String original = canonical("hotel", List.of(30, 1));
        byte[] first = resolve(original);
        byte[] same = resolve(original);
        byte[] permuted = resolve(canonical("hotel", List.of(1, 30)));
        byte[] repeated = resolve(canonical("hotel", List.of(30, 1, 1)));
        byte[] otherHotel = resolve(canonical("HOTEL", List.of(30, 1)));
        assertAll(
                () -> assertArrayEquals(first, same, "La igualdad exacta reutiliza el grupo"),
                () -> assertFalse(Arrays.equals(first, permuted), "El orden importa incluso con hash identico"),
                () -> assertFalse(Arrays.equals(first, repeated), "Las repeticiones importan"),
                () -> assertFalse(Arrays.equals(first, otherHotel), "El hotel es exacto"),
                () -> assertEquals(4, countGroups(), "Una colision no mezcla criterios"));
    }

    @Test
    void preservesLongUnicodeAndTreatsSqlCharactersAsData() {
        String value = canonical("ñ' OR '1'='1" + "界".repeat(40_000), List.of(0, Integer.MAX_VALUE));
        byte[] first = resolve(value);
        String stored = jdbc.queryForObject("SELECT criteria_json FROM criteria_group WHERE group_id = ?", String.class, first);
        assertAll(
                () -> assertEquals(value, stored, "El CLOB conserva todo el texto"),
                () -> assertArrayEquals(first, resolve(value), "Compara tambien mas alla de 32767 caracteres"),
                () -> assertFalse(Arrays.equals(first, resolve(value.replace("界\"", "別\""))),
                        "Una diferencia al final del CLOB crea otro grupo"));
    }

    @Test
    void rollsBackBucketAndGroupWithCallerTransaction() {
        transaction().executeWithoutResult(status -> {
            groups.resolve(canonical("rollback", List.of(1)), bucket);
            status.setRollbackOnly();
        });
        assertAll(
                () -> assertEquals(0, countGroups(), "El grupo no se confirma por separado"),
                () -> assertEquals(0, jdbc.queryForObject("SELECT COUNT(*) FROM criteria_group_lock WHERE criteria_hash = ?",
                        Integer.class, bucket), "El bucket tambien se revierte"),
                () -> assertNotNull(resolve(canonical("retry", List.of(1))), "Se puede reintentar despues del rollback"));
    }

    @Test
    void simultaneousCreatorsUseOneGroupForEqualCriteriaAndSeparateCollisions() throws InterruptedException, ExecutionException, TimeoutException {
        var ready = new CountDownLatch(8);
        var start = new CountDownLatch(1);
        List<Future<byte[]>> results = new ArrayList<>();
        try (var executor = Executors.newFixedThreadPool(8)) {
            for (int index = 0; index < 8; index++) {
                String criteria = canonical(index < 4 ? "same" : "other", List.of(1));
                results.add(executor.submit(() -> {
                    ready.countDown();
                    if (!start.await(10, TimeUnit.SECONDS)) {
                        throw new IllegalStateException("La barrera no se libero");
                    }
                    return resolve(criteria);
                }));
            }
            try {
                if (!ready.await(10, TimeUnit.SECONDS)) {
                    throw new IllegalStateException("Los trabajadores no llegaron a la barrera");
                }
            } finally {
                start.countDown();
            }
            List<String> identifiers = new ArrayList<>();
            for (Future<byte[]> result : results) {
                identifiers.add(HexFormat.of().formatHex(result.get(20, TimeUnit.SECONDS)));
            }
            assertAll(
                    () -> assertEquals(1, identifiers.subList(0, 4).stream().distinct().count(), "Un grupo para iguales"),
                    () -> assertEquals(1, identifiers.subList(4, 8).stream().distinct().count(), "Un grupo para el otro criterio"),
                    () -> assertEquals(2, identifiers.stream().distinct().count(), "Colisiones separadas bajo concurrencia"),
                    () -> assertEquals(2, countGroups(), "Sin grupos duplicados"));
        }
    }

    @Test
    void boundsWaitForUncommittedBucketAndCanRetryAfterRollback() throws SQLException {
        long started;
        DataAccessException failure;
        try (var owner = SOURCE.getConnection(); var watchdog = Executors.newSingleThreadScheduledExecutor()) {
            owner.setAutoCommit(false);
            try (var insert = owner.prepareStatement("INSERT INTO criteria_group_lock(criteria_hash) VALUES (?)")) {
                insert.setBytes(1, bucket);
                insert.executeUpdate();
            }
            var release = watchdog.schedule(() -> {
                rollbackOwner(owner);
                return null;
            }, 20, TimeUnit.SECONDS);
            try {
                started = System.nanoTime();
                failure = blockedResolution();
            } finally {
                release.cancel(false);
                rollbackOwner(owner);
            }
        }
        long elapsed = TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - started);
        Throwable cause = failure.getCause();
        assertAll(
                () -> assertTrue(cause instanceof SQLException sql
                        && (sql.getErrorCode() == 1013 || sql.getErrorCode() == 30006), "Debe fallar por timeout Oracle: " + failure),
                () -> assertTrue(elapsed < 20, "No debe esperar indefinidamente"),
                () -> assertEquals(0, countGroups(), "El intento fallido no crea grupos"),
                () -> assertNotNull(resolve(canonical("blocked", List.of(1))), "Puede reintentarse tras el rollback"));
    }
    private static void rollbackOwner(Connection owner) throws SQLException {
        synchronized (owner) {
            owner.rollback();
        }
    }
    private DataAccessException blockedResolution() {
        return assertThrows(DataAccessException.class,
                () -> resolve(canonical("blocked", List.of(1))), "La espera por el creador debe estar acotada");
    }
    private byte[] resolve(String canonical) {
        return transaction().execute(status -> groups.resolve(canonical, bucket));
    }

    private int countGroups() {
        return Objects.requireNonNull(jdbc.queryForObject("SELECT COUNT(*) FROM criteria_group WHERE criteria_hash = ?",
                Integer.class, bucket));
    }

    private static TransactionTemplate transaction() {
        var transaction = new TransactionTemplate(new JdbcTransactionManager(SOURCE));
        transaction.setTimeout(10);
        return transaction;
    }

    private static String canonical(String hotel, List<Integer> ages) {
        return new CanonicalCriteriaCodec().encode(new SearchCriteria(hotel,
                LocalDate.of(2026, 12, 1), LocalDate.of(2026, 12, 3), ages));
    }

    private static String required(String name) {
        return Objects.requireNonNull(System.getenv(name), "Missing integration environment: " + name);
    }
}
