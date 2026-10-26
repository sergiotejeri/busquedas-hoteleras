package com.example.busquedashoteleras.infrastructure.oracle;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.example.busquedashoteleras.domain.InvalidSearchException;
import com.example.busquedashoteleras.domain.SearchSubmission;
import com.example.busquedashoteleras.infrastructure.serialization.CanonicalCriteriaCodec;
import com.example.busquedashoteleras.infrastructure.serialization.SearchEventCodec;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.jdbc.core.JdbcTemplate;
import com.zaxxer.hikari.HikariDataSource;
import org.springframework.jdbc.support.JdbcTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

class OracleAcceptanceIT {
    private static final HikariDataSource SOURCE = OracleTestDataSources.create();
    private final JdbcTemplate jdbc = new JdbcTemplate(SOURCE);
    private final OracleAcceptanceAdapter adapter = new OracleAcceptanceAdapter(SOURCE);
    private final List<UUID> ids = new ArrayList<>();

    @AfterAll
    static void closeDataSource() {
        SOURCE.close();
    }

    @BeforeAll
    static void migrate() {
        Flyway.configure().dataSource(SOURCE).cleanDisabled(true).load().migrate();
    }

    @AfterEach
    void removeOwnRows() {
        TransactionTemplate transaction = new TransactionTemplate(new JdbcTransactionManager(SOURCE));
        transaction.executeWithoutResult(status -> {
            for (UUID id : ids) {
                jdbc.update("DELETE FROM search_outbox WHERE search_id = HEXTORAW(?)", hex(id));
                jdbc.update("DELETE FROM search_request WHERE search_id = HEXTORAW(?)", hex(id));
            }
        });
    }

    @Test
    void commitsBothRowsAndPreservesLiteralInput() {
        SearchSubmission submission = AcceptanceFixtures.submission(identifier(), "x'); DROP TABLE search_request; --");
        adapter.accept(submission);
        Stored stored = stored(submission.searchId());
        assertAll(
                () -> assertEquals(2, rowCount(submission.searchId()), "Debe guardar solicitud y evento"),
                () -> assertEquals("PENDING", stored.requestStatus(), "La solicitud espera procesamiento"),
                () -> assertEquals("READY", stored.outboxState(), "El evento espera publicacion"),
                () -> assertEquals(submission.criteria(), new CanonicalCriteriaCodec().decode(stored.criteria()),
                        "Debe conservar hotel, fechas, orden y repeticiones"),
                () -> assertEquals(submission, new SearchEventCodec().decode(stored.event()),
                        "El evento debe corresponder a la solicitud"));
    }

    @ParameterizedTest
    @ValueSource(strings = {"hotel-ñ界😀", "𐀀𠮷𝄞", "hotel-\"\\\n\t"})
    void preservesUnicodeAndDateBoundaries(String hotel) {
        var criteria = new com.example.busquedashoteleras.domain.SearchCriteria(hotel,
                java.time.LocalDate.of(1, 1, 1), java.time.LocalDate.of(9999, 12, 31), java.util.List.of(30, 0, 30));
        var submission = new SearchSubmission(identifier(), criteria);
        adapter.accept(submission);
        Stored stored = stored(submission.searchId());
        assertAll(
                () -> assertEquals(criteria, new CanonicalCriteriaCodec().decode(stored.criteria()),
                        "Oracle conserva Unicode valido y los extremos del rango de fechas"),
                () -> assertEquals(submission, new SearchEventCodec().decode(stored.event()),
                        "El evento conserva los mismos datos que la solicitud"));
    }
    @Test
    void keepsEqualSearchesWithDifferentIdentities() {
        SearchSubmission first = AcceptanceFixtures.submission(identifier(), "same-hotel");
        SearchSubmission second = AcceptanceFixtures.submission(identifier(), "same-hotel");
        adapter.accept(first);
        adapter.accept(second);
        assertAll(
                () -> assertEquals(2, rowCount(first.searchId()), "Debe conservar la primera solicitud"),
                () -> assertEquals(2, rowCount(second.searchId()), "Debe conservar la segunda solicitud"));
    }

    @Test
    void rollsBackRequestWhenOutboxInsertFails() {
        UUID id = identifier();
        String constraint = "reject_" + hex(id).substring(0, 20);
        jdbc.execute("ALTER TABLE search_outbox ADD CONSTRAINT " + constraint
                + " CHECK (search_id <> HEXTORAW('" + hex(id) + "'))");
        try {
            assertAll(
                    () -> assertThrows(DataIntegrityViolationException.class,
                            () -> adapter.accept(AcceptanceFixtures.submission(id, "rollback-hotel")),
                            "La segunda escritura debe fallar en Oracle"),
                    () -> assertEquals(0, rowCount(id), "El fallo debe deshacer tambien la solicitud"));
        } finally {
            jdbc.execute("ALTER TABLE search_outbox DROP CONSTRAINT " + constraint);
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {262143, 262144})
    void acceptsEventsUpToTheUtf8Limit(int bytes) {
        SearchSubmission submission = AcceptanceFixtures.sizedSubmission(identifier(), bytes);
        adapter.accept(submission);
        Stored stored = stored(submission.searchId());
        assertAll(
                () -> assertEquals(bytes, stored.event().getBytes(StandardCharsets.UTF_8).length,
                        "El limite incluye todo el evento y sus bytes UTF-8"),
                () -> assertEquals(submission, new SearchEventCodec().decode(stored.event()),
                        "El CLOB no debe truncar los datos"));
    }

    @Test
    void rejectsOneByteOverTheLimitWithoutRows() {
        SearchSubmission submission = AcceptanceFixtures.sizedSubmission(identifier(), 262145);
        assertAll(
                () -> {
                    InvalidSearchException failure = assertThrows(InvalidSearchException.class,
                            () -> adapter.accept(submission));
                    assertEquals("REQUEST_TOO_LARGE", failure.getCode(), "Debe identificar el exceso de tamano");
                },
                () -> assertEquals(0, rowCount(submission.searchId()), "No debe dejar datos parciales"));
    }

    @Test
    void rejectsDuplicateIdentityWithoutReplacingExistingData() {
        SearchSubmission first = AcceptanceFixtures.submission(identifier(), "original");
        adapter.accept(first);
        SearchSubmission duplicate = AcceptanceFixtures.submission(first.searchId(), "replacement");
        assertAll(
                () -> assertThrows(DataIntegrityViolationException.class, () -> adapter.accept(duplicate),
                        "Un UUID repetido no debe sobrescribir datos"),
                () -> assertEquals(first, new SearchEventCodec().decode(stored(first.searchId()).event()),
                        "El evento original debe permanecer intacto"),
                () -> assertEquals(2, rowCount(first.searchId()), "Solo debe existir el par original"));
    }

    @Test
    void confirmsItsOwnTransactionEvenWithAnOuterRollback() {
        SearchSubmission submission = AcceptanceFixtures.submission(identifier(), "independent");
        TransactionTemplate outer = new TransactionTemplate(new JdbcTransactionManager(SOURCE));
        outer.executeWithoutResult(status -> {
            adapter.accept(submission);
            status.setRollbackOnly();
        });
        assertEquals(2, rowCount(submission.searchId()), "Retornar debe implicar un commit propio confirmado");
    }

    private Stored stored(UUID id) {
        return jdbc.queryForObject("SELECT r.criteria_json, o.event_json, r.status, o.state"
                        + " FROM search_request r JOIN search_outbox o ON o.search_id = r.search_id"
                        + " WHERE r.search_id = HEXTORAW(?)",
                (result, row) -> new Stored(result.getString(1), result.getString(2),
                        result.getString(3), result.getString(4)), hex(id));
    }

    private int rowCount(UUID id) {
        return Objects.requireNonNull(jdbc.queryForObject("SELECT (SELECT COUNT(*) FROM search_request WHERE search_id = HEXTORAW(?))"
                        + " + (SELECT COUNT(*) FROM search_outbox WHERE search_id = HEXTORAW(?)) FROM DUAL",
                Integer.class, hex(id), hex(id)), "La consulta de conteo debe devolver una fila");
    }

    private UUID identifier() {
        UUID id = UUID.randomUUID();
        ids.add(id);
        return id;
    }

    private static String hex(UUID id) {
        return id.toString().replace("-", "");
    }

    private record Stored(String criteria, String event, String requestStatus, String outboxState) {
    }
}
