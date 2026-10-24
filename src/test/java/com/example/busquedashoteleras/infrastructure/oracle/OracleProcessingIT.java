package com.example.busquedashoteleras.infrastructure.oracle;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.example.busquedashoteleras.application.port.ProcessingResult;
import com.example.busquedashoteleras.application.service.ProcessSearchService;
import com.example.busquedashoteleras.domain.SearchCriteria;
import com.example.busquedashoteleras.domain.SearchSubmission;
import com.example.busquedashoteleras.infrastructure.serialization.CanonicalCriteriaCodec;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
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
import org.junit.jupiter.api.Test;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

class OracleProcessingIT {
    private static final DriverManagerDataSource SOURCE = new DriverManagerDataSource(
            required("TEST_DB_URL"), required("TEST_DB_USER"), required("TEST_DB_PASSWORD"));
    private final JdbcTemplate jdbc = new JdbcTemplate(SOURCE);
    private final ProcessSearchService service = new ProcessSearchService(new OracleProcessingAdapter(SOURCE));
    private final List<SearchSubmission> owned = new ArrayList<>();
    private final String hotel = UUID.randomUUID().toString();

    @BeforeAll
    static void migrateProjection() {
        Flyway.configure().dataSource(SOURCE).cleanDisabled(true).load().migrate();
    }

    @AfterEach
    void deleteOwnSearches() {
        for (SearchSubmission submission : owned) {
            String id = id(submission);
            jdbc.update("DELETE FROM processed_search WHERE search_id = HEXTORAW(?)", id);
            jdbc.update("DELETE FROM search_outbox WHERE search_id = HEXTORAW(?)", id);
            jdbc.update("DELETE FROM search_request WHERE search_id = HEXTORAW(?)", id);
        }
        List<byte[]> buckets = jdbc.query("SELECT DISTINCT criteria_hash FROM criteria_group "
                + "WHERE JSON_VALUE(criteria_json, '$.hotelId') = ?", (row, index) -> row.getBytes(1), hotel);
        // Each test owns a unique hotel, including groups left after an integrity check.
        jdbc.update("DELETE FROM criteria_group g WHERE JSON_VALUE(criteria_json, '$.hotelId') = ? "
                + "AND NOT EXISTS (SELECT 1 FROM processed_search p WHERE p.group_id = g.group_id)", hotel);
        for (byte[] bucket : buckets) {
            jdbc.update("DELETE FROM criteria_group_lock b WHERE criteria_hash = ? "
                    + "AND NOT EXISTS (SELECT 1 FROM criteria_group g WHERE g.criteria_hash = b.criteria_hash)", bucket);
        }
    }

    @Test
    void commitsProjectionAndStatusOnce() {
        SearchSubmission submission = accept(List.of(30, 1));
        ProcessingResult first = service.process(submission);
        String processedAt = jdbc.queryForObject("SELECT processed_at FROM search_request WHERE search_id = HEXTORAW(?)",
                String.class, id(submission));
        ProcessingResult repeated = service.process(submission);
        assertAll(
                () -> assertEquals(ProcessingResult.PROCESSED, first, "La primera entrega procesa la busqueda"),
                () -> assertEquals(ProcessingResult.ALREADY_PROCESSED, repeated, "La reentrega reconoce la busqueda guardada"),
                () -> assertEquals("PROCESSED", status(submission), "El estado se confirma junto con la proyeccion"),
                () -> assertEquals(1, projections(submission), "Solo se guarda una proyeccion"),
                () -> assertNotNull(processedAt, "Se registra la fecha de procesamiento"),
                () -> assertEquals(processedAt, jdbc.queryForObject(
                        "SELECT processed_at FROM search_request WHERE search_id = HEXTORAW(?)", String.class, id(submission)), "La reentrega conserva la fecha original"),
                () -> assertEquals(new CanonicalCriteriaCodec().encode(submission.criteria()), projected(submission), "El grupo contiene los criterios aceptados"),
                () -> assertEquals("READY", jdbc.queryForObject(
                        "SELECT state FROM search_outbox WHERE search_id = HEXTORAW(?)", String.class, id(submission)), "Procesar no confirma el envio de la outbox"));
    }

    @Test
    void rejectsUnknownAlteredAndFailedSearchesWithoutProjection() {
        SearchSubmission unknown = submission(List.of(30));
        SearchSubmission accepted = accept(List.of(30, 1));
        SearchSubmission altered = new SearchSubmission(accepted.searchId(), unknown.criteria());
        SearchSubmission failed = accept(List.of(20));
        jdbc.update("UPDATE search_request SET status = 'FAILED' WHERE search_id = HEXTORAW(?)", id(failed));
        assertAll(
                () -> assertEquals(ProcessingResult.UNKNOWN_SEARCH, service.process(unknown), "No se procesa un ID desconocido"),
                () -> assertEquals(ProcessingResult.CRITERIA_MISMATCH, service.process(altered), "Se rechazan criterios alterados"),
                () -> assertEquals(ProcessingResult.FAILED_SEARCH, service.process(failed), "No se reabre una solicitud fallida"),
                () -> assertEquals("PENDING", status(accepted), "El rechazo conserva el estado pendiente"),
                () -> assertEquals("FAILED", status(failed), "El estado fallido se conserva"),
                () -> assertEquals(0, projections(accepted) + projections(failed) + projections(unknown), "Los rechazos no crean proyecciones"));
    }

    @Test
    void doesNotAcknowledgeMissingProjectionAsSuccessfulRedelivery() {
        SearchSubmission submission = accept(List.of(30));
        service.process(submission);
        jdbc.update("DELETE FROM processed_search WHERE search_id = HEXTORAW(?)", id(submission));
        assertThrows(IllegalStateException.class, () -> service.process(submission),
                "Una incoherencia almacenada debe impedir confirmar el evento");
    }

    @Test
    void rejectsProjectionAssociatedWithAnotherGroup() {
        SearchSubmission first = accept(List.of(30, 1));
        SearchSubmission second = accept(List.of(1, 30));
        service.process(first);
        service.process(second);
        jdbc.update("UPDATE processed_search SET group_id = (SELECT group_id FROM processed_search "
                + "WHERE search_id = HEXTORAW(?)) WHERE search_id = HEXTORAW(?)", id(second), id(first));
        assertAll(
                () -> assertThrows(IllegalStateException.class, () -> service.process(first)),
                () -> assertEquals(ProcessingResult.ALREADY_PROCESSED, service.process(second), "La otra busqueda conserva una proyeccion valida"),
                () -> assertEquals("PROCESSED", status(first), "Detectar corrupcion no cambia el estado"),
                () -> assertEquals(1, projections(first), "Detectar corrupcion no inserta otra proyeccion"));
    }

    @Test
    void keepsAgeOrderAndRepetitionsInSeparateGroups() {
        SearchSubmission original = accept(List.of(30, 1));
        SearchSubmission reordered = accept(List.of(1, 30));
        SearchSubmission repeated = accept(List.of(30, 1, 1));
        for (SearchSubmission submission : List.of(original, reordered, repeated)) {
            service.process(submission);
        }
        assertAll(
                () -> assertEquals(3, jdbc.queryForObject(
                        "SELECT COUNT(*) FROM criteria_group WHERE JSON_VALUE(criteria_json, '$.hotelId') = ?",
                        Integer.class, hotel), "Orden y repeticiones definen grupos diferentes"),
                () -> assertEquals(new CanonicalCriteriaCodec().encode(original.criteria()), projected(original), "Se conserva la secuencia original"),
                () -> assertEquals(new CanonicalCriteriaCodec().encode(reordered.criteria()), projected(reordered), "Se conserva la secuencia permutada"),
                () -> assertEquals(new CanonicalCriteriaCodec().encode(repeated.criteria()), projected(repeated), "Se conservan las edades repetidas"));
    }

    @Test
    void rollsBackProjectionGroupAndStatusWhenFinalWriteFails() {
        SearchSubmission submission = accept(List.of(25));
        String constraint = "reject_" + id(submission).substring(0, 20);
        jdbc.execute("ALTER TABLE search_request ADD CONSTRAINT " + constraint
                + " CHECK(search_id <> HEXTORAW('" + id(submission) + "') OR status <> 'PROCESSED')");
        assertAll(
                () -> {
                    try {
                        assertRolledBack(submission);
                    } finally {
                        jdbc.execute("ALTER TABLE search_request DROP CONSTRAINT " + constraint);
                    }
                },
                () -> assertEquals(ProcessingResult.PROCESSED, service.process(submission),
                        "Un reintento puede completar la solicitud"));
    }

    @Test
    void concurrentRedeliveryProcessesOnlyOnce() throws InterruptedException, ExecutionException, TimeoutException {
        SearchSubmission submission = accept(List.of(40));
        List<ProcessingResult> results = concurrently(java.util.Collections.nCopies(6, submission));
        assertAll(
                () -> assertEquals(1, results.stream().filter(ProcessingResult.PROCESSED::equals).count(), "Solo una entrega realiza el procesamiento"),
                () -> assertEquals(5, results.stream().filter(ProcessingResult.ALREADY_PROCESSED::equals).count(), "Las otras entregas reconocen el procesamiento"),
                () -> assertEquals(1, projections(submission), "La concurrencia no duplica la proyeccion"));
    }

    @Test
    void concurrentDistinctIdsShareExactlyOneGroup() throws InterruptedException, ExecutionException, TimeoutException {
        List<SearchSubmission> submissions = new ArrayList<>();
        for (int index = 0; index < 6; index++) {
            submissions.add(accept(List.of(30, 1)));
        }
        List<ProcessingResult> results = concurrently(submissions);
        assertAll(
                () -> assertTrue(results.stream().allMatch(ProcessingResult.PROCESSED::equals), "Cada ID distinto se procesa"),
                () -> assertEquals(6, submissions.stream().mapToInt(this::projections).sum(), "Se conservan todas las busquedas distintas"),
                () -> assertEquals(1, jdbc.queryForObject(
                        "SELECT COUNT(*) FROM criteria_group WHERE JSON_VALUE(criteria_json, '$.hotelId') = ?",
                        Integer.class, hotel), "Los criterios iguales comparten un unico grupo"));
    }

    private List<ProcessingResult> concurrently(List<SearchSubmission> submissions)
            throws InterruptedException, ExecutionException, TimeoutException {
        CountDownLatch ready = new CountDownLatch(submissions.size());
        CountDownLatch start = new CountDownLatch(1);
        try (var executor = Executors.newFixedThreadPool(submissions.size())) {
            List<Future<ProcessingResult>> futures = new ArrayList<>();
            for (SearchSubmission submission : submissions) {
                futures.add(executor.submit(() -> {
                    ready.countDown();
                    if (!start.await(10, TimeUnit.SECONDS)) {
                        throw new IllegalStateException("Processing start timed out");
                    }
                    return service.process(submission);
                }));
            }
            try {
                if (!ready.await(10, TimeUnit.SECONDS)) {
                    throw new IllegalStateException("Workers were not ready");
                }
            } finally {
                start.countDown();
            }
            List<ProcessingResult> results = new ArrayList<>();
            for (Future<ProcessingResult> future : futures) {
                results.add(future.get(30, TimeUnit.SECONDS));
            }
            return results;
        }
    }

    private void assertRolledBack(SearchSubmission submission) {
        assertAll(
                () -> assertThrows(DataIntegrityViolationException.class, () -> service.process(submission)),
                () -> assertEquals("PENDING", status(submission), "El estado se revierte al fallar"),
                () -> assertEquals(0, projections(submission), "La proyeccion se revierte al fallar"),
                () -> assertEquals(0, jdbc.queryForObject(
                        "SELECT COUNT(*) FROM criteria_group WHERE JSON_VALUE(criteria_json, '$.hotelId') = ?",
                        Integer.class, hotel), "El grupo se revierte en la misma transaccion"),
                () -> assertNull(jdbc.queryForObject(
                        "SELECT processed_at FROM search_request WHERE search_id = HEXTORAW(?)", String.class, id(submission)), "No queda fecha de procesamiento tras rollback"));
    }

    private SearchSubmission accept(List<Integer> ages) {
        SearchSubmission submission = submission(ages);
        owned.add(submission);
        new OracleAcceptanceAdapter(SOURCE).accept(submission);
        return submission;
    }

    private SearchSubmission submission(List<Integer> ages) {
        return new SearchSubmission(UUID.randomUUID(), new SearchCriteria(hotel,
                LocalDate.of(2026, 12, 1), LocalDate.of(2026, 12, 2), ages));
    }

    private String status(SearchSubmission submission) {
        return jdbc.queryForObject("SELECT status FROM search_request WHERE search_id = HEXTORAW(?)", String.class, id(submission));
    }

    private int projections(SearchSubmission submission) {
        return Objects.requireNonNull(jdbc.queryForObject(
                "SELECT COUNT(*) FROM processed_search WHERE search_id = HEXTORAW(?)", Integer.class, id(submission)));
    }

    private String projected(SearchSubmission submission) {
        return jdbc.queryForObject("SELECT g.criteria_json FROM processed_search p JOIN criteria_group g ON g.group_id = p.group_id "
                + "WHERE p.search_id = HEXTORAW(?)", String.class, id(submission));
    }

    private static String id(SearchSubmission submission) {
        return submission.searchId().toString().replace("-", "");
    }

    private static String required(String name) {
        return Objects.requireNonNull(System.getenv(name), name);
    }
}
