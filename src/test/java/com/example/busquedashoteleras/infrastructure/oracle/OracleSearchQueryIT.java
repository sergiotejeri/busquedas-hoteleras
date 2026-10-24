package com.example.busquedashoteleras.infrastructure.oracle;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.example.busquedashoteleras.domain.CountResult;
import com.example.busquedashoteleras.domain.SearchCriteria;
import com.example.busquedashoteleras.domain.SearchStatus;
import com.example.busquedashoteleras.domain.SearchSubmission;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.core.io.ClassPathResource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.jdbc.support.JdbcTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

class OracleSearchQueryIT {
    private static final DriverManagerDataSource SOURCE = new DriverManagerDataSource(
            required("TEST_DB_URL"), required("TEST_DB_USER"), required("TEST_DB_PASSWORD"));
    private static final String OWN_REQUESTS =
            "SELECT search_id FROM search_request WHERE JSON_VALUE(criteria_json, '$.hotelId') = ?";
    private final JdbcTemplate jdbc = new JdbcTemplate(SOURCE);
    private final OracleSearchQueryAdapter query = new OracleSearchQueryAdapter(SOURCE);
    private final String hotel = UUID.randomUUID() + "' OR '1'='1 界";

    @BeforeAll
    static void migrateQuerySchema() {
        Flyway.configure().dataSource(SOURCE).cleanDisabled(true).load().migrate();
    }

    @AfterEach
    void removeOwnData() {
        List<byte[]> hashes = jdbc.query("SELECT DISTINCT criteria_hash FROM criteria_group "
                + "WHERE JSON_VALUE(criteria_json, '$.hotelId') = ?", (row, index) -> row.getBytes(1), hotel);
        for (String table : List.of("processed_search", "search_outbox", "search_request")) {
            jdbc.update("DELETE FROM " + table + " WHERE search_id IN (" + OWN_REQUESTS + ")", hotel);
        }
        jdbc.update("DELETE FROM criteria_group WHERE JSON_VALUE(criteria_json, '$.hotelId') = ?", hotel);
        for (byte[] hash : hashes) {
            jdbc.update("DELETE FROM criteria_group_lock b WHERE b.criteria_hash = ? "
                    + "AND NOT EXISTS (SELECT 1 FROM criteria_group g WHERE g.criteria_hash = b.criteria_hash)", hash);
        }
    }

    @Test
    void countIncludesOnlyProcessedSearchesAndNotRedeliveries() {
        SearchSubmission first = accept(List.of(30, 1));
        SearchSubmission second = accept(List.of(30, 1));
        OracleProcessingAdapter processor = new OracleProcessingAdapter(SOURCE);
        processor.process(first);
        CountResult before = query.find(first.searchId()).orElseThrow();
        CountResult pending = query.find(second.searchId()).orElseThrow();
        processor.process(second);
        processor.process(first);
        assertAll(
                () -> assertEquals(1L, before.count(), "Una aceptacion pendiente no cuenta"),
                () -> assertEquals(SearchStatus.PENDING, pending.status(), "El segundo ID aun estaba pendiente"),
                () -> assertNull(pending.count(), "Pendiente no devuelve un cero ficticio"),
                () -> assertEquals(2L, query.find(first.searchId()).orElseThrow().count(), "Dos IDs procesados cuentan dos veces"),
                () -> assertEquals(2L, query.find(second.searchId()).orElseThrow().count(), "Ambos IDs ven el mismo grupo"),
                () -> assertEquals(first.criteria(), before.search(), "Conserva Unicode y texto parecido a SQL como datos"));
    }

    @Test
    void differentAgeSequencesRemainSeparate() {
        SearchSubmission first = accept(List.of(30, 1));
        SearchSubmission reordered = accept(List.of(1, 30));
        SearchSubmission repeated = accept(List.of(30, 1, 1));
        OracleProcessingAdapter processor = new OracleProcessingAdapter(SOURCE);
        for (SearchSubmission submission : List.of(first, reordered, repeated)) {
            processor.process(submission);
        }
        assertAll(
                () -> assertEquals(1L, query.find(first.searchId()).orElseThrow().count(), "El orden original tiene su grupo"),
                () -> assertEquals(1L, query.find(reordered.searchId()).orElseThrow().count(), "La permutacion no se mezcla"),
                () -> assertEquals(1L, query.find(repeated.searchId()).orElseThrow().count(), "Las repeticiones no se pierden"),
                () -> assertEquals(reordered.criteria(), query.find(reordered.searchId()).orElseThrow().search(), "Devuelve el orden original"));
    }

    @Test
    void distinguishesUnknownPendingAndFailedWithoutCounting() {
        SearchSubmission pending = accept(List.of(20));
        SearchSubmission failed = accept(List.of(40));
        jdbc.update("UPDATE search_request SET status = 'FAILED' WHERE search_id = HEXTORAW(?)", hex(failed));
        CountResult result = query.find(failed.searchId()).orElseThrow();
        assertAll(
                () -> assertTrue(query.find(UUID.randomUUID()).isEmpty(), "Un ID desconocido no existe"),
                () -> assertEquals(SearchStatus.PENDING, query.find(pending.searchId()).orElseThrow().status(), "La aceptacion existe"),
                () -> assertEquals(SearchStatus.FAILED, result.status(), "Un fallo persistido no es pendiente"),
                () -> assertNull(result.search(), "Un fallo no expone resultados"),
                () -> assertNull(result.count(), "Un fallo no inventa conteo"));
    }

    @Test
    void refusesProcessedStateWithoutProjection() {
        SearchSubmission submission = accept(List.of(18));
        jdbc.update("UPDATE search_request SET status = 'PROCESSED' WHERE search_id = HEXTORAW(?)", hex(submission));
        assertThrows(IllegalStateException.class, () -> query.find(submission.searchId()));
    }

    @Test
    void refusesProjectionFromDifferentCriteria() {
        SearchSubmission first = accept(List.of(18));
        SearchSubmission second = accept(List.of(19));
        OracleProcessingAdapter processor = new OracleProcessingAdapter(SOURCE);
        processor.process(first);
        processor.process(second);
        jdbc.update("UPDATE processed_search SET group_id = (SELECT group_id FROM processed_search "
                + "WHERE search_id = HEXTORAW(?)) WHERE search_id = HEXTORAW(?)", hex(second), hex(first));
        assertThrows(IllegalStateException.class, () -> query.find(first.searchId()));
    }

    @Test
    void queryHasAValidGroupIndexAndAnOraclePlan() throws IOException {
        SearchSubmission submission = accept(List.of(21));
        new OracleProcessingAdapter(SOURCE).process(submission);
        String sql = new ClassPathResource("db/queries/find_count_result.sql").getContentAsString(StandardCharsets.UTF_8);
        String planId = "count_" + hex(submission).substring(0, 20);
        new TransactionTemplate(new JdbcTransactionManager(SOURCE)).executeWithoutResult(status -> {
            jdbc.execute("EXPLAIN PLAN SET STATEMENT_ID = '" + planId + "' FOR "
                    + sql.replace(":searchId", "'" + hex(submission) + "'"));
            try {
                List<String> plan = jdbc.queryForList(
                        "SELECT plan_table_output FROM TABLE(DBMS_XPLAN.DISPLAY('PLAN_TABLE', ?, 'BASIC'))", String.class, planId);
                assertAll(
                        () -> assertEquals("GROUP_ID", jdbc.queryForObject(
                                "SELECT column_name FROM user_ind_columns WHERE index_name = 'PROCESSED_SEARCH_GROUP_IX' "
                                + "AND column_position = 1", String.class), "El indice comienza por el grupo usado en el conteo"),
                        () -> assertEquals("VALID", jdbc.queryForObject(
                                "SELECT status FROM user_indexes WHERE index_name = 'PROCESSED_SEARCH_GROUP_IX'", String.class), "El indice esta disponible"),
                        () -> assertTrue(plan.stream().anyMatch(line -> line.contains("SELECT STATEMENT")), "Oracle puede planificar la consulta: " + plan));
                System.getLogger(OracleSearchQueryIT.class.getName()).log(System.Logger.Level.INFO,
                        "Oracle count plan:{0}{1}", System.lineSeparator(), String.join(System.lineSeparator(), plan));
            } finally {
                jdbc.update("DELETE FROM plan_table WHERE statement_id = ?", planId);
            }
        });
    }

    private SearchSubmission accept(List<Integer> ages) {
        var submission = new SearchSubmission(UUID.randomUUID(), new SearchCriteria(hotel,
                LocalDate.of(2026, 12, 1), LocalDate.of(2026, 12, 2), ages));
        new OracleAcceptanceAdapter(SOURCE).accept(submission);
        return submission;
    }

    private static String hex(SearchSubmission submission) {
        return submission.searchId().toString().replace("-", "");
    }

    private static String required(String name) {
        return Objects.requireNonNull(System.getenv(name), name);
    }
}
