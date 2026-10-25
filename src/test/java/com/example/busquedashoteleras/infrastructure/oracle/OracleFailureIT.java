package com.example.busquedashoteleras.infrastructure.oracle;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.example.busquedashoteleras.application.port.RejectedMessage;
import com.example.busquedashoteleras.domain.SearchCriteria;
import com.example.busquedashoteleras.domain.SearchStatus;
import com.example.busquedashoteleras.domain.SearchSubmission;
import java.time.LocalDate;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.jdbc.support.JdbcTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

class OracleFailureIT {
    private static final String INVALID_REASON = "INVALID_EVENT";
    private static final String AUDIT_TABLE = "rejected_message";
    private static final String RECEIPT_TABLE = "rejected_message_receipt";
    private static final DriverManagerDataSource SOURCE = new DriverManagerDataSource(
            required("TEST_DB_URL"), required("TEST_DB_USER"), required("TEST_DB_PASSWORD"));
    private final JdbcTemplate jdbc = new JdbcTemplate(SOURCE);
    private final OracleFailureAdapter adapter = new OracleFailureAdapter(SOURCE);
    private final String topic = "quarantine_" + UUID.randomUUID();
    private final UUID searchId = UUID.randomUUID();

    @BeforeAll
    static void migrateQuarantine() {
        Flyway.configure().dataSource(SOURCE).cleanDisabled(true).load().migrate();
    }

    @AfterEach
    void removeOwnedRows() {
        jdbc.update("DELETE FROM rejected_message WHERE topic_name = ?", topic);
        jdbc.update("DELETE FROM rejected_message_receipt WHERE topic_name = ?", topic);
        String id = searchId.toString().replace("-", "");
        List<byte[]> groups = jdbc.query("SELECT group_id FROM processed_search WHERE search_id = HEXTORAW(?)",
                (row, index) -> row.getBytes(1), id);
        for (String table : List.of("processed_search", "search_outbox", "search_request")) {
            jdbc.update("DELETE FROM " + table + " WHERE search_id = HEXTORAW(?)", id);
        }
        for (byte[] group : groups) {
            byte[] hash = jdbc.queryForObject("SELECT criteria_hash FROM criteria_group WHERE group_id = ?", byte[].class, group);
            jdbc.update("DELETE FROM criteria_group WHERE group_id = ?", group);
            jdbc.update("DELETE FROM criteria_group_lock b WHERE criteria_hash = ? "
                    + "AND NOT EXISTS (SELECT 1 FROM criteria_group g WHERE g.criteria_hash = b.criteria_hash)", hash);
        }
    }

    @Test
    void storesFullTextAndKeepsFirstRejectionOnReplay() {
        String payload = "{invalid ' OR '1'='1 界".repeat(3000);
        String key = "clave界".repeat(600);
        var first = new RejectedMessage(topic, Integer.MAX_VALUE, Long.MAX_VALUE, key, payload, INVALID_REASON);
        adapter.quarantine(first);
        adapter.quarantine(new RejectedMessage(topic, Integer.MAX_VALUE, Long.MAX_VALUE, "other", "other", "ALTERED_EVENT"));
        assertAll(
                () -> assertEquals(1, count(AUDIT_TABLE), "La reentrega no duplica la auditoria"),
                () -> assertEquals(1, count(RECEIPT_TABLE), "Solo hay un comprobante por coordenadas"),
                () -> assertEquals(payload, text("raw_payload"), "Conserva el CLOB completo sin truncar"),
                () -> assertEquals(key, text("event_key"), "Conserva la clave completa"),
                () -> assertEquals(INVALID_REASON, text("reason_code"), "Conserva el primer motivo"),
                () -> assertEquals(Long.MAX_VALUE, jdbc.queryForObject(
                        "SELECT offset_no FROM rejected_message WHERE topic_name = ?", Long.class, topic), "Conserva los 64 bits del offset"),
                () -> assertNotNull(text("created_at"), "Registra cuando se aislo el mensaje"));
    }

    @Test
    void supportsNullPayloadAndKey() {
        adapter.quarantine(new RejectedMessage(topic, 0, 0, null, null, INVALID_REASON));
        assertAll(
                () -> assertNull(text("raw_payload"), "Un mensaje nulo sigue siendo auditable"),
                () -> assertNull(text("event_key"), "Una clave nula sigue siendo auditable"),
                () -> assertEquals(1, count(AUDIT_TABLE), "No se descarta el rechazo"));
    }

    @Test
    void preservesEmptyTextInsteadOfConvertingItToNull() {
        adapter.quarantine(new RejectedMessage(topic, 0, 0, "", "", INVALID_REASON));
        assertAll(
                () -> assertEquals(0L, jdbc.queryForObject(
                        "SELECT CASE WHEN event_key IS NULL THEN -1 ELSE DBMS_LOB.GETLENGTH(event_key) END "
                        + "FROM rejected_message WHERE topic_name = ?", Long.class, topic), "Una clave vacia no es una clave nula"),
                () -> assertEquals(0L, jdbc.queryForObject(
                        "SELECT CASE WHEN raw_payload IS NULL THEN -1 ELSE DBMS_LOB.GETLENGTH(raw_payload) END "
                        + "FROM rejected_message WHERE topic_name = ?", Long.class, topic), "Un contenido vacio no es un tombstone"));
    }
    @Test
    void partitionsAndOffsetsIdentifyDifferentDeliveries() {
        adapter.quarantine(message(0, 0));
        adapter.quarantine(message(0, 1));
        adapter.quarantine(message(1, 0));
        assertEquals(3, count(AUDIT_TABLE), "La identidad incluye particion y offset");
    }

    @Test
    void replayDoesNotRecreateAnAuditRemovedAfterRetention() {
        adapter.quarantine(message(0, 0));
        jdbc.update("DELETE FROM rejected_message WHERE topic_name = ?", topic);
        adapter.quarantine(message(0, 0));
        assertAll(
                () -> assertEquals(0, count(AUDIT_TABLE), "El comprobante evita recrear auditoria eliminada"),
                () -> assertEquals(1, count(RECEIPT_TABLE), "El comprobante sigue presente"));
    }

    @ParameterizedTest
    @EnumSource(SearchStatus.class)
    void untrustedIdentifierNeverChangesAnAcceptedSearch(SearchStatus status) {
        var submission = new SearchSubmission(searchId, new SearchCriteria(topic, LocalDate.of(2026, 12, 1),
                LocalDate.of(2026, 12, 2), List.of(30)));
        new OracleAcceptanceAdapter(SOURCE).accept(submission);
        if (status == SearchStatus.PROCESSED) {
            new OracleProcessingAdapter(SOURCE).process(submission);
        } else if (status == SearchStatus.FAILED) {
            jdbc.update("UPDATE search_request SET status = 'FAILED' WHERE search_id = HEXTORAW(?)", searchId.toString().replace("-", ""));
        }
        adapter.quarantine(new RejectedMessage(topic, 0, 0, searchId.toString(),
                "{\"searchId\":\"" + searchId + "\",\"altered\":true}", "ALTERED_EVENT"));
        var result = new OracleSearchQueryAdapter(SOURCE).find(searchId).orElseThrow();
        assertAll(
                () -> assertEquals(status, result.status(), "Una clave ajena no autoriza cambiar la solicitud"),
                () -> assertEquals(status == SearchStatus.PROCESSED ? 1L : null, result.count(), "La cuarentena no cambia el conteo"),
                () -> assertEquals(1, count(AUDIT_TABLE), "Se conserva el evento rechazado"));
    }

    @Test
    void auditFailureRollsBackReceiptAndAllowsRetry() {
        String constraint = "reject_" + searchId.toString().replace("-", "").substring(0, 20);
        jdbc.execute("ALTER TABLE rejected_message ADD CONSTRAINT " + constraint + " CHECK(topic_name <> '" + topic + "')");
        assertAll(
                () -> {
                    try {
                        assertAll(
                                () -> assertThrows(DataIntegrityViolationException.class, () -> adapter.quarantine(message(0, 0))),
                                () -> assertEquals(0, count(RECEIPT_TABLE), "El fallo revierte el comprobante"),
                                () -> assertEquals(0, count(AUDIT_TABLE), "No queda auditoria parcial"));
                    } finally {
                        jdbc.execute("ALTER TABLE rejected_message DROP CONSTRAINT " + constraint);
                    }
                },
                () -> {
                    adapter.quarantine(message(0, 0));
                    assertEquals(1, count(AUDIT_TABLE), "El reintento puede guardar el rechazo completo");
                });
    }

    @Test
    void returnIsDurableEvenWhenCallerRollsBack() {
        new TransactionTemplate(new JdbcTransactionManager(SOURCE)).executeWithoutResult(status -> {
            adapter.quarantine(message(0, 0));
            status.setRollbackOnly();
        });
        assertAll(
                () -> assertEquals(1, count(RECEIPT_TABLE), "El comprobante se confirma antes de retornar"),
                () -> assertEquals(1, count(AUDIT_TABLE), "La auditoria se confirma antes de retornar"));
    }

    @Test
    void concurrentReplayCreatesOneReceiptAndOneAudit() throws InterruptedException, ExecutionException {
        CyclicBarrier start = new CyclicBarrier(6);
        List<Callable<Void>> tasks = IntStream.range(0, 6).mapToObj(index -> (Callable<Void>) () -> {
            start.await(10, TimeUnit.SECONDS);
            adapter.quarantine(message(0, 0));
            return null;
        }).toList();
        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            for (var result : executor.invokeAll(tasks, 30, TimeUnit.SECONDS)) {
                result.get();
            }
        }
        assertAll(
                () -> assertEquals(1, count(RECEIPT_TABLE), "Se serializa el primer registro"),
                () -> assertEquals(1, count(AUDIT_TABLE), "Solo una entrega guarda auditoria"));
    }

    private RejectedMessage message(int partition, long offset) {
        return new RejectedMessage(topic, partition, offset, "key", "{invalid", INVALID_REASON);
    }

    private int count(String table) {
        return Objects.requireNonNull(jdbc.queryForObject("SELECT COUNT(*) FROM " + table + " WHERE topic_name = ?", Integer.class, topic));
    }

    private String text(String column) {
        return jdbc.queryForObject("SELECT " + column + " FROM rejected_message WHERE topic_name = ?", String.class, topic);
    }

    private static String required(String name) {
        return Objects.requireNonNull(System.getenv(name), name);
    }
}
