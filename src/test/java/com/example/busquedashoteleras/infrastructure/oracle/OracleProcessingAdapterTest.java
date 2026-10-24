package com.example.busquedashoteleras.infrastructure.oracle;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import com.example.busquedashoteleras.application.port.ProcessingResult;
import com.example.busquedashoteleras.domain.SearchCriteria;
import com.example.busquedashoteleras.domain.SearchSubmission;
import com.example.busquedashoteleras.infrastructure.serialization.CanonicalCriteriaCodec;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDate;
import java.util.List;
import java.util.UUID;
import javax.sql.DataSource;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.NullSource;

class OracleProcessingAdapterTest {
    private static final String PENDING = "PENDING";
    private static final SearchSubmission SUBMISSION = new SearchSubmission(new UUID(0, 1),
            new SearchCriteria("hotel", LocalDate.of(2026, 12, 1), LocalDate.of(2026, 12, 2), List.of(30, 1)));
    private static final String CANONICAL = new CanonicalCriteriaCodec().encode(SUBMISSION.criteria());

    @ParameterizedTest
    @CsvSource({"PENDING,PROCESSED", "PROCESSED,ALREADY_PROCESSED", "FAILED,FAILED_SEARCH"})
    void returnsExpectedStateAndCommits(String state, ProcessingResult expected) throws SQLException {
        Fixture fixture = fixture(state, CANONICAL);
        assertAll(
                () -> assertEquals(expected, fixture.adapter().process(SUBMISSION), "El resultado refleja el estado aceptado"),
                () -> verify(fixture.connection()).commit(),
                () -> verify(fixture.connection(), never()).rollback());
    }

    @Test
    void unknownSearchDoesNotWrite() throws SQLException {
        Fixture fixture = fixture(PENDING, CANONICAL);
        rows(fixture.requestRows(), false);
        assertAll(
                () -> assertEquals(ProcessingResult.UNKNOWN_SEARCH, fixture.adapter().process(SUBMISSION), "Un ID desconocido no se procesa"),
                () -> verify(fixture.insert(), never()).executeUpdate(),
                () -> verify(fixture.update(), never()).executeUpdate());
    }

    @ParameterizedTest
    @CsvSource({"PENDING", "PROCESSED", "FAILED"})
    void alteredCriteriaNeverMutateAcceptedSearch(String state) throws SQLException {
        Fixture fixture = fixture(state, "different");
        assertAll(
                () -> assertEquals(ProcessingResult.CRITERIA_MISMATCH, fixture.adapter().process(SUBMISSION), "El mensaje debe coincidir con la solicitud"),
                () -> verify(fixture.insert(), never()).executeUpdate(),
                () -> verify(fixture.update(), never()).executeUpdate());
    }

    @ParameterizedTest
    @CsvSource({"false,unused", "true,different"})
    void inconsistentProjectionRollsBack(boolean present, String canonical) throws SQLException {
        Fixture fixture = fixture("PROCESSED", CANONICAL);
        rows(fixture.projectionRows(), present, false);
        when(fixture.projectionRows().getString(1)).thenReturn(canonical);
        assertAll(
                () -> assertThrows(IllegalStateException.class, () -> fixture.adapter().process(SUBMISSION)),
                () -> verify(fixture.connection()).rollback(),
                () -> verify(fixture.connection(), never()).commit());
    }

    @Test
    void failedStatusTransitionRollsBackProjection() throws SQLException {
        Fixture fixture = fixture(PENDING, CANONICAL);
        when(fixture.update().executeUpdate()).thenReturn(0);
        assertAll(
                () -> assertThrows(IllegalStateException.class, () -> fixture.adapter().process(SUBMISSION)),
                () -> verify(fixture.insert()).executeUpdate(),
                () -> verify(fixture.connection()).rollback(),
                () -> verify(fixture.connection(), never()).commit());
    }

    @Test
    void projectionWriteFailureRollsBackWithoutStatusChange() throws SQLException {
        Fixture fixture = fixture(PENDING, CANONICAL);
        when(fixture.insert().executeUpdate()).thenThrow(new IllegalStateException("write failed"));
        assertAll(
                () -> assertThrows(IllegalStateException.class, () -> fixture.adapter().process(SUBMISSION)),
                () -> verify(fixture.update(), never()).executeUpdate(),
                () -> verify(fixture.connection()).rollback());
    }

    @ParameterizedTest
    @NullSource
    void rejectsMissingSubmission(SearchSubmission submission) {
        DataSource source = mock(DataSource.class);
        OracleProcessingAdapter adapter = new OracleProcessingAdapter(source);
        assertAll(
                () -> assertThrows(NullPointerException.class, () -> adapter.process(submission)),
                () -> verifyNoInteractions(source));
    }

    @ParameterizedTest
    @NullSource
    void rejectsMissingDataSource(DataSource source) {
        assertThrows(NullPointerException.class, () -> new OracleProcessingAdapter(source));
    }

    @SuppressFBWarnings(value = {"OBL_UNSATISFIED_OBLIGATION", "SQL_PREPARED_STATEMENT_GENERATED_FROM_NONCONSTANT_STRING"},
            justification = "Mockito stubbing does not open JDBC resources or execute SQL")
    private static Fixture fixture(String state, String canonical) throws SQLException {
        DataSource source = mock(DataSource.class);
        Connection connection = mock(Connection.class);
        when(source.getConnection()).thenReturn(connection);
        when(connection.getAutoCommit()).thenReturn(true);
        when(connection.getTransactionIsolation()).thenReturn(Connection.TRANSACTION_READ_COMMITTED);
        PreparedStatement request = mock(PreparedStatement.class);
        PreparedStatement projection = mock(PreparedStatement.class);
        PreparedStatement group = mock(PreparedStatement.class);
        PreparedStatement insert = mock(PreparedStatement.class);
        PreparedStatement update = mock(PreparedStatement.class);
        ResultSet requestRows = mock(ResultSet.class);
        ResultSet projectionRows = mock(ResultSet.class);
        ResultSet groupRows = mock(ResultSet.class);
        when(connection.prepareStatement(anyString())).thenAnswer(invocation -> {
            String sql = invocation.getArgument(0);
            if (sql.startsWith("SELECT criteria_json")) {
                return request;
            }
            if (sql.startsWith("SELECT g.criteria_json")) {
                return projection;
            }
            if (sql.startsWith("SELECT group_id")) {
                return group;
            }
            return sql.startsWith("INSERT") ? insert : update;
        });
        when(request.executeQuery()).thenReturn(requestRows);
        when(projection.executeQuery()).thenReturn(projectionRows);
        when(group.executeQuery()).thenReturn(groupRows);
        rows(requestRows, true, false);
        rows(projectionRows, true, false);
        rows(groupRows, true, false);
        when(requestRows.getString(1)).thenReturn(canonical);
        when(requestRows.getString(2)).thenReturn(state);
        when(projectionRows.getString(1)).thenReturn(CANONICAL);
        when(groupRows.getBytes(1)).thenReturn(new byte[16]);
        when(insert.executeUpdate()).thenReturn(1);
        when(update.executeUpdate()).thenReturn(1);
        return new Fixture(new OracleProcessingAdapter(source), connection, requestRows, projectionRows, insert, update);
    }

    // Navigation is configured on a mock; no live cursor is being advanced.
    @SuppressWarnings("PMD.CheckResultSet")
    private static void rows(ResultSet result, boolean first, Boolean... rest) throws SQLException {
        when(result.next()).thenReturn(first, rest);
    }

    private record Fixture(OracleProcessingAdapter adapter, Connection connection, ResultSet requestRows,
            ResultSet projectionRows, PreparedStatement insert, PreparedStatement update) { }
}
