package com.example.busquedashoteleras.infrastructure.oracle;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import com.example.busquedashoteleras.domain.SearchCriteria;
import com.example.busquedashoteleras.domain.SearchStatus;
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
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.NullSource;
import org.springframework.dao.DataAccessException;

class OracleSearchQueryAdapterTest {
    private static final UUID ID = new UUID(0, 1);
    private static final SearchCriteria CRITERIA = new SearchCriteria("hotel", LocalDate.of(2026, 12, 1),
            LocalDate.of(2026, 12, 2), List.of(30, 1));

    @Test
    void mapsProcessedResultWithoutTruncatingLongCount() throws SQLException {
        Fixture fixture = fixture(SearchStatus.PROCESSED);
        long count = (long) Integer.MAX_VALUE + 1;
        when(fixture.rows().getLong("search_count")).thenReturn(count);
        var result = fixture.adapter().find(ID).orElseThrow();
        assertAll(
                () -> assertEquals(ID, result.searchId(), "Conserva el ID solicitado"),
                () -> assertEquals(CRITERIA, result.search(), "Decodifica los criterios originales"),
                () -> assertEquals(count, result.count(), "El conteo usa 64 bits"),
                () -> assertEquals(SearchStatus.PROCESSED, result.status(), "Conserva el estado procesado"),
                () -> verify(fixture.statement()).setString(1, "00000000000000000000000000000001"),
                () -> verifySingleQuery(fixture.statement()));
    }

    @ParameterizedTest
    @EnumSource(value = SearchStatus.class, names = {"PENDING", "FAILED"})
    void nonProcessedStatesDoNotExposeCriteriaOrCount(SearchStatus status) throws SQLException {
        Fixture fixture = fixture(status);
        var result = fixture.adapter().find(ID).orElseThrow();
        assertAll(
                () -> assertEquals(status, result.status(), "Conserva el estado sin inventar conteo"),
                () -> assertNull(result.search(), "No expone criterios sin procesar"),
                () -> assertNull(result.count(), "Pendiente no equivale a cero"));
    }

    @Test
    void missingRowReturnsEmptyAfterOneQuery() throws SQLException {
        Fixture fixture = fixture(SearchStatus.PENDING);
        rows(fixture.rows(), false);
        assertAll(
                () -> assertTrue(fixture.adapter().find(ID).isEmpty(), "No inventa una solicitud ausente"),
                () -> verifySingleQuery(fixture.statement()));
    }

    @ParameterizedTest
    @CsvSource({",0", "different,1"})
    void rejectsMissingOrMismatchingProjection(String canonical, int comparison) throws SQLException {
        Fixture fixture = fixture(SearchStatus.PROCESSED);
        when(fixture.rows().getString("criteria_json")).thenReturn(canonical);
        when(fixture.rows().getInt("criteria_comparison")).thenReturn(comparison);
        assertThrows(IllegalStateException.class, () -> fixture.adapter().find(ID));
    }

    @Test
    @SuppressFBWarnings(value = "OBL_UNSATISFIED_OBLIGATION",
            justification = "Mockito configures a failing query; no JDBC resource is opened")
    void databaseFailureIsNotNotFound() throws SQLException {
        Fixture fixture = fixture(SearchStatus.PENDING);
        when(fixture.statement().executeQuery()).thenThrow(new SQLException("connection lost", "08006"));
        assertThrows(DataAccessException.class, () -> fixture.adapter().find(ID));
    }

    @ParameterizedTest
    @NullSource
    void requiresIdBeforeOpeningConnection(UUID id) {
        DataSource source = mock(DataSource.class);
        OracleSearchQueryAdapter adapter = new OracleSearchQueryAdapter(source);
        assertAll(
                () -> assertThrows(NullPointerException.class, () -> adapter.find(id)),
                () -> verifyNoInteractions(source));
    }

    @ParameterizedTest
    @NullSource
    void requiresDataSource(DataSource source) {
        assertThrows(NullPointerException.class, () -> new OracleSearchQueryAdapter(source));
    }

    @SuppressFBWarnings(value = {"OBL_UNSATISFIED_OBLIGATION", "SQL_PREPARED_STATEMENT_GENERATED_FROM_NONCONSTANT_STRING"},
            justification = "Mockito stubbing does not open JDBC resources or execute SQL")
    private static Fixture fixture(SearchStatus status) throws SQLException {
        DataSource source = mock(DataSource.class);
        Connection connection = mock(Connection.class);
        PreparedStatement statement = mock(PreparedStatement.class);
        ResultSet result = mock(ResultSet.class);
        when(source.getConnection()).thenReturn(connection);
        when(connection.prepareStatement(anyString())).thenReturn(statement);
        when(statement.executeQuery()).thenReturn(result);
        rows(result, true, false);
        when(result.getString("status")).thenReturn(status.name());
        when(result.getString("criteria_json")).thenReturn(new CanonicalCriteriaCodec().encode(CRITERIA));
        return new Fixture(new OracleSearchQueryAdapter(source), statement, result);
    }

    @SuppressFBWarnings(value = "OBL_UNSATISFIED_OBLIGATION",
            justification = "Mockito verifies a call; it does not execute a JDBC query")
    private static void verifySingleQuery(PreparedStatement statement) throws SQLException {
        verify(statement).executeQuery();
    }
    // This only configures a mock cursor.
    @SuppressWarnings("PMD.CheckResultSet")
    private static void rows(ResultSet result, boolean first, Boolean... rest) throws SQLException {
        when(result.next()).thenReturn(first, rest);
    }

    private record Fixture(OracleSearchQueryAdapter adapter, PreparedStatement statement, ResultSet rows) { }
}
