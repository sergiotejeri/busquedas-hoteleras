package com.example.busquedashoteleras.infrastructure.oracle;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import com.example.busquedashoteleras.domain.InvalidSearchException;
import com.example.busquedashoteleras.domain.SearchSubmission;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.UUID;
import javax.sql.DataSource;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullSource;
import org.springframework.transaction.CannotCreateTransactionException;

class OracleAcceptanceAdapterTest {
    @Test
    void commitsBeforeReturning() throws SQLException {
        DataSource source = mock(DataSource.class);
        JdbcFixture fixture = connection(source);
        OracleAcceptanceAdapter adapter = new OracleAcceptanceAdapter(source);
        adapter.accept(AcceptanceFixtures.submission(UUID.randomUUID(), "hotel"));
        verify(fixture.connection()).commit();
    }

    @Test
    void rollsBackWhenAWriteFails() throws SQLException {
        DataSource source = mock(DataSource.class);
        JdbcFixture fixture = connection(source);
        IllegalStateException failure = new IllegalStateException("write failed");
        when(fixture.statement().executeUpdate()).thenThrow(failure);
        OracleAcceptanceAdapter adapter = new OracleAcceptanceAdapter(source);
        assertAll(
                () -> assertSame(failure, assertThrows(IllegalStateException.class,
                        () -> adapter.accept(AcceptanceFixtures.submission(UUID.randomUUID(), "hotel"))),
                        "Debe conservar el fallo de escritura"),
                () -> verify(fixture.connection()).rollback(),
                () -> verify(fixture.connection(), never()).commit());
    }

    @Test
    void rejectsOversizedUtf8BeforeOpeningConnection() {
        DataSource source = mock(DataSource.class);
        OracleAcceptanceAdapter adapter = new OracleAcceptanceAdapter(source);
        SearchSubmission submission = AcceptanceFixtures.sizedSubmission(UUID.randomUUID(), 262145);
        assertAll(
                () -> {
                    InvalidSearchException failure = assertThrows(InvalidSearchException.class,
                            () -> adapter.accept(submission));
                    assertAll(
                            () -> assertEquals("REQUEST_TOO_LARGE", failure.getCode(), "Codigo del limite operativo"),
                            () -> assertNull(failure.getField(), "El exceso afecta al evento completo"));
                },
                () -> verifyNoInteractions(source));
    }

    @Test
    void propagatesConnectionFailure() throws SQLException {
        DataSource source = mock(DataSource.class);
        when(source.getConnection()).thenThrow(new SQLException("unavailable"));
        OracleAcceptanceAdapter adapter = new OracleAcceptanceAdapter(source);
        assertThrows(CannotCreateTransactionException.class,
                () -> adapter.accept(AcceptanceFixtures.submission(UUID.randomUUID(), "hotel")),
                "No debe comunicar aceptacion sin conexion");
    }

    @ParameterizedTest
    @NullSource
    void rejectsMissingSubmission(SearchSubmission submission) {
        DataSource source = mock(DataSource.class);
        OracleAcceptanceAdapter adapter = new OracleAcceptanceAdapter(source);
        assertAll(
                () -> assertThrows(NullPointerException.class, () -> adapter.accept(submission),
                        "La solicitud es obligatoria"),
                () -> verifyNoInteractions(source));
    }

    @ParameterizedTest
    @NullSource
    void rejectsMissingDataSource(DataSource source) {
        assertThrows(NullPointerException.class, () -> new OracleAcceptanceAdapter(source),
                "La conexion es obligatoria");
    }

    @SuppressFBWarnings(value = {"OBL_UNSATISFIED_OBLIGATION", "SQL_PREPARED_STATEMENT_GENERATED_FROM_NONCONSTANT_STRING"},
            justification = "Mockito stubbing does not open JDBC resources or execute the matched SQL")
    private static JdbcFixture connection(DataSource source) throws SQLException {
        Connection connection = mock(Connection.class);
        PreparedStatement statement = mock(PreparedStatement.class);
        when(source.getConnection()).thenReturn(connection);
        when(connection.getAutoCommit()).thenReturn(true);
        when(connection.prepareStatement(anyString())).thenReturn(statement);
        when(statement.executeUpdate()).thenReturn(1);
        return new JdbcFixture(connection, statement);
    }

    private record JdbcFixture(Connection connection, PreparedStatement statement) {
    }
}
