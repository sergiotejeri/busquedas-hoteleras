package com.example.busquedashoteleras.infrastructure.oracle;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import com.example.busquedashoteleras.application.port.RejectedMessage;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import javax.sql.DataSource;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullSource;
import org.springframework.dao.DuplicateKeyException;

class OracleFailureAdapterTest {
    private static final RejectedMessage MESSAGE = new RejectedMessage("topic", 0, 1, "key", "{invalid", "INVALID_EVENT");

    @Test
    void commitsReceiptAndAuditBeforeReturning() throws SQLException {
        Fixture fixture = fixture();
        fixture.adapter().quarantine(MESSAGE);
        assertAll(
                () -> verify(fixture.receipt()).executeUpdate(),
                () -> verify(fixture.audit()).executeUpdate(),
                () -> verify(fixture.connection()).commit());
    }

    @Test
    void repeatedReceiptDoesNotWriteAnotherAudit() throws SQLException {
        Fixture fixture = fixture();
        when(fixture.receipt().executeUpdate()).thenThrow(new DuplicateKeyException("duplicate"));
        fixture.adapter().quarantine(MESSAGE);
        assertAll(
                () -> verify(fixture.receipt()).executeUpdate(),
                () -> verify(fixture.audit(), never()).executeUpdate(),
                () -> verify(fixture.connection()).commit());
    }

    @Test
    void receiptFailureIsNotMistakenForReplay() throws SQLException {
        Fixture fixture = fixture();
        var failure = new IllegalStateException("receipt unavailable");
        when(fixture.receipt().executeUpdate()).thenThrow(failure);
        assertAll(
                () -> assertSame(failure, assertThrows(IllegalStateException.class,
                        () -> fixture.adapter().quarantine(MESSAGE)), "Un fallo de almacenamiento no es una reentrega"),
                () -> verify(fixture.audit(), never()).executeUpdate(),
                () -> verify(fixture.connection()).rollback(),
                () -> verify(fixture.connection(), never()).commit());
    }
    @Test
    void auditFailureRollsBackReceiptAndPropagates() throws SQLException {
        Fixture fixture = fixture();
        var failure = new IllegalStateException("audit failed");
        when(fixture.audit().executeUpdate()).thenThrow(failure);
        assertAll(
                () -> assertSame(failure, assertThrows(IllegalStateException.class,
                        () -> fixture.adapter().quarantine(MESSAGE)), "No confirma un rechazo sin auditoria"),
                () -> verify(fixture.connection()).rollback(),
                () -> verify(fixture.connection(), never()).commit());
    }

    @Test
    void duplicateAuditIsAnIntegrityErrorNotAReplayedReceipt() throws SQLException {
        Fixture fixture = fixture();
        when(fixture.audit().executeUpdate()).thenThrow(new DuplicateKeyException("inconsistent audit"));
        assertAll(
                () -> assertThrows(DuplicateKeyException.class, () -> fixture.adapter().quarantine(MESSAGE)),
                () -> verify(fixture.connection()).rollback());
    }

    @Test
    void acceptsNullKeyAndPayload() throws SQLException {
        Fixture fixture = fixture();
        fixture.adapter().quarantine(new RejectedMessage("topic", 0, 2, null, null, "INVALID_EVENT"));
        assertAll(
                () -> verify(fixture.audit()).setNull(5, java.sql.Types.CLOB),
                () -> verify(fixture.audit()).setNull(7, java.sql.Types.CLOB),
                () -> verify(fixture.connection()).commit());
    }

    @Test
    void distinguishesEmptyTextFromNullAtBinding() throws SQLException {
        Fixture fixture = fixture();
        fixture.adapter().quarantine(new RejectedMessage("topic", 0, 2, "", "", "INVALID_EVENT"));
        assertAll(
                () -> verify(fixture.audit()).setInt(4, 1),
                () -> verify(fixture.audit()).setInt(6, 1),
                () -> verify(fixture.connection()).commit());
    }
    @ParameterizedTest
    @NullSource
    void rejectsMissingMessage(RejectedMessage message) {
        DataSource source = mock(DataSource.class);
        OracleFailureAdapter adapter = new OracleFailureAdapter(source);
        assertAll(
                () -> assertThrows(NullPointerException.class, () -> adapter.quarantine(message)),
                () -> verifyNoInteractions(source));
    }

    @ParameterizedTest
    @NullSource
    void rejectsMissingDataSource(DataSource source) {
        assertThrows(NullPointerException.class, () -> new OracleFailureAdapter(source));
    }

    @SuppressFBWarnings(value = {"OBL_UNSATISFIED_OBLIGATION", "SQL_PREPARED_STATEMENT_GENERATED_FROM_NONCONSTANT_STRING"},
            justification = "Mockito stubbing does not open JDBC resources or execute SQL")
    private static Fixture fixture() throws SQLException {
        DataSource source = mock(DataSource.class);
        Connection connection = mock(Connection.class);
        PreparedStatement receipt = mock(PreparedStatement.class);
        PreparedStatement audit = mock(PreparedStatement.class);
        when(source.getConnection()).thenReturn(connection);
        when(connection.getAutoCommit()).thenReturn(true);
        when(connection.getTransactionIsolation()).thenReturn(Connection.TRANSACTION_READ_COMMITTED);
        when(connection.prepareStatement(anyString())).thenAnswer(invocation -> {
            String sql = invocation.getArgument(0);
            return sql.contains("rejected_message_receipt") ? receipt : audit;
        });
        when(receipt.executeUpdate()).thenReturn(1);
        when(audit.executeUpdate()).thenReturn(1);
        return new Fixture(new OracleFailureAdapter(source), connection, receipt, audit);
    }

    private record Fixture(OracleFailureAdapter adapter, Connection connection,
            PreparedStatement receipt, PreparedStatement audit) { }
}
