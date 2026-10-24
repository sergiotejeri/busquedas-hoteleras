package com.example.busquedashoteleras.infrastructure.oracle;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.startsWith;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLIntegrityConstraintViolationException;
import javax.sql.DataSource;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.jdbc.support.JdbcTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

class OracleCriteriaGroupsTest {
    private static final byte[] HASH = new byte[32];
    private static final byte[] EXISTING = {1, 2, 3};
    private static final String CRITERIA = "canonical";

    @Test
    void requiresCallerTransactionBeforeTouchingDatabase() {
        DataSource source = mock(DataSource.class);
        var groups = new OracleCriteriaGroups(source);
        assertAll(
                () -> assertThrows(IllegalStateException.class, () -> groups.resolve(CRITERIA, HASH),
                        "Debe exigir la transaccion del procesamiento"),
                () -> verifyNoInteractions(source));
    }

    @Test
    void doesNotUseTransactionBelongingToAnotherDataSource() throws SQLException {
        Fixture fixture = fixture();
        DataSource other = mock(DataSource.class);
        var groups = new OracleCriteriaGroups(other);
        transaction(fixture).executeWithoutResult(status -> assertThrows(IllegalStateException.class,
                () -> groups.resolve(CRITERIA, HASH), "La transaccion debe pertenecer a la misma fuente"));
        verifyNoInteractions(other);
    }

    @Test
    void reusesExistingGroupWithoutBucketWrite() throws SQLException {
        Fixture fixture = fixture();
        rows(fixture.matches(), true, false);
        when(fixture.matches().getBytes(1)).thenReturn(EXISTING);
        byte[] result = transaction(fixture).execute(status -> new OracleCriteriaGroups(fixture.source()).resolve(CRITERIA, HASH));
        assertAll(
                () -> assertArrayEquals(EXISTING, result, "Reutiliza el grupo encontrado"),
                () -> verify(fixture.bucketInsert(), never()).executeUpdate(),
                () -> verify(fixture.groupInsert(), never()).executeUpdate());
    }

    @Test
    void rechecksAfterAnotherTransactionCreatedTheGroup() throws SQLException {
        Fixture fixture = fixture();
        rows(fixture.matches(), false, true, false);
        when(fixture.matches().getBytes(1)).thenReturn(EXISTING);
        when(fixture.bucketInsert().executeUpdate()).thenThrow(new SQLIntegrityConstraintViolationException("duplicate", "23505", 1));
        byte[] result = transaction(fixture).execute(status -> new OracleCriteriaGroups(fixture.source()).resolve(CRITERIA, HASH));
        assertAll(
                () -> assertArrayEquals(EXISTING, result, "Debe releer despues de adquirir el bloqueo"),
                () -> verify(fixture.groupInsert(), never()).executeUpdate());
    }

    @Test
    void insertsNewGroupInsideCallerTransaction() throws SQLException {
        Fixture fixture = fixture();
        byte[] result = transaction(fixture).execute(status -> new OracleCriteriaGroups(fixture.source()).resolve(CRITERIA, HASH));
        var inserted = ArgumentCaptor.forClass(byte[].class);

        assertAll(
                () -> {
                    verify(fixture.groupInsert()).setBytes(org.mockito.ArgumentMatchers.eq(1), inserted.capture());
                    assertArrayEquals(inserted.getValue(), result, "Devuelve el identificador insertado");
                },
                () -> assertEquals(16, result.length, "Identificador UUID"),
                () -> verify(fixture.groupInsert()).executeUpdate());
    }

    private static TransactionTemplate transaction(Fixture fixture) {
        return new TransactionTemplate(new JdbcTransactionManager(fixture.source()));
    }

    @SuppressFBWarnings(value = {"OBL_UNSATISFIED_OBLIGATION", "SQL_PREPARED_STATEMENT_GENERATED_FROM_NONCONSTANT_STRING"},
            justification = "Mockito stubbing does not open JDBC resources or execute SQL")
    private static Fixture fixture() throws SQLException {
        DataSource source = mock(DataSource.class);
        Connection connection = mock(Connection.class);
        when(source.getConnection()).thenReturn(connection);
        when(connection.getAutoCommit()).thenReturn(true);
        PreparedStatement lookup = mock(PreparedStatement.class);
        PreparedStatement bucketInsert = mock(PreparedStatement.class);
        PreparedStatement groupInsert = mock(PreparedStatement.class);
        PreparedStatement lock = mock(PreparedStatement.class);
        when(connection.prepareStatement(startsWith("SELECT group_id"))).thenReturn(lookup);
        when(connection.prepareStatement(startsWith("INSERT INTO criteria_group_lock"))).thenReturn(bucketInsert);
        when(connection.prepareStatement(startsWith("INSERT INTO criteria_group("))).thenReturn(groupInsert);
        when(connection.prepareStatement(startsWith("SELECT criteria_hash"))).thenReturn(lock);
        ResultSet matches = mock(ResultSet.class);
        when(lookup.executeQuery()).thenReturn(matches);
        ResultSet locked = mock(ResultSet.class);
        ResultSetMetaData metadata = mock(ResultSetMetaData.class);
        when(metadata.getColumnCount()).thenReturn(1);
        when(locked.getMetaData()).thenReturn(metadata);
        rows(locked, true, false);
        when(locked.getBytes(1)).thenReturn(HASH);
        when(lock.executeQuery()).thenReturn(locked);
        return new Fixture(source, matches, bucketInsert, groupInsert);
    }

    // Configures Mockito navigation; no real ResultSet is consumed here.
    @SuppressWarnings("PMD.CheckResultSet")
    private static void rows(ResultSet result, boolean first, Boolean... remaining) throws SQLException {
        when(result.next()).thenReturn(first, remaining);
    }
    private record Fixture(DataSource source, ResultSet matches, PreparedStatement bucketInsert, PreparedStatement groupInsert) { }
}
