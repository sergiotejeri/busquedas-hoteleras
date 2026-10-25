package com.example.busquedashoteleras.infrastructure.oracle;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import com.example.busquedashoteleras.application.port.OutboxClaim;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.util.UUID;
import java.util.stream.Stream;
import javax.sql.DataSource;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.NullSource;
import org.junit.jupiter.params.provider.ValueSource;

class OracleOutboxAdapterTest {
    private static final UUID ID = new UUID(0, 1);
    private static final String EVENT = "{\"searchId\":\"payload\"}";

    @Test
    void returnsCommittedClaimAndPreservesFractionalLease() throws SQLException {
        DataSource source = mock(DataSource.class);
        JdbcFixture fixture = connection(source, true);
        OutboxClaim claim = new OracleOutboxAdapter(source).claimNext(Duration.ofMillis(500)).orElseThrow();
        assertAll(
                () -> assertEquals(ID, claim.searchId(), "Debe devolver la identidad almacenada"),
                () -> assertEquals(EVENT, claim.eventJson(), "Debe conservar el evento"),
                () -> verify(fixture.statement()).setObject(2, new BigDecimal("0.500000000")),
                () -> verify(fixture.connection()).commit());
    }

    @Test
    void returnsEmptyWhenThereIsNoAvailableWork() throws SQLException {
        DataSource source = mock(DataSource.class);
        JdbcFixture fixture = connection(source, false);
        assertAll(
                () -> assertTrue(new OracleOutboxAdapter(source).claimNext(Duration.ofSeconds(30)).isEmpty(),
                        "No debe inventar reservas"),
                () -> verify(fixture.statement(), never()).executeUpdate());
    }

    @Test
    void doesNotReturnOwnershipWhenEligibilityChanged() throws SQLException {
        DataSource source = mock(DataSource.class);
        JdbcFixture fixture = connection(source, true);
        when(fixture.statement().executeUpdate()).thenReturn(0);
        assertTrue(new OracleOutboxAdapter(source).claimNext(Duration.ofSeconds(30)).isEmpty(),
                "No debe devolver un token si la fila dejo de ser elegible");
    }

    @Test
    void rollsBackWhenReservationCannotBeStored() throws SQLException {
        DataSource source = mock(DataSource.class);
        JdbcFixture fixture = connection(source, true);
        IllegalStateException failure = new IllegalStateException("write failed");
        when(fixture.statement().executeUpdate()).thenThrow(failure);
        OracleOutboxAdapter adapter = new OracleOutboxAdapter(source);
        assertAll(
                () -> assertSame(failure, assertThrows(IllegalStateException.class,
                        () -> adapter.claimNext(Duration.ofSeconds(30))), "Debe propagar el fallo"),
                () -> verify(fixture.connection()).rollback(),
                () -> verify(fixture.connection(), never()).commit());
    }

    @ParameterizedTest
    @ValueSource(ints = {0, 1})
    void reportsWhetherConfirmationWasApplied(int changedRows) throws SQLException {
        DataSource source = mock(DataSource.class);
        JdbcFixture fixture = connection(source, false);
        when(fixture.statement().executeUpdate()).thenReturn(changedRows);
        boolean marked = new OracleOutboxAdapter(source).markSent(ID, UUID.randomUUID());
        assertAll(
                () -> assertEquals(changedRows == 1, marked, "Una reserva ajena no debe aparecer confirmada"),
                () -> verify(fixture.connection()).commit());
    }

    @ParameterizedTest
    @MethodSource("invalidLeases")
    void rejectsInvalidLeaseWithoutOpeningConnection(Duration lease) {
        DataSource source = mock(DataSource.class);
        OracleOutboxAdapter adapter = new OracleOutboxAdapter(source);
        assertAll(
                () -> assertThrows(IllegalArgumentException.class, () -> adapter.claimNext(lease),
                        "La reserva debe ser positiva y acotada"),
                () -> verifyNoInteractions(source));
    }

    @ParameterizedTest
    @NullSource
    void rejectsMissingLease(Duration lease) {
        OracleOutboxAdapter adapter = new OracleOutboxAdapter(mock(DataSource.class));
        assertThrows(NullPointerException.class, () -> adapter.claimNext(lease), "La duracion es obligatoria");
    }

    @ParameterizedTest
    @NullSource
    void rejectsMissingIdentityOrToken(UUID missing) {
        DataSource source = mock(DataSource.class);
        OracleOutboxAdapter adapter = new OracleOutboxAdapter(source);
        assertAll(
                () -> assertThrows(NullPointerException.class, () -> adapter.markSent(missing, ID),
                        "La identidad es obligatoria"),
                () -> assertThrows(NullPointerException.class, () -> adapter.markSent(ID, missing),
                        "El token es obligatorio"),
                () -> verifyNoInteractions(source));
    }

    @ParameterizedTest
    @NullSource
    void rejectsMissingDataSource(DataSource source) {
        assertThrows(NullPointerException.class, () -> new OracleOutboxAdapter(source),
                "La conexion es obligatoria");
    }

    private static Stream<Duration> invalidLeases() {
        return Stream.of(Duration.ZERO, Duration.ofNanos(-1), Duration.ofHours(2));
    }

    @ParameterizedTest
    @ValueSource(ints = {-1, 0, 101, Integer.MAX_VALUE})
    void rejectsUnboundedRecoveryBeforeOpeningConnection(int limit) {
        DataSource source = mock(DataSource.class);
        assertAll(
                () -> assertThrows(IllegalArgumentException.class,
                        () -> new OracleOutboxAdapter(source).recoverPending(limit, Duration.ofMinutes(5))),
                () -> verifyNoInteractions(source));
    }

    @ParameterizedTest
    @MethodSource("invalidRecoveryAges")
    void rejectsInvalidRecoveryAgeBeforeOpeningConnection(Duration age) {
        DataSource source = mock(DataSource.class);
        assertAll(
                () -> assertThrows(IllegalArgumentException.class,
                        () -> new OracleOutboxAdapter(source).recoverPending(1, age)),
                () -> verifyNoInteractions(source));
    }

    @ParameterizedTest
    @NullSource
    void requiresRecoveryAge(Duration missing) {
        DataSource source = mock(DataSource.class);
        assertAll(
                () -> assertThrows(NullPointerException.class,
                        () -> new OracleOutboxAdapter(source).recoverPending(1, missing)),
                () -> verifyNoInteractions(source));
    }

    private static Stream<Duration> invalidRecoveryAges() {
        return Stream.of(Duration.ZERO, Duration.ofNanos(-1), Duration.ofDays(1).plusNanos(1));
    }

    @ParameterizedTest
    @MethodSource("invalidRetryDelays")
    void rejectsInvalidRetryWithoutOpeningConnection(Duration delay) {
        DataSource source = mock(DataSource.class);
        OracleOutboxAdapter adapter = new OracleOutboxAdapter(source);
        assertAll(
                () -> assertThrows(IllegalArgumentException.class, () -> adapter.retry(ID, ID, delay)),
                () -> verifyNoInteractions(source));
    }

    @ParameterizedTest
    @NullSource
    void requiresRetryAndFailureIdentities(UUID missing) {
        DataSource source = mock(DataSource.class);
        OracleOutboxAdapter adapter = new OracleOutboxAdapter(source);
        assertAll(
                () -> assertThrows(NullPointerException.class, () -> adapter.retry(missing, ID, Duration.ofSeconds(1))),
                () -> assertThrows(NullPointerException.class, () -> adapter.retry(ID, missing, Duration.ofSeconds(1))),
                () -> assertThrows(NullPointerException.class, () -> adapter.markFailed(missing, ID)),
                () -> assertThrows(NullPointerException.class, () -> adapter.markFailed(ID, missing)),
                () -> verifyNoInteractions(source));
    }

    @ParameterizedTest
    @NullSource
    void requiresRetryDelay(Duration missing) {
        DataSource source = mock(DataSource.class);
        assertAll(
                () -> assertThrows(NullPointerException.class, () -> new OracleOutboxAdapter(source).retry(ID, ID, missing)),
                () -> verifyNoInteractions(source));
    }

    @Test
    void retryPreservesFractionalDelay() throws SQLException {
        DataSource source = mock(DataSource.class);
        JdbcFixture fixture = connection(source, false);
        assertAll(
                () -> assertTrue(new OracleOutboxAdapter(source).retry(ID, ID, Duration.ofMillis(500)), "Debe aplazar"),
                () -> verify(fixture.statement()).setObject(1, new BigDecimal("0.500000000")),
                () -> verify(fixture.connection()).commit());
    }

    private static Stream<Duration> invalidRetryDelays() {
        return Stream.of(Duration.ZERO, Duration.ofNanos(-1), Duration.ofSeconds(30).plusNanos(1));
    }

    @SuppressFBWarnings(value = {"OBL_UNSATISFIED_OBLIGATION", "SQL_PREPARED_STATEMENT_GENERATED_FROM_NONCONSTANT_STRING"},
            justification = "Mockito stubbing does not open JDBC resources or execute the matched SQL")
    // Se configura next() en un mock; no se navega un ResultSet real.
    @SuppressWarnings("PMD.CheckResultSet")
    private static JdbcFixture connection(DataSource source, boolean hasRow) throws SQLException {
        Connection connection = mock(Connection.class);
        PreparedStatement statement = mock(PreparedStatement.class);
        ResultSet rows = mock(ResultSet.class);
        when(source.getConnection()).thenReturn(connection);
        when(connection.getAutoCommit()).thenReturn(true);
        when(connection.prepareStatement(anyString())).thenReturn(statement);
        when(statement.executeQuery()).thenReturn(rows);
        when(statement.executeUpdate()).thenReturn(1);
        when(rows.next()).thenReturn(hasRow);
        when(rows.getBytes(1)).thenReturn(new byte[]{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1});
        when(rows.getString(2)).thenReturn(EVENT);
        return new JdbcFixture(connection, statement);
    }

    private record JdbcFixture(Connection connection, PreparedStatement statement) { }
}
