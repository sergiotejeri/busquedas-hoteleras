package com.example.busquedashoteleras.infrastructure.oracle;

import com.example.busquedashoteleras.application.port.OutboxClaim;
import com.example.busquedashoteleras.application.port.OutboxPort;
import com.example.busquedashoteleras.domain.SearchStatus;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import javax.sql.DataSource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.support.JdbcTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.TransactionTemplate;

public final class OracleOutboxAdapter implements OutboxPort {
    private static final String SELECT_NEXT = """
            SELECT search_id, event_json, retry_attempts FROM search_outbox
            WHERE state = 'READY' AND available_at <= SYSTIMESTAMP
            ORDER BY available_at, search_id FOR UPDATE SKIP LOCKED
            """;
    private static final String RESERVE = """
            UPDATE search_outbox SET lease_token = ?,
                available_at = SYSTIMESTAMP + NUMTODSINTERVAL(?, 'SECOND')
            WHERE search_id = ? AND state = 'READY' AND available_at <= SYSTIMESTAMP
            """;
    private static final String MARK_SENT = """
            UPDATE search_outbox SET state = 'SENT', lease_token = NULL, available_at = SYSTIMESTAMP
            WHERE search_id = ? AND lease_token = ? AND state = 'READY'
                AND available_at > SYSTIMESTAMP
            """;
    private static final String DEFER_EVENT = """
            UPDATE search_outbox SET lease_token = NULL, retry_attempts = LEAST(retry_attempts + 1, 6),
                available_at = SYSTIMESTAMP + NUMTODSINTERVAL(?, 'SECOND')
            WHERE search_id = ? AND lease_token = ? AND state = 'READY' AND available_at > SYSTIMESTAMP
            """;
    private static final String CLOSE = """
            UPDATE search_outbox SET state = ?, lease_token = NULL
            WHERE search_id = ? AND lease_token = ? AND state = 'READY' AND available_at > SYSTIMESTAMP
            """;
    private static final String SELECT_RECOVERABLE = """
            SELECT r.search_id FROM search_outbox o
            JOIN search_request r ON r.search_id = o.search_id
            WHERE o.state = 'SENT' AND r.status = 'PENDING'
                AND o.available_at <= SYSTIMESTAMP - NUMTODSINTERVAL(?, 'SECOND')
            ORDER BY o.available_at, o.search_id FOR UPDATE OF r.status SKIP LOCKED
            """;
    private static final String RECOVER = """
            UPDATE search_outbox o SET state = 'READY', lease_token = NULL, available_at = SYSTIMESTAMP
            WHERE o.search_id = ? AND o.state = 'SENT'
                AND o.available_at <= SYSTIMESTAMP - NUMTODSINTERVAL(?, 'SECOND')
                AND EXISTS (SELECT 1 FROM search_request r WHERE r.search_id = o.search_id AND r.status = 'PENDING')
            """;
    private final JdbcTemplate jdbc;
    private final TransactionTemplate transaction;

    public OracleOutboxAdapter(DataSource source) {
        Objects.requireNonNull(source, "dataSource must not be null");
        jdbc = new JdbcTemplate(source);
        transaction = new TransactionTemplate(new JdbcTransactionManager(source));
        transaction.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRES_NEW);
        transaction.setTimeout(15);
    }

    @Override
    public int recoverPending(int limit, Duration minimumAge) {
        Objects.requireNonNull(minimumAge, "minimumAge must not be null");
        if (limit < 1 || limit > 100 || minimumAge.isZero() || minimumAge.isNegative()
                || minimumAge.compareTo(Duration.ofDays(1)) > 0) {
            throw new IllegalArgumentException("recovery needs a batch of 1 to 100 and an age greater than zero up to one day");
        }
        BigDecimal ageSeconds = seconds(minimumAge);
        // Lock requests first, as processing and permanent failure do; skip active consumers.
        return Objects.requireNonNull(transaction.execute(status -> jdbc.query(SELECT_RECOVERABLE, statement -> {
            statement.setBigDecimal(1, ageSeconds);
            statement.setFetchSize(1);
            statement.setMaxRows(limit);
        }, rows -> {
            int recovered = 0;
            while (rows.next()) {
                recovered += jdbc.update(RECOVER, rows.getBytes(1), ageSeconds);
            }
            return recovered;
        })), "Recovery transaction must return a result");
    }

    @Override
    public Optional<OutboxClaim> claimNext(Duration lease) {
        Objects.requireNonNull(lease, "lease must not be null");
        if (lease.isZero() || lease.isNegative() || lease.compareTo(Duration.ofHours(1)) > 0) {
            throw new IllegalArgumentException("lease must be positive and no longer than one hour");
        }
        BigDecimal seconds = seconds(lease);
        return Objects.requireNonNull(transaction.execute(status -> jdbc.query(SELECT_NEXT, statement -> {
            statement.setFetchSize(1);
            statement.setMaxRows(1);
        }, rows -> {
            if (!rows.next()) {
                return Optional.empty();
            }
            ByteBuffer id = ByteBuffer.wrap(rows.getBytes(1));
            OutboxClaim claim = new OutboxClaim(new UUID(id.getLong(), id.getLong()), UUID.randomUUID(),
                    rows.getString(2), rows.getInt(3));
            int updated = jdbc.update(RESERVE, raw(claim.token()), seconds, raw(claim.searchId()));
            return updated == 1 ? Optional.of(claim) : Optional.empty();
        })), "Reservation transaction must return a result");
    }

    @Override
    public boolean markSent(UUID searchId, UUID token) {
        Objects.requireNonNull(searchId, "searchId must not be null");
        Objects.requireNonNull(token, "token must not be null");
        return Boolean.TRUE.equals(transaction.execute(status -> jdbc.update(MARK_SENT, raw(searchId), raw(token)) == 1));
    }

    private static byte[] raw(UUID id) {
        return ByteBuffer.allocate(16).putLong(id.getMostSignificantBits()).putLong(id.getLeastSignificantBits()).array();
    }

    @Override
    public boolean retry(UUID searchId, UUID token, Duration delay) {
        Objects.requireNonNull(searchId, "searchId must not be null");
        Objects.requireNonNull(token, "token must not be null");
        Objects.requireNonNull(delay, "delay must not be null");
        if (delay.isZero() || delay.isNegative() || delay.compareTo(Duration.ofSeconds(30)) > 0) {
            throw new IllegalArgumentException("delay must be positive and no longer than 30 seconds");
        }
        return Boolean.TRUE.equals(transaction.execute(status ->
                jdbc.update(DEFER_EVENT, seconds(delay), raw(searchId), raw(token)) == 1));
    }

    @Override
    public boolean markFailed(UUID searchId, UUID token) {
        Objects.requireNonNull(searchId, "searchId must not be null");
        Objects.requireNonNull(token, "token must not be null");
        return Boolean.TRUE.equals(transaction.execute(status -> closeFailedEvent(searchId, token)));
    }

    private boolean closeFailedEvent(UUID searchId, UUID token) {
        // El consumidor tambien debera bloquear primero la solicitud y despues la outbox.
        var states = jdbc.query("SELECT status FROM search_request WHERE search_id = ? FOR UPDATE",
                (row, index) -> row.getString(1), raw(searchId));
        if (states.isEmpty()) {
            return false;
        }
        SearchStatus requestState = SearchStatus.valueOf(states.getFirst());
        String outboxState = requestState == SearchStatus.PROCESSED ? "SENT" : "FAILED";
        boolean closed = jdbc.update(CLOSE, outboxState, raw(searchId), raw(token)) == 1;
        if (!closed) {
            return false;
        }
        if (requestState == SearchStatus.PENDING) {
            jdbc.update("UPDATE search_request SET status = 'FAILED' WHERE search_id = ?", raw(searchId));
        }
        return true;
    }

    private static BigDecimal seconds(Duration duration) {
        return BigDecimal.valueOf(duration.getSeconds()).add(BigDecimal.valueOf(duration.getNano(), 9));
    }
}
