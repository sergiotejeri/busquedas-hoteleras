package com.example.busquedashoteleras.infrastructure.oracle;

import com.example.busquedashoteleras.application.port.OutboxClaim;
import com.example.busquedashoteleras.application.port.OutboxPort;
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
            SELECT search_id, event_json FROM search_outbox
            WHERE state = 'READY' AND available_at <= SYSTIMESTAMP
            ORDER BY available_at, search_id FOR UPDATE SKIP LOCKED
            """;
    private static final String RESERVE = """
            UPDATE search_outbox SET lease_token = ?,
                available_at = SYSTIMESTAMP + NUMTODSINTERVAL(?, 'SECOND')
            WHERE search_id = ? AND state = 'READY' AND available_at <= SYSTIMESTAMP
            """;
    private static final String MARK_SENT = """
            UPDATE search_outbox SET state = 'SENT', lease_token = NULL
            WHERE search_id = ? AND lease_token = ? AND state = 'READY'
                AND available_at > SYSTIMESTAMP
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
    public Optional<OutboxClaim> claimNext(Duration lease) {
        Objects.requireNonNull(lease, "lease must not be null");
        if (lease.isZero() || lease.isNegative() || lease.compareTo(Duration.ofHours(1)) > 0) {
            throw new IllegalArgumentException("lease must be positive and no longer than one hour");
        }
        BigDecimal seconds = BigDecimal.valueOf(lease.getSeconds()).add(BigDecimal.valueOf(lease.getNano(), 9));
        return Objects.requireNonNull(transaction.execute(status -> jdbc.query(SELECT_NEXT, statement -> {
            statement.setFetchSize(1);
            statement.setMaxRows(1);
        }, rows -> {
            if (!rows.next()) {
                return Optional.empty();
            }
            ByteBuffer id = ByteBuffer.wrap(rows.getBytes(1));
            OutboxClaim claim = new OutboxClaim(new UUID(id.getLong(), id.getLong()), UUID.randomUUID(), rows.getString(2));
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
}
