package com.example.busquedashoteleras.infrastructure.oracle;

import java.io.StringReader;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import javax.sql.DataSource;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.support.TransactionSynchronizationManager;

/** Resolves immutable groups in the caller's READ_COMMITTED transaction and data source. */
final class OracleCriteriaGroups {
    private final DataSource source;
    private final JdbcTemplate jdbc;

    OracleCriteriaGroups(DataSource source) {
        this.source = Objects.requireNonNull(source, "dataSource must not be null");
        jdbc = new JdbcTemplate(source);
        jdbc.setQueryTimeout(5);
    }

    byte[] resolve(String canonical, byte[] hash) {
        if (!TransactionSynchronizationManager.isActualTransactionActive()
                || !TransactionSynchronizationManager.hasResource(source)) {
            throw new IllegalStateException("Criteria groups require the caller's transaction");
        }
        List<byte[]> existing = find(canonical, hash);
        if (!existing.isEmpty()) {
            return existing.getFirst();
        }
        lockBucket(hash);
        existing = find(canonical, hash);
        if (!existing.isEmpty()) {
            return existing.getFirst();
        }
        UUID identifier = UUID.randomUUID();
        byte[] groupId = ByteBuffer.allocate(16).putLong(identifier.getMostSignificantBits())
                .putLong(identifier.getLeastSignificantBits()).array();
        jdbc.update("INSERT INTO criteria_group(group_id, criteria_hash, criteria_json) VALUES (?, ?, ?)", statement -> {
            statement.setBytes(1, groupId);
            statement.setBytes(2, hash);
            statement.setClob(3, new StringReader(canonical));
        });
        return groupId;
    }

    private void lockBucket(byte[] hash) {
        try {
            jdbc.update("INSERT INTO criteria_group_lock(criteria_hash) VALUES (?)", hash);
        } catch (DuplicateKeyException existingBucket) {
            // A new insert already owns the row; an existing bucket needs an explicit lock.
            jdbc.queryForObject("SELECT criteria_hash FROM criteria_group_lock WHERE criteria_hash = ? FOR UPDATE WAIT 5",
                    byte[].class, hash);
        }
    }

    private List<byte[]> find(String canonical, byte[] hash) {
        return jdbc.query("SELECT group_id FROM criteria_group WHERE criteria_hash = ? AND DBMS_LOB.COMPARE(criteria_json, ?) = 0",
                statement -> {
                    statement.setBytes(1, hash);
                    statement.setClob(2, new StringReader(canonical));
                }, (row, index) -> row.getBytes(1));
    }
}
