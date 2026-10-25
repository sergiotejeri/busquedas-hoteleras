package com.example.busquedashoteleras.infrastructure.oracle;

import com.example.busquedashoteleras.application.port.FailurePort;
import com.example.busquedashoteleras.application.port.RejectedMessage;
import java.io.StringReader;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Objects;
import javax.sql.DataSource;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.support.JdbcTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.TransactionTemplate;

public final class OracleFailureAdapter implements FailurePort {
    private final JdbcTemplate jdbc;
    private final TransactionTemplate transaction;

    public OracleFailureAdapter(DataSource source) {
        Objects.requireNonNull(source, "dataSource must not be null");
        jdbc = new JdbcTemplate(source);
        transaction = new TransactionTemplate(new JdbcTransactionManager(source));
        transaction.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRES_NEW);
        transaction.setIsolationLevel(TransactionDefinition.ISOLATION_READ_COMMITTED);
        transaction.setTimeout(15);
    }

    @Override
    public void quarantine(RejectedMessage message) {
        Objects.requireNonNull(message, "message must not be null");
        transaction.executeWithoutResult(status -> persist(message));
    }

    private void persist(RejectedMessage message) {
        try {
            jdbc.update("INSERT INTO rejected_message_receipt(topic_name, partition_no, offset_no) VALUES (?, ?, ?)",
                    message.topic(), message.partition(), message.offset());
        } catch (DuplicateKeyException replay) {
            return;
        }
        jdbc.update("INSERT INTO rejected_message(topic_name, partition_no, offset_no, event_key, raw_payload, reason_code) "
                + "VALUES (?, ?, ?, CASE WHEN ? = 1 THEN EMPTY_CLOB() ELSE ? END, "
                + "CASE WHEN ? = 1 THEN EMPTY_CLOB() ELSE ? END, ?)", statement -> {
                    statement.setString(1, message.topic());
                    statement.setInt(2, message.partition());
                    statement.setLong(3, message.offset());
                    bindText(statement, 4, message.eventKey());
                    bindText(statement, 6, message.rawPayload());
                    statement.setString(8, message.reasonCode());
                });
    }

    private static void bindText(PreparedStatement statement, int index, String value) throws SQLException {
        // Oracle binds an empty Reader as NULL; the flag preserves an empty CLOB.
        statement.setInt(index, value != null && value.isEmpty() ? 1 : 0);
        if (value == null) {
            statement.setNull(index + 1, Types.CLOB);
        } else {
            statement.setClob(index + 1, new StringReader(value));
        }
    }
}
