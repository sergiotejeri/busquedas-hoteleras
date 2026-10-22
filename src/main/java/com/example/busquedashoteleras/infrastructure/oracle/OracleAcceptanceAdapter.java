package com.example.busquedashoteleras.infrastructure.oracle;

import com.example.busquedashoteleras.application.port.AcceptSearchPort;
import com.example.busquedashoteleras.domain.InvalidSearchException;
import com.example.busquedashoteleras.domain.SearchSubmission;
import com.example.busquedashoteleras.infrastructure.serialization.CanonicalCriteriaCodec;
import com.example.busquedashoteleras.infrastructure.serialization.SearchEventCodec;
import java.nio.charset.StandardCharsets;
import java.sql.Types;
import java.util.Objects;
import javax.sql.DataSource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.SqlParameterValue;
import org.springframework.jdbc.support.JdbcTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.TransactionTemplate;

public final class OracleAcceptanceAdapter implements AcceptSearchPort {
    private static final int MAX_EVENT_BYTES = 262144;
    private static final String INSERT_REQUEST =
            "INSERT INTO search_request (search_id, criteria_json, status) VALUES (HEXTORAW(?), ?, 'PENDING')";
    private static final String INSERT_OUTBOX =
            "INSERT INTO search_outbox (search_id, event_json) VALUES (HEXTORAW(?), ?)";

    private final JdbcTemplate jdbc;
    private final TransactionTemplate transaction;
    private final CanonicalCriteriaCodec criteriaCodec = new CanonicalCriteriaCodec();
    private final SearchEventCodec eventCodec = new SearchEventCodec();

    public OracleAcceptanceAdapter(DataSource dataSource) {
        Objects.requireNonNull(dataSource, "dataSource must not be null");
        jdbc = new JdbcTemplate(dataSource);
        transaction = new TransactionTemplate(new JdbcTransactionManager(dataSource));
        transaction.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRES_NEW);
        transaction.setTimeout(15);
    }

    @Override
    public void accept(SearchSubmission submission) {
        Objects.requireNonNull(submission, "submission must not be null");
        byte[] eventBytes = eventCodec.encode(submission);
        if (eventBytes.length > MAX_EVENT_BYTES) {
            throw new InvalidSearchException("REQUEST_TOO_LARGE", null, "Search event exceeds the size limit");
        }
        String eventJson = new String(eventBytes, StandardCharsets.UTF_8);
        String criteriaJson = criteriaCodec.encode(submission.criteria());
        String id = submission.searchId().toString().replace("-", "");
        transaction.executeWithoutResult(status -> {
            jdbc.update(INSERT_REQUEST, id, new SqlParameterValue(Types.CLOB, criteriaJson));
            jdbc.update(INSERT_OUTBOX, id, new SqlParameterValue(Types.CLOB, eventJson));
        });
    }
}
