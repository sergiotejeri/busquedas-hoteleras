package com.example.busquedashoteleras.infrastructure.oracle;

import com.example.busquedashoteleras.application.port.ProcessSearchPort;
import com.example.busquedashoteleras.application.port.ProcessingResult;
import com.example.busquedashoteleras.domain.SearchStatus;
import com.example.busquedashoteleras.domain.SearchSubmission;
import com.example.busquedashoteleras.infrastructure.serialization.CanonicalCriteriaCodec;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.Objects;
import javax.sql.DataSource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.support.JdbcTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.TransactionTemplate;

public final class OracleProcessingAdapter implements ProcessSearchPort {
    private static final int SINGLE_REQUEST = 1;
    private final JdbcTemplate jdbc;
    private final TransactionTemplate transaction;
    private final OracleCriteriaGroups groups;
    private final CanonicalCriteriaCodec codec = new CanonicalCriteriaCodec();

    public OracleProcessingAdapter(DataSource source) {
        Objects.requireNonNull(source, "dataSource must not be null");
        jdbc = new JdbcTemplate(source);
        groups = new OracleCriteriaGroups(source);
        transaction = new TransactionTemplate(new JdbcTransactionManager(source));
        transaction.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRES_NEW);
        transaction.setIsolationLevel(TransactionDefinition.ISOLATION_READ_COMMITTED);
        transaction.setTimeout(15);
    }

    @Override
    public ProcessingResult process(SearchSubmission submission) {
        Objects.requireNonNull(submission, "submission must not be null");
        String id = submission.searchId().toString().replace("-", "");
        String canonical = codec.encode(submission.criteria());
        return Objects.requireNonNull(transaction.execute(status -> processLocked(id, canonical)));
    }

    private ProcessingResult processLocked(String id, String canonical) {
        List<AcceptedSearch> accepted = jdbc.query(
                "SELECT criteria_json, status FROM search_request WHERE search_id = HEXTORAW(?) FOR UPDATE WAIT 5",
                (row, index) -> new AcceptedSearch(row.getString(1), SearchStatus.valueOf(row.getString(2))), id);
        if (accepted.isEmpty()) {
            return ProcessingResult.UNKNOWN_SEARCH;
        }
        AcceptedSearch request = accepted.getFirst();
        if (!canonical.equals(request.canonical())) {
            return ProcessingResult.CRITERIA_MISMATCH;
        }
        return switch (request.status()) {
            case FAILED -> ProcessingResult.FAILED_SEARCH;
            case PROCESSED -> verifyProjection(id, canonical);
            case PENDING -> persist(id, canonical);
        };
    }

    private ProcessingResult verifyProjection(String id, String canonical) {
        List<String> stored = jdbc.query("SELECT g.criteria_json FROM processed_search p "
                + "JOIN criteria_group g ON g.group_id = p.group_id WHERE p.search_id = HEXTORAW(?)",
                (row, index) -> row.getString(1), id);
        if (stored.isEmpty() || !canonical.equals(stored.getFirst())) {
            throw new IllegalStateException("Processed search has an inconsistent projection");
        }
        return ProcessingResult.ALREADY_PROCESSED;
    }

    private ProcessingResult persist(String id, String canonical) {
        byte[] groupId = groups.resolve(canonical, hash(canonical));
        jdbc.update("INSERT INTO processed_search(search_id, group_id) VALUES (HEXTORAW(?), ?)", id, groupId);
        int changed = jdbc.update("UPDATE search_request SET status = 'PROCESSED', processed_at = SYSTIMESTAMP "
                + "WHERE search_id = HEXTORAW(?) AND status = 'PENDING'", id);
        if (changed != SINGLE_REQUEST) {
            throw new IllegalStateException("Accepted search changed while processing");
        }
        return ProcessingResult.PROCESSED;
    }

    private static byte[] hash(String canonical) {
        try {
            return MessageDigest.getInstance("SHA-256").digest(canonical.getBytes(StandardCharsets.UTF_8));
        } catch (NoSuchAlgorithmException unavailable) {
            throw new IllegalStateException("SHA-256 is required", unavailable);
        }
    }

    private record AcceptedSearch(String canonical, SearchStatus status) { }
}
