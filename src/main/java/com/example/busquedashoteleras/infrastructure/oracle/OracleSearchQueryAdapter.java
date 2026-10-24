package com.example.busquedashoteleras.infrastructure.oracle;

import com.example.busquedashoteleras.application.port.SearchQueryPort;
import com.example.busquedashoteleras.domain.CountResult;
import com.example.busquedashoteleras.domain.SearchStatus;
import com.example.busquedashoteleras.infrastructure.serialization.CanonicalCriteriaCodec;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import javax.sql.DataSource;
import org.springframework.core.io.ClassPathResource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

public final class OracleSearchQueryAdapter implements SearchQueryPort {
    private final NamedParameterJdbcTemplate jdbc;
    private final String query;
    private final CanonicalCriteriaCodec codec = new CanonicalCriteriaCodec();

    public OracleSearchQueryAdapter(DataSource source) {
        Objects.requireNonNull(source, "dataSource must not be null");
        JdbcTemplate template = new JdbcTemplate(source);
        template.setQueryTimeout(5);
        jdbc = new NamedParameterJdbcTemplate(template);
        query = loadQuery();
    }

    @Override
    public Optional<CountResult> find(UUID searchId) {
        Objects.requireNonNull(searchId, "searchId must not be null");
        var parameters = Map.of("searchId", searchId.toString().replace("-", ""));
        return Objects.requireNonNull(jdbc.query(query, parameters, rows -> {
            if (!rows.next()) {
                return Optional.empty();
            }
            return Optional.of(readResult(searchId, rows));
        }));
    }

    private CountResult readResult(UUID searchId, ResultSet row) throws SQLException {
        SearchStatus status = SearchStatus.valueOf(row.getString("status"));
        if (status != SearchStatus.PROCESSED) {
            return new CountResult(searchId, status, null, null);
        }
        String canonical = row.getString("criteria_json");
        if (canonical == null || row.getInt("criteria_comparison") != 0) {
            throw new IllegalStateException("Processed search has an inconsistent projection");
        }
        return new CountResult(searchId, status, codec.decode(canonical), row.getLong("search_count"));
    }

    private static String loadQuery() {
        try {
            return new ClassPathResource("db/queries/find_count_result.sql").getContentAsString(StandardCharsets.UTF_8);
        } catch (IOException exception) {
            throw new IllegalStateException("Could not load search count query", exception);
        }
    }
}
