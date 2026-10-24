package com.example.busquedashoteleras.application.service;

import com.example.busquedashoteleras.application.port.SearchQueryPort;
import com.example.busquedashoteleras.domain.CountResult;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

public final class GetSearchCountService {
    private final SearchQueryPort port;

    public GetSearchCountService(SearchQueryPort port) {
        this.port = Objects.requireNonNull(port, "port must not be null");
    }

    public Optional<CountResult> find(UUID searchId) {
        Objects.requireNonNull(searchId, "searchId must not be null");
        return Objects.requireNonNull(port.find(searchId), "query result must not be null");
    }
}
