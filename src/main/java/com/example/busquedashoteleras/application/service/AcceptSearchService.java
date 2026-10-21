package com.example.busquedashoteleras.application.service;

import com.example.busquedashoteleras.application.port.AcceptSearchPort;
import com.example.busquedashoteleras.domain.SearchCriteria;
import com.example.busquedashoteleras.domain.SearchSubmission;
import java.util.Objects;
import java.util.UUID;
import java.util.function.Supplier;

public final class AcceptSearchService {
    private final AcceptSearchPort acceptPort;
    private final Supplier<UUID> idGenerator;

    public AcceptSearchService(AcceptSearchPort acceptPort, Supplier<UUID> idGenerator) {
        this.acceptPort = Objects.requireNonNull(acceptPort, "acceptPort must not be null");
        this.idGenerator = Objects.requireNonNull(idGenerator, "idGenerator must not be null");
    }

    public UUID accept(SearchCriteria criteria) {
        Objects.requireNonNull(criteria, "criteria must not be null");
        UUID searchId = Objects.requireNonNull(idGenerator.get(), "generated searchId must not be null");
        acceptPort.accept(new SearchSubmission(searchId, criteria));
        return searchId;
    }
}
