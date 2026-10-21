package com.example.busquedashoteleras.domain;

import java.util.Objects;
import java.util.UUID;

public record CountResult(UUID searchId, SearchStatus status, SearchCriteria search, Long count) {
    public CountResult {
        Objects.requireNonNull(searchId, "searchId must not be null");
        Objects.requireNonNull(status, "status must not be null");
        if (status == SearchStatus.PROCESSED) {
            if (search == null || count == null || count < 1) {
                throw new IllegalArgumentException("processed result requires criteria and positive count");
            }
        } else if (search != null || count != null) {
            throw new IllegalArgumentException("non-processed result must not expose criteria or count");
        }
    }
}
