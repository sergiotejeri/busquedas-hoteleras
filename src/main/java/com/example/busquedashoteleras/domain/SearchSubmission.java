package com.example.busquedashoteleras.domain;

import java.util.Objects;
import java.util.UUID;

public record SearchSubmission(UUID searchId, SearchCriteria criteria) {
    public SearchSubmission {
        Objects.requireNonNull(searchId, "searchId must not be null");
        Objects.requireNonNull(criteria, "criteria must not be null");
    }
}
