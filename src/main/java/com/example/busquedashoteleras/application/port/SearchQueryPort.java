package com.example.busquedashoteleras.application.port;

import com.example.busquedashoteleras.domain.CountResult;
import java.util.Optional;
import java.util.UUID;

@FunctionalInterface
public interface SearchQueryPort {
    Optional<CountResult> find(UUID searchId);
}
