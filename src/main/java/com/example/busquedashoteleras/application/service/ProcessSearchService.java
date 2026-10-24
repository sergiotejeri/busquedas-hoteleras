package com.example.busquedashoteleras.application.service;

import com.example.busquedashoteleras.application.port.ProcessSearchPort;
import com.example.busquedashoteleras.application.port.ProcessingResult;
import com.example.busquedashoteleras.domain.SearchSubmission;
import java.util.Objects;

public final class ProcessSearchService {
    private final ProcessSearchPort port;

    public ProcessSearchService(ProcessSearchPort port) {
        this.port = Objects.requireNonNull(port, "port must not be null");
    }

    public ProcessingResult process(SearchSubmission submission) {
        Objects.requireNonNull(submission, "submission must not be null");
        return Objects.requireNonNull(port.process(submission), "processing result must not be null");
    }
}
