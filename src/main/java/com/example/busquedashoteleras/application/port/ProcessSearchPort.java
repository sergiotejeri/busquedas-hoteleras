package com.example.busquedashoteleras.application.port;

import com.example.busquedashoteleras.domain.SearchSubmission;

@FunctionalInterface
public interface ProcessSearchPort {
    ProcessingResult process(SearchSubmission submission);
}
