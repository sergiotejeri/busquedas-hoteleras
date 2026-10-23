package com.example.busquedashoteleras.infrastructure.web;

import com.example.busquedashoteleras.application.service.AcceptSearchService;
import java.util.UUID;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

@RestController
public final class SearchController {
    private final AcceptSearchService service;

    public SearchController(AcceptSearchService service) {
        this.service = service;
    }

    @PostMapping(value = "/search", consumes = "application/json", produces = "application/json")
    @ResponseStatus(HttpStatus.ACCEPTED)
    public AcceptedSearch accept(@RequestBody SearchRequest request) {
        return new AcceptedSearch(service.accept(request.toCriteria()));
    }

    public record AcceptedSearch(UUID searchId) { }
}
