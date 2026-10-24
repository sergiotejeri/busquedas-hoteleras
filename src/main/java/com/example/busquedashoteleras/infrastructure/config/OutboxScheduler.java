package com.example.busquedashoteleras.infrastructure.config;

import com.example.busquedashoteleras.application.service.PublishOutboxService;
import java.util.Objects;
import org.springframework.scheduling.annotation.Scheduled;

public final class OutboxScheduler {
    private final PublishOutboxService service;

    public OutboxScheduler(PublishOutboxService service) {
        this.service = Objects.requireNonNull(service, "service must not be null");
    }

    @Scheduled(fixedDelayString = "${app.outbox.fixed-delay:1000}")
    public void publishPending() {
        service.publishBatch(100);
    }
}
