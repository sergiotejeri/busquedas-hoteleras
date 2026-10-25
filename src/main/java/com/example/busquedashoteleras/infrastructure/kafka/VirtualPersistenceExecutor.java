package com.example.busquedashoteleras.infrastructure.kafka;

import com.example.busquedashoteleras.application.port.ProcessingResult;
import com.example.busquedashoteleras.application.service.ProcessSearchService;
import com.example.busquedashoteleras.domain.SearchSubmission;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public final class VirtualPersistenceExecutor implements AutoCloseable {
    private final ProcessSearchService service;
    private final ExecutorService executor;

    public VirtualPersistenceExecutor(ProcessSearchService service) {
        this(service, Executors.newVirtualThreadPerTaskExecutor());
    }

    VirtualPersistenceExecutor(ProcessSearchService service, ExecutorService executor) {
        this.service = Objects.requireNonNull(service, "service must not be null");
        this.executor = Objects.requireNonNull(executor, "executor must not be null");
    }

    public ProcessingResult process(SearchSubmission submission) {
        Future<ProcessingResult> task = executor.submit(() -> service.process(submission));
        try {
            return task.get(30, TimeUnit.SECONDS);
        } catch (InterruptedException interrupted) {
            task.cancel(true);
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while waiting for persistence", interrupted);
        } catch (TimeoutException timeout) {
            task.cancel(true);
            throw new IllegalStateException("Persistence did not finish within 30 seconds", timeout);
        } catch (ExecutionException failed) {
            throw new IllegalStateException("Persistence failed", failed);
        }
    }

    @Override
    public void close() {
        executor.close();
    }
}
