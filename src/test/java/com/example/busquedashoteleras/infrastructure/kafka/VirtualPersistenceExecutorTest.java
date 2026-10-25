package com.example.busquedashoteleras.infrastructure.kafka;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.example.busquedashoteleras.application.port.ProcessingResult;
import com.example.busquedashoteleras.application.service.ProcessSearchService;
import com.example.busquedashoteleras.domain.SearchCriteria;
import com.example.busquedashoteleras.domain.SearchSubmission;
import java.time.LocalDate;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

class VirtualPersistenceExecutorTest {
    private final SearchSubmission submission = new SearchSubmission(UUID.randomUUID(),
            new SearchCriteria("executor", LocalDate.of(2026, 12, 1), LocalDate.of(2026, 12, 3), List.of(30)));

    @Test
    void waitsForPersistenceOnVirtualThreadAndReturnsItsResult() {
        var virtual = new AtomicBoolean();
        var service = new ProcessSearchService(search -> {
            virtual.set(Thread.currentThread().isVirtual());
            return ProcessingResult.PROCESSED;
        });
        try (var executor = new VirtualPersistenceExecutor(service)) {
            var result = executor.process(submission);
            assertAll(
                    () -> assertEquals(ProcessingResult.PROCESSED, result, "Devuelve el resultado confirmado"),
                    () -> assertTrue(virtual.get(), "La persistencia se ejecuta en un hilo virtual"));
        }
    }

    @Test
    void preservesStorageFailureForRetry() {
        var failure = new IllegalStateException("storage failed");
        try (var executor = new VirtualPersistenceExecutor(new ProcessSearchService(search -> { throw failure; }))) {
            assertAll(() -> assertSame(failure,
                    assertThrows(IllegalStateException.class, () -> executor.process(submission), "Propaga el fallo")
                            .getCause().getCause(), "El error de ejecucion debe conservar la causa original"));
        }
    }

    @ParameterizedTest
    @EnumSource(WaitFailure.class)
    // Mockito cannot retain the generic Future type at runtime.
    @SuppressWarnings("unchecked")
    void failedWaitNeverReturnsSuccessAndCancelsUnfinishedTask(WaitFailure scenario)
            throws InterruptedException, ExecutionException, TimeoutException {
        var pool = mock(ExecutorService.class);
        Future<ProcessingResult> task = mock(Future.class);
        when(pool.submit(org.mockito.ArgumentMatchers.<Callable<ProcessingResult>>any())).thenReturn(task);
        Exception failure = switch (scenario) {
            case INTERRUPTED -> new InterruptedException("interrupted");
            case TIMEOUT -> new TimeoutException("timeout");
            case TASK_FAILURE -> new ExecutionException(new AssertionError("task failed"));
        };
        when(task.get(30, TimeUnit.SECONDS)).thenThrow(failure);
        try (var executor = new VirtualPersistenceExecutor(new ProcessSearchService(search -> ProcessingResult.PROCESSED), pool)) {
            assertAll(
                    () -> assertThrows(IllegalStateException.class, () -> executor.process(submission)),
                    () -> assertEquals(scenario == WaitFailure.INTERRUPTED, Thread.currentThread().isInterrupted(),
                            "Solo la interrupcion debe restaurar la marca"),
                    () -> {
                        if (scenario != WaitFailure.TASK_FAILURE) {
                            verify(task).cancel(true);
                        }
                    });
        } finally {
            Thread.interrupted();
        }
    }

    private enum WaitFailure { INTERRUPTED, TIMEOUT, TASK_FAILURE }
}
