package com.example.busquedashoteleras.application;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import com.example.busquedashoteleras.application.port.ProcessSearchPort;
import com.example.busquedashoteleras.application.port.ProcessingResult;
import com.example.busquedashoteleras.application.service.ProcessSearchService;
import com.example.busquedashoteleras.domain.SearchCriteria;
import com.example.busquedashoteleras.domain.SearchSubmission;
import java.time.LocalDate;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.NullSource;

class ProcessSearchServiceTest {
    private static final SearchSubmission SUBMISSION = new SearchSubmission(new UUID(0, 1),
            new SearchCriteria("hotel", LocalDate.of(2026, 12, 1), LocalDate.of(2026, 12, 2), List.of(30, 1)));
    private final ProcessSearchPort port = mock(ProcessSearchPort.class);

    @ParameterizedTest
    @EnumSource(ProcessingResult.class)
    void preservesIdentityAndProcessingOutcome(ProcessingResult result) {
        when(port.process(SUBMISSION)).thenReturn(result);
        var actual = new ProcessSearchService(port).process(SUBMISSION);
        assertAll(
                () -> assertEquals(result, actual, "Debe conservar el resultado del procesamiento"),
                () -> verify(port).process(SUBMISSION));
    }

    @Test
    void propagatesStorageFailureInsteadOfConfirmingProcessing() {
        var failure = new IllegalStateException("storage unavailable");
        when(port.process(SUBMISSION)).thenThrow(failure);
        assertAll(() -> assertSame(failure, assertThrows(IllegalStateException.class,
                () -> new ProcessSearchService(port).process(SUBMISSION)), "Debe conservar el error"));
    }

    @Test
    void rejectsMissingPortResult() {
        assertThrows(NullPointerException.class, () -> new ProcessSearchService(port).process(SUBMISSION),
                "Un resultado ausente no confirma el procesamiento");
    }

    @ParameterizedTest
    @NullSource
    void rejectsMissingSubmission(SearchSubmission submission) {
        assertAll(
                () -> assertThrows(NullPointerException.class, () -> new ProcessSearchService(port).process(submission),
                        "La solicitud es obligatoria"),
                () -> verifyNoInteractions(port));
    }

    @ParameterizedTest
    @NullSource
    void rejectsMissingPort(ProcessSearchPort missing) {
        assertThrows(NullPointerException.class, () -> new ProcessSearchService(missing), "El puerto es obligatorio");
    }
}
