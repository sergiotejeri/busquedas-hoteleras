package com.example.busquedashoteleras.application;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import com.example.busquedashoteleras.application.port.SearchQueryPort;
import com.example.busquedashoteleras.application.service.GetSearchCountService;
import com.example.busquedashoteleras.domain.CountResult;
import com.example.busquedashoteleras.domain.SearchStatus;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullSource;

class GetSearchCountServiceTest {
    private final SearchQueryPort port = mock(SearchQueryPort.class);
    private final UUID id = UUID.randomUUID();

    @Test
    void returnsStoredResultForRequestedId() {
        var expected = Optional.of(new CountResult(id, SearchStatus.PENDING, null, null));
        when(port.find(id)).thenReturn(expected);
        assertAll(
                () -> assertSame(expected, new GetSearchCountService(port).find(id), "Conserva el resultado del puerto"),
                () -> verify(port).find(id));
    }

    @Test
    void preservesMissingSearch() {
        when(port.find(id)).thenReturn(Optional.empty());
        assertAll(
                () -> assertEquals(Optional.empty(), new GetSearchCountService(port).find(id), "No inventa resultados"),
                () -> verify(port).find(id));
    }

    @Test
    void propagatesFailureInsteadOfReturningNotFound() {
        var failure = new IllegalStateException("unavailable");
        when(port.find(id)).thenThrow(failure);
        assertAll(() -> assertSame(failure, assertThrows(IllegalStateException.class,
                () -> new GetSearchCountService(port).find(id)), "Un fallo no equivale a un ID desconocido"));
    }

    @Test
    void rejectsNullPortResult() {
        when(port.find(id)).thenReturn(null);
        assertThrows(NullPointerException.class, () -> new GetSearchCountService(port).find(id));
    }

    @ParameterizedTest
    @NullSource
    void requiresIdentifier(UUID missing) {
        assertAll(
                () -> assertThrows(NullPointerException.class, () -> new GetSearchCountService(port).find(missing)),
                () -> verifyNoInteractions(port));
    }

    @ParameterizedTest
    @NullSource
    void requiresPort(SearchQueryPort missing) {
        assertThrows(NullPointerException.class, () -> new GetSearchCountService(missing));
    }
}
