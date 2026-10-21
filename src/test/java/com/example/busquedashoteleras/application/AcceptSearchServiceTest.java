package com.example.busquedashoteleras.application;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.example.busquedashoteleras.application.port.AcceptSearchPort;
import com.example.busquedashoteleras.application.service.AcceptSearchService;
import com.example.busquedashoteleras.domain.SearchCriteria;
import com.example.busquedashoteleras.domain.SearchSubmission;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.NullSource;

class AcceptSearchServiceTest {
    private static final UUID FIRST = new UUID(0, 1);
    private static final UUID SECOND = new UUID(0, 2);
    private static final SearchCriteria CRITERIA = new SearchCriteria("1234",
            LocalDate.of(2026, 1, 1), LocalDate.of(2026, 1, 2), List.of(30, 1));

    @Test
    void assignsNewIdentityAndRegistersEveryRequest() {
        List<SearchSubmission> accepted = new ArrayList<>();
        AtomicInteger calls = new AtomicInteger();
        List<UUID> identifiers = List.of(FIRST, SECOND);
        AcceptSearchService service = new AcceptSearchService(accepted::add,
                () -> identifiers.get(calls.getAndIncrement()));
        UUID first = service.accept(CRITERIA);
        UUID second = service.accept(CRITERIA);
        assertAll(
                () -> assertEquals(FIRST, first, "Debe devolver el primer ID asignado"),
                () -> assertEquals(SECOND, second, "No debe reutilizar el ID para criterios iguales"),
                () -> assertEquals(List.of(new SearchSubmission(FIRST, CRITERIA),
                        new SearchSubmission(SECOND, CRITERIA)), accepted,
                        "Debe registrar exactamente las dos solicitudes con sus criterios"));
    }

    // Se comprueba tanto el lanzamiento como la conservacion de la excepcion original.
    @SuppressWarnings("PMD.UnitTestContainsTooManyAsserts")
    @Test
    void propagatesRegistrationFailureInsteadOfReturningSuccess() {
        IllegalStateException failure = new IllegalStateException("registration unavailable");
        AcceptSearchService service = new AcceptSearchService(submission -> { throw failure; }, () -> FIRST);
        assertEquals(failure, assertThrows(IllegalStateException.class, () -> service.accept(CRITERIA)),
                "No debe ocultar el fallo del puerto ni devolver una aceptacion");
    }

    @ParameterizedTest
    @NullSource
    void rejectsNullCriteriaBeforeGeneratingIdentity(SearchCriteria criteria) {
        List<SearchSubmission> accepted = new ArrayList<>();
        AtomicInteger calls = new AtomicInteger();
        AcceptSearchService service = new AcceptSearchService(accepted::add, () -> {
            calls.incrementAndGet();
            return FIRST;
        });
        assertAll(
                () -> assertThrows(NullPointerException.class, () -> service.accept(criteria)),
                () -> assertEquals(0, calls.get(), "No debe generar un ID sin criterios"),
                () -> assertEquals(List.of(), accepted, "No debe registrar una entrada nula"));
    }

    @Test
    void rejectsNullGeneratedIdentityWithoutRegistering() {
        List<SearchSubmission> accepted = new ArrayList<>();
        AcceptSearchService service = new AcceptSearchService(accepted::add, () -> null);
        assertAll(
                () -> assertThrows(NullPointerException.class, () -> service.accept(CRITERIA)),
                () -> assertEquals(List.of(), accepted, "No debe registrar una solicitud sin ID"));
    }

    @Test
    void propagatesGeneratorFailureWithoutRegistering() {
        List<SearchSubmission> accepted = new ArrayList<>();
        IllegalStateException failure = new IllegalStateException("generator unavailable");
        AcceptSearchService service = new AcceptSearchService(accepted::add, () -> { throw failure; });
        assertAll(
                () -> assertEquals(failure, assertThrows(IllegalStateException.class,
                        () -> service.accept(CRITERIA)), "Debe conservar el fallo original"),
                () -> assertEquals(List.of(), accepted, "No debe registrar tras fallar el generador"));
    }

    @ParameterizedTest
    @MethodSource("missingDependencies")
    void rejectsMissingDependencies(AcceptSearchPort port, Supplier<UUID> generator) {
        assertThrows(NullPointerException.class, () -> new AcceptSearchService(port, generator));
    }

    private static Stream<Arguments> missingDependencies() {
        AcceptSearchPort port = submission -> { throw new AssertionError("No debe invocarse"); };
        Supplier<UUID> generator = () -> FIRST;
        return Stream.of(Arguments.of(null, generator), Arguments.of(port, null));
    }
}
