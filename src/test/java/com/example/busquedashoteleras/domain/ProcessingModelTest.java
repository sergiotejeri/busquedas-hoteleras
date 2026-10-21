package com.example.busquedashoteleras.domain;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.time.LocalDate;
import java.util.List;
import java.util.UUID;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

class ProcessingModelTest {
    private static final UUID ID = UUID.fromString("00000000-0000-0000-0000-000000000001");
    private static final SearchCriteria CRITERIA = new SearchCriteria("1234",
            LocalDate.of(2026, 1, 1), LocalDate.of(2026, 1, 2), List.of(30, 1));

    @Test
    void submissionRetainsIdentityAndCriteria() {
        SearchSubmission submission = new SearchSubmission(ID, CRITERIA);
        assertAll(
                () -> assertEquals(ID, submission.searchId(), "Debe conservar el identificador asignado"),
                () -> assertEquals(CRITERIA, submission.criteria(), "Debe conservar los criterios validados"));
    }

    @ParameterizedTest
    @MethodSource("incompleteSubmissions")
    void submissionRejectsMissingIdentityOrCriteria(UUID id, SearchCriteria criteria) {
        assertThrows(NullPointerException.class, () -> new SearchSubmission(id, criteria));
    }

    private static Stream<Arguments> incompleteSubmissions() {
        return Stream.of(Arguments.of(null, CRITERIA), Arguments.of(ID, null));
    }

    @ParameterizedTest
    @ValueSource(longs = {1, 2, Long.MAX_VALUE})
    void processedResultRequiresPositiveCount(long count) {
        CountResult result = new CountResult(ID, SearchStatus.PROCESSED, CRITERIA, count);
        assertAll(
                () -> assertEquals(ID, result.searchId(), "Debe identificar la busqueda consultada"),
                () -> assertEquals(SearchStatus.PROCESSED, result.status(), "Debe conservar el estado procesado"),
                () -> assertEquals(CRITERIA, result.search(), "Debe incluir los criterios contabilizados"),
                () -> assertEquals(count, result.count(), "Debe conservar el conteo sin desbordamiento"));
    }

    @ParameterizedTest
    @EnumSource(value = SearchStatus.class, names = {"PENDING", "FAILED"})
    void unfinishedResultDoesNotInventCount(SearchStatus status) {
        CountResult result = new CountResult(ID, status, null, null);
        assertAll(
                () -> assertEquals(status, result.status(), "Debe distinguir pendiente de fallida"),
                () -> assertNull(result.search(), "No debe exponer criterios como resultado procesado"),
                () -> assertNull(result.count(), "Ausencia de conteo no equivale a cero"));
    }

    @ParameterizedTest
    @MethodSource("invalidResults")
    void rejectsInconsistentResults(SearchStatus status, SearchCriteria criteria, Long count) {
        assertThrows(IllegalArgumentException.class, () -> new CountResult(ID, status, criteria, count));
    }

    private static Stream<Arguments> invalidResults() {
        return Stream.of(
                Arguments.of(SearchStatus.PROCESSED, null, 1L),
                Arguments.of(SearchStatus.PROCESSED, CRITERIA, null),
                Arguments.of(SearchStatus.PROCESSED, CRITERIA, 0L),
                Arguments.of(SearchStatus.PROCESSED, CRITERIA, -1L),
                Arguments.of(SearchStatus.PENDING, CRITERIA, null),
                Arguments.of(SearchStatus.PENDING, null, 0L),
                Arguments.of(SearchStatus.PENDING, CRITERIA, 1L),
                Arguments.of(SearchStatus.FAILED, CRITERIA, null),
                Arguments.of(SearchStatus.FAILED, null, 1L));
    }

    @ParameterizedTest
    @MethodSource("incompleteResults")
    void resultRejectsMissingIdentityOrStatus(UUID id, SearchStatus status) {
        assertThrows(NullPointerException.class, () -> new CountResult(id, status, null, null));
    }

    private static Stream<Arguments> incompleteResults() {
        return Stream.of(Arguments.of(null, SearchStatus.PENDING), Arguments.of(ID, null));
    }
}
