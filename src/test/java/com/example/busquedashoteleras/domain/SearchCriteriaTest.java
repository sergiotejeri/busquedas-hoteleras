package com.example.busquedashoteleras.domain;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class SearchCriteriaTest {
    private static final String HOTEL = "1234";
    private static final LocalDate CHECK_IN = LocalDate.of(2025, 1, 1);
    private static final LocalDate CHECK_OUT = CHECK_IN.plusDays(1);

    private static final String INVALID_STAY = "INVALID_STAY";
    private static final String INVALID_AGES = "INVALID_AGES";
    private static final String AGES_FIELD = "ages";
    private static final String AGES_MESSAGE = "ages must contain non-negative values";

    // assertThrows obtiene el error; assertAll comprueba sus datos como un unico resultado.
    @SuppressWarnings("PMD.UnitTestContainsTooManyAsserts")
    @ParameterizedTest
    @MethodSource("invalidCriteria")
    void rejectsInvalidCriteria(String hotel, LocalDate checkIn, LocalDate checkOut,
            List<Integer> ages, String code, String field, String message) {
        InvalidSearchException error = assertThrows(InvalidSearchException.class,
                () -> new SearchCriteria(hotel, checkIn, checkOut, ages));
        assertAll(
                () -> assertEquals(code, error.getCode(), "Debe identificar la regla incumplida"),
                () -> assertEquals(field, error.getField(), "Debe identificar el campo incorrecto"),
                () -> assertEquals(message, error.getMessage(), "Debe explicar el motivo del rechazo"));
    }

    private static Stream<Arguments> invalidCriteria() {
        return Stream.of(
                Arguments.of(null, CHECK_IN, CHECK_OUT, List.of(30), "INVALID_HOTEL_ID", "hotelId", "hotelId must not be blank"),
                Arguments.of("", CHECK_IN, CHECK_OUT, List.of(30), "INVALID_HOTEL_ID", "hotelId", "hotelId must not be blank"),
                Arguments.of(" \t\n", CHECK_IN, CHECK_OUT, List.of(30), "INVALID_HOTEL_ID", "hotelId", "hotelId must not be blank"),
                Arguments.of(HOTEL, null, CHECK_OUT, List.of(30), INVALID_STAY, "checkIn", "checkIn must not be null"),
                Arguments.of(HOTEL, CHECK_IN, null, List.of(30), INVALID_STAY, "checkOut", "checkOut must not be null"),
                Arguments.of(HOTEL, CHECK_IN, CHECK_IN, List.of(30), INVALID_STAY, "checkOut", "checkOut must be after checkIn"),
                Arguments.of(HOTEL, CHECK_IN, CHECK_IN.minusDays(1), List.of(30), INVALID_STAY, "checkOut", "checkOut must be after checkIn"),
                Arguments.of(HOTEL, CHECK_IN, CHECK_OUT, null, INVALID_AGES, AGES_FIELD, AGES_MESSAGE),
                Arguments.of(HOTEL, CHECK_IN, CHECK_OUT, List.of(), INVALID_AGES, AGES_FIELD, AGES_MESSAGE),
                Arguments.of(HOTEL, CHECK_IN, CHECK_OUT, Arrays.asList(30, null), INVALID_AGES, AGES_FIELD, AGES_MESSAGE),
                Arguments.of(HOTEL, CHECK_IN, CHECK_OUT, List.of(30, -1), INVALID_AGES, AGES_FIELD, AGES_MESSAGE));
    }

    @Test
    void retainsValuesAndProtectsAgesFromMutation() {
        List<Integer> ages = new ArrayList<>(List.of(30, 0, Integer.MAX_VALUE, 30));
        SearchCriteria criteria = new SearchCriteria(HOTEL, CHECK_IN, CHECK_OUT, ages);
        ages.set(0, 99);
        assertAll(
                () -> assertEquals(HOTEL, criteria.hotelId(), "Debe conservar el hotel"),
                () -> assertEquals(CHECK_IN, criteria.checkIn(), "Debe conservar la entrada aunque sea pasada"),
                () -> assertEquals(CHECK_OUT, criteria.checkOut(), "Debe conservar la salida"),
                () -> assertEquals(List.of(30, 0, Integer.MAX_VALUE, 30), criteria.ages(), "La copia debe conservar orden, limites y repeticiones"),
                () -> assertThrows(UnsupportedOperationException.class, () -> criteria.ages().add(5)));
    }

    @Test
    void equalityIncludesEveryFieldAndAgeOrderAndMultiplicity() {
        SearchCriteria criteria = new SearchCriteria(HOTEL, CHECK_IN, CHECK_OUT, List.of(30, 1));
        SearchCriteria equal = new SearchCriteria(HOTEL, CHECK_IN, CHECK_OUT, List.of(30, 1));
        assertAll(
                () -> assertEquals(criteria, equal, "Los mismos valores deben producir criterios iguales"),
                () -> assertEquals(criteria.hashCode(), equal.hashCode(), "Criterios iguales deben tener el mismo hash"),
                () -> assertNotEquals(criteria, new SearchCriteria(" " + HOTEL, CHECK_IN, CHECK_OUT, List.of(30, 1))),
                () -> assertNotEquals(criteria, new SearchCriteria(HOTEL, CHECK_IN.minusDays(1), CHECK_OUT, List.of(30, 1))),
                () -> assertNotEquals(criteria, new SearchCriteria(HOTEL, CHECK_IN, CHECK_OUT.plusDays(1), List.of(30, 1))),
                () -> assertNotEquals(criteria, new SearchCriteria(HOTEL, CHECK_IN, CHECK_OUT, List.of(1, 30))),
                () -> assertNotEquals(criteria, new SearchCriteria(HOTEL, CHECK_IN, CHECK_OUT, List.of(30, 1, 1))));
    }
}
