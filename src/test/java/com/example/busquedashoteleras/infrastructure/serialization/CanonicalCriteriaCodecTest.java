package com.example.busquedashoteleras.infrastructure.serialization;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.example.busquedashoteleras.domain.SearchCriteria;
import java.time.LocalDate;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;

class CanonicalCriteriaCodecTest {
    private final CanonicalCriteriaCodec codec = new CanonicalCriteriaCodec();

    @Test
    void producesStableCriteriaWithoutRequestMetadata() {
        SearchCriteria criteria = criteria(List.of(30, 1, 30));
        String expected = """
                {"hotelId":"Hotel Ñ","checkIn":"2024-02-29","checkOut":"2024-03-01","ages":[30,1,30]}
                """.strip();
        assertAll(
                () -> assertEquals(expected, codec.encode(criteria), "Debe producir una representacion estable"),
                () -> assertEquals(criteria, codec.decode(expected), "Debe recuperar todos los criterios"),
                () -> assertNotEquals(codec.encode(criteria), codec.encode(criteria(List.of(1, 30, 30)))),
                () -> assertNotEquals(codec.encode(criteria), codec.encode(criteria(List.of(30, 1)))));
    }

    @ParameterizedTest
    @NullAndEmptySource
    @ValueSource(strings = {"null", "{}", "[]", "{broken"})
    void rejectsInvalidStoredCriteria(String json) {
        assertThrows(IllegalStateException.class, () -> codec.decode(json));
    }

    private static SearchCriteria criteria(List<Integer> ages) {
        return new SearchCriteria("Hotel Ñ", LocalDate.of(2024, 2, 29), LocalDate.of(2024, 3, 1), ages);
    }
}
