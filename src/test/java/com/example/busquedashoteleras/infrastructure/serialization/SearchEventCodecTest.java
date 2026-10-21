package com.example.busquedashoteleras.infrastructure.serialization;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.example.busquedashoteleras.domain.SearchCriteria;
import com.example.busquedashoteleras.domain.SearchSubmission;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;

class SearchEventCodecTest {
    private static final String JSON = """
            {"schemaVersion":1,"searchId":"00000000-0000-0000-0000-000000000001","hotelId":"Hotel Ã‘","checkIn":"29/02/2024","checkOut":"01/03/2024","ages":[30,0,30]}
            """.strip();
    private static final String AGES_JSON = "[30,0,30]";
    private final SearchEventCodec codec = new SearchEventCodec();

    @Test
    void writesAndReadsVersionedUtf8Contract() {
        SearchSubmission expected = new SearchSubmission(new UUID(0, 1), new SearchCriteria("Hotel Ã‘",
                LocalDate.of(2024, 2, 29), LocalDate.of(2024, 3, 1), List.of(30, 0, 30)));
        assertAll(
                () -> assertEquals(JSON, new String(codec.encode(expected), StandardCharsets.UTF_8),
                        "Debe escribir el contrato versionado con fechas y edades exactas"),
                () -> assertEquals(expected, codec.decode(JSON), "Debe reconstruir la solicitud original"));
    }

    @ParameterizedTest
    @MethodSource("invalidFields")
    void rejectsInvalidFields(String original, String replacement) {
        assertThrows(InvalidSearchEventException.class, () -> codec.decode(JSON.replace(original, replacement)));
    }

    private static Stream<Arguments> invalidFields() {
        return Stream.of(
                Arguments.of("1,", "null,"),
                Arguments.of("1,", "1.5,"),
                Arguments.of("1,", "\"1\","),
                Arguments.of("\"schemaVersion\":1,", ""),
                Arguments.of("00000000-0000-0000-0000-000000000001", "not-a-uuid"),
                Arguments.of("\"Hotel Ã‘\"", "null"),
                Arguments.of("\"Hotel Ã‘\"", "123"),
                Arguments.of("\"Hotel Ã‘\"", "\" \""),
                Arguments.of("29/02/2024", "29/02/2023"),
                Arguments.of("29/02/2024", "2024-02-29"),
                Arguments.of("01/03/2024", "29/02/2024"),
                Arguments.of(AGES_JSON, "[]"),
                Arguments.of(AGES_JSON, "null"),
                Arguments.of(AGES_JSON, "[30,null]"),
                Arguments.of(AGES_JSON, "[-1]"),
                Arguments.of(AGES_JSON, "[1.5]"),
                Arguments.of(AGES_JSON, "[\"30\"]"),
                Arguments.of(AGES_JSON, "[2147483648]"));
    }

    @ParameterizedTest
    @NullAndEmptySource
    @ValueSource(strings = {"null", "{}", "[]", "{broken"})
    void rejectsMissingOrMalformedEvent(String json) {
        assertThrows(InvalidSearchEventException.class, () -> codec.decode(json));
    }

    @Test
    void rejectsAmbiguousJson() {
        assertAll(
                () -> assertThrows(InvalidSearchEventException.class, () -> codec.decode(JSON + " {}")),
                () -> assertThrows(InvalidSearchEventException.class,
                        () -> codec.decode(JSON.replace("{", "{\"extra\":true,"))),
                () -> assertThrows(InvalidSearchEventException.class,
                        () -> codec.decode(JSON.replace("{", "{\"schemaVersion\":1,"))));
    }

    // El lanzamiento y su motivo describen un unico resultado de rechazo.
    @SuppressWarnings("PMD.UnitTestContainsTooManyAsserts")
    @Test
    void distinguishesUnsupportedVersion() {
        assertEquals("UNSUPPORTED_SCHEMA",
                assertThrows(InvalidSearchEventException.class,
                        () -> codec.decode(JSON.replace("1,", "2,"))).getReasonCode(),
                "Debe distinguir una version desconocida de un evento mal formado");
    }

    @Test
    void eventDefensivelyCopiesAges() {
        List<Integer> ages = new ArrayList<>(List.of(30, 1));
        SearchEvent event = new SearchEvent(1, new UUID(0, 1), "hotel", "01/01/2026", "02/01/2026", ages);
        ages.clear();
        assertAll(
                () -> assertEquals(List.of(30, 1), event.ages(), "Debe conservar su propia copia"),
                () -> assertThrows(UnsupportedOperationException.class, () -> event.ages().add(2)));
    }
}
