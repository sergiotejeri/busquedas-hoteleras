package com.example.busquedashoteleras.infrastructure.web;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

class SearchDetailsResponseTest {
    @Test
    void defensivelyCopiesAgesAndPreventsExternalMutation() {
        var ages = new ArrayList<>(List.of(30, 1, 30));
        var response = new SearchDetailsResponse("hotel", "01/12/2026", "03/12/2026", ages);
        ages.clear();
        assertAll(
                () -> assertEquals(List.of(30, 1, 30), response.ages(), "Conserva orden y repeticiones"),
                () -> assertThrows(UnsupportedOperationException.class, () -> response.ages().clear(), "La respuesta es inmutable"));
    }
}
