package com.example.busquedashoteleras.infrastructure.web;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

class SearchRequestTest {
    @Test
    void keepsImmutableSnapshotOfInputAges() {
        List<Integer> ages = new ArrayList<>(List.of(30, 1));
        SearchRequest request = new SearchRequest("hotel", "01/01/2024", "02/01/2024", ages);
        ages.set(0, 99);
        assertAll(
                () -> assertEquals(List.of(30, 1), request.ages(), "No debe conservar la lista recibida"),
                () -> assertThrows(UnsupportedOperationException.class, () -> request.ages().add(50),
                        "No debe permitir modificar las edades desde el accessor"),
                () -> assertEquals(List.of(30, 1), request.toCriteria().ages(),
                        "El dominio debe recibir la instantanea original"));
    }
}
