package com.example.busquedashoteleras.infrastructure.serialization;

import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import java.util.List;
import java.util.UUID;

@JsonPropertyOrder({"schemaVersion", "searchId", "hotelId", "checkIn", "checkOut", "ages"})
public record SearchEvent(int schemaVersion, UUID searchId, String hotelId,
        String checkIn, String checkOut, List<Integer> ages) {
    public SearchEvent {
        ages = List.copyOf(ages);
    }
}
