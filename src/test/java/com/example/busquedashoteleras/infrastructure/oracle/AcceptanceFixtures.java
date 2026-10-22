package com.example.busquedashoteleras.infrastructure.oracle;

import com.example.busquedashoteleras.domain.SearchCriteria;
import com.example.busquedashoteleras.domain.SearchSubmission;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.util.List;
import java.util.UUID;

final class AcceptanceFixtures {
    private AcceptanceFixtures() {
    }

    static SearchSubmission submission(UUID id, String hotel) {
        return new SearchSubmission(id, new SearchCriteria(hotel, LocalDate.of(2026, 12, 1),
                LocalDate.of(2026, 12, 2), List.of(30, 1, 30)));
    }

    static SearchSubmission sizedSubmission(UUID id, int bytes) {
        String emptyEvent = "{\"schemaVersion\":1,\"searchId\":\"" + id
                + "\",\"hotelId\":\"\",\"checkIn\":\"01/12/2026\",\"checkOut\":\"02/12/2026\",\"ages\":[30,1,30]}";
        int hotelBytes = bytes - emptyEvent.getBytes(StandardCharsets.UTF_8).length;
        return submission(id, "ñ".repeat(hotelBytes / 2) + "a".repeat(hotelBytes % 2));
    }
}
