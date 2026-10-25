package com.example.busquedashoteleras.infrastructure.web;

import com.example.busquedashoteleras.domain.SearchCriteria;
import io.swagger.v3.oas.annotations.media.Schema;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Locale;

public record SearchDetailsResponse(String hotelId,
        @Schema(type = "string", example = "01/12/2026", pattern = "^[0-9]{2}/[0-9]{2}/[0-9]{4}$") String checkIn,
        @Schema(type = "string", example = "03/12/2026", pattern = "^[0-9]{2}/[0-9]{2}/[0-9]{4}$") String checkOut,
        List<Integer> ages) {
    private static final DateTimeFormatter DATE = DateTimeFormatter.ofPattern("dd/MM/uuuu", Locale.ROOT);

    public SearchDetailsResponse {
        ages = List.copyOf(ages);
    }

    static SearchDetailsResponse from(SearchCriteria criteria) {
        return new SearchDetailsResponse(criteria.hotelId(), DATE.format(criteria.checkIn()),
                DATE.format(criteria.checkOut()), criteria.ages());
    }
}
