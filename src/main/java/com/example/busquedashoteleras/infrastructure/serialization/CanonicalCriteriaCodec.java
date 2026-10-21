package com.example.busquedashoteleras.infrastructure.serialization;

import com.example.busquedashoteleras.domain.SearchCriteria;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import java.time.LocalDate;
import java.time.DateTimeException;
import java.util.List;
import tools.jackson.core.JacksonException;

public final class CanonicalCriteriaCodec {
    public String encode(SearchCriteria criteria) {
        return SearchJson.MAPPER.writeValueAsString(new CanonicalCriteria(criteria.hotelId(),
                criteria.checkIn().toString(), criteria.checkOut().toString(), criteria.ages()));
    }

    public SearchCriteria decode(String canonicalJson) {
        try {
            CanonicalCriteria value = SearchJson.MAPPER.readValue(canonicalJson, CanonicalCriteria.class);
            if (value == null) {
                throw new IllegalStateException("Stored canonical criteria are invalid");
            }
            return new SearchCriteria(value.hotelId(), LocalDate.parse(value.checkIn()),
                    LocalDate.parse(value.checkOut()), value.ages());
        } catch (JacksonException | IllegalArgumentException | DateTimeException exception) {
            throw new IllegalStateException("Stored canonical criteria are invalid", exception);
        }
    }

    @JsonPropertyOrder({"hotelId", "checkIn", "checkOut", "ages"})
    private record CanonicalCriteria(String hotelId, String checkIn, String checkOut, List<Integer> ages) {
        private CanonicalCriteria {
            ages = List.copyOf(ages);
        }
    }
}
