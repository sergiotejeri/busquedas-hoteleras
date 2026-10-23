package com.example.busquedashoteleras.infrastructure.web;

import com.example.busquedashoteleras.domain.InvalidSearchException;
import com.example.busquedashoteleras.domain.SearchCriteria;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.time.format.ResolverStyle;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;

public record SearchRequest(String hotelId, String checkIn, String checkOut, List<Integer> ages) {
    private static final DateTimeFormatter DATE = DateTimeFormatter.ofPattern("dd/MM/uuuu", Locale.ROOT)
            .withResolverStyle(ResolverStyle.STRICT);

    public SearchRequest {
        ages = Collections.unmodifiableList(new ArrayList<>(ages == null ? List.of() : ages));
    }

    public SearchCriteria toCriteria() {
        return new SearchCriteria(hotelId, parseDate(checkIn, "checkIn"), parseDate(checkOut, "checkOut"), ages);
    }

    private static LocalDate parseDate(String value, String field) {
        if (value == null) {
            return null;
        }
        if (!value.matches("[0-9]{2}/[0-9]{2}/[0-9]{4}")) {
            throw invalidDate(field);
        }
        try {
            return LocalDate.parse(value, DATE);
        } catch (DateTimeParseException exception) {
            InvalidSearchException failure = invalidDate(field);
            failure.initCause(exception);
            throw failure;
        }
    }

    private static InvalidSearchException invalidDate(String field) {
        return new InvalidSearchException("INVALID_DATE", field, field + " must be a valid date in dd/MM/yyyy format");
    }
}
