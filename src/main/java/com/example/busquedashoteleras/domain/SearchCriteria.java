package com.example.busquedashoteleras.domain;

import java.time.LocalDate;
import java.util.List;

public record SearchCriteria(String hotelId, LocalDate checkIn, LocalDate checkOut, List<Integer> ages) {
    public SearchCriteria {
        if (hotelId == null || hotelId.isBlank()) {
            throw new InvalidSearchException("INVALID_HOTEL_ID", "hotelId", "hotelId must not be blank");
        }
        if (checkIn == null) {
            throw new InvalidSearchException("INVALID_STAY", "checkIn", "checkIn must not be null");
        }
        if (checkOut == null) {
            throw new InvalidSearchException("INVALID_STAY", "checkOut", "checkOut must not be null");
        }
        if (!checkOut.isAfter(checkIn)) {
            throw new InvalidSearchException("INVALID_STAY", "checkOut", "checkOut must be after checkIn");
        }
        if (ages == null || ages.isEmpty() || ages.stream().anyMatch(age -> age == null || age < 0)) {
            throw new InvalidSearchException("INVALID_AGES", "ages", "ages must contain non-negative values");
        }
        ages = List.copyOf(ages);
    }
}
