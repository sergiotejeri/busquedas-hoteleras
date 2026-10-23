package com.example.busquedashoteleras.infrastructure.web;

import com.example.busquedashoteleras.domain.InvalidSearchException;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.dao.TransientDataAccessException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.transaction.CannotCreateTransactionException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

@RestControllerAdvice
public final class ApiExceptionHandler {
    @ExceptionHandler(InvalidSearchException.class)
    public ResponseEntity<ApiError> invalidSearch(InvalidSearchException exception) {
        return ResponseEntity.badRequest().body(new ApiError(exception.getCode(), exception.getField(), exception.getMessage()));
    }

    @ExceptionHandler(HttpMessageNotReadableException.class)
    public ResponseEntity<ApiError> malformedJson() {
        return ResponseEntity.badRequest().body(new ApiError("INVALID_REQUEST", null,
                "Expected a JSON object with hotelId, checkIn, checkOut and integer ages."));
    }

    @ExceptionHandler({CannotCreateTransactionException.class, DataAccessResourceFailureException.class,
            TransientDataAccessException.class})
    public ResponseEntity<ApiError> storageUnavailable() {
        return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE).body(new ApiError("STORAGE_UNAVAILABLE", null,
                "Search could not be confirmed. Try again later."));
    }

    public record ApiError(String code, String field, String message) { }
}
