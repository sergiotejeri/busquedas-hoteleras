package com.example.busquedashoteleras.domain;

import java.io.Serial;

public final class InvalidSearchException extends IllegalArgumentException {
    @Serial
    private static final long serialVersionUID = 1L;

    private final String code;
    private final String field;

    public InvalidSearchException(String code, String field, String message) {
        super(message);
        this.code = code;
        this.field = field;
    }

    public String getCode() {
        return code;
    }

    public String getField() {
        return field;
    }
}
