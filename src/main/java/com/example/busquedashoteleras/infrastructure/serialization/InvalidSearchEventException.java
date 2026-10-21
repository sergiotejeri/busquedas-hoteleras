package com.example.busquedashoteleras.infrastructure.serialization;

import java.io.Serial;

public final class InvalidSearchEventException extends RuntimeException {
    @Serial
    private static final long serialVersionUID = 1L;
    private final String reasonCode;

    public InvalidSearchEventException(String reasonCode, String message, Throwable cause) {
        super(message, cause);
        this.reasonCode = reasonCode;
    }

    public String getReasonCode() {
        return reasonCode;
    }
}
