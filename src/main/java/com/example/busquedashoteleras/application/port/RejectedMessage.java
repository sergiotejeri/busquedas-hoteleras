package com.example.busquedashoteleras.application.port;

public record RejectedMessage(String topic, int partition, long offset, String eventKey,
        String rawPayload, String reasonCode) {
    public RejectedMessage {
        requireText(topic, 249, "topic");
        requireText(reasonCode, 80, "reasonCode");
        if (partition < 0 || offset < 0) {
            throw new IllegalArgumentException("delivery coordinates must not be negative");
        }
    }

    private static void requireText(String value, int maxLength, String field) {
        if (value == null || value.isBlank() || value.length() > maxLength) {
            throw new IllegalArgumentException(field + " must be non-blank and within its size limit");
        }
    }
}
