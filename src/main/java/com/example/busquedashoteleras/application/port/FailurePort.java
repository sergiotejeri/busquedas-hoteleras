package com.example.busquedashoteleras.application.port;

@FunctionalInterface
public interface FailurePort {
    /** Normal return confirms durable quarantine. Coordinates identify a delivery within one topic lifetime. */
    void quarantine(RejectedMessage message);
}
