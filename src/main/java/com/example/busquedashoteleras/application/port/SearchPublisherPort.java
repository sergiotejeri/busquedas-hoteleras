package com.example.busquedashoteleras.application.port;

import java.util.UUID;

@FunctionalInterface
public interface SearchPublisherPort {
    /** Returns CONFIRMED only after broker acknowledgement. Permanent failure is specific to this event;
     * connectivity and global configuration failures must remain retryable. Unexpected errors propagate. */
    PublicationResult publish(UUID searchId, String eventJson);
}
