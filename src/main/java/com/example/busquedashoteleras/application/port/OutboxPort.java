package com.example.busquedashoteleras.application.port;

import java.time.Duration;
import java.util.Optional;
import java.util.UUID;

public interface OutboxPort {
    /** Reserves at most one event and commits before returning; an empty result means no unlocked work is due.
     * Lease duration must be positive and at most one hour. */
    Optional<OutboxClaim> claimNext(Duration lease);

    /** Marks publication only for the current, unexpired token. Call after broker confirmation.
     * Returns false if ownership has expired, changed or already been completed. */
    boolean markSent(UUID searchId, UUID token);
}
