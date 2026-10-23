package com.example.busquedashoteleras.application.service;

import com.example.busquedashoteleras.application.port.OutboxClaim;
import com.example.busquedashoteleras.application.port.OutboxPort;
import com.example.busquedashoteleras.application.port.PublicationResult;
import com.example.busquedashoteleras.application.port.SearchPublisherPort;
import java.time.Duration;
import java.util.Objects;

public final class PublishOutboxService {
    private static final Duration LEASE = Duration.ofSeconds(60);
    private final OutboxPort outbox;
    private final SearchPublisherPort publisher;

    public PublishOutboxService(OutboxPort outbox, SearchPublisherPort publisher) {
        this.outbox = Objects.requireNonNull(outbox, "outbox must not be null");
        this.publisher = Objects.requireNonNull(publisher, "publisher must not be null");
    }

    /** Publishes at most limit events, reserving each immediately before sending.
     * Returns the number of broker confirmations also committed to the outbox.
     * Stops on transient failure or lost ownership; unexpected errors propagate with the lease recoverable. */
    public int publishBatch(int limit) {
        validateBatchLimit(limit);
        int sent = 0;
        for (int index = 0; index < limit; index++) {
            var available = outbox.claimNext(LEASE);
            if (available.isEmpty()) {
                break;
            }
            OutboxClaim claim = available.orElseThrow();
            PublicationResult result = Objects.requireNonNull(publisher.publish(claim.searchId(), claim.eventJson()),
                    "publisher must return a result");
            if (result == PublicationResult.RETRYABLE_FAILURE) {
                long seconds = Math.min(30, 1L << Math.min(claim.retryAttempts(), 5));
                outbox.retry(claim.searchId(), claim.token(), Duration.ofSeconds(seconds));
                break;
            }
            boolean recorded = result == PublicationResult.CONFIRMED
                    ? outbox.markSent(claim.searchId(), claim.token()) : outbox.markFailed(claim.searchId(), claim.token());
            if (!recorded) {
                break;
            }
            if (result == PublicationResult.CONFIRMED) {
                sent++;
            }
        }
        return sent;
    }

    private static void validateBatchLimit(int limit) {
        if (limit < 1 || limit > 100) {
            throw new IllegalArgumentException("batch limit must be between 1 and 100");
        }
    }
}
