package com.example.busquedashoteleras.application;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import com.example.busquedashoteleras.application.port.OutboxClaim;
import com.example.busquedashoteleras.application.port.OutboxPort;
import com.example.busquedashoteleras.application.port.PublicationResult;
import com.example.busquedashoteleras.application.port.SearchPublisherPort;
import com.example.busquedashoteleras.application.service.PublishOutboxService;
import java.time.Duration;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.NullSource;
import org.junit.jupiter.params.provider.ValueSource;

class PublishOutboxServiceTest {
    private static final OutboxClaim FIRST = new OutboxClaim(new UUID(0, 1), new UUID(0, 2), "event-one", 0);
    private static final OutboxClaim SECOND = new OutboxClaim(new UUID(0, 3), new UUID(0, 4), "event-two", 0);
    private final OutboxPort outbox = mock(OutboxPort.class);
    private final SearchPublisherPort publisher = mock(SearchPublisherPort.class);

    @Test
    void publishesSequentiallyAndCountsOnlyCommittedConfirmations() {
        when(outbox.claimNext(Duration.ofSeconds(60))).thenReturn(Optional.of(FIRST))
                .thenReturn(Optional.of(SECOND)).thenReturn(Optional.empty());
        when(publisher.publish(any(), any())).thenReturn(PublicationResult.CONFIRMED);
        when(outbox.markSent(any(), any())).thenReturn(true);
        int sent = new PublishOutboxService(outbox, publisher).publishBatch(100);
        var order = inOrder(outbox, publisher);
        assertAll(
                () -> assertEquals(2, sent, "Debe confirmar ambos eventos"),
                () -> {
                    order.verify(outbox).recoverPending(100, Duration.ofMinutes(5));
                    order.verify(outbox).claimNext(Duration.ofSeconds(60));
                    order.verify(publisher).publish(FIRST.searchId(), FIRST.eventJson());
                    order.verify(outbox).markSent(FIRST.searchId(), FIRST.token());
                    order.verify(outbox).claimNext(Duration.ofSeconds(60));
                    order.verify(publisher).publish(SECOND.searchId(), SECOND.eventJson());
                    order.verify(outbox).markSent(SECOND.searchId(), SECOND.token());
                    order.verify(outbox).claimNext(Duration.ofSeconds(60));
                    order.verifyNoMoreInteractions();
                });
    }

    @Test
    void recoveryFailurePreventsClaimingAndPublishing() {
        when(outbox.recoverPending(1, Duration.ofMinutes(5))).thenThrow(new IllegalStateException("recovery unavailable"));
        assertAll(
                () -> assertThrows(IllegalStateException.class, () -> new PublishOutboxService(outbox, publisher).publishBatch(1)),
                () -> verify(outbox, never()).claimNext(any()),
                () -> verifyNoInteractions(publisher));
    }

    @Test
    void respectsBatchLimitWithoutReservingExtraWork() {
        ready(FIRST, PublicationResult.CONFIRMED);
        when(outbox.markSent(any(), any())).thenReturn(true);
        assertAll(
                () -> assertEquals(1, new PublishOutboxService(outbox, publisher).publishBatch(1), "Lote de un evento"),
                () -> verify(outbox).claimNext(Duration.ofSeconds(60)),
                () -> verify(outbox).recoverPending(1, Duration.ofMinutes(5)));
    }

    @ParameterizedTest
    @CsvSource({"0,1", "1,2", "2,4", "3,8", "4,16", "5,30", "6,30"})
    void defersTransientFailureWithCappedBackoffAndStopsCycle(int attempts, int seconds) {
        OutboxClaim claim = new OutboxClaim(FIRST.searchId(), FIRST.token(), FIRST.eventJson(), attempts);
        ready(claim, PublicationResult.RETRYABLE_FAILURE);
        assertAll(
                () -> assertEquals(0, new PublishOutboxService(outbox, publisher).publishBatch(100), "No hay envio confirmado"),
                () -> verify(outbox).retry(claim.searchId(), claim.token(), Duration.ofSeconds(seconds)),
                () -> verify(outbox).claimNext(Duration.ofSeconds(60)),
                () -> verify(outbox, never()).markSent(any(), any()),
                () -> verify(outbox, never()).markFailed(any(), any()));
    }

    @Test
    void isolatesPermanentEventFailureAndContinuesWithNext() {
        when(outbox.claimNext(any())).thenReturn(Optional.of(FIRST))
                .thenReturn(Optional.of(SECOND)).thenReturn(Optional.empty());
        when(publisher.publish(FIRST.searchId(), FIRST.eventJson())).thenReturn(PublicationResult.PERMANENT_FAILURE);
        when(publisher.publish(SECOND.searchId(), SECOND.eventJson())).thenReturn(PublicationResult.CONFIRMED);
        when(outbox.markFailed(FIRST.searchId(), FIRST.token())).thenReturn(true);
        when(outbox.markSent(SECOND.searchId(), SECOND.token())).thenReturn(true);
        assertAll(
                () -> assertEquals(1, new PublishOutboxService(outbox, publisher).publishBatch(100), "Solo uno se publica"),
                () -> verify(outbox).markFailed(FIRST.searchId(), FIRST.token()),
                () -> verify(outbox, never()).markSent(FIRST.searchId(), FIRST.token()));
    }

    @ParameterizedTest
    @ValueSource(strings = {"CONFIRMED", "PERMANENT_FAILURE"})
    void stopsWhenOwnershipWasLost(String outcome) {
        ready(FIRST, PublicationResult.valueOf(outcome));
        assertAll(
                () -> assertEquals(0, new PublishOutboxService(outbox, publisher).publishBatch(100), "No debe fingir confirmacion"),
                () -> verify(outbox).claimNext(Duration.ofSeconds(60)));
    }

    @Test
    void propagatesUnexpectedPublisherFailureWithoutChangingReservation() {
        when(outbox.claimNext(any())).thenReturn(Optional.of(FIRST));
        when(publisher.publish(any(), any())).thenThrow(new IllegalStateException("unexpected"));
        assertAll(
                () -> assertThrows(IllegalStateException.class, () -> new PublishOutboxService(outbox, publisher).publishBatch(100)),
                () -> verify(outbox, never()).markSent(any(), any()),
                () -> verify(outbox, never()).retry(any(), any(), any()),
                () -> verify(outbox, never()).markFailed(any(), any()));
    }

    @Test
    void propagatesDatabaseFailureAfterPublishingWithoutRetryingInline() {
        ready(FIRST, PublicationResult.CONFIRMED);
        when(outbox.markSent(any(), any())).thenThrow(new IllegalStateException("database unavailable"));
        assertAll(
                () -> assertThrows(IllegalStateException.class, () -> new PublishOutboxService(outbox, publisher).publishBatch(100)),
                () -> verify(publisher).publish(FIRST.searchId(), FIRST.eventJson()),
                () -> verify(outbox).claimNext(Duration.ofSeconds(60)),
                () -> verify(outbox, never()).retry(any(), any(), any()));
    }

    @Test
    void emptyQueueDoesNotPublish() {
        when(outbox.claimNext(any())).thenReturn(Optional.empty());
        assertAll(
                () -> assertEquals(0, new PublishOutboxService(outbox, publisher).publishBatch(100), "Cola vacia"),
                () -> verify(outbox).claimNext(Duration.ofSeconds(60)),
                () -> verifyNoInteractions(publisher));
    }

    @Test
    void nullPublisherResultIsNotSuccess() {
        when(outbox.claimNext(any())).thenReturn(Optional.of(FIRST));
        assertAll(
                () -> assertThrows(NullPointerException.class, () -> new PublishOutboxService(outbox, publisher).publishBatch(1)),
                () -> verify(outbox, never()).markSent(any(), any()));
    }

    @ParameterizedTest
    @ValueSource(ints = {-1, 0, 101, Integer.MAX_VALUE})
    void rejectsInvalidBatchBeforeClaiming(int limit) {
        assertAll(
                () -> assertThrows(IllegalArgumentException.class, () -> new PublishOutboxService(outbox, publisher).publishBatch(limit)),
                () -> verifyNoInteractions(outbox, publisher));
    }

    @ParameterizedTest
    @NullSource
    void rejectsMissingOutbox(OutboxPort missing) {
        assertThrows(NullPointerException.class, () -> new PublishOutboxService(missing, publisher));
    }

    @ParameterizedTest
    @NullSource
    void rejectsMissingPublisher(SearchPublisherPort missing) {
        assertThrows(NullPointerException.class, () -> new PublishOutboxService(outbox, missing));
    }

    private void ready(OutboxClaim claim, PublicationResult result) {
        when(outbox.claimNext(any())).thenReturn(Optional.of(claim));
        when(publisher.publish(claim.searchId(), claim.eventJson())).thenReturn(result);
    }
}
