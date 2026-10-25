package com.example.busquedashoteleras.infrastructure.kafka;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import com.example.busquedashoteleras.application.port.FailurePort;
import com.example.busquedashoteleras.application.port.ProcessingResult;
import com.example.busquedashoteleras.application.port.RejectedMessage;
import com.example.busquedashoteleras.domain.SearchCriteria;
import com.example.busquedashoteleras.domain.SearchSubmission;
import com.example.busquedashoteleras.infrastructure.serialization.SearchEventCodec;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.util.List;
import java.util.UUID;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.kafka.support.Acknowledgment;

class KafkaSearchConsumerTest {
    private static final String TOPIC = "consumer-test";
    private static final String EVENT_KEY = "key";
    private final VirtualPersistenceExecutor persistence = mock(VirtualPersistenceExecutor.class);
    private final FailurePort failures = mock(FailurePort.class);
    private final Acknowledgment acknowledgment = mock(Acknowledgment.class);
    private final KafkaSearchConsumer consumer = new KafkaSearchConsumer(persistence, failures);
    private final SearchSubmission submission = new SearchSubmission(UUID.randomUUID(),
            new SearchCriteria("hotel", LocalDate.of(2026, 12, 1), LocalDate.of(2026, 12, 3), List.of(30, 1)));
    private final String payload = new String(new SearchEventCodec().encode(submission), StandardCharsets.UTF_8);

    @ParameterizedTest
    @EnumSource(value = ProcessingResult.class, names = {"PROCESSED", "ALREADY_PROCESSED"})
    void acknowledgesOnlyAfterSuccessfulPersistence(ProcessingResult result) {
        when(persistence.process(submission)).thenReturn(result);
        consumer.onMessage(record(submission.searchId().toString(), payload), acknowledgment);
        var order = inOrder(persistence, acknowledgment);
        assertAll(
                () -> order.verify(persistence).process(submission),
                () -> order.verify(acknowledgment).acknowledge(),
                () -> verifyNoInteractions(failures));
    }

    @ParameterizedTest
    @EnumSource(value = ProcessingResult.class, names = {"UNKNOWN_SEARCH", "CRITERIA_MISMATCH", "FAILED_SEARCH"})
    void quarantinesRejectedResultBeforeAcknowledging(ProcessingResult result) {
        when(persistence.process(submission)).thenReturn(result);
        consumer.onMessage(record(submission.searchId().toString(), payload), acknowledgment);
        var order = inOrder(failures, acknowledgment);
        assertAll(
                () -> order.verify(failures).quarantine(rejection(submission.searchId().toString(), payload, result.name())),
                () -> order.verify(acknowledgment).acknowledge());
    }

    @ParameterizedTest
    @NullAndEmptySource
    @ValueSource(strings = {"{}", "null", "invalid"})
    void quarantinesInvalidPayloadWithoutProcessing(String raw) {
        consumer.onMessage(record(EVENT_KEY, raw), acknowledgment);
        var order = inOrder(failures, acknowledgment);
        assertAll(
                () -> order.verify(failures).quarantine(rejection(EVENT_KEY, raw, "INVALID_EVENT")),
                () -> order.verify(acknowledgment).acknowledge(),
                () -> verifyNoInteractions(persistence));
    }

    @ParameterizedTest
    @NullAndEmptySource
    @ValueSource(strings = "different-id")
    void quarantinesMismatchedKey(String key) {
        consumer.onMessage(record(key, payload), acknowledgment);
        var order = inOrder(failures, acknowledgment);
        assertAll(
                () -> order.verify(failures).quarantine(rejection(key, payload, "KEY_ID_MISMATCH")),
                () -> order.verify(acknowledgment).acknowledge(),
                () -> verifyNoInteractions(persistence));
    }

    @Test
    void persistenceFailureDoesNotAcknowledgeOrQuarantine() {
        var failure = new IllegalStateException("storage unavailable");
        when(persistence.process(submission)).thenThrow(failure);
        assertAll(
                () -> assertSame(failure, assertThrows(IllegalStateException.class,
                        () -> consumer.onMessage(record(submission.searchId().toString(), payload), acknowledgment), "Propaga el fallo"), "Conserva la excepcion"),
                () -> verifyNoInteractions(acknowledgment, failures));
    }

    @Test
    void quarantineFailureDoesNotAcknowledge() {
        var failure = new IllegalStateException("quarantine unavailable");
        doThrow(failure).when(failures).quarantine(rejection(EVENT_KEY, "{}", "INVALID_EVENT"));
        assertAll(
                () -> assertSame(failure, assertThrows(IllegalStateException.class,
                        () -> consumer.onMessage(record(EVENT_KEY, "{}"), acknowledgment), "Propaga el fallo"), "Conserva la excepcion"),
                () -> verifyNoInteractions(acknowledgment));
    }

    @Test
    void unknownSchemaHasSpecificReason() {
        String unknown = payload.replace("\"schemaVersion\":1", "\"schemaVersion\":99");
        consumer.onMessage(record(submission.searchId().toString(), unknown), acknowledgment);
        var order = inOrder(failures, acknowledgment);
        assertAll(
                () -> order.verify(failures).quarantine(rejection(submission.searchId().toString(), unknown, "UNSUPPORTED_SCHEMA")),
                () -> order.verify(acknowledgment).acknowledge());
    }

    private static ConsumerRecord<String, String> record(String key, String raw) {
        return new ConsumerRecord<>(TOPIC, 2, 42L, key, raw);
    }

    private static RejectedMessage rejection(String key, String raw, String reason) {
        return new RejectedMessage(TOPIC, 2, 42L, key, raw, reason);
    }
}
