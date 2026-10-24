package com.example.busquedashoteleras.infrastructure.kafka;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import com.example.busquedashoteleras.application.port.PublicationResult;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.errors.RecordTooLargeException;
import org.apache.kafka.common.errors.SerializationException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.NullSource;
import org.junit.jupiter.params.provider.ValueSource;

class KafkaSearchPublisherTest {
    private static final UUID ID = new UUID(0, 1);
    private static final String EVENT = """
            {"schemaVersion":1,"searchId":"00000000-0000-0000-0000-000000000001",\
            "hotelId":"hotel-ñ","checkIn":"01/12/2026","checkOut":"03/12/2026","ages":[30,1]}
            """;
    @SuppressWarnings("unchecked")
    private final Producer<String, String> producer = mock(Producer.class);
    @SuppressWarnings("unchecked")
    private final Future<RecordMetadata> acknowledgement = mock(Future.class);

    @Test
    void confirmsOnlyAfterAcknowledgementAndPreservesOriginalRecord()
            throws InterruptedException, ExecutionException, TimeoutException {
        when(producer.send(any())).thenReturn(acknowledgement);
        when(acknowledgement.get(10, TimeUnit.SECONDS)).thenReturn(
                new RecordMetadata(new TopicPartition("hotel_availability_searches", 0), 7, 0, 0, 36, 150));
        PublicationResult result = new KafkaSearchPublisher(producer).publish(ID, EVENT);
        var order = inOrder(producer, acknowledgement);
        assertAll(
                () -> assertEquals(PublicationResult.CONFIRMED, result, "Debe esperar confirmacion"),
                () -> {
                    order.verify(producer).send(new ProducerRecord<>("hotel_availability_searches", ID.toString(), EVENT));
                    order.verify(acknowledgement).get(10, TimeUnit.SECONDS);
                });
    }

    @ParameterizedTest
    @CsvSource({"size,PERMANENT_FAILURE", "serialization,PERMANENT_FAILURE", "authorization,RETRYABLE_FAILURE", "timeout,RETRYABLE_FAILURE"})
    void classifiesSynchronousAndAsynchronousBrokerFailures(String reason, PublicationResult expected)
            throws InterruptedException, ExecutionException, TimeoutException {
        RuntimeException failure = switch (reason) {
            case "size" -> new RecordTooLargeException("oversized");
            case "serialization" -> new SerializationException("invalid record");
            case "authorization" -> new AuthorizationException("global configuration");
            default -> new org.apache.kafka.common.errors.TimeoutException("broker unavailable");
        };
        when(producer.send(any())).thenThrow(failure);
        PublicationResult synchronous = new KafkaSearchPublisher(producer).publish(ID, EVENT);
        org.mockito.Mockito.doReturn(acknowledgement).when(producer).send(any());
        when(acknowledgement.get(10, TimeUnit.SECONDS)).thenThrow(new ExecutionException(failure));
        PublicationResult asynchronous = new KafkaSearchPublisher(producer).publish(ID, EVENT);
        assertAll(
                () -> assertEquals(expected, synchronous, "Debe clasificar el fallo al enviar"),
                () -> assertEquals(expected, asynchronous, "Debe clasificar el fallo de confirmacion"));
    }

    @Test
    void acknowledgementTimeoutDoesNotConfirmPublication()
            throws InterruptedException, ExecutionException, TimeoutException {
        when(producer.send(any())).thenReturn(acknowledgement);
        when(acknowledgement.get(10, TimeUnit.SECONDS)).thenThrow(new TimeoutException("no acknowledgement"));
        assertEquals(PublicationResult.RETRYABLE_FAILURE, new KafkaSearchPublisher(producer).publish(ID, EVENT),
                "Un timeout no prueba que el broker haya rechazado ni confirmado");
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void restoresInterruptAndLeavesPublicationRetryable(boolean duringSend)
            throws InterruptedException, ExecutionException, TimeoutException {
        if (duringSend) {
            when(producer.send(any())).thenThrow(new InterruptException(new InterruptedException("stop")));
        } else {
            when(producer.send(any())).thenReturn(acknowledgement);
            when(acknowledgement.get(10, TimeUnit.SECONDS)).thenThrow(new InterruptedException("stop"));
        }
        try {
            assertAll(
                    () -> assertEquals(PublicationResult.RETRYABLE_FAILURE, new KafkaSearchPublisher(producer).publish(ID, EVENT),
                            "Interrumpir no confirma ni rechaza el evento"),
                    () -> assertTrue(Thread.currentThread().isInterrupted(), "Debe conservar la interrupcion"));
        } finally {
            Thread.interrupted();
        }
    }

    @Test
    void unexpectedFailureIsNotSilentlyClassifiedAsEventRejection()
            throws InterruptedException, ExecutionException, TimeoutException {
        when(producer.send(any())).thenReturn(acknowledgement);
        when(acknowledgement.get(10, TimeUnit.SECONDS)).thenThrow(new ExecutionException(new IllegalArgumentException("bug")));
        assertThrows(IllegalStateException.class, () -> new KafkaSearchPublisher(producer).publish(ID, EVENT));
    }

    @ParameterizedTest
    @ValueSource(strings = {"{}", "null", "not-json"})
    void rejectsInvalidStoredPayloadBeforeSending(String payload) {
        assertAll(
                () -> assertEquals(PublicationResult.PERMANENT_FAILURE, new KafkaSearchPublisher(producer).publish(ID, payload),
                        "Un evento invalido no debe enviarse"),
                () -> verifyNoInteractions(producer));
    }

    @Test
    void rejectsMismatchedIdentityBeforeSending() {
        assertAll(
                () -> assertEquals(PublicationResult.PERMANENT_FAILURE,
                        new KafkaSearchPublisher(producer).publish(new UUID(0, 2), EVENT), "La key debe corresponder al evento"),
                () -> verifyNoInteractions(producer));
    }

    @ParameterizedTest
    @NullSource
    void requiresProducer(Producer<String, String> missing) {
        assertThrows(NullPointerException.class, () -> new KafkaSearchPublisher(missing));
    }

    @ParameterizedTest
    @NullSource
    void requiresIdentity(UUID missing) {
        assertAll(
                () -> assertThrows(NullPointerException.class, () -> new KafkaSearchPublisher(producer).publish(missing, EVENT)),
                () -> verifyNoInteractions(producer));
    }

    @ParameterizedTest
    @NullSource
    void requiresPayload(String missing) {
        assertAll(
                () -> assertThrows(NullPointerException.class, () -> new KafkaSearchPublisher(producer).publish(ID, missing)),
                () -> verifyNoInteractions(producer));
    }
}
