package com.example.busquedashoteleras.infrastructure.kafka;

import com.example.busquedashoteleras.application.port.PublicationResult;
import com.example.busquedashoteleras.application.port.SearchPublisherPort;
import com.example.busquedashoteleras.infrastructure.serialization.InvalidSearchEventException;
import com.example.busquedashoteleras.infrastructure.serialization.SearchEventCodec;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.errors.RecordTooLargeException;
import org.apache.kafka.common.errors.SerializationException;

public final class KafkaSearchPublisher implements SearchPublisherPort {
    public static final String TOPIC = "hotel_availability_searches";
    private final Producer<String, String> producer;
    private final SearchEventCodec codec = new SearchEventCodec();

    /** The caller owns the producer lifecycle and must configure acknowledgements and timeouts. */
    public KafkaSearchPublisher(Producer<String, String> producer) {
        this.producer = Objects.requireNonNull(producer, "producer must not be null");
    }

    @Override
    public PublicationResult publish(UUID searchId, String eventJson) {
        Objects.requireNonNull(searchId, "searchId must not be null");
        Objects.requireNonNull(eventJson, "eventJson must not be null");
        try {
            if (!searchId.equals(codec.decode(eventJson).searchId())) {
                return PublicationResult.PERMANENT_FAILURE;
            }
            producer.send(new ProducerRecord<>(TOPIC, searchId.toString(), eventJson)).get(10, TimeUnit.SECONDS);
            return PublicationResult.CONFIRMED;
        } catch (InvalidSearchEventException exception) {
            return PublicationResult.PERMANENT_FAILURE;
        } catch (InterruptedException | InterruptException exception) {
            Thread.currentThread().interrupt();
            return PublicationResult.RETRYABLE_FAILURE;
        } catch (TimeoutException exception) {
            return PublicationResult.RETRYABLE_FAILURE;
        } catch (ExecutionException exception) {
            return classify(exception.getCause());
        } catch (KafkaException exception) {
            return classify(exception);
        }
    }

    private static PublicationResult classify(Throwable failure) {
        if (failure instanceof RecordTooLargeException || failure instanceof SerializationException) {
            return PublicationResult.PERMANENT_FAILURE;
        }
        if (failure instanceof KafkaException) {
            return PublicationResult.RETRYABLE_FAILURE;
        }
        throw new IllegalStateException("Unexpected Kafka publication failure", failure);
    }
}
