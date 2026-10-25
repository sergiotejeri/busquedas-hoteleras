package com.example.busquedashoteleras.infrastructure.kafka;

import com.example.busquedashoteleras.application.port.FailurePort;
import com.example.busquedashoteleras.application.port.ProcessingResult;
import com.example.busquedashoteleras.application.port.RejectedMessage;
import com.example.busquedashoteleras.domain.SearchSubmission;
import com.example.busquedashoteleras.infrastructure.serialization.InvalidSearchEventException;
import com.example.busquedashoteleras.infrastructure.serialization.SearchEventCodec;
import java.util.Objects;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.support.Acknowledgment;

public final class KafkaSearchConsumer {
    private final VirtualPersistenceExecutor persistence;
    private final FailurePort failures;
    private final SearchEventCodec codec = new SearchEventCodec();

    public KafkaSearchConsumer(VirtualPersistenceExecutor persistence, FailurePort failures) {
        this.persistence = Objects.requireNonNull(persistence, "persistence must not be null");
        this.failures = Objects.requireNonNull(failures, "failures must not be null");
    }

    public void onMessage(ConsumerRecord<String, String> record, Acknowledgment acknowledgment) {
        SearchSubmission submission;
        try {
            submission = codec.decode(record.value());
        } catch (InvalidSearchEventException invalid) {
            quarantine(record, invalid.getReasonCode());
            acknowledgment.acknowledge();
            return;
        }
        if (!submission.searchId().toString().equals(record.key())) {
            quarantine(record, "KEY_ID_MISMATCH");
        } else {
            ProcessingResult result = persistence.process(submission);
            switch (result) {
                case PROCESSED, ALREADY_PROCESSED -> { }
                case UNKNOWN_SEARCH, CRITERIA_MISMATCH, FAILED_SEARCH -> quarantine(record, result.name());
            }
        }
        acknowledgment.acknowledge();
    }

    private void quarantine(ConsumerRecord<String, String> record, String reason) {
        failures.quarantine(new RejectedMessage(record.topic(), record.partition(), record.offset(),
                record.key(), record.value(), reason));
    }
}
