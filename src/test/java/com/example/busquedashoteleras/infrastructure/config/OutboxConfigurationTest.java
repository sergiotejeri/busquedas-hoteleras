package com.example.busquedashoteleras.infrastructure.config;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.example.busquedashoteleras.application.port.OutboxPort;
import com.example.busquedashoteleras.application.port.SearchPublisherPort;
import com.example.busquedashoteleras.infrastructure.kafka.KafkaSearchPublisher;
import com.example.busquedashoteleras.infrastructure.oracle.OracleOutboxAdapter;
import java.time.Duration;
import java.util.Optional;
import javax.sql.DataSource;
import org.junit.jupiter.api.Test;

class OutboxConfigurationTest {
    @Test
    void connectsRealAdaptersAndDelegatesScheduledWork() {
        var configuration = new OutboxConfiguration();
        var outbox = mock(OutboxPort.class);
        when(outbox.claimNext(Duration.ofSeconds(60))).thenReturn(Optional.empty());
        try (var producer = configuration.searchProducer("127.0.0.1:1")) {
            SearchPublisherPort publisher = configuration.searchPublisher(producer);
            var scheduler = configuration.outboxScheduler(configuration.publishOutboxService(outbox, publisher));
            scheduler.publishPending();
            assertAll(
                    () -> assertInstanceOf(KafkaSearchPublisher.class, publisher, "Publicador real"),
                    () -> assertInstanceOf(OracleOutboxAdapter.class, configuration.outboxPort(mock(DataSource.class)),
                            "Persistencia real"),
                    () -> verify(outbox).claimNext(Duration.ofSeconds(60)));
        }
    }

    @Test
    void leavesUnexpectedFailureVisibleAndAllowsNextInvocation() {
        var configuration = new OutboxConfiguration();
        var outbox = mock(OutboxPort.class);
        when(outbox.claimNext(Duration.ofSeconds(60)))
                .thenThrow(new IllegalStateException("database unavailable")).thenReturn(Optional.empty());
        var scheduler = configuration.outboxScheduler(configuration.publishOutboxService(outbox, mock(SearchPublisherPort.class)));
        assertAll(
                () -> assertThrows(IllegalStateException.class, scheduler::publishPending, "Debe propagar el fallo"),
                () -> {
                    scheduler.publishPending();
                    verify(outbox, org.mockito.Mockito.times(2)).claimNext(Duration.ofSeconds(60));
                });
    }
}
