package com.example.busquedashoteleras.infrastructure.config;

import com.example.busquedashoteleras.application.port.OutboxPort;
import com.example.busquedashoteleras.application.port.SearchPublisherPort;
import com.example.busquedashoteleras.application.service.PublishOutboxService;
import com.example.busquedashoteleras.infrastructure.kafka.KafkaProducerSettings;
import com.example.busquedashoteleras.infrastructure.kafka.KafkaSearchPublisher;
import com.example.busquedashoteleras.infrastructure.oracle.OracleOutboxAdapter;
import javax.sql.DataSource;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableScheduling;

@Configuration(proxyBeanMethods = false)
@EnableScheduling
@ConditionalOnProperty(name = "app.outbox.enabled", havingValue = "true", matchIfMissing = true)
public class OutboxConfiguration {
    @Bean(destroyMethod = "close")
    Producer<String, String> searchProducer(@Value("${app.kafka.bootstrap-servers}") String servers) {
        return new KafkaProducer<>(KafkaProducerSettings.properties(servers));
    }

    @Bean
    SearchPublisherPort searchPublisher(Producer<String, String> producer) {
        return new KafkaSearchPublisher(producer);
    }

    @Bean
    OutboxPort outboxPort(DataSource dataSource) {
        return new OracleOutboxAdapter(dataSource);
    }

    @Bean
    PublishOutboxService publishOutboxService(OutboxPort outbox, SearchPublisherPort publisher) {
        return new PublishOutboxService(outbox, publisher);
    }

    @Bean
    OutboxScheduler outboxScheduler(PublishOutboxService service) {
        return new OutboxScheduler(service);
    }
}
