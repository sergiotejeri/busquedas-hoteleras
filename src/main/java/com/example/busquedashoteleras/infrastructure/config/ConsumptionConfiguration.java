package com.example.busquedashoteleras.infrastructure.config;

import com.example.busquedashoteleras.application.port.FailurePort;
import com.example.busquedashoteleras.application.port.ProcessSearchPort;
import com.example.busquedashoteleras.application.service.ProcessSearchService;
import com.example.busquedashoteleras.infrastructure.kafka.KafkaSearchConsumer;
import com.example.busquedashoteleras.infrastructure.kafka.KafkaSearchPublisher;
import com.example.busquedashoteleras.infrastructure.kafka.VirtualPersistenceExecutor;
import com.example.busquedashoteleras.infrastructure.oracle.OracleFailureAdapter;
import com.example.busquedashoteleras.infrastructure.oracle.OracleProcessingAdapter;
import java.time.Duration;
import java.util.Map;
import javax.sql.DataSource;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.AcknowledgingMessageListener;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

@Configuration(proxyBeanMethods = false)
@ConditionalOnProperty(name = "app.consumer.enabled", havingValue = "true", matchIfMissing = true)
public class ConsumptionConfiguration {
    @Bean
    ProcessSearchPort processingPort(DataSource source) {
        return new OracleProcessingAdapter(source);
    }

    @Bean
    FailurePort failurePort(DataSource source) {
        return new OracleFailureAdapter(source);
    }

    @Bean(destroyMethod = "close")
    VirtualPersistenceExecutor persistenceExecutor(ProcessSearchPort port) {
        return new VirtualPersistenceExecutor(new ProcessSearchService(port));
    }

    @Bean
    KafkaSearchConsumer searchConsumer(VirtualPersistenceExecutor persistence, FailurePort failures) {
        return new KafkaSearchConsumer(persistence, failures);
    }

    @Bean
    ConcurrentMessageListenerContainer<String, String> searchListener(
            KafkaSearchConsumer listener,
            @Value("${app.kafka.bootstrap-servers}") String servers,
            @Value("${app.kafka.consumer-group:hotel-search-processing}") String group,
            @Value("${app.consumer.concurrency:3}") int concurrency) {
        var properties = new ContainerProperties(KafkaSearchPublisher.TOPIC);
        properties.setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
        properties.setSyncCommits(true);
        properties.setSyncCommitTimeout(Duration.ofSeconds(10));
        properties.setShutdownTimeout(40_000);
        properties.setMessageListener((AcknowledgingMessageListener<String, String>) listener::onMessage);
        var container = new ConcurrentMessageListenerContainer<>(
                new DefaultKafkaConsumerFactory<String, String>(consumerSettings(servers, group)), properties);
        container.setConcurrency(concurrency);
        var errors = new DefaultErrorHandler(new FixedBackOff(1_000, FixedBackOff.UNLIMITED_ATTEMPTS));
        errors.setClassifications(Map.of(), true);
        errors.setAckAfterHandle(false);
        container.setCommonErrorHandler(errors);
        return container;
    }

    static Map<String, Object> consumerSettings(String servers, String group) {
        return Map.ofEntries(
                Map.entry(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, servers),
                Map.entry(ConsumerConfig.GROUP_ID_CONFIG, group),
                Map.entry(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class),
                Map.entry(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class),
                Map.entry(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false),
                Map.entry(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"),
                Map.entry(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 1),
                Map.entry(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 120_000),
                Map.entry(ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG, false));
    }
}
