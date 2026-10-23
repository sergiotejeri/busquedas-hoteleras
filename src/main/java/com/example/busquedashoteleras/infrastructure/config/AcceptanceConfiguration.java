package com.example.busquedashoteleras.infrastructure.config;

import com.example.busquedashoteleras.application.port.AcceptSearchPort;
import com.example.busquedashoteleras.application.service.AcceptSearchService;
import com.example.busquedashoteleras.infrastructure.oracle.OracleAcceptanceAdapter;
import java.util.UUID;
import javax.sql.DataSource;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration(proxyBeanMethods = false)
public class AcceptanceConfiguration {
    @Bean
    AcceptSearchPort acceptancePort(DataSource dataSource) {
        return new OracleAcceptanceAdapter(dataSource);
    }

    @Bean
    AcceptSearchService acceptSearchService(AcceptSearchPort port) {
        return new AcceptSearchService(port, UUID::randomUUID);
    }
}
