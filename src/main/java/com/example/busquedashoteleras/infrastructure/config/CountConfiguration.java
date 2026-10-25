package com.example.busquedashoteleras.infrastructure.config;

import com.example.busquedashoteleras.application.port.SearchQueryPort;
import com.example.busquedashoteleras.application.service.GetSearchCountService;
import com.example.busquedashoteleras.infrastructure.oracle.OracleSearchQueryAdapter;
import javax.sql.DataSource;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration(proxyBeanMethods = false)
public class CountConfiguration {
    @Bean
    SearchQueryPort searchQueryPort(DataSource source) {
        return new OracleSearchQueryAdapter(source);
    }

    @Bean
    GetSearchCountService getSearchCountService(SearchQueryPort port) {
        return new GetSearchCountService(port);
    }
}
