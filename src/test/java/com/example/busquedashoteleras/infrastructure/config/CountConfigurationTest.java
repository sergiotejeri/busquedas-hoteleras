package com.example.busquedashoteleras.infrastructure.config;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.example.busquedashoteleras.application.port.SearchQueryPort;
import com.example.busquedashoteleras.infrastructure.oracle.OracleSearchQueryAdapter;
import java.util.Optional;
import java.util.UUID;
import javax.sql.DataSource;
import org.junit.jupiter.api.Test;

class CountConfigurationTest {
    @Test
    void wiresOracleAndDelegatesToQueryPort() {
        var configuration = new CountConfiguration();
        var port = mock(SearchQueryPort.class);
        var id = UUID.randomUUID();
        when(port.find(id)).thenReturn(Optional.empty());
        assertAll(
                () -> assertInstanceOf(OracleSearchQueryAdapter.class, configuration.searchQueryPort(mock(DataSource.class)), "Oracle real"),
                () -> assertEquals(Optional.empty(), configuration.getSearchCountService(port).find(id), "Conecta el caso de uso"));
    }
}
