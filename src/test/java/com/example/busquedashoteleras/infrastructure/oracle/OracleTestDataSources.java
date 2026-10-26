package com.example.busquedashoteleras.infrastructure.oracle;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.util.Objects;

/** Each integration class owns and closes its pool after all its tests. */
public final class OracleTestDataSources {
    private OracleTestDataSources() { }

    public static HikariDataSource create() {
        HikariConfig configuration = new HikariConfig();
        configuration.setJdbcUrl(required("TEST_DB_URL"));
        configuration.setUsername(required("TEST_DB_USER"));
        configuration.setPassword(required("TEST_DB_PASSWORD"));
        configuration.setMaximumPoolSize(10);
        configuration.setMinimumIdle(0);
        configuration.setConnectionTimeout(3_000);
        configuration.setValidationTimeout(1_000);
        configuration.addDataSourceProperty("oracle.net.CONNECT_TIMEOUT", "3000");
        configuration.addDataSourceProperty("oracle.net.OUTBOUND_CONNECT_TIMEOUT", "3000ms");
        configuration.addDataSourceProperty("oracle.jdbc.ReadTimeout", "20000");
        return new HikariDataSource(configuration);
    }

    private static String required(String name) {
        return Objects.requireNonNull(System.getenv(name), name);
    }
}
