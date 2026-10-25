package com.example.busquedashoteleras.infrastructure.oracle;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Objects;
import java.util.UUID;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class EnvironmentIT {
    private static final String URL = requiredEnvironment("TEST_DB_URL");
    private static final String USER = requiredEnvironment("TEST_DB_USER");
    private static final String PASSWORD = requiredEnvironment("TEST_DB_PASSWORD");
    private static final String INSERT_REQUEST =
            "INSERT INTO search_request (search_id, criteria_json, status) VALUES (HEXTORAW(?), ?, ?)";

    @BeforeAll
    static void migrateSchema() {
        flyway().migrate();
    }

    @Test
    void connectsToTestSchemaAndKeepsMigrationRepeatable() throws SQLException {
        try (Connection connection = connection();
                PreparedStatement query = connection.prepareStatement("SELECT USER FROM DUAL");
                ResultSet result = query.executeQuery()) {
            requireRow(result);
            String actualUser = result.getString(1);
            Flyway migrations = flyway();
            migrations.validate();
            assertAll(
                    () -> assertEquals("SEARCH_IT", actualUser, "Debe usar el esquema aislado de pruebas"),
                    () -> assertEquals("6", migrations.info().current().getVersion().getVersion(),
                            "Debe haber aplicado las migraciones de aceptacion, reservas, reintentos, grupos, procesamiento y cuarentena"),
                    () -> assertEquals(0, migrations.migrate().migrationsExecuted,
                            "Repetir la migracion no debe recrear las tablas"));
        }
    }

    @Test
    void rollsBackRequestAndOutboxTogether() throws SQLException {
        String id = identifier();
        try (Connection connection = connection()) {
            connection.setAutoCommit(false);
            insertRequest(connection, id, "PENDING");
            try (PreparedStatement outbox = connection.prepareStatement(
                    "INSERT INTO search_outbox (search_id, event_json) VALUES (HEXTORAW(?), ?)")) {
                outbox.setString(1, id);
                outbox.setString(2, "{}");
                outbox.executeUpdate();
            }
            connection.rollback();
            try (PreparedStatement query = connection.prepareStatement(
                    "SELECT (SELECT COUNT(*) FROM search_request WHERE search_id = HEXTORAW(?))"
                    + " + (SELECT COUNT(*) FROM search_outbox WHERE search_id = HEXTORAW(?)) FROM DUAL")) {
                query.setString(1, id);
                query.setString(2, id);
                try (ResultSet result = query.executeQuery()) {
                    requireRow(result);
                    assertEquals(0, result.getInt(1), "El rollback debe deshacer ambas escrituras");
                }
            }
        }
    }

    @Test
    // El tipo de excepcion y el codigo Oracle comprueban un mismo rechazo.
    @SuppressWarnings("PMD.UnitTestContainsTooManyAsserts")
    void rejectsOutboxWithoutRequest() throws SQLException {
        try (Connection connection = connection();
                PreparedStatement insert = connection.prepareStatement(
                        "INSERT INTO search_outbox (search_id, event_json) VALUES (HEXTORAW(?), ?)")) {
            connection.setAutoCommit(false);
            insert.setString(1, identifier());
            insert.setString(2, "{}");
            assertEquals(2291, assertThrows(SQLException.class, insert::executeUpdate).getErrorCode(),
                    "La clave foranea debe impedir eventos huerfanos");
            connection.rollback();
        }
    }

    @Test
    // El tipo de excepcion y el codigo Oracle comprueban un mismo rechazo.
    @SuppressWarnings("PMD.UnitTestContainsTooManyAsserts")
    void rejectsInvalidRequestState() throws SQLException {
        try (Connection connection = connection()) {
            connection.setAutoCommit(false);
            assertEquals(2290, assertThrows(SQLException.class,
                    () -> insertRequest(connection, identifier(), "UNKNOWN")).getErrorCode(),
                    "La restriccion debe rechazar estados desconocidos");
            connection.rollback();
        }
    }

    @Test
    // El tipo de excepcion y el codigo Oracle comprueban un mismo rechazo.
    @SuppressWarnings("PMD.UnitTestContainsTooManyAsserts")
    void rejectsDuplicateRequestIdentity() throws SQLException {
        try (Connection connection = connection()) {
            connection.setAutoCommit(false);
            String id = identifier();
            insertRequest(connection, id, "PENDING");
            assertEquals(1, assertThrows(SQLException.class,
                    () -> insertRequest(connection, id, "PENDING")).getErrorCode(),
                    "La clave primaria debe impedir IDs duplicados");
            connection.rollback();
        }
    }

    private static void insertRequest(Connection connection, String id, String status) throws SQLException {
        try (PreparedStatement insert = connection.prepareStatement(INSERT_REQUEST)) {
            insert.setString(1, id);
            insert.setString(2, "{}");
            insert.setString(3, status);
            insert.executeUpdate();
        }
    }

    private static void requireRow(ResultSet result) throws SQLException {
        if (!result.next()) {
            throw new SQLException("La consulta debe devolver una fila");
        }
    }

    private static Flyway flyway() {
        return Flyway.configure().dataSource(URL, USER, PASSWORD).cleanDisabled(true)
                .locations("classpath:db/migration").load();
    }

    private static Connection connection() throws SQLException {
        return DriverManager.getConnection(URL, USER, PASSWORD);
    }

    private static String identifier() {
        return UUID.randomUUID().toString().replace("-", "");
    }

    private static String requiredEnvironment(String name) {
        return Objects.requireNonNull(System.getenv(name), "Missing integration environment: " + name);
    }
}
