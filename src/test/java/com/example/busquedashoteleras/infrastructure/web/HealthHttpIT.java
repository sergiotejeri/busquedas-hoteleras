package com.example.busquedashoteleras.infrastructure.web;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.reset;

import java.io.IOException;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.sql.SQLException;
import java.sql.DriverManager;
import java.time.Duration;
import java.util.Objects;
import java.util.UUID;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import com.zaxxer.hikari.HikariDataSource;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import javax.sql.DataSource;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.availability.AvailabilityChangeEvent;
import org.springframework.boot.availability.ReadinessState;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean;
import tools.jackson.databind.json.JsonMapper;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = {"app.outbox.enabled=false", "app.consumer.enabled=false"})
class HealthHttpIT {
    private static final String READINESS = "/actuator/health/readiness";
    private static final String LIVENESS = "/actuator/health/liveness";
    private static final JsonMapper JSON = JsonMapper.builder().build();
    @Autowired
    private ConfigurableApplicationContext context;
    @MockitoSpyBean
    private DataSource dataSource;

    @DynamicPropertySource
    static void database(DynamicPropertyRegistry properties) {
        properties.add("spring.datasource.url", () -> required("TEST_DB_URL"));
        properties.add("spring.datasource.username", () -> required("TEST_DB_USER"));
        properties.add("spring.datasource.password", () -> required("TEST_DB_PASSWORD"));
    }

    @ParameterizedTest
    @ValueSource(strings = {READINESS, LIVENESS})
    void exposesHealthWithoutInternalDetails(String path) throws IOException, InterruptedException {
        checkHealth(get(path), 200, "UP");
    }

    @Test
    @SuppressFBWarnings(value = "ODR_OPEN_DATABASE_RESOURCE",
            justification = "Mockito records getConnection stubbing; no JDBC connection is acquired")
    void databaseFailureChangesReadinessButNotLivenessAndRecovers()
            throws IOException, InterruptedException, SQLException {
        var before = get(READINESS);
        doThrow(new SQLException("private-database-address-and-password", "08006")).when(dataSource).getConnection();
        HttpResponse<String> readiness;
        HttpResponse<String> liveness;
        HttpResponse<String> count;
        try {
            readiness = get(READINESS);
            liveness = get(LIVENESS);
            count = get("/count?searchId=" + UUID.randomUUID());
        } finally {
            reset(dataSource);
        }
        var after = get(READINESS);
        assertAll(
                () -> checkHealth(before, 200, "UP"),
                () -> checkHealth(readiness, 503, "DOWN"),
                () -> checkHealth(liveness, 200, "UP"),
                () -> checkHealth(after, 200, "UP"),
                () -> assertEquals(503, count.statusCode(), "Una base de datos inaccesible no es un ID inexistente"),
                () -> assertEquals("STORAGE_UNAVAILABLE", JSON.readTree(count.body()).get("code").asString(), "Error seguro"),
                () -> assertFalse(count.body().contains("private-database"), "No expone la causa interna"));
    }

    @Test
    void refusingTrafficChangesReadinessWithoutBreakingLiveness() throws IOException, InterruptedException {
        AvailabilityChangeEvent.publish(context, ReadinessState.REFUSING_TRAFFIC);
        HttpResponse<String> readiness;
        HttpResponse<String> liveness;
        try {
            readiness = get(READINESS);
            liveness = get(LIVENESS);
        } finally {
            AvailabilityChangeEvent.publish(context, ReadinessState.ACCEPTING_TRAFFIC);
        }
        assertAll(
                () -> checkHealth(readiness, 503, "OUT_OF_SERVICE"),
                () -> checkHealth(liveness, 200, "UP"),
                () -> checkHealth(get(READINESS), 200, "UP"));
    }

    @ParameterizedTest
    @ValueSource(strings = {"env", "configprops", "beans", "loggers", "heapdump", "shutdown"})
    void doesNotExposeOtherManagementEndpoints(String endpoint) throws IOException, InterruptedException {
        assertEquals(404, get("/actuator/" + endpoint).statusCode(), "Solo se expone health");
    }

    @Test
    void boundsOracleHandshakeWhenListenerDoesNotRespond()
            throws IOException, InterruptedException, ExecutionException, TimeoutException {
        Properties properties = new Properties();
        properties.putAll(((HikariDataSource) dataSource).getDataSourceProperties());
        properties.setProperty("user", "probe");
        properties.setProperty("password", "probe");
        try (var listener = new ServerSocket(0); var worker = Executors.newSingleThreadExecutor()) {
            listener.setSoTimeout(10_000);
            var accepted = worker.submit(() -> {
                try (var socket = listener.accept()) {
                    socket.setSoTimeout(30_000);
                    socket.getInputStream().transferTo(OutputStream.nullOutputStream());
                }
                return null;
            });
            long started = System.nanoTime();
            SQLException failure = connectionFailure(listener.getLocalPort(), properties);
            Duration elapsed = Duration.ofNanos(System.nanoTime() - started);
            accepted.get(5, TimeUnit.SECONDS);
            assertAll(
                    () -> assertTrue(elapsed.compareTo(Duration.ofSeconds(10)) < 0,
                            "La negociacion no debe esperar al timeout de lectura de 20 segundos: " + elapsed),
                    () -> assertEquals(12170, failure.getErrorCode(), "Oracle identifica el timeout de conexion"));
        }
    }

    private static SQLException connectionFailure(int port, Properties properties) {
        return assertThrows(SQLException.class, () -> {
            try (var connection = DriverManager.getConnection(
                    "jdbc:oracle:thin:@//127.0.0.1:" + port + "/UNRESPONSIVE", properties)) {
                assertFalse(connection.isValid(1), "Un listener sin protocolo Oracle no puede dar una conexion valida");
            }
        }, "Un listener sin respuesta no puede establecer una sesion Oracle");
    }

    private HttpResponse<String> get(String path) throws IOException, InterruptedException {
        String port = context.getEnvironment().getRequiredProperty("local.server.port");
        try (var client = HttpClient.newHttpClient()) {
            return client.send(HttpRequest.newBuilder(URI.create("http://localhost:" + port + path))
                    .timeout(Duration.ofSeconds(10)).GET().build(), HttpResponse.BodyHandlers.ofString());
        }
    }

    private static void checkHealth(HttpResponse<String> response, int status, String state) {
        var body = JSON.readTree(response.body());
        assertAll(
                () -> assertEquals(status, response.statusCode(), response.body()),
                () -> assertEquals(state, body.get("status").asString(), "Estado de salud"),
                () -> assertEquals(1, body.size(), "Solo informa del estado, sin detalles internos"));
    }

    private static String required(String name) {
        return Objects.requireNonNull(System.getenv(name), name);
    }
}
