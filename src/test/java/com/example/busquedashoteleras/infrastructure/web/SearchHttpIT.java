package com.example.busquedashoteleras.infrastructure.web;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.example.busquedashoteleras.infrastructure.serialization.SearchEventCodec;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.core.env.Environment;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import tools.jackson.databind.json.JsonMapper;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class SearchHttpIT {
    private static final JsonMapper JSON = JsonMapper.builder().build();
    private static final String BODY = "{\"hotelId\":\"http-hotel\",\"checkIn\":\"01/01/2024\","
            + "\"checkOut\":\"02/01/2024\",\"ages\":[30,1]}";
    private final List<String> ids = new ArrayList<>();
    @Autowired
    private Environment environment;
    @Autowired
    private JdbcTemplate jdbc;

    @DynamicPropertySource
    static void database(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", () -> required("TEST_DB_URL"));
        registry.add("spring.datasource.username", () -> required("TEST_DB_USER"));
        registry.add("spring.datasource.password", () -> required("TEST_DB_PASSWORD"));
    }

    @AfterEach
    void removeOwnRequests() {
        for (String id : ids) {
            jdbc.update("DELETE FROM search_outbox WHERE search_id = HEXTORAW(?)", id);
            jdbc.update("DELETE FROM search_request WHERE search_id = HEXTORAW(?)", id);
        }
    }

    @Test
    void confirmsBothRowsBeforeReturningAndPreservesIndependentIds() throws IOException, InterruptedException {
        UUID first = accepted();
        UUID second = accepted();
        assertAll(
                () -> assertNotEquals(first, second, "Las solicitudes iguales deben tener IDs distintos"),
                () -> checkStored(first),
                () -> checkStored(second));
    }

    @Test
    void rejectsExcessiveEventAsGlobalErrorWithoutWriting() throws IOException, InterruptedException {
        int before = Objects.requireNonNull(jdbc.queryForObject("SELECT COUNT(*) FROM search_request", Integer.class));
        HttpResponse<String> response = post(BODY.replace("http-hotel", "x".repeat(262144)));
        var error = JSON.readTree(response.body());
        assertAll(
                () -> assertEquals(400, response.statusCode(), "El exceso de tamano debe ser BAD_REQUEST"),
                () -> assertEquals("REQUEST_TOO_LARGE", error.get("code").asString(), "Debe identificar el tamano"),
                () -> assertTrue(error.get("field").isNull(), "El limite es global, no de hotelId"),
                () -> assertEquals(before, jdbc.queryForObject("SELECT COUNT(*) FROM search_request", Integer.class),
                        "El rechazo no debe guardar la solicitud"));
    }

    private UUID accepted() throws IOException, InterruptedException {
        HttpResponse<String> response = post(BODY);
        assertEquals(202, response.statusCode(), response.body());
        UUID id = UUID.fromString(JSON.readTree(response.body()).get("searchId").asString());
        ids.add(id.toString().replace("-", ""));
        return id;
    }

    private void checkStored(UUID id) {
        String hex = id.toString().replace("-", "");
        String event = jdbc.queryForObject("SELECT event_json FROM search_outbox WHERE search_id = HEXTORAW(?)",
                String.class, hex);
        var submission = new SearchEventCodec().decode(event);
        assertAll(
                () -> assertEquals(id, submission.searchId(), "El evento debe corresponder al ID devuelto"),
                () -> assertEquals(List.of(30, 1), submission.criteria().ages(), "Debe conservar el orden de edades"),
                () -> assertEquals("PENDING", jdbc.queryForObject(
                        "SELECT status FROM search_request WHERE search_id = HEXTORAW(?)", String.class, hex),
                        "La solicitud debe estar confirmada, aun sin procesar"),
                () -> assertEquals("READY", jdbc.queryForObject(
                        "SELECT state FROM search_outbox WHERE search_id = HEXTORAW(?)", String.class, hex),
                        "El evento debe estar pendiente de publicacion"));
    }

    private HttpResponse<String> post(String body) throws IOException, InterruptedException {
        URI uri = URI.create("http://localhost:" + environment.getRequiredProperty("local.server.port") + "/search");
        try (HttpClient client = HttpClient.newHttpClient()) {
            return client.send(HttpRequest.newBuilder(uri).header("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofString(body)).build(), HttpResponse.BodyHandlers.ofString());
        }
    }

    private static String required(String variable) {
        return Objects.requireNonNull(System.getenv(variable), "Missing integration environment: " + variable);
    }
}
