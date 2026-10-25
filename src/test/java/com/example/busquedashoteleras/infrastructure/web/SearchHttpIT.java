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

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, properties = {"app.outbox.enabled=false", "app.consumer.enabled=false"})
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
    void countExposesPendingThenFailedAndUnknownStates() throws IOException, InterruptedException {
        UUID id = accepted();
        var pending = get("/count?searchId=" + id);
        jdbc.update("UPDATE search_request SET status = 'FAILED' WHERE search_id = HEXTORAW(?)", id.toString().replace("-", ""));
        var failed = get("/count?searchId=" + id);
        var unknown = get("/count?searchId=" + UUID.randomUUID());
        assertAll(
                () -> assertEquals(202, pending.statusCode(), "La aceptacion pendiente debe distinguirse"),
                () -> assertEquals("1", pending.headers().firstValue("Retry-After").orElseThrow(), "Sugerencia de reconsulta"),
                () -> assertEquals("PENDING", JSON.readTree(pending.body()).get("status").asString(), "Estado pendiente"),
                () -> assertEquals(503, failed.statusCode(), "El fallo terminal es explicito"),
                () -> assertEquals("SEARCH_PROCESSING_FAILED", JSON.readTree(failed.body()).get("code").asString(), "Motivo seguro"),
                () -> assertEquals(404, unknown.statusCode(), "El ID ajeno no existe"));
    }

    @Test
    void servesOpenApiContractAndSwaggerUi() throws IOException, InterruptedException {
        var specification = get("/v3/api-docs");
        var root = JSON.readTree(specification.body());
        var count = root.at("/paths/~1count/get");
        var ui = get("/swagger-ui/index.html");
        var entry = get("/swagger-ui.html");
        var script = get("/swagger-ui/swagger-ui-bundle.js");
        assertAll(
                () -> assertEquals(200, specification.statusCode(), "OpenAPI disponible"),
                () -> assertEquals(200, ui.statusCode(), "Swagger disponible"),
                () -> assertTrue(ui.body().contains("swagger-ui"), "Se sirve la interfaz"),
                () -> assertEquals(302, entry.statusCode(), "La URL del README redirige a Swagger"),
                () -> assertTrue(entry.headers().firstValue("Location").orElseThrow().contains("swagger-ui/index.html"), "Destino correcto"),
                () -> assertEquals(200, script.statusCode(), "El recurso JavaScript de Swagger esta disponible"),
                () -> assertTrue(root.at("/paths/~1search/post/responses/202").isObject(), "Documenta POST aceptado"),
                () -> assertEquals("searchId", count.at("/parameters/0/name").asString(), "Parametro real"),
                () -> assertTrue(count.at("/parameters/0/required").asBoolean(), "Identificador obligatorio"),
                () -> assertEquals("#/components/schemas/CountResponse", count.at("/responses/200/content/application~1json/schema/$ref").asString(), "Respuesta tipada"),
                () -> assertEquals("int64", root.at("/components/schemas/CountResponse/properties/count/format").asString(), "Conteo de 64 bits"),
                () -> assertTrue(root.at("/components/schemas/SearchRequest/required").toString().contains("ages"), "Edades obligatorias"),
                () -> assertEquals(1, root.at("/components/schemas/SearchRequest/properties/ages/minItems").asInt(), "Lista no vacia"),
                () -> assertTrue(count.at("/responses/202").isObject(), "Documenta pendiente"),
                () -> assertTrue(count.at("/responses/400").isObject(), "Documenta validacion"),
                () -> assertTrue(count.at("/responses/404").isObject(), "Documenta inexistente"),
                () -> assertTrue(count.at("/responses/503").isObject(), "Documenta indisponibilidad"));
    }

    private HttpResponse<String> get(String path) throws IOException, InterruptedException {
        URI uri = URI.create("http://localhost:" + environment.getRequiredProperty("local.server.port") + path);
        try (var client = HttpClient.newHttpClient()) {
            return client.send(HttpRequest.newBuilder(uri).timeout(java.time.Duration.ofSeconds(5)).GET().build(), HttpResponse.BodyHandlers.ofString());
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
