package com.example.busquedashoteleras.infrastructure.web;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

/** Comprobacion optativa contra la aplicacion de Compose, fuera de la suite habitual. */
final class HttpLoadCheck {
    private static final int REQUESTS = 200;
    private static final int CLIENTS = 16;
    private static final int WARMUP = 10;
    private static final String RUN_MODE = "run";
    private static final String VERIFY_MODE = "verify";
    private static final String RUN_FIELD = "run";
    private static final String STATUS_FIELD = "status";
    private static final JsonMapper JSON = JsonMapper.builder().build();
    private final String base = Objects.requireNonNull(System.getenv("LOAD_BASE_URL"), "LOAD_BASE_URL");
    private final Path report = Path.of(Objects.requireNonNull(System.getenv("LOAD_REPORT"), "LOAD_REPORT"));

    @Test
    void measuresBoundedLoadOrVerifiesSavedRequests() {
        assertAll(this::runScenario);
    }

    private void runScenario() throws IOException, InterruptedException, ExecutionException {
        try (var client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build()) {
            String mode = Objects.requireNonNullElse(System.getenv("LOAD_MODE"), RUN_MODE);
            assertTrue(RUN_MODE.equals(mode) || VERIFY_MODE.equals(mode), "LOAD_MODE debe ser run o verify");
            if (VERIFY_MODE.equals(mode)) {
                verifySavedRequests(client);
                return;
            }
            String run = "load-" + UUID.randomUUID();
            Files.writeString(report, JSON.writeValueAsString(Map.of(STATUS_FIELD, "RUNNING", RUN_FIELD, run)));
            Files.writeString(report.resolveSibling("http-load-restart.json"), JSON.writeValueAsString(
                    Map.of(STATUS_FIELD, "NOT_VERIFIED", RUN_FIELD, run)));
            awaitReady(client);
            List<Sample> warmup = new ArrayList<>();
            for (int index = 0; index < WARMUP; index++) {
                warmup.add(submit(client, run + "-warmup-" + index, 1));
            }
            awaitCounts(client, warmup);
            List<Callable<Sample>> posts = new ArrayList<>();
            for (int index = 0; index < REQUESTS; index++) {
                boolean shared = index < REQUESTS / 2;
                String hotel = shared ? run + "-shared" : run + "-unique-" + index;
                int expected = shared ? REQUESTS / 2 : 1;
                posts.add(() -> submit(client, hotel, expected));
            }
            long started = System.nanoTime();
            List<Sample> accepted = parallel(posts);
            var postStats = stats(accepted, elapsedMillis(started));
            List<Sample> all = new ArrayList<>(warmup);
            all.addAll(accepted);
            assertEquals(REQUESTS + WARMUP, all.stream().map(Sample::id).distinct().count(), "IDs independientes");
            long verificationStarted = System.nanoTime();
            awaitCounts(client, all);
            double verificationMillis = elapsedMillis(verificationStarted);
            List<Callable<Sample>> reads = new ArrayList<>();
            for (Sample sample : accepted) {
                reads.add(() -> measureRead(client, sample));
                reads.add(() -> measureRead(client, sample));
            }
            long readsStarted = System.nanoTime();
            List<Sample> measuredReads = parallel(reads);
            Map<String, Object> result = Map.of(STATUS_FIELD, "PASSED", RUN_FIELD, run, "clients", CLIENTS,
                    "warmupRequests", WARMUP, "post", postStats, "get", stats(measuredReads, elapsedMillis(readsStarted)),
                    "verificationMillis", verificationMillis, "accepted", all);
            Files.writeString(report, JSON.writerWithDefaultPrettyPrinter().writeValueAsString(result));
        }
    }

    private void verifySavedRequests(HttpClient client) throws IOException, InterruptedException {
        JsonNode saved = JSON.readTree(Files.readString(report));
        Files.writeString(report.resolveSibling("http-load-restart.json"), JSON.writeValueAsString(
                Map.of(STATUS_FIELD, "RUNNING", RUN_FIELD, saved.path(RUN_FIELD).asString())));
        assertEquals("PASSED", saved.path(STATUS_FIELD).asString(), "Solo se verifica una carga terminada");
        List<Sample> accepted = new ArrayList<>();
        for (JsonNode sample : saved.path("accepted")) {
            accepted.add(JSON.treeToValue(sample, Sample.class));
        }
        assertEquals(REQUESTS + WARMUP, accepted.size(), "El informe conserva todos los IDs");
        awaitReady(client);
        awaitCounts(client, accepted);
        Files.writeString(report.resolveSibling("http-load-restart.json"), JSON.writeValueAsString(
                Map.of(STATUS_FIELD, "PASSED", RUN_FIELD, saved.path(RUN_FIELD).asString(), "verifiedRequests", accepted.size())));
    }

    private void awaitReady(HttpClient client) throws IOException, InterruptedException {
        long deadline = System.nanoTime() + Duration.ofSeconds(90).toNanos();
        IOException lastFailure = null;
        while (System.nanoTime() < deadline) {
            try {
                if (get(client, "/actuator/health/readiness").statusCode() == HttpStatus.OK.value()) {
                    return;
                }
            } catch (IOException unavailable) {
                lastFailure = unavailable;
            }
            TimeUnit.MILLISECONDS.sleep(250);
        }
        throw new AssertionError("La aplicacion no estuvo disponible en 90 segundos", lastFailure);
    }

    private Sample submit(HttpClient client, String hotel, int expected) throws IOException, InterruptedException {
        String body = JSON.writeValueAsString(Map.of("hotelId", hotel, "checkIn", "01/12/2026",
                "checkOut", "03/12/2026", "ages", List.of(30, 1, 1)));
        var request = HttpRequest.newBuilder(URI.create(base + "/search")).timeout(Duration.ofSeconds(10))
                .header("Content-Type", "application/json").POST(HttpRequest.BodyPublishers.ofString(body)).build();
        long started = System.nanoTime();
        var response = client.send(request, HttpResponse.BodyHandlers.ofString());
        assertEquals(202, response.statusCode(), response.body());
        UUID id = UUID.fromString(JSON.readTree(response.body()).path("searchId").asString());
        return new Sample(id, hotel, expected, elapsedMillis(started));
    }

    private void awaitCounts(HttpClient client, List<Sample> accepted) throws IOException, InterruptedException {
        long deadline = System.nanoTime() + Duration.ofSeconds(120).toNanos();
        for (Sample sample : accepted) {
            assertTrue(System.nanoTime() < deadline, "La comprobacion excedio los 120 segundos");
            while (!matchesCount(client, sample)) {
                assertTrue(System.nanoTime() < deadline, "El procesamiento no convergio en 120 segundos: " + sample.id());
                TimeUnit.MILLISECONDS.sleep(100);
            }
        }
    }

    private Sample measureRead(HttpClient client, Sample sample) throws IOException, InterruptedException {
        long started = System.nanoTime();
        assertTrue(matchesCount(client, sample), "Una busqueda ya procesada conserva su conteo");
        return new Sample(sample.id(), sample.hotel(), sample.expected(), elapsedMillis(started));
    }

    private boolean matchesCount(HttpClient client, Sample sample) throws IOException, InterruptedException {
        var response = get(client, "/count?searchId=" + sample.id());
        if (response.statusCode() == HttpStatus.ACCEPTED.value()) {
            return false;
        }
        assertEquals(200, response.statusCode(), response.body());
        JsonNode body = JSON.readTree(response.body());
        long count = body.path("count").asLong();
        assertAll(
                () -> assertEquals(sample.id().toString(), body.path("searchId").asString(), "ID consultado"),
                () -> assertEquals(sample.hotel(), body.at("/search/hotelId").asString(), "Hotel conservado"),
                () -> assertEquals("01/12/2026", body.at("/search/checkIn").asString(), "Fecha de entrada"),
                () -> assertEquals("03/12/2026", body.at("/search/checkOut").asString(), "Fecha de salida"),
                () -> assertEquals("[30,1,1]", body.at("/search/ages").toString(), "Orden y repeticiones"),
                () -> assertTrue(count > 0 && count <= sample.expected(), "No mezcla grupos ni duplica el conteo"));
        return count == sample.expected();
    }

    private HttpResponse<String> get(HttpClient client, String path) throws IOException, InterruptedException {
        return client.send(HttpRequest.newBuilder(URI.create(base + path)).timeout(Duration.ofSeconds(5)).GET().build(),
                HttpResponse.BodyHandlers.ofString());
    }

    private static List<Sample> parallel(List<Callable<Sample>> tasks) throws InterruptedException, ExecutionException {
        try (var executor = Executors.newFixedThreadPool(CLIENTS)) {
            var futures = executor.invokeAll(tasks, 120, TimeUnit.SECONDS);
            List<Sample> results = new ArrayList<>();
            for (var future : futures) {
                results.add(future.get());
            }
            return results;
        }
    }

    private static Map<String, Object> stats(List<Sample> samples, double millis) {
        double[] sorted = samples.stream().mapToDouble(Sample::millis).sorted().toArray();
        return Map.of("requests", sorted.length, "millis", millis, "requestsPerSecond", sorted.length * 1000.0 / millis,
                "p50Millis", sorted[(int) Math.ceil(sorted.length * 0.50) - 1],
                "p95Millis", sorted[(int) Math.ceil(sorted.length * 0.95) - 1],
                "p99Millis", sorted[(int) Math.ceil(sorted.length * 0.99) - 1]);
    }

    private static double elapsedMillis(long start) {
        return (System.nanoTime() - start) / 1_000_000.0;
    }

    private record Sample(UUID id, String hotel, int expected, double millis) { }
}
