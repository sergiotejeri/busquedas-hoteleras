package com.example.busquedashoteleras.infrastructure.web;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import jakarta.servlet.ServletInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;
import tools.jackson.databind.json.JsonMapper;

class RequestBodyLimitFilterTest {
    private static final int LIMIT = RequestBodyLimitFilter.MAX_BODY_BYTES;
    private final RequestBodyLimitFilter filter = new RequestBodyLimitFilter(JsonMapper.builder().build());

    @ParameterizedTest
    @ValueSource(longs = {1_048_577L, Long.MAX_VALUE})
    void rejectsOversizedDeclaredLengthWithoutReading(long declared) throws IOException, ServletException {
        var reads = new AtomicInteger();
        var request = request(declared, new byte[1], reads);
        var response = new MockHttpServletResponse();
        var chain = mock(FilterChain.class);
        filter.doFilter(request, response, chain);
        var error = JsonMapper.builder().build().readTree(response.getContentAsByteArray());
        assertAll(
                () -> assertEquals(400, response.getStatus(), "Rechazo antes de deserializar"),
                () -> assertEquals(0, reads.get(), "No lee un cuerpo declarado excesivo"),
                () -> assertEquals("REQUEST_TOO_LARGE", error.get("code").asString(), "Codigo global"),
                () -> assertTrue(error.get("field").isNull(), "No culpa a hotelId"),
                () -> verifyNoInteractions(chain));
    }

    @ParameterizedTest
    @ValueSource(longs = {-1, 0, 12})
    void doesNotTrustMissingOrUnderstatedLength(long declared) throws IOException, ServletException {
        var reads = new AtomicInteger();
        var response = new MockHttpServletResponse();
        var chain = mock(FilterChain.class);
        filter.doFilter(request(declared, new byte[LIMIT + 100], reads), response, chain);
        assertAll(
                () -> assertEquals(400, response.getStatus(), "El exceso real se rechaza"),
                () -> assertEquals(LIMIT + 1, reads.get(), "Solo lee el limite mas un byte"),
                () -> verifyNoInteractions(chain));
    }

    @ParameterizedTest
    @ValueSource(longs = {-1, 1_048_576})
    void acceptsExactlyTheLimitAndPreservesAllBytes(long declared) throws IOException, ServletException {
        byte[] body = "x".repeat(LIMIT).getBytes(StandardCharsets.UTF_8);
        var calls = new AtomicInteger();
        var received = new AtomicReference<byte[]>();
        filter.doFilter(request(declared, body, new AtomicInteger()), new MockHttpServletResponse(), (req, res) -> {
            calls.incrementAndGet();
            received.set(req.getInputStream().readAllBytes());
        });
        assertAll(
                () -> assertArrayEquals(body, received.get(), "La lectura posterior conserva todos los bytes"),
                () -> assertEquals(1, calls.get(), "Invoca una sola vez el resto de la cadena"));
    }

    @Test
    void preservesUnicodeForReaderAndExposesSynchronousStream() throws IOException, ServletException {
        byte[] body = "á漢🙂".getBytes(StandardCharsets.UTF_8);
        filter.doFilter(request(-1, body, new AtomicInteger()), new MockHttpServletResponse(), (req, res) -> {
            ServletInputStream input = req.getInputStream();
            assertAll(
                    () -> assertTrue(input.isReady(), "Buffer disponible"),
                    () -> assertFalse(input.isFinished(), "Todavia contiene datos"),
                    () -> assertThrows(UnsupportedOperationException.class, () -> input.setReadListener(null), "Solo lectura sincronica"),
                    () -> assertEquals("á漢🙂", req.getReader().readLine(), "Conserva UTF-8"),
                    () -> assertTrue(input.isFinished(), "Se consume el mismo buffer"),
                    () -> assertEquals(-1, input.read(), "Fin de entrada"));
        });
    }

    @Test
    void leavesNonPostRequestsUntouched() throws IOException, ServletException {
        var reads = new AtomicInteger();
        var request = request(Long.MAX_VALUE, new byte[1], reads);
        request.setMethod("GET");
        var calls = new AtomicInteger();
        filter.doFilter(request, new MockHttpServletResponse(), (req, res) -> calls.incrementAndGet());
        assertAll(
                () -> assertEquals(0, reads.get(), "No lee el cuerpo de GET"),
                () -> assertEquals(1, calls.get(), "Continua con la consulta"));
    }

    @Test
    void countsBytesRatherThanUnicodeCharacters() throws IOException, ServletException {
        byte[] body = "é".repeat(LIMIT / 2 + 1).getBytes(StandardCharsets.UTF_8);
        var response = new MockHttpServletResponse();
        filter.doFilter(request(-1, body, new AtomicInteger()), response, mock(FilterChain.class));
        assertEquals(400, response.getStatus(), "El limite se expresa en bytes");
    }

    private static MockHttpServletRequest request(long declared, byte[] body, AtomicInteger reads) {
        return new MockHttpServletRequest("POST", "/search") {
            private final ServletInputStream source = new org.springframework.mock.web.DelegatingServletInputStream(
                    new java.io.ByteArrayInputStream(body)) {
                @Override
                public int read() throws IOException {
                    int value = super.read();
                    if (value >= 0) {
                        reads.incrementAndGet();
                    }
                    return value;
                }
            };

            @Override
            public long getContentLengthLong() {
                return declared;
            }

            @Override
            @SuppressFBWarnings(value = "EI_EXPOSE_REP", justification = "The Servlet API exposes the same consumable input stream")
            public ServletInputStream getInputStream() {
                return source;
            }
        };
    }
}
