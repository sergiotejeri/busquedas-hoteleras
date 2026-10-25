package com.example.busquedashoteleras.infrastructure.web;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ReadListener;
import jakarta.servlet.ServletException;
import jakarta.servlet.ServletInputStream;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletRequestWrapper;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.ObjectWriter;

@Component
@Order(Ordered.HIGHEST_PRECEDENCE)
public final class RequestBodyLimitFilter extends OncePerRequestFilter {
    public static final int MAX_BODY_BYTES = 1_048_576;
    private final ObjectWriter errorWriter;

    public RequestBodyLimitFilter(ObjectMapper mapper) {
        errorWriter = mapper.writerFor(ApiExceptionHandler.ApiError.class);
    }

    @Override
    protected boolean shouldNotFilter(HttpServletRequest request) {
        return !"POST".equals(request.getMethod());
    }

    @Override
    // Servlet owns the request stream; accessing it is the filter protocol.
    @SuppressWarnings("PMD.LawOfDemeter")
    protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response, FilterChain chain)
            throws IOException, ServletException {
        if (request.getContentLengthLong() > MAX_BODY_BYTES) {
            reject(response);
            return;
        }
        byte[] body = request.getInputStream().readNBytes(MAX_BODY_BYTES + 1);
        if (body.length > MAX_BODY_BYTES) {
            reject(response);
            return;
        }
        chain.doFilter(new BufferedRequest(request, body), response);
    }

    // Writing through the Servlet response stream is the adapter protocol.
    @SuppressWarnings("PMD.LawOfDemeter")
    private void reject(HttpServletResponse response) throws IOException {
        response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
        response.setContentType("application/json");
        var output = response.getOutputStream();
        byte[] error = errorWriter.writeValueAsBytes(new ApiExceptionHandler.ApiError(
                "REQUEST_TOO_LARGE", null, "Request body must not exceed 1048576 bytes."));
        output.write(error);
    }

    private static final class BufferedRequest extends HttpServletRequestWrapper {
        private final ServletInputStream input;

        private BufferedRequest(HttpServletRequest request, byte[] body) {
            super(request);
            input = new BufferedInput(body);
        }

        @Override
        public ServletInputStream getInputStream() {
            return input;
        }

        @Override
        public BufferedReader getReader() {
            return new BufferedReader(new InputStreamReader(input, StandardCharsets.UTF_8));
        }
    }

    private static final class BufferedInput extends ServletInputStream {
        private final ByteArrayInputStream input;

        private BufferedInput(byte[] body) {
            input = new ByteArrayInputStream(body);
        }

        @Override
        public int read() {
            return input.read();
        }

        @Override
        public int read(byte[] target, int offset, int length) {
            return input.read(target, offset, length);
        }

        @Override
        public boolean isFinished() {
            return input.available() == 0;
        }

        @Override
        public boolean isReady() {
            return true;
        }

        @Override
        public void setReadListener(ReadListener listener) {
            throw new UnsupportedOperationException("This bounded request supports synchronous reads only");
        }
    }
}
