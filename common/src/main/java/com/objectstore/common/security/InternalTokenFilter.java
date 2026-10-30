package com.objectstore.common.security;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.objectstore.common.error.ErrorCode;
import com.objectstore.common.error.ErrorResponse;
import com.objectstore.common.http.InternalHeaders;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.web.filter.OncePerRequestFilter;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.Instant;
import java.util.Map;

/**
 * Protects all {@code /internal/**} endpoints using a shared secret in the
 * {@link InternalHeaders#INTERNAL_TOKEN} header (plan.md D18).
 */
public class InternalTokenFilter extends OncePerRequestFilter {

    private final byte[] expectedTokenBytes;
    private final ObjectMapper objectMapper;

    public InternalTokenFilter(String expectedToken, ObjectMapper objectMapper) {
        if (expectedToken == null || expectedToken.isBlank()) {
            throw new IllegalArgumentException("Expected internal API token must not be blank");
        }
        this.expectedTokenBytes = expectedToken.getBytes(StandardCharsets.UTF_8);
        this.objectMapper = objectMapper;
    }

    @Override
    protected boolean shouldNotFilter(HttpServletRequest request) {
        String uri = request.getRequestURI();
        return uri == null || !uri.startsWith("/internal/");
    }

    @Override
    protected void doFilterInternal(HttpServletRequest request,
                                    HttpServletResponse response,
                                    FilterChain filterChain) throws ServletException, IOException {
        String provided = request.getHeader(InternalHeaders.INTERNAL_TOKEN);
        if (provided == null || !MessageDigest.isEqual(
                expectedTokenBytes, provided.getBytes(StandardCharsets.UTF_8))) {
            ErrorResponse body = new ErrorResponse(
                    Instant.now(),
                    HttpStatus.UNAUTHORIZED.value(),
                    HttpStatus.UNAUTHORIZED.getReasonPhrase(),
                    ErrorCode.UNAUTHORIZED_INTERNAL,
                    "Missing or invalid " + InternalHeaders.INTERNAL_TOKEN + " header",
                    request.getRequestURI(),
                    Map.of());
            response.setStatus(HttpStatus.UNAUTHORIZED.value());
            response.setContentType(MediaType.APPLICATION_JSON_VALUE);
            objectMapper.writeValue(response.getOutputStream(), body);
            return;
        }
        filterChain.doFilter(request, response);
    }
}

