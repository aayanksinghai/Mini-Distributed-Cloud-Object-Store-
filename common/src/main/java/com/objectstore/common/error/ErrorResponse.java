package com.objectstore.common.error;

import com.fasterxml.jackson.annotation.JsonInclude;

import java.time.Instant;
import java.util.Map;

/**
 * Uniform JSON error body returned by every service for all non-2xx responses.
 *
 * @param timestamp when the error was produced (UTC)
 * @param status    HTTP status code
 * @param error     HTTP reason phrase (e.g. "Not Found")
 * @param code      stable, machine-readable error code (see {@link ErrorCode})
 * @param message   human-readable explanation
 * @param path      request path that caused the error
 * @param details   optional extra structured information (e.g. field errors)
 */
@JsonInclude(JsonInclude.Include.NON_EMPTY)
public record ErrorResponse(
        Instant timestamp,
        int status,
        String error,
        String code,
        String message,
        String path,
        Map<String, Object> details) {
}

