package com.objectstore.common.error;

import java.util.Map;

/**
 * Base class for all domain exceptions that map directly to an HTTP error response.
 * Subclasses (e.g. ObjectNotFoundException) supply the status and error code.
 */
public class ApiException extends RuntimeException {

    private final int status;
    private final String code;
    private final transient Map<String, Object> details;

    public ApiException(int status, String code, String message) {
        this(status, code, message, Map.of(), null);
    }

    public ApiException(int status, String code, String message, Map<String, Object> details, Throwable cause) {
        super(message, cause);
        this.status = status;
        this.code = code;
        this.details = details == null ? Map.of() : Map.copyOf(details);
    }

    public int getStatus() {
        return status;
    }

    public String getCode() {
        return code;
    }

    public Map<String, Object> getDetails() {
        return details;
    }
}

