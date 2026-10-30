package com.objectstore.common.error;

/**
 * Stable machine-readable error codes shared by the controller and workers.
 */
public final class ErrorCode {

    public static final String VALIDATION_ERROR = "VALIDATION_ERROR";
    public static final String MALFORMED_REQUEST = "MALFORMED_REQUEST";
    public static final String PAYLOAD_TOO_LARGE = "PAYLOAD_TOO_LARGE";
    public static final String NOT_FOUND = "NOT_FOUND";
    public static final String METHOD_NOT_ALLOWED = "METHOD_NOT_ALLOWED";
    public static final String UNSUPPORTED_MEDIA_TYPE = "UNSUPPORTED_MEDIA_TYPE";
    public static final String CONFLICT = "CONFLICT";
    public static final String SERVICE_UNAVAILABLE = "SERVICE_UNAVAILABLE";
    public static final String INTERNAL_ERROR = "INTERNAL_ERROR";

    // Internal authentication & worker registry codes
    public static final String UNAUTHORIZED_INTERNAL = "UNAUTHORIZED_INTERNAL";
    public static final String UNKNOWN_WORKER = "UNKNOWN_WORKER";
    public static final String WORKER_ID_CONFLICT = "WORKER_ID_CONFLICT";
    public static final String WORKER_UNAVAILABLE = "WORKER_UNAVAILABLE";

    // Chunk storage & integrity codes
    public static final String INVALID_CHUNK_ID = "INVALID_CHUNK_ID";
    public static final String CHECKSUM_MISMATCH = "CHECKSUM_MISMATCH";
    public static final String CHUNK_ALREADY_EXISTS = "CHUNK_ALREADY_EXISTS";
    public static final String CHUNK_NOT_FOUND = "CHUNK_NOT_FOUND";
    public static final String CHUNK_CORRUPTED = "CHUNK_CORRUPTED";
    public static final String STORAGE_IO_ERROR = "STORAGE_IO_ERROR";

    // Object lifecycle & placement codes
    public static final String MISSING_FILE = "MISSING_FILE";
    public static final String INVALID_METADATA = "INVALID_METADATA";
    public static final String INSUFFICIENT_STORAGE_NODES = "INSUFFICIENT_STORAGE_NODES";
    public static final String UPLOAD_FAILED = "UPLOAD_FAILED";
    public static final String OBJECT_NOT_FOUND = "OBJECT_NOT_FOUND";
    public static final String OBJECT_NOT_READY = "OBJECT_NOT_READY";
    public static final String CHUNK_UNAVAILABLE = "CHUNK_UNAVAILABLE";

    private ErrorCode() {
    }
}
