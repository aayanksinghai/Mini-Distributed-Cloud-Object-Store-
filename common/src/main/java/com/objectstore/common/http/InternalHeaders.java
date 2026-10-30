package com.objectstore.common.http;

/**
 * HTTP header names used on the internal controller &lt;-&gt; worker interface.
 */
public final class InternalHeaders {

    /** Shared secret authenticating internal calls (see plan.md D18). */
    public static final String INTERNAL_TOKEN = "X-Internal-Token";
    /** Hex-encoded SHA-256 of a chunk's bytes. */
    public static final String CHUNK_CHECKSUM = "X-Chunk-Checksum";
    /** Owning object of a chunk. */
    public static final String OBJECT_ID = "X-Object-Id";
    /** Zero-based position of a chunk within its object. */
    public static final String CHUNK_INDEX = "X-Chunk-Index";

    private InternalHeaders() {
    }
}

