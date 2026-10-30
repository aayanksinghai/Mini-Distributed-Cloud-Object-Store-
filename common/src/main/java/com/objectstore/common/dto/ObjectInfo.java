package com.objectstore.common.dto;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;

/**
 * Summary information about a stored object returned on upload ({@code POST /api/v1/objects})
 * and object listing ({@code GET /api/v1/objects}).
 */
public record ObjectInfo(
        UUID objectId,
        String name,
        long size,
        String contentType,
        String checksum,
        int chunkCount,
        Instant createdAt,
        Instant updatedAt,
        Map<String, String> customMetadata) {

    public ObjectInfo {
        customMetadata = customMetadata == null ? Map.of() : Map.copyOf(customMetadata);
    }
}

