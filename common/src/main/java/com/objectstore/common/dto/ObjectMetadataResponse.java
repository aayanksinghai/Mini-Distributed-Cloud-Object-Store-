package com.objectstore.common.dto;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Complete system-maintained and custom metadata for an object returned by
 * {@code GET /api/v1/objects/{objectId}/metadata} and {@code PATCH /api/v1/objects/{objectId}/metadata}.
 */
public record ObjectMetadataResponse(
        UUID objectId,
        String name,
        long size,
        String contentType,
        String checksum,
        int chunkCount,
        int chunkSize,
        int replicationFactor,
        ReplicationStatus replicationStatus,
        Instant createdAt,
        Instant updatedAt,
        Map<String, String> customMetadata,
        List<ChunkMetadataInfo> chunks) {

    public ObjectMetadataResponse {
        customMetadata = customMetadata == null ? Map.of() : Map.copyOf(customMetadata);
        chunks = chunks == null ? List.of() : List.copyOf(chunks);
    }
}

