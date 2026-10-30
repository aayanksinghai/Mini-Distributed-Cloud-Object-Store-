package com.objectstore.common.dto;

import java.time.Instant;

/**
 * Metadata about a single chunk stored on a worker.
 */
public record StoredChunkInfo(
        String chunkId,
        String objectId,
        Integer chunkIndex,
        long size,
        String checksum,
        Instant storedAt,
        Boolean valid) {
}

