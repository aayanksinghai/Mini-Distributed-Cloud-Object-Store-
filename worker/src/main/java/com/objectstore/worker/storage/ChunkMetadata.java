package com.objectstore.worker.storage;

import java.time.Instant;

/**
 * Sidecar JSON persisted alongside each {@code .chunk} file on a worker's volume.
 */
public record ChunkMetadata(
        String chunkId,
        String objectId,
        Integer chunkIndex,
        long size,
        String sha256,
        Instant storedAt) {
}

