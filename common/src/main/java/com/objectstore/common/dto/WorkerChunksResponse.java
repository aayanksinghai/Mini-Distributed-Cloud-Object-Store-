package com.objectstore.common.dto;

import java.util.List;

/**
 * Response returned by {@code GET /internal/workers/{workerId}/chunks}.
 */
public record WorkerChunksResponse(
        String workerId,
        int chunkCount,
        long usedBytes,
        List<StoredChunkInfo> chunks) {

    public WorkerChunksResponse {
        chunks = chunks == null ? List.of() : List.copyOf(chunks);
    }
}

