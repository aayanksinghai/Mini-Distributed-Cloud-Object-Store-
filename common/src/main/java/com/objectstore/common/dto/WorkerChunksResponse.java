package com.objectstore.common.dto;

import com.fasterxml.jackson.annotation.JsonInclude;

import java.util.List;

/**
 * Response returned by {@code GET /internal/workers/{workerId}/chunks}.
 * <p>
 * When returned directly by a worker, contains the on-disk inventory ({@code workerId}, {@code chunkCount},
 * {@code usedBytes}, {@code chunks}). When returned by the controller (plan.md D4), also includes the
 * metadata reconciliation diff ({@code expectedChunkIds}, {@code missingChunkIds}, {@code orphanChunkIds},
 * {@code corruptChunkIds}).
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record WorkerChunksResponse(
        String workerId,
        int chunkCount,
        long usedBytes,
        List<StoredChunkInfo> chunks,
        List<String> expectedChunkIds,
        List<String> missingChunkIds,
        List<String> orphanChunkIds,
        List<String> corruptChunkIds) {

    public WorkerChunksResponse {
        chunks = chunks == null ? List.of() : List.copyOf(chunks);
        expectedChunkIds = expectedChunkIds == null ? null : List.copyOf(expectedChunkIds);
        missingChunkIds = missingChunkIds == null ? null : List.copyOf(missingChunkIds);
        orphanChunkIds = orphanChunkIds == null ? null : List.copyOf(orphanChunkIds);
        corruptChunkIds = corruptChunkIds == null ? null : List.copyOf(corruptChunkIds);
    }

    public WorkerChunksResponse(String workerId, int chunkCount, long usedBytes, List<StoredChunkInfo> chunks) {
        this(workerId, chunkCount, usedBytes, chunks, null, null, null, null);
    }
}
