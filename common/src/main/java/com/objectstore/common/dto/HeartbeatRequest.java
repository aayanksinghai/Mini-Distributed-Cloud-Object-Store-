package com.objectstore.common.dto;

import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;

import java.util.List;

/**
 * Payload sent by a worker to {@code POST /internal/workers/heartbeat}.
 */
public record HeartbeatRequest(
        @NotBlank String workerId,
        @Min(0) long usedBytes,
        @Min(0) long freeBytes,
        @Min(0) int chunkCount,
        List<String> corruptChunkIds) {

    public HeartbeatRequest {
        corruptChunkIds = corruptChunkIds == null ? List.of() : List.copyOf(corruptChunkIds);
    }
}

