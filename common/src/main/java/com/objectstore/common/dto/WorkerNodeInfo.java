package com.objectstore.common.dto;

import java.time.Instant;

/**
 * Public view of a worker node returned by {@code GET /api/v1/cluster/workers}.
 */
public record WorkerNodeInfo(
        String workerId,
        String baseUrl,
        WorkerStatus status,
        long capacityBytes,
        long usedBytes,
        long freeBytes,
        int chunkCount,
        Instant registeredAt,
        Instant lastHeartbeatAt,
        Instant downSince) {
}

