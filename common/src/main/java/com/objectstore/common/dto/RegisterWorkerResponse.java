package com.objectstore.common.dto;

import java.time.Instant;

/**
 * Response returned by {@code POST /internal/workers/register}.
 */
public record RegisterWorkerResponse(
        String workerId,
        WorkerStatus status,
        long heartbeatIntervalMs,
        Instant registeredAt) {
}

