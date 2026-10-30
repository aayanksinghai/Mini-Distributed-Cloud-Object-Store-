package com.objectstore.common.dto;

import java.time.Instant;

/**
 * Response returned by {@code POST /internal/workers/heartbeat}.
 */
public record HeartbeatResponse(
        String workerId,
        WorkerStatus status,
        Instant receivedAt) {
}

