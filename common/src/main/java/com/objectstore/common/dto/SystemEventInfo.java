package com.objectstore.common.dto;

import java.time.Instant;
import java.util.UUID;

/**
 * Public view of a system event returned by {@code GET /api/v1/admin/events}.
 */
public record SystemEventInfo(
        long id,
        Instant occurredAt,
        String type,
        EventSeverity severity,
        UUID objectId,
        String chunkId,
        String workerId,
        String message) {
}

