package com.objectstore.common.dto;

import java.time.Instant;

/**
 * Location and health of a single chunk replica returned in object metadata responses.
 */
public record ChunkReplicaInfo(
        String workerId,
        ReplicaState state,
        WorkerStatus workerStatus,
        Instant lastVerifiedAt) {
}

