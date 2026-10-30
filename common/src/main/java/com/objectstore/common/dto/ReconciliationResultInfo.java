package com.objectstore.common.dto;

import java.time.Instant;
import java.util.List;

/**
 * Summary of reconciling a worker's on-disk chunk inventory against controller metadata on rejoin (plan.md D8, D15).
 */
public record ReconciliationResultInfo(
        String workerId,
        Instant reconciledAt,
        int pendingDeletionsExecuted,
        int orphanChunksDeleted,
        int replicasReadopted,
        int missingReplicasRemoved,
        int corruptReplicasDetected,
        int surplusReplicasTrimmed,
        List<String> trimmedChunkIds) {

    public ReconciliationResultInfo {
        trimmedChunkIds = trimmedChunkIds == null ? List.of() : List.copyOf(trimmedChunkIds);
    }
}

