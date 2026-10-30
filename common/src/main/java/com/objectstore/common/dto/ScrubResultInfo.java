package com.objectstore.common.dto;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

/**
 * Result summary of an on-demand or background integrity scrub across replicas (`POST /api/v1/admin/integrity/scrub`).
 */
public record ScrubResultInfo(
        Instant scrubbedAt,
        UUID objectId,
        int replicasExamined,
        int replicasValid,
        int corruptReplicasDetected,
        int missingReplicasDetected,
        int replicasRepaired,
        List<String> corruptedChunkIds,
        List<String> repairedChunkIds) {

    public ScrubResultInfo {
        corruptedChunkIds = corruptedChunkIds == null ? List.of() : List.copyOf(corruptedChunkIds);
        repairedChunkIds = repairedChunkIds == null ? List.of() : List.copyOf(repairedChunkIds);
    }
}

