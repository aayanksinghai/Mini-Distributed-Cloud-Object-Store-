package com.objectstore.common.dto;

import java.time.Instant;
import java.util.List;

/**
 * Summary returned by {@code POST /api/v1/admin/replication/repair} and scheduled repair runs.
 */
public record RepairResultInfo(
        Instant executedAt,
        int examinedChunks,
        int underReplicatedChunksFound,
        int repairedChunks,
        int corruptedReplicasRepaired,
        int skippedWithinGracePeriod,
        int unrepairableChunks,
        List<String> repairedChunkIds) {

    public RepairResultInfo {
        repairedChunkIds = repairedChunkIds == null ? List.of() : List.copyOf(repairedChunkIds);
    }
}

