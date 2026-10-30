package com.objectstore.common.dto;

import java.util.List;

/**
 * Metadata and replica placement details for a single ordered chunk within an object.
 */
public record ChunkMetadataInfo(
        String chunkId,
        int index,
        int size,
        String checksum,
        ReplicationStatus replicationStatus,
        List<ChunkReplicaInfo> replicas) {

    public ChunkMetadataInfo {
        replicas = replicas == null ? List.of() : List.copyOf(replicas);
    }
}

