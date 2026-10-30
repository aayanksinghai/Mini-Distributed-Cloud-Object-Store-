package com.objectstore.common.dto;

/**
 * Cluster-wide health and replication summary returned by {@code GET /api/v1/cluster/status}.
 */
public record ClusterStatusInfo(
        int replicationFactor,
        int workersTotal,
        int workersUp,
        int workersDown,
        long activeObjects,
        long totalChunks,
        long healthyChunks,
        long underReplicatedChunks,
        long unavailableChunks,
        long totalHealthyReplicas,
        long corruptedReplicas,
        ReplicationStatus replicationStatus) {
}

