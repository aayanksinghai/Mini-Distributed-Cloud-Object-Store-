package com.objectstore.common.dto;

/**
 * Replication health of a chunk, object, or the overall cluster.
 * <ul>
 *     <li>{@code HEALTHY}: effective replication factor meets or exceeds target (RF = 2 on distinct UP workers)</li>
 *     <li>{@code DEGRADED}: at least 1 valid copy is available on an UP worker, but fewer than RF</li>
 *     <li>{@code UNAVAILABLE}: 0 valid copies are currently reachable on UP workers</li>
 * </ul>
 */
public enum ReplicationStatus {
    HEALTHY,
    DEGRADED,
    UNAVAILABLE
}

