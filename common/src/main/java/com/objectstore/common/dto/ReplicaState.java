package com.objectstore.common.dto;

/**
 * Health state of an individual chunk replica on a worker (plan.md §5.5).
 */
public enum ReplicaState {
    PENDING,
    HEALTHY,
    CORRUPTED,
    UNAVAILABLE
}

