package com.objectstore.controller.domain;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import java.time.Instant;

/**
 * Queued deletion of a chunk replica on a worker that was {@code DOWN} or unreachable when its
 * owning object was deleted (plan.md D15).
 */
@Entity
@Table(name = "pending_deletions")
public class PendingDeletionEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "chunk_id", length = 128, nullable = false)
    private String chunkId;

    @Column(name = "worker_id", length = 64, nullable = false)
    private String workerId;

    @Column(name = "created_at", nullable = false)
    private Instant createdAt;

    protected PendingDeletionEntity() {
    }

    public PendingDeletionEntity(String chunkId, String workerId, Instant createdAt) {
        this.chunkId = chunkId;
        this.workerId = workerId;
        this.createdAt = createdAt;
    }

    public Long getId() {
        return id;
    }

    public String getChunkId() {
        return chunkId;
    }

    public String getWorkerId() {
        return workerId;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }
}

