package com.objectstore.controller.domain;

import com.objectstore.common.dto.ReplicaState;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import java.time.Instant;

/**
 * Persistent mapping of a chunk replica to a specific worker node and its health state.
 */
@Entity
@Table(name = "chunk_replicas")
public class ChunkReplicaEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "chunk_id", length = 128, nullable = false)
    private String chunkId;

    @Column(name = "worker_id", length = 64, nullable = false)
    private String workerId;

    @Enumerated(EnumType.STRING)
    @Column(name = "state", length = 32, nullable = false)
    private ReplicaState state;

    @Column(name = "created_at", nullable = false)
    private Instant createdAt;

    @Column(name = "last_verified_at")
    private Instant lastVerifiedAt;

    protected ChunkReplicaEntity() {
    }

    public ChunkReplicaEntity(String chunkId,
                              String workerId,
                              ReplicaState state,
                              Instant createdAt,
                              Instant lastVerifiedAt) {
        this.chunkId = chunkId;
        this.workerId = workerId;
        this.state = state;
        this.createdAt = createdAt;
        this.lastVerifiedAt = lastVerifiedAt;
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

    public ReplicaState getState() {
        return state;
    }

    public void setState(ReplicaState state) {
        this.state = state;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }

    public Instant getLastVerifiedAt() {
        return lastVerifiedAt;
    }

    public void setLastVerifiedAt(Instant lastVerifiedAt) {
        this.lastVerifiedAt = lastVerifiedAt;
    }
}

