package com.objectstore.controller.domain;

import com.objectstore.common.dto.WorkerStatus;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import java.time.Instant;

/**
 * Persistent record of a storage worker registered with the controller.
 */
@Entity
@Table(name = "workers")
public class WorkerEntity {

    @Id
    @Column(name = "id", length = 64, nullable = false)
    private String id;

    @Column(name = "base_url", length = 255, nullable = false)
    private String baseUrl;

    @Enumerated(EnumType.STRING)
    @Column(name = "status", length = 16, nullable = false)
    private WorkerStatus status;

    @Column(name = "capacity_bytes", nullable = false)
    private long capacityBytes;

    @Column(name = "used_bytes", nullable = false)
    private long usedBytes;

    @Column(name = "free_bytes", nullable = false)
    private long freeBytes;

    @Column(name = "chunk_count", nullable = false)
    private int chunkCount;

    @Column(name = "registered_at", nullable = false)
    private Instant registeredAt;

    @Column(name = "last_heartbeat_at", nullable = false)
    private Instant lastHeartbeatAt;

    @Column(name = "down_since")
    private Instant downSince;

    protected WorkerEntity() {
    }

    public WorkerEntity(String id,
                        String baseUrl,
                        WorkerStatus status,
                        long capacityBytes,
                        long usedBytes,
                        long freeBytes,
                        int chunkCount,
                        Instant registeredAt,
                        Instant lastHeartbeatAt) {
        this.id = id;
        this.baseUrl = baseUrl;
        this.status = status;
        this.capacityBytes = capacityBytes;
        this.usedBytes = usedBytes;
        this.freeBytes = freeBytes;
        this.chunkCount = chunkCount;
        this.registeredAt = registeredAt;
        this.lastHeartbeatAt = lastHeartbeatAt;
    }

    public String getId() {
        return id;
    }

    public String getBaseUrl() {
        return baseUrl;
    }

    public void setBaseUrl(String baseUrl) {
        this.baseUrl = baseUrl;
    }

    public WorkerStatus getStatus() {
        return status;
    }

    public void setStatus(WorkerStatus status) {
        this.status = status;
    }

    public long getCapacityBytes() {
        return capacityBytes;
    }

    public void setCapacityBytes(long capacityBytes) {
        this.capacityBytes = capacityBytes;
    }

    public long getUsedBytes() {
        return usedBytes;
    }

    public void setUsedBytes(long usedBytes) {
        this.usedBytes = usedBytes;
    }

    public long getFreeBytes() {
        return freeBytes;
    }

    public void setFreeBytes(long freeBytes) {
        this.freeBytes = freeBytes;
    }

    public int getChunkCount() {
        return chunkCount;
    }

    public void setChunkCount(int chunkCount) {
        this.chunkCount = chunkCount;
    }

    public Instant getRegisteredAt() {
        return registeredAt;
    }

    public void setRegisteredAt(Instant registeredAt) {
        this.registeredAt = registeredAt;
    }

    public Instant getLastHeartbeatAt() {
        return lastHeartbeatAt;
    }

    public void setLastHeartbeatAt(Instant lastHeartbeatAt) {
        this.lastHeartbeatAt = lastHeartbeatAt;
    }

    public Instant getDownSince() {
        return downSince;
    }

    public void setDownSince(Instant downSince) {
        this.downSince = downSince;
    }
}

