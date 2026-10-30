package com.objectstore.controller.domain;

import com.objectstore.common.dto.EventSeverity;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import java.time.Instant;
import java.util.UUID;

/**
 * Audit and operational event log entry persisted in {@code system_events}.
 */
@Entity
@Table(name = "system_events")
public class SystemEventEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "occurred_at", nullable = false)
    private Instant occurredAt;

    @Column(name = "type", length = 64, nullable = false)
    private String type;

    @Enumerated(EnumType.STRING)
    @Column(name = "severity", length = 16, nullable = false)
    private EventSeverity severity;

    @Column(name = "object_id")
    private UUID objectId;

    @Column(name = "chunk_id", length = 128)
    private String chunkId;

    @Column(name = "worker_id", length = 64)
    private String workerId;

    @Column(name = "message", nullable = false, columnDefinition = "TEXT")
    private String message;

    protected SystemEventEntity() {
    }

    public SystemEventEntity(Instant occurredAt,
                             String type,
                             EventSeverity severity,
                             UUID objectId,
                             String chunkId,
                             String workerId,
                             String message) {
        this.occurredAt = occurredAt;
        this.type = type;
        this.severity = severity;
        this.objectId = objectId;
        this.chunkId = chunkId;
        this.workerId = workerId;
        this.message = message;
    }

    public Long getId() {
        return id;
    }

    public Instant getOccurredAt() {
        return occurredAt;
    }

    public String getType() {
        return type;
    }

    public EventSeverity getSeverity() {
        return severity;
    }

    public UUID getObjectId() {
        return objectId;
    }

    public String getChunkId() {
        return chunkId;
    }

    public String getWorkerId() {
        return workerId;
    }

    public String getMessage() {
        return message;
    }
}

