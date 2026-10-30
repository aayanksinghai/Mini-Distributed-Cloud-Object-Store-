package com.objectstore.controller.domain;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import java.util.UUID;

/**
 * Persistent record of an ordered chunk belonging to an object.
 */
@Entity
@Table(name = "chunks")
public class ChunkEntity {

    @Id
    @Column(name = "id", length = 128, nullable = false)
    private String id;

    @Column(name = "object_id", nullable = false)
    private UUID objectId;

    @Column(name = "chunk_index", nullable = false)
    private int chunkIndex;

    @Column(name = "size_bytes", nullable = false)
    private int sizeBytes;

    @Column(name = "checksum_sha256", length = 64, nullable = false)
    private String checksumSha256;

    protected ChunkEntity() {
    }

    public ChunkEntity(String id, UUID objectId, int chunkIndex, int sizeBytes, String checksumSha256) {
        this.id = id;
        this.objectId = objectId;
        this.chunkIndex = chunkIndex;
        this.sizeBytes = sizeBytes;
        this.checksumSha256 = checksumSha256;
    }

    public String getId() {
        return id;
    }

    public UUID getObjectId() {
        return objectId;
    }

    public int getChunkIndex() {
        return chunkIndex;
    }

    public int getSizeBytes() {
        return sizeBytes;
    }

    public String getChecksumSha256() {
        return checksumSha256;
    }
}

