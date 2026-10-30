-- V3: objects, custom metadata, chunks, and chunk replicas tables (Phase 3).

CREATE TABLE objects (
    id              UUID PRIMARY KEY,
    name            VARCHAR(512)             NOT NULL,
    size_bytes      BIGINT                   NOT NULL DEFAULT 0,
    content_type    VARCHAR(255)             NOT NULL,
    checksum_sha256 VARCHAR(64)              NOT NULL,
    chunk_count     INTEGER                  NOT NULL DEFAULT 0,
    chunk_size      INTEGER                  NOT NULL,
    status          VARCHAR(16)              NOT NULL,
    created_at      TIMESTAMP WITH TIME ZONE NOT NULL,
    updated_at      TIMESTAMP WITH TIME ZONE NOT NULL,
    version         BIGINT                   NOT NULL DEFAULT 0
);

CREATE INDEX idx_objects_status_created_at ON objects (status, created_at DESC);

CREATE TABLE object_metadata (
    object_id  UUID          NOT NULL REFERENCES objects (id) ON DELETE CASCADE,
    meta_key   VARCHAR(128)  NOT NULL,
    meta_value VARCHAR(1024) NOT NULL,
    PRIMARY KEY (object_id, meta_key)
);

CREATE TABLE chunks (
    id              VARCHAR(128) PRIMARY KEY,
    object_id       UUID         NOT NULL REFERENCES objects (id) ON DELETE CASCADE,
    chunk_index     INTEGER      NOT NULL,
    size_bytes      INTEGER      NOT NULL,
    checksum_sha256 VARCHAR(64)  NOT NULL,
    CONSTRAINT uq_chunks_object_index UNIQUE (object_id, chunk_index)
);

CREATE INDEX idx_chunks_object_index ON chunks (object_id, chunk_index);

CREATE TABLE chunk_replicas (
    id               BIGSERIAL PRIMARY KEY,
    chunk_id         VARCHAR(128)             NOT NULL REFERENCES chunks (id) ON DELETE CASCADE,
    worker_id        VARCHAR(64)              NOT NULL REFERENCES workers (id),
    state            VARCHAR(32)              NOT NULL,
    created_at       TIMESTAMP WITH TIME ZONE NOT NULL,
    last_verified_at TIMESTAMP WITH TIME ZONE,
    CONSTRAINT uq_chunk_replicas_chunk_worker UNIQUE (chunk_id, worker_id)
);

CREATE INDEX idx_chunk_replicas_worker_id ON chunk_replicas (worker_id);
CREATE INDEX idx_chunk_replicas_state ON chunk_replicas (state);

