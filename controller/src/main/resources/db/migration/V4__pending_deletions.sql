-- V4: pending chunk deletions table for deferred cleanup on unreachable/DOWN workers (Phase 6, plan.md D15).

CREATE TABLE pending_deletions (
    id         BIGSERIAL PRIMARY KEY,
    chunk_id   VARCHAR(128)             NOT NULL,
    worker_id  VARCHAR(64)              NOT NULL REFERENCES workers (id) ON DELETE CASCADE,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL,
    CONSTRAINT uq_pending_deletions_chunk_worker UNIQUE (chunk_id, worker_id)
);

CREATE INDEX idx_pending_deletions_worker_id ON pending_deletions (worker_id);

