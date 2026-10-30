-- V2: worker registry and system events tables (Phase 2).

CREATE TABLE workers (
    id                 VARCHAR(64) PRIMARY KEY,
    base_url           VARCHAR(255)             NOT NULL,
    status             VARCHAR(16)              NOT NULL,
    capacity_bytes     BIGINT                   NOT NULL DEFAULT 0,
    used_bytes         BIGINT                   NOT NULL DEFAULT 0,
    free_bytes         BIGINT                   NOT NULL DEFAULT 0,
    chunk_count        INTEGER                  NOT NULL DEFAULT 0,
    registered_at      TIMESTAMP WITH TIME ZONE NOT NULL,
    last_heartbeat_at  TIMESTAMP WITH TIME ZONE NOT NULL,
    down_since         TIMESTAMP WITH TIME ZONE
);

CREATE INDEX idx_workers_status ON workers (status);

CREATE TABLE system_events (
    id          BIGSERIAL PRIMARY KEY,
    occurred_at TIMESTAMP WITH TIME ZONE NOT NULL,
    type        VARCHAR(64)              NOT NULL,
    severity    VARCHAR(16)              NOT NULL,
    object_id   UUID,
    chunk_id    VARCHAR(128),
    worker_id   VARCHAR(64),
    message     TEXT                     NOT NULL
);

CREATE INDEX idx_system_events_occurred_at ON system_events (occurred_at DESC);
