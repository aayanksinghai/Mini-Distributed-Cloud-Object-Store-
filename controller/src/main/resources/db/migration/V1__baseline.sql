-- V1: baseline migration for the controller metadata store.
-- Intentionally contains no tables: the schema is introduced incrementally per phase
-- (V2 workers/system_events in Phase 2, V3 objects/chunks/replicas in Phase 3, ...).
-- This migration verifies that Flyway is wired to PostgreSQL and records the baseline.
SELECT 1;

