#!/usr/bin/env bash
# ==============================================================================
# Mini Distributed Cloud Object Store — Corrupt Chunk Demo Utility (PDF §8, §10.6)
# ==============================================================================
# Intentionally corrupts a stored chunk file inside a running worker container's
# persistent volume (`/data/chunks/<shard>/<chunkId>.chunk`) so the system can
# demonstrate SHA-256 corruption detection, read failover to the valid replica,
# and automatic repair.
#
# Usage:
#   ./scripts/demo/corrupt-chunk.sh <worker-container> <chunk-id>
#
# Example:
#   ./scripts/demo/corrupt-chunk.sh worker1 3f2a9c10-4b2e-4f1a-8e2d-91c2b3a4d5e6_00000
# ==============================================================================
set -euo pipefail

if [[ $# -lt 2 ]]; then
  echo "Usage: $0 <worker-container> <chunk-id>" >&2
  echo "Example: $0 worker1 3f2a9c10-4b2e-4f1a-8e2d-91c2b3a4d5e6_00000" >&2
  exit 1
fi

WORKER_CONTAINER="$1"
CHUNK_ID="$2"
STORAGE_ROOT="${STORAGE_DIR:-/data/chunks}"

if [[ ${#CHUNK_ID} -lt 2 ]]; then
  echo "Error: chunk-id '${CHUNK_ID}' must be at least 2 characters long." >&2
  exit 1
fi

SHARD="$(printf '%s' "${CHUNK_ID:0:2}" | tr '[:upper:]' '[:lower:]')"
CHUNK_PATH="${STORAGE_ROOT}/${SHARD}/${CHUNK_ID}.chunk"

echo "==> Inspecting chunk on ${WORKER_CONTAINER}: ${CHUNK_PATH}"
BEFORE_HASH="$(docker exec "${WORKER_CONTAINER}" sh -c "sha256sum '${CHUNK_PATH}' | awk '{print \$1}'")"
echo "    SHA-256 before corruption: ${BEFORE_HASH}"

echo "==> Overwriting first 16 bytes of ${CHUNK_PATH} on ${WORKER_CONTAINER} (preserving file size)..."
docker exec "${WORKER_CONTAINER}" sh -c "printf 'CORRUPTED_BYTES!' | dd of='${CHUNK_PATH}' bs=1 count=16 conv=notrunc status=none"

AFTER_HASH="$(docker exec "${WORKER_CONTAINER}" sh -c "sha256sum '${CHUNK_PATH}' | awk '{print \$1}'")"
echo "    SHA-256 after corruption : ${AFTER_HASH}"

if [[ "${BEFORE_HASH}" == "${AFTER_HASH}" ]]; then
  echo "Error: checksum did not change!" >&2
  exit 1
fi

echo "==> Successfully corrupted chunk ${CHUNK_ID} on ${WORKER_CONTAINER}."

