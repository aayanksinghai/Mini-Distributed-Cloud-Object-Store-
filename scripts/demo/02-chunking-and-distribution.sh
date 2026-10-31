#!/usr/bin/env bash
# ==============================================================================
# Demonstration 2 (PDF §10.2): Chunking and Distribution
#   - Upload a multi-megabyte binary file (~5.3 MiB)
#   - Show how the file is split into 1 MiB (1,048,576 B) chunks (6 chunks total,
#     with the final chunk smaller)
#   - Show how chunks are distributed across all 4 worker nodes and inspect
#     worker storage volumes
# ==============================================================================
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=./_common.sh
source "${SCRIPT_DIR}/_common.sh"

banner "DEMO 2: 1 MiB Chunking and Distribution Across 4 Workers"
wait_for_controller
wait_for_workers_up 4

TMP_DIR="$(mktemp -d)"
trap 'rm -rf "${TMP_DIR}"' EXIT

MULTI_MB_FILE="${TMP_DIR}/multi-megabyte.bin"
# 5 * 1,048,576 + 314,572 = 5,557,452 bytes (~5.3 MiB -> 6 chunks)
dd if=/dev/urandom of="${MULTI_MB_FILE}" bs=1024 count=5427 status=none

step "Uploading ~5.3 MiB binary file via POST ${CONTROLLER_URL}/api/v1/objects"
UPLOAD_JSON="$(curl -fsS -X POST "${CONTROLLER_URL}/api/v1/objects" \
  -F "file=@${MULTI_MB_FILE};type=application/octet-stream" \
  -F 'metadata={"demo":"02-chunking-and-distribution"}')"
OBJECT_ID="$(echo "${UPLOAD_JSON}" | jq -r '.objectId')"
CHUNK_COUNT="$(echo "${UPLOAD_JSON}" | jq -r '.chunkCount')"
assert_eq "${CHUNK_COUNT}" "6" "5.3 MiB file split into 6 chunks (5 x 1 MiB + 1 smaller final chunk)"

step "Fetching chunk & replica distribution via GET /api/v1/objects/${OBJECT_ID}/metadata"
META_JSON="$(curl -fsS "${CONTROLLER_URL}/api/v1/objects/${OBJECT_ID}/metadata")"
echo "${META_JSON}" | jq '{objectId, name, size, chunkSize, chunkCount, replicationFactor, chunks: [.chunks[] | {index, chunkId, size, checksum, workers: [.replicas[].workerId]}]}'

FIRST_CHUNK_SIZE="$(echo "${META_JSON}" | jq -r '.chunks[0].size')"
LAST_CHUNK_SIZE="$(echo "${META_JSON}" | jq -r '.chunks[5].size')"
assert_eq "${FIRST_CHUNK_SIZE}" "1048576" "Chunks 0..4 are exactly 1,048,576 bytes (1 MiB)"
if [[ "${LAST_CHUNK_SIZE}" -ge 1048576 || "${LAST_CHUNK_SIZE}" -le 0 ]]; then
  fail "Expected final chunk size to be < 1,048,576 bytes, got ${LAST_CHUNK_SIZE}"
fi
pass "Final chunk (#5) is smaller (${LAST_CHUNK_SIZE} bytes)"

step "Verifying chunk distribution across all 4 workers"
DISTINCT_WORKERS="$(echo "${META_JSON}" | jq -r '[.chunks[].replicas[].workerId] | unique | length')"
assert_eq "${DISTINCT_WORKERS}" "4" "Chunks are distributed across all 4 workers (worker1..worker4)"

for w in worker1 worker2 worker3 worker4; do
  if docker ps --format '{{.Names}}' 2>/dev/null | grep -qx "${w}"; then
    COUNT_ON_DISK="$(docker exec "${w}" sh -c "find /data/chunks -name '${OBJECT_ID}_*.chunk' | wc -l" | tr -d ' ')"
    echo "    ${w} persistent volume (/data/chunks) holds ${COUNT_ON_DISK} chunk file(s) for object ${OBJECT_ID}"
  fi
done

pass "Demo 2 completed successfully."

