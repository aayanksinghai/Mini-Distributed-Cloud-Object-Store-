#!/usr/bin/env bash
# ==============================================================================
# Demonstration 6 (PDF §10.6): Data Integrity Verification & Recovery
#   - Upload a binary object
#   - Intentionally corrupt one stored chunk replica on a worker's disk using
#     `scripts/demo/corrupt-chunk.sh <worker> <chunkId>`
#   - Download the object -> controller/worker detects checksum mismatch, marks
#     the corrupted replica CORRUPTED, serves the valid replica, and the
#     downloaded file matches the original SHA-256
#   - Run repair / integrity scrub -> corrupted replica is overwritten from the
#     healthy replica and verified clean on disk
# ==============================================================================
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=./_common.sh
source "${SCRIPT_DIR}/_common.sh"

banner "DEMO 6: Data Integrity Verification, Corruption Detection & Automatic Recovery"
wait_for_controller
wait_for_workers_up 4

TMP_DIR="$(mktemp -d)"
trap 'rm -rf "${TMP_DIR}"' EXIT

ORIG_FILE="${TMP_DIR}/integrity-source.bin"
DOWN_FILE="${TMP_DIR}/integrity-downloaded.bin"
dd if=/dev/urandom of="${ORIG_FILE}" bs=1024 count=2200 status=none
ORIG_SHA="$(sha256sum "${ORIG_FILE}" | awk '{print $1}')"

step "1. Uploading ~2.15 MiB binary object (3 chunks)"
UPLOAD_JSON="$(curl -fsS -X POST "${CONTROLLER_URL}/api/v1/objects" \
  -F "file=@${ORIG_FILE};type=application/octet-stream" \
  -F 'metadata={"demo":"06-data-integrity"}')"
OBJECT_ID="$(echo "${UPLOAD_JSON}" | jq -r '.objectId')"

META_JSON="$(curl -fsS "${CONTROLLER_URL}/api/v1/objects/${OBJECT_ID}/metadata")"
TARGET_CHUNK_ID="$(echo "${META_JSON}" | jq -r '.chunks[0].chunkId')"
TARGET_CHUNK_SHA="$(echo "${META_JSON}" | jq -r '.chunks[0].checksum' | sed 's/^sha256://')"
VICTIM_WORKER="$(echo "${META_JSON}" | jq -r '.chunks[0].replicas[0].workerId')"

step "2. Intentionally corrupting chunk ${TARGET_CHUNK_ID} on ${VICTIM_WORKER} via corrupt-chunk.sh"
"${SCRIPT_DIR}/corrupt-chunk.sh" "${VICTIM_WORKER}" "${TARGET_CHUNK_ID}"

step "3. Downloading object ${OBJECT_ID} -> corruption detected & served from valid replica"
curl -fsS "${CONTROLLER_URL}/api/v1/objects/${OBJECT_ID}" -o "${DOWN_FILE}"
DOWN_SHA="$(sha256sum "${DOWN_FILE}" | awk '{print $1}')"
assert_eq "${DOWN_SHA}" "${ORIG_SHA}" "Downloaded object matches original SHA-256 despite corrupted replica on ${VICTIM_WORKER}"

step "4. Running integrity scrub & automatic repair via POST /api/v1/admin/integrity/scrub?objectId=${OBJECT_ID}&repair=true"
SCRUB_JSON="$(curl -fsS -X POST "${CONTROLLER_URL}/api/v1/admin/integrity/scrub?objectId=${OBJECT_ID}&repair=true")"
echo "${SCRUB_JSON}" | jq .

REPAIR_JSON="$(curl -fsS -X POST "${CONTROLLER_URL}/api/v1/admin/replication/repair")"
echo "${REPAIR_JSON}" | jq .

step "5. Verifying on-disk chunk file on ${VICTIM_WORKER} is restored to expected SHA-256"
SHARD="$(printf '%s' "${TARGET_CHUNK_ID:0:2}" | tr '[:upper:]' '[:lower:]')"
RESTORED_DISK_SHA="$(docker exec "${VICTIM_WORKER}" sh -c "sha256sum '/data/chunks/${SHARD}/${TARGET_CHUNK_ID}.chunk' | awk '{print \$1}'")"
assert_eq "${RESTORED_DISK_SHA}" "${TARGET_CHUNK_SHA}" "On-disk chunk on ${VICTIM_WORKER} restored to exact SHA-256"

step "6. Inspecting recent CHUNK_CORRUPTED and CHUNK_REPAIRED events"
curl -fsS "${CONTROLLER_URL}/api/v1/admin/events?limit=15" | jq '[.[] | select(.type=="CHUNK_CORRUPTED" or .type=="CHUNK_REPAIRED")]'

pass "Demo 6 completed successfully."

