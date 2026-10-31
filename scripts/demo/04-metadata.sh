#!/usr/bin/env bash
# ==============================================================================
# Demonstration 4 (PDF §10.4): Metadata Operations, Listing & Deletion
#   - Upload an object with custom metadata
#   - Retrieve metadata via GET /api/v1/objects/{id}/metadata
#   - Update custom metadata via PATCH /api/v1/objects/{id}/metadata without
#     re-uploading object bytes (chunk checksums and locations remain identical)
#   - List objects via GET /api/v1/objects
#   - Delete a temporary object via DELETE /api/v1/objects/{id}
# ==============================================================================
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=./_common.sh
source "${SCRIPT_DIR}/_common.sh"

banner "DEMO 4: Metadata Operations (GET / PATCH without Re-Upload / LIST / DELETE)"
wait_for_controller
wait_for_workers_up 4

TMP_DIR="$(mktemp -d)"
trap 'rm -rf "${TMP_DIR}"' EXIT

FILE="${TMP_DIR}/dataset.bin"
dd if=/dev/urandom of="${FILE}" bs=1024 count=1500 status=none

step "1. Uploading object with initial custom metadata {\"owner\":\"alice\",\"env\":\"staging\"}"
UPLOAD_JSON="$(curl -fsS -X POST "${CONTROLLER_URL}/api/v1/objects" \
  -F "file=@${FILE};type=application/octet-stream" \
  -F 'metadata={"owner":"alice","env":"staging"}')"
OBJECT_ID="$(echo "${UPLOAD_JSON}" | jq -r '.objectId')"
echo "${UPLOAD_JSON}" | jq .

step "2. Retrieving metadata via GET /api/v1/objects/${OBJECT_ID}/metadata"
META_BEFORE="$(curl -fsS "${CONTROLLER_URL}/api/v1/objects/${OBJECT_ID}/metadata")"
echo "${META_BEFORE}" | jq '{objectId, customMetadata, updatedAt, chunks: [.chunks[] | {chunkId, checksum, workers: [.replicas[].workerId]}]}'
CHUNKS_BEFORE="$(echo "${META_BEFORE}" | jq -c '[.chunks[] | {chunkId, checksum, workers: [.replicas[].workerId]}]')"

step "3. Updating custom metadata via PATCH /api/v1/objects/${OBJECT_ID}/metadata (owner->bob, add tag=verified, remove env)"
META_AFTER="$(curl -fsS -X PATCH "${CONTROLLER_URL}/api/v1/objects/${OBJECT_ID}/metadata" \
  -H "Content-Type: application/json" \
  -d '{"owner":"bob","tag":"verified","env":null}')"
echo "${META_AFTER}" | jq '{objectId, customMetadata, updatedAt}'

OWNER_AFTER="$(echo "${META_AFTER}" | jq -r '.customMetadata.owner')"
TAG_AFTER="$(echo "${META_AFTER}" | jq -r '.customMetadata.tag')"
ENV_AFTER="$(echo "${META_AFTER}" | jq -r '.customMetadata.env // "REMOVED"')"
CHUNKS_AFTER="$(echo "${META_AFTER}" | jq -c '[.chunks[] | {chunkId, checksum, workers: [.replicas[].workerId]}]')"

assert_eq "${OWNER_AFTER}" "bob" "Custom metadata 'owner' updated to 'bob'"
assert_eq "${TAG_AFTER}" "verified" "Custom metadata 'tag' added as 'verified'"
assert_eq "${ENV_AFTER}" "REMOVED" "Custom metadata 'env' removed via null merge-patch"
assert_eq "${CHUNKS_AFTER}" "${CHUNKS_BEFORE}" "Chunk IDs, checksums, and replica locations unchanged (zero re-upload)"

step "4. Listing stored objects via GET /api/v1/objects?page=0&size=10"
curl -fsS "${CONTROLLER_URL}/api/v1/objects?page=0&size=10" | jq '{page, size, totalItems, items: [.items[] | {objectId, name, size, customMetadata}]}'

step "5. Deleting object ${OBJECT_ID} via DELETE /api/v1/objects/${OBJECT_ID}"
HTTP_CODE="$(curl -s -o /dev/null -w '%{http_code}' -X DELETE "${CONTROLLER_URL}/api/v1/objects/${OBJECT_ID}")"
assert_eq "${HTTP_CODE}" "204" "DELETE /api/v1/objects/${OBJECT_ID} returned 204 No Content"

NOT_FOUND_CODE="$(curl -s -o /dev/null -w '%{http_code}' "${CONTROLLER_URL}/api/v1/objects/${OBJECT_ID}/metadata")"
assert_eq "${NOT_FOUND_CODE}" "404" "Subsequent metadata GET returns 404 Not Found"

pass "Demo 4 completed successfully."

