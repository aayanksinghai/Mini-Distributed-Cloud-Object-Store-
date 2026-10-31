#!/usr/bin/env bash
# ==============================================================================
# Demonstration 1 (PDF §10.1): Object Storage
#   - Upload a binary file via POST /api/v1/objects
#   - Download it via GET /api/v1/objects/{object_id}
#   - Verify original and downloaded files are identical using sha256sum
# ==============================================================================
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=./_common.sh
source "${SCRIPT_DIR}/_common.sh"

banner "DEMO 1: Object Storage (Upload -> Download -> SHA-256 Verification)"
wait_for_controller
wait_for_workers_up 4

TMP_DIR="$(mktemp -d)"
trap 'rm -rf "${TMP_DIR}"' EXIT

ORIG_FILE="${TMP_DIR}/sample-binary.bin"
DOWN_FILE="${TMP_DIR}/downloaded-binary.bin"

step "Generating 2.4 MiB random binary file (${ORIG_FILE})"
dd if=/dev/urandom of="${ORIG_FILE}" bs=1024 count=2458 status=none
ORIG_SHA="$(sha256sum "${ORIG_FILE}" | awk '{print $1}')"
echo "    Original SHA-256: ${ORIG_SHA}"

step "Uploading object via POST ${CONTROLLER_URL}/api/v1/objects"
UPLOAD_JSON="$(curl -fsS -X POST "${CONTROLLER_URL}/api/v1/objects" \
  -F "file=@${ORIG_FILE};type=application/octet-stream" \
  -F 'metadata={"demo":"01-object-storage","author":"coursework"}')"
echo "${UPLOAD_JSON}" | jq .

OBJECT_ID="$(echo "${UPLOAD_JSON}" | jq -r '.objectId')"
STORED_SHA="$(echo "${UPLOAD_JSON}" | jq -r '.checksum' | sed 's/^sha256://')"
assert_eq "${STORED_SHA}" "${ORIG_SHA}" "Controller stored object SHA-256 matches original file"

step "Downloading object ${OBJECT_ID} via GET ${CONTROLLER_URL}/api/v1/objects/${OBJECT_ID}"
curl -fsS "${CONTROLLER_URL}/api/v1/objects/${OBJECT_ID}" -o "${DOWN_FILE}"
DOWN_SHA="$(sha256sum "${DOWN_FILE}" | awk '{print $1}')"
echo "    Downloaded SHA-256: ${DOWN_SHA}"

assert_eq "${DOWN_SHA}" "${ORIG_SHA}" "sha256sum of downloaded object equals original file"
pass "Demo 1 completed successfully."

