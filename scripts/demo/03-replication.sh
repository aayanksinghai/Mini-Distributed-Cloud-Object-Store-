#!/usr/bin/env bash
# ==============================================================================
# Demonstration 3 (PDF §10.3): Replication (RF = 2)
#   - Upload a multi-chunk object
#   - Show that each chunk has 2 replicas stored on 2 distinct workers
#   - Verify cluster status reports 0 under-replicated chunks and HEALTHY status
# ==============================================================================
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=./_common.sh
source "${SCRIPT_DIR}/_common.sh"

banner "DEMO 3: Replication Factor = 2 on Distinct Workers"
wait_for_controller
wait_for_workers_up 4

TMP_DIR="$(mktemp -d)"
trap 'rm -rf "${TMP_DIR}"' EXIT

FILE="${TMP_DIR}/replicated-object.bin"
dd if=/dev/urandom of="${FILE}" bs=1048576 count=4 status=none

step "Uploading 4 MiB (4-chunk) object"
UPLOAD_JSON="$(curl -fsS -X POST "${CONTROLLER_URL}/api/v1/objects" \
  -F "file=@${FILE};type=application/octet-stream" \
  -F 'metadata={"demo":"03-replication"}')"
OBJECT_ID="$(echo "${UPLOAD_JSON}" | jq -r '.objectId')"

step "Inspecting replica locations for every chunk of ${OBJECT_ID}"
META_JSON="$(curl -fsS "${CONTROLLER_URL}/api/v1/objects/${OBJECT_ID}/metadata")"
echo "${META_JSON}" | jq '.chunks[] | {chunkId, index, replicationStatus, replicas: [.replicas[] | {workerId, state, workerStatus}]}'

for idx in 0 1 2 3; do
  REPLICA_COUNT="$(echo "${META_JSON}" | jq -r ".chunks[${idx}].replicas | length")"
  DISTINCT_HOLDERS="$(echo "${META_JSON}" | jq -r "[.chunks[${idx}].replicas[].workerId] | unique | length")"
  assert_eq "${REPLICA_COUNT}" "2" "Chunk #${idx} has 2 replicas"
  assert_eq "${DISTINCT_HOLDERS}" "2" "Chunk #${idx} replicas reside on 2 distinct workers"
done

step "Querying cluster-wide replication status via GET /api/v1/cluster/status"
STATUS_JSON="$(curl -fsS "${CONTROLLER_URL}/api/v1/cluster/status")"
echo "${STATUS_JSON}" | jq .

RF="$(echo "${STATUS_JSON}" | jq -r '.replicationFactor')"
UNDER_REP="$(echo "${STATUS_JSON}" | jq -r '.underReplicatedChunks')"
CLUSTER_STATE="$(echo "${STATUS_JSON}" | jq -r '.replicationStatus')"
assert_eq "${RF}" "2" "Cluster replicationFactor is 2"
assert_eq "${UNDER_REP}" "0" "Zero under-replicated chunks"
assert_eq "${CLUSTER_STATE}" "HEALTHY" "Cluster replicationStatus is HEALTHY"

pass "Demo 3 completed successfully."

