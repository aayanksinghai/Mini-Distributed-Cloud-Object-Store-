#!/usr/bin/env bash
# ==============================================================================
# Demonstration 5 (PDF §10.5): Worker Failure Handling & Rejoin Reconciliation
#   - Show cluster status before failure (4 workers UP, RF = 2)
#   - Stop one worker container: `docker stop worker2`
#   - Wait for heartbeat timeout -> worker2 marked DOWN, replicas on worker2
#     marked UNAVAILABLE
#   - Download the stored object -> succeeds byte-identically from surviving replicas
#   - Restore replication factor (RF = 2) across surviving workers (worker1/3/4)
#   - Restart worker2: `docker start worker2` -> rejoin reconciliation trims
#     surplus 3rd copies so RF = 2 is maintained across the 4 UP workers
# ==============================================================================
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=./_common.sh
source "${SCRIPT_DIR}/_common.sh"

banner "DEMO 5: Worker Failure Handling (docker stop worker2 -> Read Failover -> RF Repair -> Rejoin)"
wait_for_controller
wait_for_workers_up 4

TMP_DIR="$(mktemp -d)"
trap 'docker start worker2 >/dev/null 2>&1 || true; rm -rf "${TMP_DIR}"' EXIT

ORIG_FILE="${TMP_DIR}/resilient-object.bin"
DOWN_FILE="${TMP_DIR}/downloaded-while-worker2-down.bin"
dd if=/dev/urandom of="${ORIG_FILE}" bs=1048576 count=4 status=none
ORIG_SHA="$(sha256sum "${ORIG_FILE}" | awk '{print $1}')"

step "1. Uploading 4 MiB (4-chunk) object while all 4 workers are UP"
UPLOAD_JSON="$(curl -fsS -X POST "${CONTROLLER_URL}/api/v1/objects" \
  -F "file=@${ORIG_FILE};type=application/octet-stream" \
  -F 'metadata={"demo":"05-worker-failure"}')"
OBJECT_ID="$(echo "${UPLOAD_JSON}" | jq -r '.objectId')"

step "2. Cluster status BEFORE worker failure"
curl -fsS "${CONTROLLER_URL}/api/v1/cluster/status" | jq .

step "3. Stopping worker2 container: docker stop worker2"
docker stop worker2 >/dev/null
echo "    Waiting for controller heartbeat monitor to mark worker2 DOWN..."
wait_for_worker_status "worker2" "DOWN" 30

step "4. Cluster status while worker2 is DOWN"
STATUS_DOWN="$(curl -fsS "${CONTROLLER_URL}/api/v1/cluster/status")"
echo "${STATUS_DOWN}" | jq .
WORKERS_UP="$(echo "${STATUS_DOWN}" | jq -r '.workersUp')"
WORKERS_DOWN="$(echo "${STATUS_DOWN}" | jq -r '.workersDown')"
assert_eq "${WORKERS_UP}" "3" "3 workers remain UP"
assert_eq "${WORKERS_DOWN}" "1" "1 worker (worker2) is DOWN"

step "5. Downloading object ${OBJECT_ID} while worker2 is DOWN (automatic read failover)"
curl -fsS "${CONTROLLER_URL}/api/v1/objects/${OBJECT_ID}" -o "${DOWN_FILE}"
DOWN_SHA="$(sha256sum "${DOWN_FILE}" | awk '{print $1}')"
assert_eq "${DOWN_SHA}" "${ORIG_SHA}" "Downloaded object while worker2 is DOWN matches original SHA-256"

step "6. Restoring replication factor (RF = 2) onto surviving workers (worker1, worker3, worker4)"
REPAIR_JSON="$(curl -fsS -X POST "${CONTROLLER_URL}/api/v1/admin/replication/repair")"
echo "${REPAIR_JSON}" | jq .

STATUS_RESTORED="$(curl -fsS "${CONTROLLER_URL}/api/v1/cluster/status")"
echo "${STATUS_RESTORED}" | jq .
UNDER_REP_AFTER_REPAIR="$(echo "${STATUS_RESTORED}" | jq -r '.underReplicatedChunks')"
assert_eq "${UNDER_REP_AFTER_REPAIR}" "0" "All chunks restored to RF = 2 across the 3 surviving UP workers"

step "7. Restarting worker2: docker start worker2 (triggers automatic rejoin reconciliation)"
docker start worker2 >/dev/null
wait_for_worker_status "worker2" "UP" 30

META_FINAL="$(curl -fsS "${CONTROLLER_URL}/api/v1/objects/${OBJECT_ID}/metadata")"
MAX_REPLICAS_PER_CHUNK="$(echo "${META_FINAL}" | jq -r '[.chunks[].replicas | length] | max')"
MIN_REPLICAS_PER_CHUNK="$(echo "${META_FINAL}" | jq -r '[.chunks[].replicas | length] | min')"
assert_eq "${MAX_REPLICAS_PER_CHUNK}" "2" "Surplus 3rd copies trimmed on rejoin (max replicas per chunk = 2)"
assert_eq "${MIN_REPLICAS_PER_CHUNK}" "2" "Every chunk has at least 2 healthy replicas (min replicas per chunk = 2)"

pass "Demo 5 completed successfully."

