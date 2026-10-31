#!/usr/bin/env bash
# ==============================================================================
# Mini Distributed Cloud Object Store — Full Automated E2E Test Suite
# ==============================================================================
# Starts the full Docker Compose stack (PostgreSQL + Controller + 4 Workers +
# Dashboard), executes all 6 mandatory coursework demonstration scenarios
# (PDF §10.1–§10.6), verifies dashboard reachability on port 3000, and tests
# persistent volume survival across `docker-compose down` -> `docker-compose up -d`.
# ==============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/../.." && pwd)"
DEMO_DIR="${ROOT_DIR}/scripts/demo"
# shellcheck source=../demo/_common.sh
source "${DEMO_DIR}/_common.sh"

cd "${ROOT_DIR}"

COMPOSE_CMD="docker-compose"
if ! command -v docker-compose >/dev/null 2>&1 && docker compose version >/dev/null 2>&1; then
  COMPOSE_CMD="docker compose"
fi

banner "Starting Mini Distributed Cloud Object Store via ${COMPOSE_CMD}"
${COMPOSE_CMD} up -d --build

wait_for_controller 90
wait_for_workers_up 4 90

step "Verifying Web Dashboard is reachable on http://localhost:${DASHBOARD_PORT:-3000}"
DASH_HTML="$(curl -fsS "http://localhost:${DASHBOARD_PORT:-3000}/")"
if [[ "${DASH_HTML}" != *"Mini Distributed Cloud Object Store"* ]]; then
  fail "Dashboard HTML did not contain expected title"
fi
pass "Dashboard is serving SPA on port ${DASHBOARD_PORT:-3000} and proxying /api/v1"

"${DEMO_DIR}/01-object-storage.sh"
"${DEMO_DIR}/02-chunking-and-distribution.sh"
"${DEMO_DIR}/03-replication.sh"
"${DEMO_DIR}/04-metadata.sh"
"${DEMO_DIR}/05-worker-failure.sh"
"${DEMO_DIR}/06-data-integrity.sh"

banner "Persistence Verification: Restarting Compose Stack (Preserving Named Volumes)"
TMP_DIR="$(mktemp -d)"
trap 'rm -rf "${TMP_DIR}"' EXIT

PERSIST_FILE="${TMP_DIR}/persist-check.bin"
PERSIST_DOWN="${TMP_DIR}/persist-downloaded.bin"
dd if=/dev/urandom of="${PERSIST_FILE}" bs=1024 count=1600 status=none
PERSIST_SHA="$(sha256sum "${PERSIST_FILE}" | awk '{print $1}')"

UPLOAD_JSON="$(curl -fsS -X POST "${CONTROLLER_URL}/api/v1/objects" \
  -F "file=@${PERSIST_FILE};type=application/octet-stream" \
  -F 'metadata={"persistence":"verified"}')"
PERSIST_ID="$(echo "${UPLOAD_JSON}" | jq -r '.objectId')"

step "Stopping containers with '${COMPOSE_CMD} down' (without -v so named volumes persist)..."
${COMPOSE_CMD} down
step "Bringing containers back up with '${COMPOSE_CMD} up -d'..."
${COMPOSE_CMD} up -d
wait_for_controller 90
wait_for_workers_up 4 90

step "Downloading object ${PERSIST_ID} after full cluster restart"
curl -fsS "${CONTROLLER_URL}/api/v1/objects/${PERSIST_ID}" -o "${PERSIST_DOWN}"
AFTER_RESTART_SHA="$(sha256sum "${PERSIST_DOWN}" | awk '{print $1}')"
assert_eq "${AFTER_RESTART_SHA}" "${PERSIST_SHA}" "Object survived full cluster restart across persistent Docker volumes"

banner "ALL 6 MANDATORY DEMONSTRATIONS + PERSISTENCE E2E SUITE PASSED!"

