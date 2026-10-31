#!/usr/bin/env bash
# ==============================================================================
# Shared helper library for Demonstration & E2E scripts (PDF §10)
# ==============================================================================
set -euo pipefail

CONTROLLER_URL="${CONTROLLER_URL:-http://localhost:${CONTROLLER_PORT:-8080}}"
INTERNAL_API_TOKEN="${INTERNAL_API_TOKEN:-change-me-internal-token}"

banner() {
  printf '\n==============================================================================\n'
  printf '  %s\n' "$1"
  printf '==============================================================================\n'
}

step() {
  printf '\n--> %s\n' "$1"
}

pass() {
  printf '    [PASS] %s\n' "$1"
}

fail() {
  printf '    [FAIL] %s\n' "$1" >&2
  exit 1
}

assert_eq() {
  local actual="$1"
  local expected="$2"
  local msg="$3"
  if [[ "${actual}" != "${expected}" ]]; then
    fail "${msg} (expected='${expected}', actual='${actual}')"
  fi
  pass "${msg} (${actual})"
}

wait_for_controller() {
  local max_wait="${1:-60}"
  local elapsed=0
  while ! curl -fsS "${CONTROLLER_URL}/actuator/health" >/dev/null 2>&1; do
    sleep 1
    elapsed=$((elapsed + 1))
    if [[ "${elapsed}" -ge "${max_wait}" ]]; then
      fail "Controller at ${CONTROLLER_URL} did not become healthy within ${max_wait}s"
    fi
  done
}

wait_for_workers_up() {
  local expected_up="${1:-4}"
  local max_wait="${2:-60}"
  local elapsed=0
  while true; do
    local up_count
    up_count="$(curl -fsS "${CONTROLLER_URL}/api/v1/cluster/status" | jq -r '.workersUp')"
    if [[ "${up_count}" == "${expected_up}" ]]; then
      return 0
    fi
    sleep 1
    elapsed=$((elapsed + 1))
    if [[ "${elapsed}" -ge "${max_wait}" ]]; then
      fail "Expected ${expected_up} UP workers within ${max_wait}s, but found ${up_count}"
    fi
  done
}

wait_for_worker_status() {
  local worker_id="$1"
  local expected_status="$2"
  local max_wait="${3:-30}"
  local elapsed=0
  while true; do
    local status
    status="$(curl -fsS "${CONTROLLER_URL}/api/v1/cluster/workers" | jq -r ".[] | select(.workerId==\"${worker_id}\") | .status")"
    if [[ "${status}" == "${expected_status}" ]]; then
      return 0
    fi
    sleep 1
    elapsed=$((elapsed + 1))
    if [[ "${elapsed}" -ge "${max_wait}" ]]; then
      fail "Worker ${worker_id} did not reach status ${expected_status} within ${max_wait}s (last=${status})"
    fi
  done
}

