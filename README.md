# Mini Distributed Cloud Object Store

A distributed cloud object storage system built with **Java 21**, **Spring Boot 3.5.7**, **PostgreSQL 16**, **React 18 + TypeScript 5 + Vite 5**, and **Docker & Docker Compose** (strictly without Kubernetes, per coursework specification).

The system implements a **1 Controller + 4 independent Storage Worker** architecture that splits arbitrary binary objects into **1 MiB (1,048,576-byte)** chunks, distributes them across workers with a **Replication Factor of 2 (`RF = 2`)** on distinct workers, verifies **SHA-256** checksums at both chunk and object granularity, detects worker failures via heartbeats, automatically re-replicates under-replicated chunks, reconciles rejoining workers back to `RF = 2`, detects and repairs on-disk chunk corruption, and provides a real-time **Web Dashboard** on port `3000`.

---

## Table of Contents

1. [Implementation Status](#1-implementation-status)
2. [System Architecture & Core Workflows](#2-system-architecture--core-workflows)
3. [Repository Structure](#3-repository-structure)
4. [Prerequisites & Quick Start](#4-prerequisites--quick-start)
5. [Web Dashboard Guide (`:3000`)](#5-web-dashboard-guide-3000)
6. [Complete REST API Reference (with `curl` Examples)](#6-complete-rest-api-reference-with-curl-examples)
   - [Client-Facing Object APIs (6 Mandatory Endpoints)](#61-client-facing-object-apis-controller-8080)
   - [Internal Controller ↔ Worker APIs (6 Mandatory Endpoints)](#62-internal-controller--worker-apis)
   - [Cluster, Admin & Demo APIs](#63-cluster-admin--demo-apis-controller-8080)
   - [Uniform Error Response Format](#64-uniform-error-response-format)
7. [Demonstration Scenarios Walkthrough (PDF §10.1 – §10.6)](#7-demonstration-scenarios-walkthrough-pdf-101--106)
8. [Configuration (Environment Variables)](#8-configuration-environment-variables)
9. [Testing & Verification](#9-testing--verification)
10. [Key Design Decisions & Known Limitations](#10-key-design-decisions--known-limitations)

---

## 1. Implementation Status

All 11 phases from [`plan.md`](./plan.md) are **Completed** and verified:

- **Phase 0 — Architecture & Plan:** Completed ([`plan.md`](./plan.md))
- **Phase 1 — Project Foundation & Docker Environment:** Completed
- **Phase 2 — Controller ↔ Worker Communication:** Completed
- **Phase 3 — Object Upload & Chunking (1 MiB):** Completed
- **Phase 4 — Download & Reconstruction (Streaming + Failover):** Completed
- **Phase 5 — Replication (`RF = 2`) & Repair Engine:** Completed
- **Phase 6 — Metadata Operations, Listing & Deletion:** Completed
- **Phase 7 — Worker Failure Handling & Rejoin Reconciliation:** Completed
- **Phase 8 — Data Integrity Verification & Recovery:** Completed
- **Phase 9 — Web Dashboard Integration (React + TypeScript + Nginx):** Completed
- **Phase 10 — Comprehensive Testing & Demo Scripts:** Completed
- **Phase 11 — Final Documentation:** Completed

---

## 2. System Architecture & Core Workflows

### 2.1 Cluster Topology

```text
  Clients (Browser / curl / Demo Scripts)
         │                     │
         │ HTTP (:3000)        │ REST API (:8080)
         ▼                     ▼
┌─────────────────┐   ┌──────────────────────────┐        ┌──────────────────────────┐
│    dashboard    │──▶│        controller        │───────▶│     postgres (:5432)     │
│ (React + Nginx) │   │ (Spring Boot 3.5.7, J21) │        │ Named volume:            │
└─────────────────┘   └────────────┬─────────────┘        │   postgres-data          │
                                   │                      └──────────────────────────┘
                                   │ Internal HTTP (/internal/**, X-Internal-Token)
            ┌──────────────────────┼──────────────────────┬──────────────────────┐
            ▼                      ▼                      ▼                      ▼
   ┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
   │     worker1     │    │     worker2     │    │     worker3     │    │     worker4     │
   │     (:8081)     │    │     (:8081)     │    │     (:8081)     │    │     (:8081)     │
   └────────┬────────┘    └────────┬────────┘    └────────┬────────┘    └────────┬────────┘
            ▼                      ▼                      ▼                      ▼
     Named volume:          Named volume:          Named volume:          Named volume:
     worker1-data           worker2-data           worker3-data           worker4-data
    (/data/chunks)         (/data/chunks)         (/data/chunks)         (/data/chunks)
```

### 2.2 Storage Separation & Persistent Volumes

- **Controller Metadata Store (`postgres`):** Stores only structured metadata (`workers`, `objects`, `object_metadata`, `chunks`, `chunk_replicas`, `pending_deletions`, `system_events`) in PostgreSQL 16 backed by named volume `postgres-data`. The Controller never persists raw object chunks on its own disk.
- **Independent Worker Volumes (`worker1-data` .. `worker4-data`):** Each of the 4 worker containers mounts its own dedicated named Docker volume at `/data/chunks`. Chunks are stored atomically (`temp/` write → `fsync` → atomic `ATOMIC_MOVE` into two-level sharded directory `chunks/<2-hex>/<chunk_id>.chunk` alongside `<chunk_id>.meta.json`).

### 2.3 Upload, 1 MiB Chunking & `RF = 2` Placement Workflow

1. Client sends `POST /api/v1/objects` (`multipart/form-data` with `file` and optional JSON `metadata`).
2. Controller creates an `objects` record in state `UPLOADING` and streams the input through `Chunker` in exact **1,048,576-byte (1 MiB)** buffers (the final chunk may be smaller; 0-byte files are also supported).
3. For each chunk (`{objectId}_{index:05d}`):
   - Controller computes the chunk's **SHA-256** checksum and updates the rolling whole-object SHA-256 digest.
   - `PlacementService` selects **2 distinct `UP` workers** using least-loaded sorting (`chunk_count ASC, used_bytes ASC`) with a rotating tie-break cursor so chunks spread evenly across all 4 workers.
   - Controller writes the chunk to both selected workers in parallel using Java 21 virtual threads (`PUT /internal/chunks/{chunkId}` with `X-Chunk-Checksum`). Each worker recomputes SHA-256 before committing atomically to disk.
   - If one target worker fails mid-write, Controller automatically selects a replacement among remaining `UP` workers so `RF = 2` is still satisfied on upload.
4. Once all chunks are stored, Controller transitions the object to `ACTIVE` and returns `201 Created` with `ObjectInfo`. If upload cannot achieve `RF = 2`, written chunks are rolled back.

### 2.4 Download, Checksum Verification & Automatic Failover

1. Client sends `GET /api/v1/objects/{objectId}`.
2. Controller verifies the object is `ACTIVE` and pre-fetches/verifies the first chunk so that if all replicas of a chunk are unavailable or corrupted on disk, a clean `503 Service Unavailable` (`CHUNK_UNAVAILABLE`) JSON response is returned before HTTP `200 OK` headers are committed.
3. For each chunk in `chunk_index` order (`0 .. chunkCount - 1`):
   - Controller orders candidate replicas (`HEALTHY` replicas on `UP` workers first) and requests `GET /internal/chunks/{chunkId}`.
   - Both the Worker (on disk read) and the Controller (on receive) verify the chunk's **SHA-256** checksum against the expected metadata checksum.
   - If a worker is unreachable or returns `409 CHUNK_CORRUPTED` / mismatched bytes, Controller marks that replica `CORRUPTED` (recording a `CHUNK_CORRUPTED` event) and **transparently fails over** to the second replica without interrupting the client's download stream.

### 2.5 Worker Failure Detection, Re-Replication & Rejoin Reconciliation

1. **Heartbeat Monitoring:** Workers send `POST /internal/workers/heartbeat` every `3s`. If a worker is silent for `> 10s` (`HEARTBEAT_TIMEOUT_MS`), `HeartbeatMonitor` marks the worker `DOWN`, transitions its replicas to `UNAVAILABLE`, and logs `WORKER_DOWN` + `UNDER_REPLICATED` events.
2. **Automatic Re-Replication:** After `REREPLICATION_GRACE_PERIOD_MS` (`30s` on scheduled runs, or immediately via `POST /api/v1/admin/replication/repair`), `ReplicationService` copies each under-replicated chunk (`RF == 1`) from its surviving `HEALTHY` worker to a new distinct `UP` worker and logs `RF_RESTORED`.
3. **Rejoin Reconciliation:** When a previously `DOWN` worker restarts and re-registers (or via `POST /api/v1/admin/workers/{workerId}/reconcile`), `ReconciliationService`:
   - Executes any deferred `pending_deletions` queued while the worker was offline.
   - Deletes orphan chunks belonging to deleted or non-existent objects.
   - Re-adopts valid on-disk replicas if the chunk still needs a replica (`RF < 2`).
   - **Trims surplus replicas** (`DELETE /internal/chunks/{chunkId}`) if the chunk was already re-replicated onto two other workers while this worker was down — restoring **exact `RF = 2`** across the cluster.

---

## 3. Repository Structure

```text
.
├── pom.xml                                  # Parent Maven POM (Java 21, Spring Boot 3.5.7)
├── docker-compose.yml                       # 1 Controller + 4 Workers + Postgres 16 + Dashboard
├── .env.example                             # Template environment variables
├── Mini_Distributed_Cloud_Object_Store.pdf  # Coursework problem statement
├── plan.md                                  # Detailed architecture & phase-by-phase implementation plan
├── README.md                                # Project documentation & API reference
├── docs/
│   └── DEMO_GUIDE.md                        # Quick-reference evaluation & demonstration cheat sheet
├── common/                                  # Shared DTOs, error codes, exception hierarchy, SHA-256 utility
├── controller/                              # Controller service (:8080), Flyway migrations V1..V4, JPA, REST APIs
├── worker/                                  # Storage Worker service (:8081), atomic ChunkStore, Scrubber, Heartbeat
├── dashboard/                               # React 18 + TypeScript 5 + Vite 5 SPA & Nginx reverse proxy (:3000)
└── scripts/
    ├── demo/
    │   ├── _common.sh                       # Shared helpers, assertions, readiness checks
    │   ├── corrupt-chunk.sh                 # Helper to corrupt a chunk on a worker for §10.6 demos
    │   ├── 01-object-storage.sh             # §10.1 Object upload, download & SHA-256 verification
    │   ├── 02-chunking-and-distribution.sh  # §10.2 1 MiB chunking & 4-worker distribution
    │   ├── 03-replication.sh                # §10.3 RF = 2 distinct-worker verification
    │   ├── 04-metadata.sh                   # §10.4 Metadata upload, GET, PATCH merge, LIST & DELETE
    │   ├── 05-worker-failure.sh             # §10.5 Worker failure, failover, re-replication & rejoin reconciliation
    │   └── 06-data-integrity.sh             # §10.6 Chunk corruption detection, failover & automatic repair
    └── e2e/
        └── run-all.sh                       # Full-stack Docker Compose E2E runner + volume persistence test
```

---

## 4. Prerequisites & Quick Start

### Prerequisites
- **Docker** (24+) and **Docker Compose** (`docker-compose` or `docker compose`)
- **Java 21** (OpenJDK 21+) and **Maven 3.9+** (for running unit/integration tests outside Docker)
- **Node.js 20+** and **npm** (optional, for local dashboard development outside Docker)
- Standard CLI utilities for demo scripts: `bash`, `curl`, `jq`, `sha256sum`, `python3`

### Start the Full System with Docker Compose

```bash
# 1. Optional: copy .env.example to .env if you want custom ports or timings
cp .env.example .env

# 2. Build and start Postgres, Controller, 4 Storage Workers, and Web Dashboard
docker-compose up -d --build

# 3. Check container health
docker-compose ps
```

Once running:
- **Web Dashboard:** [http://localhost:3000](http://localhost:3000)
- **Controller REST API:** [http://localhost:8080/api/v1/cluster/status](http://localhost:8080/api/v1/cluster/status)
- **Controller Health:** [http://localhost:8080/actuator/health](http://localhost:8080/actuator/health)

Verify that PostgreSQL and all 4 workers have separate persistent Docker volumes:

```bash
docker volume ls | grep -E "postgres-data|worker[1-4]-data"
```

Stop the cluster while preserving persistent volumes:

```bash
docker-compose down
```

Stop and wipe all volumes (clean slate):

```bash
docker-compose down -v
```

---

## 5. Web Dashboard Guide (`:3000`)

Open **[http://localhost:3000](http://localhost:3000)** in your browser. Nginx serves the compiled React 18 application and reverse-proxies `/api/*` and `/actuator/*` to `controller:8080`.

1. **Objects View (`#/objects`):**
   - **Drag-and-Drop Upload Card:** Select any file, attach optional custom metadata key-value rows, and upload with a live progress bar.
   - **Paginated & Sortable Object Browser:** Sort stored objects by `createdAt`, `updatedAt`, `name`, or `size`; download, inspect metadata/chunks, or delete objects with confirmation.
2. **Object Detail View (`#/objects/{objectId}`):**
   - **System Metadata & Client-Side SHA-256 Verifier:** Click **"Verify Download SHA-256"** to stream the object into the browser, compute its SHA-256 via WebCrypto (`crypto.subtle.digest`), and compare it against the Controller's stored `X-Object-Checksum-SHA256`.
   - **Proportional Chunk Strip (`ChunkStrip`):** Visualizes ordered 1 MiB chunks (`#0`, `#1`, ...) and the smaller final chunk, colored by replication status (`HEALTHY`, `DEGRADED`, `UNAVAILABLE`).
   - **Chunk × Worker Replica Matrix (`ReplicaMatrix`):** Interactive table mapping every chunk across `worker1`..`worker4`, showing replica states (`HEALTHY`, `CORRUPTED`, `UNAVAILABLE`). Clicking a replica cell opens a detail drawer with a **"Corrupt this replica (demo)"** button (when `WORKER_DEBUG_ENDPOINTS=true`) and **"Scrub & Repair Object"**.
   - **Live Metadata Editor:** Add, modify, or remove custom metadata keys via `PATCH /api/v1/objects/{id}/metadata` without re-uploading the file.
3. **Cluster View (`#/cluster`):**
   - **Cluster Replication Banner:** Real-time counts of active objects, total chunks, `RF >= 2` healthy chunks, `RF == 1` under-replicated chunks, `RF == 0` unavailable chunks, and corrupted replicas.
   - **4 Worker Node Cards (`worker1`..`worker4`):** Shows `UP`/`DOWN` badge, endpoint URL, heartbeat age, stored chunk count, used/total capacity bars, and a per-worker **"Reconcile Inventory"** button.
   - **Admin Actions:** One-click **"Run Replication Repair Now"** (`POST /api/v1/admin/replication/repair`) and **"Run Integrity Scrub (SHA-256)"** (`POST /api/v1/admin/integrity/scrub?repair=true`).
4. **Operations Log View (`#/events`):**
   - Live-polled stream of `system_events` (`WORKER_REGISTERED`, `WORKER_DOWN`, `WORKER_UP`, `OBJECT_UPLOADED`, `OBJECT_METADATA_UPDATED`, `OBJECT_DELETED`, `UNDER_REPLICATED`, `RF_RESTORED`, `CHUNK_CORRUPTED`, `CHUNK_REPAIRED`, `WORKER_RECONCILED`) with severity (`INFO`, `WARN`, `ERROR`) and event-type filters.

---

## 6. Complete REST API Reference (with `curl` Examples)

### 6.1 Client-Facing Object APIs (Controller `:8080`)

#### 1. Upload Object — `POST /api/v1/objects`
Uploads an arbitrary binary file (`multipart/form-data`), splits it into 1 MiB chunks, computes SHA-256 checksums, and replicates each chunk to 2 distinct `UP` workers.

```bash
curl -i -X POST http://localhost:8080/api/v1/objects \
  -F "file=@./sample.bin;filename=sample.bin;type=application/octet-stream" \
  -F 'metadata={"owner":"alice","env":"prod","department":"cloud"}'
```

- **Success:** `201 Created` (with `Location: /api/v1/objects/{objectId}`)
```json
{
  "objectId": "6b2c0a3a-9c77-4f8f-9a21-512e4ef63c01",
  "objectName": "sample.bin",
  "sizeBytes": 2465792,
  "contentType": "application/octet-stream",
  "checksumSha256": "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
  "chunkSizeBytes": 1048576,
  "chunkCount": 3,
  "replicationFactor": 2,
  "status": "ACTIVE",
  "createdAt": "2026-10-31T10:00:00Z",
  "updatedAt": "2026-10-31T10:00:00Z",
  "metadata": {
    "department": "cloud",
    "env": "prod",
    "owner": "alice"
  }
}
```
- **Errors:** `400 Bad Request` (`MISSING_FILE`, `INVALID_METADATA`), `413 Payload Too Large`, `503 Service Unavailable` (`INSUFFICIENT_STORAGE_NODES` when fewer than 2 workers are `UP`).

---

#### 2. Download Object — `GET /api/v1/objects/{object_id}`
Reconstructs and streams the original binary object in chunk order, verifying each chunk's SHA-256 on read and failing over automatically if a worker is down or a replica is corrupted.

```bash
curl -sS -D headers.txt \
  http://localhost:8080/api/v1/objects/6b2c0a3a-9c77-4f8f-9a21-512e4ef63c01 \
  -o downloaded.bin
```

- **Success:** `200 OK`
  - Headers: `Content-Type`, `Content-Length`, `Content-Disposition: attachment; filename="sample.bin"`, `ETag: "<sha256>"`, `X-Object-Checksum-SHA256: <sha256>`
- **Errors:** `400 Bad Request` (invalid UUID), `404 Not Found` (`OBJECT_NOT_FOUND`), `409 Conflict` (`OBJECT_NOT_READY`), `503 Service Unavailable` (`CHUNK_UNAVAILABLE` if all replicas of any chunk are unreachable or corrupted).

---

#### 3. Retrieve Object Metadata & Chunk Map — `GET /api/v1/objects/{object_id}/metadata`
Returns system metadata, custom key-value metadata, overall `replicationStatus` (`HEALTHY`, `DEGRADED`, `UNAVAILABLE`), and the ordered list of chunks with their replica placements.

```bash
curl -sS http://localhost:8080/api/v1/objects/6b2c0a3a-9c77-4f8f-9a21-512e4ef63c01/metadata | jq .
```

- **Success:** `200 OK`
```json
{
  "objectId": "6b2c0a3a-9c77-4f8f-9a21-512e4ef63c01",
  "objectName": "sample.bin",
  "sizeBytes": 2465792,
  "contentType": "application/octet-stream",
  "checksumSha256": "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
  "chunkSizeBytes": 1048576,
  "chunkCount": 3,
  "replicationFactor": 2,
  "status": "ACTIVE",
  "replicationStatus": "HEALTHY",
  "createdAt": "2026-10-31T10:00:00Z",
  "updatedAt": "2026-10-31T10:00:00Z",
  "metadata": {
    "department": "cloud",
    "env": "prod",
    "owner": "alice"
  },
  "chunks": [
    {
      "chunkId": "6b2c0a3a-9c77-4f8f-9a21-512e4ef63c01_00000",
      "chunkIndex": 0,
      "sizeBytes": 1048576,
      "checksumSha256": "7d865e959b2466918c9863afca942d0fb89d7c9ac0c99bafc3749504ded97730",
      "replicas": [
        { "workerId": "worker1", "workerStatus": "UP", "state": "HEALTHY", "verifiedAt": "2026-10-31T10:00:00Z" },
        { "workerId": "worker2", "workerStatus": "UP", "state": "HEALTHY", "verifiedAt": "2026-10-31T10:00:00Z" }
      ]
    }
  ]
}
```
- **Errors:** `404 Not Found` (`OBJECT_NOT_FOUND`), `409 Conflict` (`OBJECT_NOT_READY`).

---

#### 4. Update Custom Metadata — `PATCH /api/v1/objects/{object_id}/metadata`
Updates custom key-value metadata using **JSON Merge Patch** semantics without re-uploading the object or touching any worker chunks:
- String value (`"key": "newVal"`) → adds or updates `key`
- `null` value (`"key": null`) → deletes `key`
- Omitted keys → preserved unchanged

```bash
curl -sS -X PATCH http://localhost:8080/api/v1/objects/6b2c0a3a-9c77-4f8f-9a21-512e4ef63c01/metadata \
  -H "Content-Type: application/json" \
  -d '{"owner":"bob-team","stage":"verified","department":null}' | jq .
```

- **Success:** `200 OK` (returns updated `ObjectMetadataResponse` with bumped `updatedAt`)
- **Errors:** `400 Bad Request` (`INVALID_METADATA`), `404 Not Found` (`OBJECT_NOT_FOUND`), `409 Conflict` (`OBJECT_NOT_READY` / concurrent version conflict).

---

#### 5. Delete Object — `DELETE /api/v1/objects/{object_id}`
Deletes the object's metadata and removes all stored chunk replicas across `UP` workers (queueing deferred deletions in `pending_deletions` for any `DOWN` worker so they are purged when the worker rejoins).

```bash
curl -i -X DELETE http://localhost:8080/api/v1/objects/6b2c0a3a-9c77-4f8f-9a21-512e4ef63c01
```

- **Success:** `204 No Content`
- **Errors:** `404 Not Found` (`OBJECT_NOT_FOUND`), `409 Conflict` (`OBJECT_NOT_READY`).

---

#### 6. List Stored Objects — `GET /api/v1/objects`
Returns a paginated, sorted list of `ACTIVE` objects.

```bash
curl -sS "http://localhost:8080/api/v1/objects?page=0&size=20&sort=createdAt,desc" | jq .
```

- **Success:** `200 OK` (`ObjectListPage`: `items`, `page`, `size`, `totalElements`, `totalPages`)
- **Errors:** `400 Bad Request` (invalid pagination or sort parameters).

---

### 6.2 Internal Controller ↔ Worker APIs

All internal endpoints require the `X-Internal-Token: <INTERNAL_API_TOKEN>` header (returns `401 Unauthorized` if missing or invalid).

| # | Method & Path | Target | Success Codes | Error Codes | Description |
|---|---------------|--------|---------------|-------------|-------------|
| 1 | `POST /internal/workers/register` | Controller (`:8080`) | `201 Created` (first registration), `200 OK` (re-registration + auto-reconcile) | `400`, `401`, `409` | Registers a worker (`workerId`, `endpointUrl`, `totalCapacityBytes`, `usedBytes`, `freeBytes`, `chunkCount`). |
| 2 | `POST /internal/workers/heartbeat` | Controller (`:8080`) | `200 OK` | `400`, `401`, `404` (`WORKER_NOT_FOUND`) | Periodic worker liveness, storage capacity metrics, and `corruptChunkIds` list discovered by background `Scrubber`. |
| 3 | `PUT /internal/chunks/{chunk_id}[?overwrite=true]` | Worker (`:8081`) | `201 Created`, `200 OK` | `400` (`CHECKSUM_MISMATCH`, `INVALID_CHUNK_ID`), `401`, `409`, `507` | Atomically stores raw chunk bytes (`application/octet-stream`) after verifying `X-Chunk-Checksum` (SHA-256). |
| 4 | `GET /internal/chunks/{chunk_id}` | Worker (`:8081`) | `200 OK` (`X-Chunk-Checksum`) | `400`, `401`, `404` (`CHUNK_NOT_FOUND`), `409` (`CHUNK_CORRUPTED`) | Reads chunk from disk, verifies SHA-256 against sidecar metadata, and streams bytes (or returns `409` if corrupted). |
| 5 | `DELETE /internal/chunks/{chunk_id}` | Worker (`:8081`) | `204 No Content` | `400`, `401`, `404` (`CHUNK_NOT_FOUND`) | Deletes `<chunk_id>.chunk` and `<chunk_id>.meta.json` from worker volume. |
| 6 | `GET /internal/workers/{worker_id}/chunks[?verify=true]` | Worker (`:8081`) & Controller (`:8080`) | `200 OK` | `400`, `401`, `404` (`WORKER_NOT_FOUND`), `503` | Lists chunks stored on the worker; on the Controller side, also computes the reconciliation diff (`expectedChunkIds`, `missingChunkIds`, `orphanChunkIds`, `corruptChunkIds`). |

Example internal inventory inspection via the Controller:

```bash
curl -sS -H "X-Internal-Token: change-me-internal-token" \
  "http://localhost:8080/internal/workers/worker1/chunks?verify=true" | jq .
```

---

### 6.3 Cluster, Admin & Demo APIs (Controller `:8080`)

```bash
# List all 4 workers and their health/capacity
curl -sS http://localhost:8080/api/v1/cluster/workers | jq .

# Get cluster-wide replication & health summary
curl -sS http://localhost:8080/api/v1/cluster/status | jq .

# Trigger immediate replication repair (bypasses grace period)
curl -sS -X POST http://localhost:8080/api/v1/admin/replication/repair | jq .

# Trigger immediate inventory reconciliation for a specific worker
curl -sS -X POST http://localhost:8080/api/v1/admin/workers/worker2/reconcile | jq .

# Trigger on-demand SHA-256 integrity scrub across all UP workers (with auto-repair)
curl -sS -X POST "http://localhost:8080/api/v1/admin/integrity/scrub?repair=true" | jq .

# Corrupt a specific replica on a worker (requires WORKER_DEBUG_ENDPOINTS=true)
curl -sS -X POST http://localhost:8080/api/v1/admin/workers/worker1/chunks/<chunk_id>/corrupt | jq .

# Poll cluster & storage lifecycle events
curl -sS "http://localhost:8080/api/v1/admin/events?limit=50" | jq .
```

---

### 6.4 Uniform Error Response Format

Every error across both Controller and Worker returns a consistent JSON payload (`ErrorResponse`):

```json
{
  "timestamp": "2026-10-31T10:05:12.345Z",
  "status": 404,
  "error": "Not Found",
  "code": "OBJECT_NOT_FOUND",
  "message": "Object not found: 6b2c0a3a-9c77-4f8f-9a21-512e4ef63c01",
  "path": "/api/v1/objects/6b2c0a3a-9c77-4f8f-9a21-512e4ef63c01",
  "details": {}
}
```

---

## 7. Demonstration Scenarios Walkthrough (PDF §10.1 – §10.6)

All six mandatory coursework demonstrations from **Section 10** of `Mini_Distributed_Cloud_Object_Store.pdf` are automated as narrated scripts in [`scripts/demo/`](./scripts/demo) and orchestrated end-to-end by [`scripts/e2e/run-all.sh`](./scripts/e2e/run-all.sh).

### Run All 6 Scenarios + Volume Persistence Check Automatically

```bash
./scripts/e2e/run-all.sh
```

Or run each scenario individually against a running `docker-compose up -d` cluster:

| Scenario | Script | What It Demonstrates & Verifies |
|----------|--------|---------------------------------|
| **§10.1 Object Storage** | `./scripts/demo/01-object-storage.sh` | Generates a ~2.35 MiB binary file, uploads via `POST /api/v1/objects`, downloads via `GET /api/v1/objects/{id}`, and verifies `sha256sum` equality (`original == controller metadata == downloaded`). |
| **§10.2 Chunking & Distribution** | `./scripts/demo/02-chunking-and-distribution.sh` | Uploads a ~5.3 MiB file (`5,557,452` bytes), verifies `chunkCount = 6` (`5 × 1,048,576 B` + `1 × 314,572 B`), verifies chunks are distributed across all 4 workers (`worker1`..`worker4`), and inspects `/data/chunks/chunks` inside each worker container via `docker exec`. |
| **§10.3 Replication (`RF = 2`)** | `./scripts/demo/03-replication.sh` | Verifies `replicationFactor = 2`, checks that every chunk has 2 `HEALTHY` replicas on **distinct** `workerId`s, and asserts `GET /api/v1/cluster/status` reports `0` under-replicated and `0` unavailable chunks. |
| **§10.4 Metadata Lifecycle** | `./scripts/demo/04-metadata.sh` | Uploads an object with custom metadata, fetches `GET /api/v1/objects/{id}/metadata`, patches metadata via `PATCH /api/v1/objects/{id}/metadata` (updating, adding, and deleting keys via `null`) without re-uploading data (verifying chunk checksums and placements remain untouched), lists objects (`GET /api/v1/objects`), and deletes the object (`204 No Content`). |
| **§10.5 Worker Failure & Recovery** | `./scripts/demo/05-worker-failure.sh` | Records cluster state before failure → stops `worker2` (`docker stop worker2`) → waits for heartbeat timeout (`worker2` marked `DOWN`, chunks marked `DEGRADED`) → downloads the object and verifies byte-identical `sha256sum` from surviving replicas → triggers repair to restore `RF = 2` on surviving workers (`worker1`, `worker3`, `worker4`) → restarts `worker2` (`docker start worker2`) → verifies rejoin reconciliation trims surplus 3rd copies so every chunk has exact `RF = 2`. |
| **§10.6 Data Integrity & Recovery** | `./scripts/demo/06-data-integrity.sh` | Intentionally corrupts a stored chunk on disk using `./scripts/demo/corrupt-chunk.sh <worker> <chunk_id>` → verifies `GET /api/v1/objects/{id}` still returns a byte-identical file served from the healthy replica while logging `CHUNK_CORRUPTED` → runs `POST /api/v1/admin/integrity/scrub?repair=true` → verifies the corrupted on-disk file is overwritten with verified bytes (`sha256sum` on disk restored) and `CHUNK_REPAIRED` is logged. |

See [`docs/DEMO_GUIDE.md`](./docs/DEMO_GUIDE.md) for manual step-by-step `curl` commands for live grading demonstrations.

---

## 8. Configuration (Environment Variables)

All configuration parameters have defaults in [`docker-compose.yml`](./docker-compose.yml) and [`.env.example`](./.env.example):

| Variable | Service | Default | Description |
|----------|---------|---------|-------------|
| `POSTGRES_DB` | postgres | `objectstore` | PostgreSQL database name |
| `POSTGRES_USER` / `POSTGRES_PASSWORD` | postgres | `objectstore` | PostgreSQL credentials |
| `POSTGRES_PORT` | postgres | `5432` | Host port mapped to PostgreSQL |
| `CONTROLLER_PORT` | controller | `8080` | Host port mapped to Controller REST API |
| `DASHBOARD_PORT` | dashboard | `3000` | Host port mapped to Nginx Web Dashboard |
| `CHUNK_SIZE_BYTES` | controller | `1048576` | Fixed chunk split size (1 MiB) |
| `REPLICATION_FACTOR` | controller | `2` | Target replicas per chunk on distinct workers |
| `MAX_OBJECT_SIZE` | controller | `1GB` | Maximum multipart upload size |
| `HEARTBEAT_TIMEOUT_MS` | controller | `10000` | Silence threshold before marking a worker `DOWN` |
| `HEARTBEAT_CHECK_INTERVAL_MS` | controller | `2000` | Controller heartbeat monitor check period |
| `REREPLICATION_GRACE_PERIOD_MS` | controller | `30000` | Grace period after `DOWN` before background re-replication |
| `REPAIR_INTERVAL_MS` | controller | `10000` | Background replication repair scheduler interval |
| `WORKER_REQUEST_TIMEOUT_MS` | controller | `5000` | HTTP read/connect timeout for Controller → Worker calls |
| `INTERNAL_API_TOKEN` | controller, workers | `change-me-internal-token` | Shared token sent in `X-Internal-Token` header |
| `WORKER_ID` | worker | `worker1`..`worker4` | Unique worker node identifier |
| `WORKER_ADVERTISED_URL` | worker | `http://${WORKER_ID}:8081` | Worker URL advertised during registration |
| `CONTROLLER_URL` | worker | `http://controller:8080` | Controller base URL for registration & heartbeats |
| `STORAGE_DIR` | worker | `/data/chunks` | Mount path for the worker's dedicated Docker volume |
| `HEARTBEAT_INTERVAL_MS` | worker | `3000` | Worker heartbeat frequency |
| `SCRUB_INTERVAL_MS` | worker | `300000` | Background on-disk SHA-256 scrub interval |
| `WORKER_DEBUG_ENDPOINTS` | worker | `false` (`true` in demos) | Enables `/internal/debug/chunks/{id}/corrupt` for §10.6 demos |

---

## 9. Testing & Verification

### Backend Unit & Integration Tests (Maven / JUnit 5)
The backend test suite comprises **63 automated tests** across `common` (13 tests), `worker` (15 tests), and `controller` (35 tests, including full multi-worker HTTP + PostgreSQL integration tests covering upload, download failover, `RF = 2` repair, metadata merge-patch, worker failure & rejoin reconciliation, bit-rot detection & recovery, and 8-thread concurrent uploads):

```bash
mvn clean verify
```

### Frontend Build & Unit/API Tests (TypeScript / Vite / Node Test Runner)
The dashboard test suite comprises **8 test suites** covering formatting utilities, metadata validation & JSON Merge Patch construction, replica matrix & status badges, hash routing & event deduplication, and typed REST client calls:

```bash
npm --prefix dashboard run build
npm --prefix dashboard test
```

### End-to-End Docker Compose Suite
```bash
./scripts/e2e/run-all.sh
```

---

## 10. Key Design Decisions & Known Limitations

### Key Design Decisions
1. **Deterministic Chunk IDs (`{objectId}_{index:05d}`):** Human-readable in logs, worker directories, and the dashboard while guaranteeing global uniqueness and natural lexical ordering.
2. **Least-Loaded Placement with Rotating Cursor:** Ensures every chunk's 2 replicas land on **distinct** `UP` workers while distributing chunks evenly across all 4 workers (`worker1`..`worker4`).
3. **Atomic Write-Then-Rename on Worker Volumes:** Workers write incoming chunk streams to `/data/chunks/temp/<uuid>.tmp`, verify SHA-256, `fsync`, and atomically move to `/data/chunks/chunks/<2-hex>/<chunk_id>.chunk` alongside `<chunk_id>.meta.json` — preventing partial or corrupt writes on crash.
4. **Two-Layer Integrity Verification:** Both the Storage Worker (on disk read against `.meta.json`) and the Controller (against PostgreSQL `chunks.checksum_sha256`) verify SHA-256 on every read and repair operation.
5. **Grace Period + Rejoin Trim:** Prevents unnecessary data movement during brief container hiccups while ensuring that if a worker stays down long enough for its chunks to be re-replicated elsewhere, its eventual rejoin trims surplus 3rd copies back to exact `RF = 2`.
6. **Deferred Deletion Queue (`pending_deletions`):** Deleting an object while a worker is `DOWN` succeeds immediately (`204 No Content`) and records pending chunk deletions that are executed automatically as soon as the worker rejoins.

### Known Limitations & Trade-Offs
- **Single Controller Node (by specification):** Per coursework requirements, the cluster has 1 Controller and 4 Workers. While metadata is persisted durably in PostgreSQL and workers automatically re-register if the Controller restarts, the Controller is on the data path and represents a single point of coordination.
- **Controller-Mediated Streaming:** Uploads, downloads, and chunk repairs stream through the Controller rather than using direct client-to-worker presigned URLs or worker-to-worker transfers. Streaming buffers (`StreamingResponseBody`) and Java 21 virtual threads keep memory bounded (`O(chunk_size)`).
- **Simultaneous Dual-Worker Loss:** With `RF = 2`, the cluster tolerates any single worker failure without data unavailability. If 2 workers go down simultaneously and both replicas of a specific chunk resided on those 2 workers, downloads of that object return `503 CHUNK_UNAVAILABLE` until at least one of those workers returns `UP`.
