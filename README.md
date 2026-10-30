# Mini Distributed Cloud Object Store

A distributed cloud object storage system built with **Java 21**, **Spring Boot 3.5.7**, **PostgreSQL 16**, and **Docker Compose** (without Kubernetes, per coursework specification).

The system consists of **1 Controller** node and **4 independent Storage Worker** nodes, splitting arbitrary binary objects into **1 MiB (1,048,576 byte)** chunks, distributing them across workers with a **replication factor of 2** on distinct workers, verifying SHA-256 data integrity, detecting worker failures, and automatically restoring replication and recovering from data corruption.

---

## Current Implementation Status

- **Phase 0 — Architecture & Plan:** Completed (`plan.md`)
- **Phase 1 — Project Foundation & Docker Environment:** Completed
- **Phase 2 — Controller ↔ Worker Communication:** Completed
- **Phase 3 — Object Upload & Chunking:** Completed
- **Phase 4 — Download & Reconstruction:** Completed
- **Phase 5 — Replication (RF = 2) & Repair Engine:** Completed
- **Phase 6 — Metadata Operations, Listing & Deletion:** Completed
- **Phase 7 — Worker Failure Handling & Rejoin Reconciliation:** Pending

See [`plan.md`](./plan.md) for the full architecture, database schema, REST API specifications, and phased roadmap.

---

## Architecture Overview

```text
Clients (curl / scripts / Dashboard)
        │
        ▼  REST API (:8080)
┌──────────────────────┐        ┌──────────────────────┐
│      controller      │───────▶│   postgres (:5432)   │
│  (Spring Boot 3.5.7) │        │  Named vol: pg-data  │
└──────────┬───────────┘        └──────────────────────┘
           │  Internal HTTP (/internal/**)
     ┌─────┴──────┬────────────┬────────────┐
     ▼            ▼            ▼            ▼
┌─────────┐  ┌─────────┐  ┌─────────┐  ┌─────────┐
│ worker1 │  │ worker2 │  │ worker3 │  │ worker4 │
│  :8081  │  │  :8081  │  │  :8081  │  │  :8081  │
└────┬────┘  └────┬────┘  └────┬────┘  └────┬────┘
  vol:w1       vol:w2       vol:w3       vol:w4
```

### Modules

| Module | Description |
|--------|-------------|
| `common` | Shared DTOs (`ErrorResponse`), error codes (`ErrorCode`), base exception hierarchy (`ApiException`, `AbstractApiExceptionHandler`), and internal HTTP header constants (`InternalHeaders`). |
| `controller` | Metadata store (PostgreSQL + Flyway), cluster coordination, chunk placement, replication, and client-facing REST APIs (`:8080`). |
| `worker` | Independent storage node persisting chunk files on its dedicated local filesystem volume (`:8081`), reporting storage health via `/actuator/health`. |

---

## Prerequisites

- **Java 21** (OpenJDK 21+)
- **Maven 3.8+**
- **Docker** & **Docker Compose** (`docker-compose`)

---

## Building and Running

### 1. Build and run unit tests locally

```bash
mvn clean test
```

To run the full verification lifecycle (including Testcontainers integration tests when Docker is running):

```bash
mvn clean verify
```

### 2. Start the full cluster with Docker Compose

Optional: copy `.env.example` to `.env` if you want to customize ports, credentials, or timeouts.

```bash
cp .env.example .env
docker-compose up -d --build
```

Check container status and health:

```bash
docker-compose ps
```

Verify the Controller health endpoint:

```bash
curl -s http://localhost:8080/actuator/health | jq .
```

Verify persistent volumes created for PostgreSQL and each of the 4 workers:

```bash
docker volume ls | grep -E "postgres-data|worker[1-4]-data"
```

Stop the cluster (preserving persistent volumes):

```bash
docker-compose down
```

---

## Implemented Endpoints (Phases 2–6)

### Client Object API (Controller `:8080`)
- `POST /api/v1/objects` (`multipart/form-data`: required `file`, optional JSON string `metadata`) — Splits the uploaded object into 1 MiB chunks, computes SHA-256 checksums, places 2 replicas of each chunk in parallel on distinct `UP` workers, and returns `201 Created` with `ObjectInfo`.
- `GET /api/v1/objects/{objectId}` — Streams the complete reconstructed object in chunk order with its original `Content-Type`, `Content-Length`, `Content-Disposition`, `ETag`, and `X-Object-Checksum-SHA256` (`200 OK` / `404 Not Found` / `409 Conflict` / `503 Service Unavailable`). Verifies each chunk's SHA-256 on read and transparently fails over to surviving replicas if a worker is down or a replica is corrupted.
- `GET /api/v1/objects/{objectId}/metadata` — Returns system-maintained metadata, custom key-value metadata, `replicationStatus`, and the ordered chunk + replica placement map without downloading object bytes (`200 OK` / `404 Not Found` / `409 Conflict`).
- `PATCH /api/v1/objects/{objectId}/metadata` (`application/json`) — Adds, updates, or deletes (when value is `null`) custom metadata keys without re-uploading the object (`200 OK` / `400 Bad Request` / `404 Not Found` / `409 Conflict`).
- `DELETE /api/v1/objects/{objectId}` — Deletes the object metadata and removes all stored chunk replicas from `UP` workers (queueing deferred deletions in `pending_deletions` for any `DOWN` workers) (`204 No Content` / `404 Not Found` / `409 Conflict`).
- `GET /api/v1/objects?page=0&size=20&sort=createdAt,desc` — Returns a paginated list of stored (`ACTIVE`) objects (`200 OK` / `400 Bad Request`).

### Cluster & Admin Endpoints (Controller `:8080`)
- `GET /api/v1/cluster/workers` — List all registered workers (`UP` / `DOWN`, capacity, used/free bytes, chunk count, last heartbeat).
- `GET /api/v1/cluster/status` — Cluster-wide summary: workers `UP`/`DOWN`, active objects, total chunks, healthy (`RF >= 2`), under-replicated (`1 <= RF < 2`), unavailable (`RF == 0`), and corrupted replica counts.
- `POST /api/v1/admin/replication/repair` — Trigger immediate controller-mediated replication repair (bypasses `reReplicationGracePeriod`) to copy under-replicated chunks to distinct `UP` workers and overwrite `CORRUPTED` replicas from verified healthy copies.
- `GET /api/v1/admin/events?since={id}&limit=100` — Inspect cluster and storage lifecycle events (`WORKER_REGISTERED`, `WORKER_UP`, `WORKER_DOWN`, `OBJECT_UPLOADED`, `CHUNK_CORRUPTED`, `CHUNK_REPAIRED`, `RF_RESTORED`).

### Internal Worker Endpoints (`X-Internal-Token` required)
- `POST /internal/workers/register` (Controller) — Register a worker node (`201 Created` / `200 OK` / `409 Conflict`).
- `POST /internal/workers/heartbeat` (Controller) — Report worker liveness and storage metrics (`200 OK` / `404 Not Found`).
- `PUT /internal/chunks/{chunkId}[?overwrite=true]` (Worker) — Atomically store a chunk on disk after verifying `X-Chunk-Checksum` (`201 Created` / `200 OK` / `400 Bad Request` / `409 Conflict`).
- `GET /internal/chunks/{chunkId}` (Worker) — Retrieve a chunk after verifying its on-disk SHA-256 (`200 OK` / `404 Not Found` / `409 CHUNK_CORRUPTED`).
- `DELETE /internal/chunks/{chunkId}` (Worker) — Delete a stored chunk (`204 No Content` / `404 Not Found`).
- `GET /internal/workers/{workerId}/chunks[?verify=true]` (Worker & Controller) — List chunks stored by a worker, optionally re-verifying SHA-256 checksums on disk.

---

## Configuration (Environment Variables)

All services are configured via environment variables (with sensible defaults in `docker-compose.yml` and `.env.example`):

| Variable | Service | Default | Description |
|----------|---------|---------|-------------|
| `SERVER_PORT` | controller / worker | `8080` / `8081` | HTTP listen port |
| `DB_URL` | controller | `jdbc:postgresql://postgres:5432/objectstore` | PostgreSQL JDBC URL |
| `DB_USER` / `DB_PASSWORD` | controller | `objectstore` | PostgreSQL credentials |
| `CHUNK_SIZE_BYTES` | controller | `1048576` | Chunk size (1 MiB) |
| `REPLICATION_FACTOR` | controller | `2` | Replicas per chunk |
| `MAX_OBJECT_SIZE` | controller | `1GB` | Maximum upload size |
| `HEARTBEAT_TIMEOUT_MS` | controller | `10000` | Time before marking a silent worker `DOWN` |
| `HEARTBEAT_CHECK_INTERVAL_MS` | controller | `2000` | Liveness monitor period |
| `REREPLICATION_GRACE_PERIOD_MS` | controller | `30000` | Wait time before re-replicating chunks of a `DOWN` worker |
| `REPAIR_INTERVAL_MS` | controller | `10000` | Background replication repair interval |
| `WORKER_REQUEST_TIMEOUT_MS` | controller | `5000` | HTTP timeout for controller-to-worker requests |
| `INTERNAL_API_TOKEN` | controller, workers | `change-me-internal-token` | Shared secret sent in `X-Internal-Token` |
| `WORKER_ID` | worker | `worker1`..`worker4` | Unique worker identifier |
| `WORKER_ADVERTISED_URL` | worker | `http://${WORKER_ID}:8081` | Base URL advertised to the controller |
| `CONTROLLER_URL` | worker | `http://controller:8080` | Controller base URL |
| `STORAGE_DIR` | worker | `/data/chunks` | On-disk chunk storage path (mounted Docker volume) |
| `HEARTBEAT_INTERVAL_MS` | worker | `3000` | Worker heartbeat frequency |
| `SCRUB_INTERVAL_MS` | worker | `300000` | Background integrity verification interval |
| `WORKER_DEBUG_ENDPOINTS` | worker | `false` | Enables opt-in debug endpoints for corruption demos |

