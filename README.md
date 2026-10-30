# Mini Distributed Cloud Object Store

A distributed cloud object storage system built with **Java 21**, **Spring Boot 3.5.7**, **PostgreSQL 16**, and **Docker Compose** (without Kubernetes, per coursework specification).

The system consists of **1 Controller** node and **4 independent Storage Worker** nodes, splitting arbitrary binary objects into **1 MiB (1,048,576 byte)** chunks, distributing them across workers with a **replication factor of 2** on distinct workers, verifying SHA-256 data integrity, detecting worker failures, and automatically restoring replication and recovering from data corruption.

---

## Current Implementation Status

- **Phase 0 — Architecture & Plan:** Completed (`plan.md`)
- **Phase 1 — Project Foundation & Docker Environment:** Completed
- **Phase 2 — Controller ↔ Worker Communication:** Pending

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

