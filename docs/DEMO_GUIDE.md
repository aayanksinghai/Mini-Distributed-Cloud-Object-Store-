# Live Evaluation & Demonstration Guide (PDF §10)

This guide provides both automated script commands and manual `curl` / `docker` commands for demonstrating each of the six required evaluation scenarios in **Section 10** of `Mini_Distributed_Cloud_Object_Store.pdf`.

---

## 0. Start the Cluster

```bash
# Enable opt-in debug corruption endpoint for Demo §10.6 and start all containers
WORKER_DEBUG_ENDPOINTS=true docker-compose up -d --build

# Verify all 6 containers (postgres, controller, worker1..worker4, dashboard) are Up
docker-compose ps

# Verify all 4 workers registered as UP
curl -s http://localhost:8080/api/v1/cluster/workers | jq .
```

Open **[http://localhost:3000](http://localhost:3000)** in a browser alongside your terminal to observe all 6 demonstrations visually in real time.

---

## 1. Object Storage Demonstration (PDF §10.1)

**Automated:**
```bash
./scripts/demo/01-object-storage.sh
```

**Manual Steps:**
```bash
# 1. Create a ~2.35 MiB binary test file and record its SHA-256
head -c 2465792 /dev/urandom > /tmp/demo1.bin
sha256sum /tmp/demo1.bin

# 2. Upload the object
OBJ_ID=$(curl -s -X POST http://localhost:8080/api/v1/objects \
  -F "file=@/tmp/demo1.bin;filename=demo1.bin" \
  -F 'metadata={"scenario":"10.1"}' | jq -r '.objectId')
echo "Object ID: $OBJ_ID"

# 3. Download the object and compare SHA-256
curl -s http://localhost:8080/api/v1/objects/$OBJ_ID -o /tmp/demo1.downloaded.bin
sha256sum /tmp/demo1.bin /tmp/demo1.downloaded.bin
```

---

## 2. Chunking and Distribution Demonstration (PDF §10.2)

**Automated:**
```bash
./scripts/demo/02-chunking-and-distribution.sh
```

**Manual Steps:**
```bash
# 1. Create a ~5.3 MiB binary file (5,557,452 bytes -> 5 x 1 MiB chunks + 1 x 314,572 B final chunk)
head -c 5557452 /dev/urandom > /tmp/demo2-5mb.bin

# 2. Upload the file
OBJ_ID=$(curl -s -X POST http://localhost:8080/api/v1/objects \
  -F "file=@/tmp/demo2-5mb.bin;filename=demo2-5mb.bin" | jq -r '.objectId')

# 3. Inspect chunk list, sizes, and worker placements
curl -s http://localhost:8080/api/v1/objects/$OBJ_ID/metadata \
  | jq '{chunkSizeBytes, chunkCount, chunks: [.chunks[] | {chunkIndex, chunkId, sizeBytes, workers: [.replicas[].workerId]}]}'

# 4. Inspect on-disk chunk files inside each worker container's persistent volume
for w in worker1 worker2 worker3 worker4; do
  echo "=== $w (/data/chunks) ==="
  docker exec $w find /data/chunks/chunks -name "${OBJ_ID}_*.chunk" -exec ls -lh {} +
done
```

---

## 3. Replication Demonstration (PDF §10.3)

**Automated:**
```bash
./scripts/demo/03-replication.sh
```

**Manual Steps:**
```bash
# 1. Inspect replica pairs for every chunk of the uploaded object
curl -s http://localhost:8080/api/v1/objects/$OBJ_ID/metadata \
  | jq '{replicationFactor, replicationStatus, replicasPerChunk: [.chunks[] | {chunkIndex, replicas: [.replicas[] | {workerId, state}]}]}'

# 2. Check cluster-wide replication status
curl -s http://localhost:8080/api/v1/cluster/status | jq .
```

---

## 4. Metadata Demonstration (PDF §10.4)

**Automated:**
```bash
./scripts/demo/04-metadata.sh
```

**Manual Steps:**
```bash
# 1. Retrieve current metadata
curl -s http://localhost:8080/api/v1/objects/$OBJ_ID/metadata | jq '{updatedAt, metadata}'

# 2. Patch metadata (update existing key, add new key, remove key with null) without re-uploading
curl -s -X PATCH http://localhost:8080/api/v1/objects/$OBJ_ID/metadata \
  -H "Content-Type: application/json" \
  -d '{"owner":"bob","stage":"reviewed","scenario":null}' | jq '{updatedAt, metadata}'
```

---

## 5. Worker Failure & Recovery Demonstration (PDF §10.5)

**Automated:**
```bash
./scripts/demo/05-worker-failure.sh
```

**Manual Steps:**
```bash
# 1. Stop worker2 to simulate node failure
docker stop worker2

# 2. Wait ~12s for heartbeat timeout (10s) and inspect cluster status
sleep 12
curl -s http://localhost:8080/api/v1/cluster/status | jq .

# 3. Verify the object is still downloadable and byte-identical from surviving replicas
curl -s http://localhost:8080/api/v1/objects/$OBJ_ID -o /tmp/demo2-during-down.bin
sha256sum /tmp/demo2-5mb.bin /tmp/demo2-during-down.bin

# 4. Trigger replication repair (or wait for background scheduler after grace period)
curl -s -X POST http://localhost:8080/api/v1/admin/replication/repair | jq .
curl -s http://localhost:8080/api/v1/cluster/status | jq .

# 5. Restart worker2 and verify rejoin reconciliation trims surplus 3rd copies back to RF = 2
docker start worker2
sleep 5
curl -s -X POST http://localhost:8080/api/v1/admin/workers/worker2/reconcile | jq .
curl -s http://localhost:8080/api/v1/objects/$OBJ_ID/metadata \
  | jq '[.chunks[] | {chunkIndex, replicaCount: (.replicas | length), workers: [.replicas[].workerId]}]'
```

---

## 6. Data Integrity Verification & Recovery Demonstration (PDF §10.6)

**Automated:**
```bash
./scripts/demo/06-data-integrity.sh
```

**Manual Steps:**
```bash
# 1. Pick chunk 0 and one of its worker replicas
CHUNK_ID="${OBJ_ID}_00000"
TARGET_WORKER=$(curl -s http://localhost:8080/api/v1/objects/$OBJ_ID/metadata \
  | jq -r '.chunks[0].replicas[0].workerId')
echo "Corrupting $CHUNK_ID on $TARGET_WORKER"

# 2. Corrupt the on-disk chunk file on that worker
./scripts/demo/corrupt-chunk.sh "$TARGET_WORKER" "$CHUNK_ID"

# 3. Download the object -> Controller detects corruption, marks replica CORRUPTED, and serves from healthy replica
curl -s http://localhost:8080/api/v1/objects/$OBJ_ID -o /tmp/demo2-after-corrupt.bin
sha256sum /tmp/demo2-5mb.bin /tmp/demo2-after-corrupt.bin

# 4. Trigger integrity scrub & repair to overwrite the corrupted replica from the healthy copy
curl -s -X POST "http://localhost:8080/api/v1/admin/integrity/scrub?objectId=$OBJ_ID&repair=true" | jq .

# 5. Inspect CHUNK_CORRUPTED and CHUNK_REPAIRED events
curl -s "http://localhost:8080/api/v1/admin/events?limit=20" | jq .
```

