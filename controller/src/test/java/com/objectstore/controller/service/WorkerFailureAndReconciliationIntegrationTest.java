package com.objectstore.controller.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.objectstore.common.dto.ChunkMetadataInfo;
import com.objectstore.common.dto.ChunkReplicaInfo;
import com.objectstore.common.dto.ClusterStatusInfo;
import com.objectstore.common.dto.ObjectInfo;
import com.objectstore.common.dto.ObjectMetadataResponse;
import com.objectstore.common.dto.ReconciliationResultInfo;
import com.objectstore.common.dto.RegisterWorkerRequest;
import com.objectstore.common.dto.RepairResultInfo;
import com.objectstore.common.dto.ReplicaState;
import com.objectstore.common.dto.ReplicationStatus;
import com.objectstore.common.dto.StoredChunkInfo;
import com.objectstore.common.dto.SystemEventInfo;
import com.objectstore.common.dto.WorkerChunksResponse;
import com.objectstore.common.dto.WorkerStatus;
import com.objectstore.common.http.InternalHeaders;
import com.objectstore.common.util.Sha256;
import com.objectstore.controller.domain.ChunkEntity;
import com.objectstore.controller.domain.ChunkReplicaEntity;
import com.objectstore.controller.repository.ChunkReplicaRepository;
import com.objectstore.controller.repository.ChunkRepository;
import com.objectstore.controller.repository.ObjectMetadataRepository;
import com.objectstore.controller.repository.PendingDeletionRepository;
import com.objectstore.controller.repository.StoredObjectRepository;
import com.objectstore.controller.repository.SystemEventRepository;
import com.objectstore.controller.repository.WorkerRepository;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.core.io.ByteArrayResource;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;

import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end integration tests for Phase 7 (Worker Failure Handling & Rejoin Reconciliation, PDF §7 & §10.5):
 * <ul>
 *     <li>Heartbeat timeout marks a worker {@code DOWN}, transitions its replicas to {@code UNAVAILABLE},
 *     records {@code WORKER_DOWN} and {@code UNDER_REPLICATED} events, and updates object/cluster status to {@code DEGRADED}.</li>
 *     <li>Downloading the object while a worker is {@code DOWN} succeeds byte-for-byte from surviving replicas.</li>
 *     <li>Case A (Rejoin within grace period before re-replication): worker restarts and re-registers ->
 *     {@link ReconciliationService} re-adopts valid on-disk replicas ({@code UNAVAILABLE} -> {@code HEALTHY})
 *     without copying data, restoring {@code RF = 2}.</li>
 *     <li>Case B (Rejoin after re-replication + object deletion while DOWN): while {@code worker2} is {@code DOWN},
 *     repair restores {@code RF = 2} across surviving workers {@code worker1}/{@code worker3}/{@code worker4},
 *     and a deleted object queues {@code pending_deletions}. When {@code worker2} restarts, reconciliation
 *     executes pending deletions, garbage-collects orphans, and trims surplus 3rd copies so every active chunk
 *     has <b>exactly 2 {@code HEALTHY} replicas on distinct {@code UP} workers</b>.</li>
 *     <li>{@code GET /internal/workers/{workerId}/chunks} computes the expected-vs-actual inventory diff (plan.md D4).</li>
 * </ul>
 */
@SpringBootTest(
        webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = {
                "spring.datasource.url=jdbc:h2:mem:phase7failure;MODE=PostgreSQL;DATABASE_TO_LOWER=TRUE;DEFAULT_NULL_ORDERING=HIGH;DB_CLOSE_DELAY=-1",
                "spring.datasource.driver-class-name=org.h2.Driver",
                "spring.datasource.username=sa",
                "spring.datasource.password=",
                "spring.jpa.database-platform=org.hibernate.dialect.H2Dialect",
                "objectstore.internal.api-token=test-internal-token",
                "objectstore.cluster.heartbeat-timeout=5s",
                "objectstore.cluster.re-replication-grace-period=30s",
                "controller.scheduling.enabled=false"
        })
class WorkerFailureAndReconciliationIntegrationTest {

    private static final String TOKEN = "test-internal-token";
    private static final int ONE_MIB = 1_048_576;
    private static final ParameterizedTypeReference<List<SystemEventInfo>> EVENT_LIST = new ParameterizedTypeReference<>() {
    };

    @Autowired
    TestRestTemplate rest;

    @Autowired
    WorkerRegistryService workerRegistryService;

    @Autowired
    ReplicationService replicationService;

    @Autowired
    ReconciliationService reconciliationService;

    @Autowired
    StoredObjectRepository objectRepository;

    @Autowired
    ObjectMetadataRepository metadataRepository;

    @Autowired
    ChunkRepository chunkRepository;

    @Autowired
    ChunkReplicaRepository replicaRepository;

    @Autowired
    PendingDeletionRepository pendingDeletionRepository;

    @Autowired
    WorkerRepository workerRepository;

    @Autowired
    SystemEventRepository eventRepository;

    @Autowired
    ObjectMapper objectMapper;

    private final Map<String, HttpServer> workerServers = new ConcurrentHashMap<>();
    private final Map<String, String> workerBaseUrls = new ConcurrentHashMap<>();
    private final Map<String, Map<String, byte[]>> storedByWorker = new ConcurrentHashMap<>();

    @BeforeEach
    void setUp() {
        pendingDeletionRepository.deleteAll();
        replicaRepository.deleteAll();
        chunkRepository.deleteAll();
        metadataRepository.deleteAll();
        objectRepository.deleteAll();
        workerRepository.deleteAll();
        eventRepository.deleteAll();
        storedByWorker.clear();
        workerBaseUrls.clear();
    }

    @AfterEach
    void tearDown() {
        for (HttpServer s : workerServers.values()) {
            s.stop(0);
        }
        workerServers.clear();
    }

    @Test
    void workerDownMarksReplicasUnavailableContinuesReadsAndReadoptsOnFastRejoin() throws Exception {
        startAndRegisterStubWorkers(4);

        byte[] payload = new byte[4 * ONE_MIB];
        new Random(701L).nextBytes(payload);
        ObjectInfo uploaded = upload("fast-rejoin.bin", payload);
        assertThat(uploaded.chunkCount()).isEqualTo(4);

        // Simulate worker2 missing heartbeats while worker1, worker3, worker4 remain fresh
        Instant staleTime = Instant.now().minusSeconds(20);
        workerRepository.findById("worker2").ifPresent(w -> {
            w.setLastHeartbeatAt(staleTime);
            workerRepository.save(w);
        });

        List<String> newlyDown = workerRegistryService.markStaleWorkersDownAt(Instant.now());
        assertThat(newlyDown).containsExactly("worker2");

        // All replicas on worker2 must now be marked UNAVAILABLE
        List<ChunkReplicaEntity> worker2Replicas = replicaRepository.findByWorkerId("worker2");
        assertThat(worker2Replicas).isNotEmpty()
                .allMatch(r -> r.getState() == ReplicaState.UNAVAILABLE);

        // Object metadata reports DEGRADED and shows worker2 replicas as UNAVAILABLE on a DOWN worker
        ObjectMetadataResponse degradedMeta = rest.getForEntity(
                "/api/v1/objects/" + uploaded.objectId() + "/metadata", ObjectMetadataResponse.class).getBody();
        assertThat(degradedMeta.replicationStatus()).isEqualTo(ReplicationStatus.DEGRADED);
        List<ChunkReplicaInfo> metaWorker2Replicas = degradedMeta.chunks().stream()
                .flatMap(c -> c.replicas().stream())
                .filter(r -> "worker2".equals(r.workerId()))
                .toList();
        assertThat(metaWorker2Replicas).isNotEmpty()
                .allMatch(r -> r.state() == ReplicaState.UNAVAILABLE && r.workerStatus() == WorkerStatus.DOWN);

        // Download still succeeds byte-for-byte from surviving replicas on worker1/worker3/worker4
        ResponseEntity<byte[]> downloadResp = rest.getForEntity(
                "/api/v1/objects/" + uploaded.objectId(), byte[].class);
        assertThat(downloadResp.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(Sha256.hash(downloadResp.getBody())).isEqualTo(Sha256.hash(payload));

        // worker2 restarts within grace period (before re-replication) and re-registers
        Map<String, byte[]> worker2Map = storedByWorker.get("worker2");
        long w2Used = worker2Map.values().stream().mapToLong(b -> b.length).sum();
        RegisterWorkerRequest rejoinReq = new RegisterWorkerRequest(
                "worker2", workerBaseUrls.get("worker2"), 10_000_000_000L, w2Used, worker2Map.size());
        ResponseEntity<Map> rejoinResp = rest.exchange(
                "/internal/workers/register",
                HttpMethod.POST,
                new HttpEntity<>(rejoinReq, authJsonHeaders()),
                Map.class);
        assertThat(rejoinResp.getStatusCode()).isEqualTo(HttpStatus.OK);

        // Replicas on worker2 were re-adopted back to HEALTHY without needing to copy any chunks
        assertThat(replicaRepository.findByWorkerId("worker2"))
                .hasSize(worker2Replicas.size())
                .allMatch(r -> r.getState() == ReplicaState.HEALTHY);

        ClusterStatusInfo afterRejoin = rest.getForEntity("/api/v1/cluster/status", ClusterStatusInfo.class).getBody();
        assertThat(afterRejoin.workersUp()).isEqualTo(4);
        assertThat(afterRejoin.healthyChunks()).isEqualTo(4);
        assertThat(afterRejoin.underReplicatedChunks()).isZero();
        assertThat(afterRejoin.totalHealthyReplicas()).isEqualTo(8);
        assertThat(afterRejoin.replicationStatus()).isEqualTo(ReplicationStatus.HEALTHY);
    }

    @Test
    void fullFailureReReplicationPendingDeleteAndRejoinReconciliationTrimsSurplusToExactRfTwo() throws Exception {
        startAndRegisterStubWorkers(4);

        // Upload object A (4 chunks = 4 MiB) and object B (2 chunks = 2 MiB)
        byte[] objectABytes = new byte[4 * ONE_MIB];
        byte[] objectBBytes = new byte[2 * ONE_MIB];
        new Random(702L).nextBytes(objectABytes);
        new Random(703L).nextBytes(objectBBytes);

        ObjectInfo objectA = upload("object-a.bin", objectABytes);
        ObjectInfo objectB = upload("object-b.bin", objectBBytes);

        // 1. Simulate `docker stop worker2` -> heartbeat timeout marks worker2 DOWN
        Instant downTime = Instant.now();
        workerRepository.findById("worker2").ifPresent(w -> {
            w.setLastHeartbeatAt(downTime.minusSeconds(20));
            workerRepository.save(w);
        });
        assertThat(workerRegistryService.markStaleWorkersDownAt(downTime)).containsExactly("worker2");

        // 2. Delete objectB while worker2 is DOWN -> worker2's replicas for objectB are queued in pending_deletions
        ResponseEntity<Void> deleteResp = rest.exchange(
                "/api/v1/objects/" + objectB.objectId(), HttpMethod.DELETE, null, Void.class);
        assertThat(deleteResp.getStatusCode()).isEqualTo(HttpStatus.NO_CONTENT);
        assertThat(pendingDeletionRepository.findByWorkerIdOrderByIdAsc("worker2")).isNotEmpty();

        // 3. Also plant a stray orphan chunk directly on worker2's disk
        storedByWorker.get("worker2").put("00000000-0000-0000-0000-000000000000_00000", "orphan-bytes".getBytes());

        // 4. Advance past the 30s grace period -> Repair Engine re-replicates objectA's under-replicated chunks
        // onto surviving UP workers (worker1, worker3, worker4)
        RepairResultInfo repair = replicationService.repairUnderReplicatedChunksAt(downTime.plusSeconds(45), false);
        assertThat(repair.repairedChunks()).isGreaterThan(0);
        assertThat(repair.unrepairableChunks()).isZero();

        // While worker2 is still DOWN, objectA already has 2 HEALTHY replicas per chunk on worker1/3/4,
        // PLUS the dormant UNAVAILABLE replica on worker2 (total 3 replica rows for those chunks)
        ClusterStatusInfo statusWhileDown = rest.getForEntity("/api/v1/cluster/status", ClusterStatusInfo.class).getBody();
        assertThat(statusWhileDown.workersUp()).isEqualTo(3);
        assertThat(statusWhileDown.workersDown()).isEqualTo(1);
        assertThat(statusWhileDown.healthyChunks()).isEqualTo(4);
        assertThat(statusWhileDown.underReplicatedChunks()).isZero();
        assertThat(statusWhileDown.replicationStatus()).isEqualTo(ReplicationStatus.HEALTHY);

        // 5. Simulate `docker start worker2` -> worker2 re-registers with controller
        // Reconciliation automatically runs:
        //   - executes pending_deletions for objectB on worker2
        //   - garbage-collects the stray orphan chunk on worker2
        //   - re-adopts worker2's replicas for objectA and trims surplus 3rd copies so every chunk of objectA
        //     settles to EXACTLY 2 HEALTHY replicas on distinct UP workers!
        Map<String, byte[]> worker2Map = storedByWorker.get("worker2");
        RegisterWorkerRequest rejoinReq = new RegisterWorkerRequest(
                "worker2",
                workerBaseUrls.get("worker2"),
                10_000_000_000L,
                worker2Map.values().stream().mapToLong(b -> b.length).sum(),
                worker2Map.size());
        ResponseEntity<Map> rejoinResp = rest.exchange(
                "/internal/workers/register",
                HttpMethod.POST,
                new HttpEntity<>(rejoinReq, authJsonHeaders()),
                Map.class);
        assertThat(rejoinResp.getStatusCode()).isEqualTo(HttpStatus.OK);

        // Pending deletions queue for worker2 is now completely drained
        assertThat(pendingDeletionRepository.findByWorkerIdOrderByIdAsc("worker2")).isEmpty();
        // Stray orphan chunk was removed from worker2's disk
        assertThat(storedByWorker.get("worker2")).doesNotContainKey("00000000-0000-0000-0000-000000000000_00000");

        // Every chunk of objectA has EXACTLY 2 replicas in the DB and on disk, both HEALTHY on distinct UP workers
        List<ChunkEntity> objectAChunks = chunkRepository.findByObjectIdOrderByChunkIndexAsc(objectA.objectId());
        assertThat(objectAChunks).hasSize(4);
        for (ChunkEntity chunk : objectAChunks) {
            List<ChunkReplicaEntity> replicas = replicaRepository.findByChunkIdOrderByWorkerIdAsc(chunk.getId());
            assertThat(replicas).hasSize(2);
            assertThat(replicas.get(0).getWorkerId()).isNotEqualTo(replicas.get(1).getWorkerId());
            assertThat(replicas).allMatch(r -> r.getState() == ReplicaState.HEALTHY);

            // Count how many workers actually store this chunk on disk -> must also be 2
            long workersHoldingOnDisk = storedByWorker.values().stream()
                    .filter(map -> map.containsKey(chunk.getId()))
                    .count();
            assertThat(workersHoldingOnDisk).isEqualTo(2);
        }

        // Cluster status reports 4 UP workers, 4 healthy chunks, and 8 total healthy replicas (4 chunks * RF 2)
        ClusterStatusInfo finalStatus = rest.getForEntity("/api/v1/cluster/status", ClusterStatusInfo.class).getBody();
        assertThat(finalStatus.workersUp()).isEqualTo(4);
        assertThat(finalStatus.workersDown()).isZero();
        assertThat(finalStatus.activeObjects()).isEqualTo(1);
        assertThat(finalStatus.totalChunks()).isEqualTo(4);
        assertThat(finalStatus.healthyChunks()).isEqualTo(4);
        assertThat(finalStatus.underReplicatedChunks()).isZero();
        assertThat(finalStatus.totalHealthyReplicas()).isEqualTo(8);
        assertThat(finalStatus.replicationStatus()).isEqualTo(ReplicationStatus.HEALTHY);

        // Downloading objectA after full failover + repair + rejoin reconciliation is byte-for-byte identical
        ResponseEntity<byte[]> downloadA = rest.getForEntity(
                "/api/v1/objects/" + objectA.objectId(), byte[].class);
        assertThat(downloadA.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(Sha256.hash(downloadA.getBody())).isEqualTo(Sha256.hash(objectABytes));

        // Verify GET /internal/workers/worker2/chunks returns clean expected-vs-actual diff
        ResponseEntity<WorkerChunksResponse> inventoryResp = rest.exchange(
                "/internal/workers/worker2/chunks?verify=true",
                HttpMethod.GET,
                new HttpEntity<>(authJsonHeaders()),
                WorkerChunksResponse.class);
        assertThat(inventoryResp.getStatusCode()).isEqualTo(HttpStatus.OK);
        WorkerChunksResponse inv = inventoryResp.getBody();
        assertThat(inv.missingChunkIds()).isEmpty();
        assertThat(inv.orphanChunkIds()).isEmpty();
        assertThat(inv.corruptChunkIds()).isEmpty();

        // Verify system_events recorded WORKER_DOWN, UNDER_REPLICATED, RF_RESTORED, WORKER_UP, and WORKER_RECONCILED
        List<SystemEventInfo> events = rest.exchange(
                "/api/v1/admin/events?limit=100", HttpMethod.GET, null, EVENT_LIST).getBody();
        assertThat(events).extracting(SystemEventInfo::type)
                .contains(
                        EventService.WORKER_DOWN,
                        EventService.UNDER_REPLICATED,
                        EventService.RF_RESTORED,
                        EventService.WORKER_UP,
                        EventService.WORKER_RECONCILED);
    }

    @Test
    void workerChunksEndpointReportsMissingOrphanAndCorruptDiffBeforeReconciliationCleansUp() throws Exception {
        startAndRegisterStubWorkers(4);

        byte[] data = new byte[2 * ONE_MIB];
        new Random(704L).nextBytes(data);
        ObjectInfo info = upload("diff-check.bin", data);

        // Find a worker that holds at least one replica
        List<ChunkReplicaEntity> allReplicas = replicaRepository.findAll();
        String targetWorkerId = allReplicas.getFirst().getWorkerId();
        String expectedChunkId = allReplicas.getFirst().getChunkId();

        // Simulate an on-disk discrepancy on targetWorker:
        // 1) Remove expectedChunkId from disk (so it shows as missing)
        // 2) Add an unexpected orphan chunk on disk (so it shows as orphan)
        Map<String, byte[]> disk = storedByWorker.get(targetWorkerId);
        disk.remove(expectedChunkId);
        String orphanId = "99999999-9999-9999-9999-999999999999_00000";
        disk.put(orphanId, "stray-orphan".getBytes());

        ResponseEntity<WorkerChunksResponse> diffResp = rest.exchange(
                "/internal/workers/" + targetWorkerId + "/chunks?verify=true",
                HttpMethod.GET,
                new HttpEntity<>(authJsonHeaders()),
                WorkerChunksResponse.class);
        assertThat(diffResp.getStatusCode()).isEqualTo(HttpStatus.OK);
        WorkerChunksResponse diff = diffResp.getBody();
        assertThat(diff.expectedChunkIds()).contains(expectedChunkId);
        assertThat(diff.missingChunkIds()).contains(expectedChunkId);
        assertThat(diff.orphanChunkIds()).containsExactly(orphanId);

        // Trigger manual reconciliation via POST /api/v1/admin/workers/{workerId}/reconcile
        ResponseEntity<ReconciliationResultInfo> recResp = rest.postForEntity(
                "/api/v1/admin/workers/" + targetWorkerId + "/reconcile",
                null,
                ReconciliationResultInfo.class);
        assertThat(recResp.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(recResp.getBody().orphanChunksDeleted()).isEqualTo(1);
        assertThat(recResp.getBody().missingReplicasRemoved()).isEqualTo(1);

        // After reconciliation + repair, the cluster is back to full RF=2 health and diff is clean
        rest.postForEntity("/api/v1/admin/replication/repair", null, RepairResultInfo.class);
        WorkerChunksResponse afterDiff = rest.exchange(
                "/internal/workers/" + targetWorkerId + "/chunks?verify=true",
                HttpMethod.GET,
                new HttpEntity<>(authJsonHeaders()),
                WorkerChunksResponse.class).getBody();
        assertThat(afterDiff.missingChunkIds()).isEmpty();
        assertThat(afterDiff.orphanChunkIds()).isEmpty();
        assertThat(afterDiff.corruptChunkIds()).isEmpty();
        assertThat(info.objectId()).isNotNull();
    }

    private ObjectInfo upload(String filename, byte[] data) {
        ByteArrayResource fileResource = new ByteArrayResource(data) {
            @Override
            public String getFilename() {
                return filename;
            }
        };
        HttpHeaders fileHeaders = new HttpHeaders();
        fileHeaders.setContentType(MediaType.APPLICATION_OCTET_STREAM);
        MultiValueMap<String, Object> multipart = new LinkedMultiValueMap<>();
        multipart.add("file", new HttpEntity<>(fileResource, fileHeaders));

        HttpHeaders topHeaders = new HttpHeaders();
        topHeaders.setContentType(MediaType.MULTIPART_FORM_DATA);
        ResponseEntity<ObjectInfo> resp = rest.exchange(
                "/api/v1/objects", HttpMethod.POST, new HttpEntity<>(multipart, topHeaders), ObjectInfo.class);
        assertThat(resp.getStatusCode()).isEqualTo(HttpStatus.CREATED);
        return resp.getBody();
    }

    private void startAndRegisterStubWorkers(int count) throws Exception {
        for (int i = 1; i <= count; i++) {
            String workerId = "worker" + i;
            Map<String, byte[]> workerMap = new ConcurrentHashMap<>();
            storedByWorker.put(workerId, workerMap);

            HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
            server.createContext("/internal/chunks/", exchange -> {
                String path = exchange.getRequestURI().getPath();
                String chunkId = path.substring("/internal/chunks/".length());
                if ("PUT".equals(exchange.getRequestMethod())) {
                    byte[] body = exchange.getRequestBody().readAllBytes();
                    String expectedSha = exchange.getRequestHeaders().getFirst(InternalHeaders.CHUNK_CHECKSUM);
                    assertThat(Sha256.hash(body)).isEqualTo(expectedSha);
                    workerMap.put(chunkId, body);
                    StoredChunkInfo dto = new StoredChunkInfo(chunkId, null, null, body.length, expectedSha, Instant.now(), true);
                    byte[] resp = objectMapper.writeValueAsBytes(dto);
                    exchange.getResponseHeaders().add("Content-Type", "application/json");
                    exchange.sendResponseHeaders(201, resp.length);
                    try (OutputStream os = exchange.getResponseBody()) {
                        os.write(resp);
                    }
                } else if ("GET".equals(exchange.getRequestMethod())) {
                    byte[] stored = workerMap.get(chunkId);
                    if (stored == null) {
                        exchange.sendResponseHeaders(404, -1);
                        return;
                    }
                    exchange.getResponseHeaders().add("Content-Type", "application/octet-stream");
                    exchange.sendResponseHeaders(200, stored.length);
                    try (OutputStream os = exchange.getResponseBody()) {
                        os.write(stored);
                    }
                } else if ("DELETE".equals(exchange.getRequestMethod())) {
                    workerMap.remove(chunkId);
                    exchange.sendResponseHeaders(204, -1);
                }
            });
            server.createContext("/internal/workers/" + workerId + "/chunks", exchange -> {
                List<StoredChunkInfo> list = workerMap.entrySet().stream()
                        .sorted(Map.Entry.comparingByKey())
                        .map(e -> new StoredChunkInfo(
                                e.getKey(),
                                null,
                                null,
                                e.getValue().length,
                                Sha256.hash(e.getValue()),
                                Instant.now(),
                                true))
                        .collect(Collectors.toList());
                long usedBytes = workerMap.values().stream().mapToLong(b -> b.length).sum();
                WorkerChunksResponse inv = new WorkerChunksResponse(workerId, list.size(), usedBytes, list);
                byte[] resp = objectMapper.writeValueAsBytes(inv);
                exchange.getResponseHeaders().add("Content-Type", "application/json");
                exchange.sendResponseHeaders(200, resp.length);
                try (OutputStream os = exchange.getResponseBody()) {
                    os.write(resp);
                }
            });
            server.start();
            workerServers.put(workerId, server);
            String baseUrl = "http://127.0.0.1:" + server.getAddress().getPort();
            workerBaseUrls.put(workerId, baseUrl);
            workerRegistryService.register(new RegisterWorkerRequest(workerId, baseUrl, 10_000_000_000L, 0L, 0));
        }
    }

    private static HttpHeaders authJsonHeaders() {
        HttpHeaders h = new HttpHeaders();
        h.setContentType(MediaType.APPLICATION_JSON);
        h.set(InternalHeaders.INTERNAL_TOKEN, TOKEN);
        return h;
    }
}
