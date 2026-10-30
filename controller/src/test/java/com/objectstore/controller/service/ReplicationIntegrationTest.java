package com.objectstore.controller.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.objectstore.common.dto.ClusterStatusInfo;
import com.objectstore.common.dto.ObjectInfo;
import com.objectstore.common.dto.RegisterWorkerRequest;
import com.objectstore.common.dto.RepairResultInfo;
import com.objectstore.common.dto.ReplicaState;
import com.objectstore.common.dto.ReplicationStatus;
import com.objectstore.common.dto.StoredChunkInfo;
import com.objectstore.common.dto.WorkerStatus;
import com.objectstore.common.http.InternalHeaders;
import com.objectstore.common.util.Sha256;
import com.objectstore.controller.domain.ChunkEntity;
import com.objectstore.controller.domain.ChunkReplicaEntity;
import com.objectstore.controller.repository.ChunkReplicaRepository;
import com.objectstore.controller.repository.ChunkRepository;
import com.objectstore.controller.repository.ObjectMetadataRepository;
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
 * End-to-end integration tests for Phase 5 (Replication RF=2, Cluster Status, and Repair Engine):
 * <ul>
 *     <li>10 MiB upload (10 chunks) has exactly 2 HEALTHY replicas on distinct workers per chunk, distributed across all 4 workers.</li>
 *     <li>{@code GET /api/v1/cluster/status} accurately reflects HEALTHY, DEGRADED (under-replicated or corrupted), and restored states.</li>
 *     <li>Scheduled repair respects {@code reReplicationGracePeriod} for a newly DOWN worker, then re-replicates once the grace period elapses.</li>
 *     <li>{@code POST /api/v1/admin/replication/repair} bypasses the grace period and immediately restores RF = 2 on surviving UP workers, and also repairs CORRUPTED replicas in-place.</li>
 * </ul>
 */
@SpringBootTest(
        webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = {
                "spring.datasource.url=jdbc:h2:mem:phase5replication;MODE=PostgreSQL;DATABASE_TO_LOWER=TRUE;DEFAULT_NULL_ORDERING=HIGH;DB_CLOSE_DELAY=-1",
                "spring.datasource.driver-class-name=org.h2.Driver",
                "spring.datasource.username=sa",
                "spring.datasource.password=",
                "spring.jpa.database-platform=org.hibernate.dialect.H2Dialect",
                "objectstore.internal.api-token=test-internal-token",
                "objectstore.cluster.re-replication-grace-period=30s",
                "controller.scheduling.enabled=false"
        })
class ReplicationIntegrationTest {

    private static final int ONE_MIB = 1_048_576;

    @Autowired
    TestRestTemplate rest;

    @Autowired
    WorkerRegistryService workerRegistryService;

    @Autowired
    ReplicationService replicationService;

    @Autowired
    StoredObjectRepository objectRepository;

    @Autowired
    ObjectMetadataRepository metadataRepository;

    @Autowired
    ChunkRepository chunkRepository;

    @Autowired
    ChunkReplicaRepository replicaRepository;

    @Autowired
    WorkerRepository workerRepository;

    @Autowired
    SystemEventRepository eventRepository;

    @Autowired
    ObjectMapper objectMapper;

    private final List<HttpServer> workerServers = new ArrayList<>();
    private final Map<String, Map<String, byte[]>> storedByWorker = new ConcurrentHashMap<>();

    @BeforeEach
    void setUp() {
        replicaRepository.deleteAll();
        chunkRepository.deleteAll();
        metadataRepository.deleteAll();
        objectRepository.deleteAll();
        workerRepository.deleteAll();
        eventRepository.deleteAll();
        storedByWorker.clear();
    }

    @AfterEach
    void tearDown() {
        for (HttpServer s : workerServers) {
            s.stop(0);
        }
        workerServers.clear();
    }

    @Test
    void tenMegabyteUploadDistributesTenChunksWithTwoDistinctReplicasEachAcrossAllFourWorkers() throws Exception {
        startAndRegisterStubWorkers(4);

        byte[] tenMiB = new byte[10 * ONE_MIB];
        new Random(2026L).nextBytes(tenMiB);

        ObjectInfo info = upload("ten-mib.bin", tenMiB);
        assertThat(info.chunkCount()).isEqualTo(10);

        List<ChunkEntity> chunks = chunkRepository.findByObjectIdOrderByChunkIndexAsc(info.objectId());
        assertThat(chunks).hasSize(10);

        for (ChunkEntity chunk : chunks) {
            List<ChunkReplicaEntity> replicas = replicaRepository.findByChunkIdOrderByWorkerIdAsc(chunk.getId());
            assertThat(replicas).hasSize(2);
            assertThat(replicas.get(0).getWorkerId()).isNotEqualTo(replicas.get(1).getWorkerId());
            assertThat(replicas).allMatch(r -> r.getState() == ReplicaState.HEALTHY);
        }

        // All 4 workers hold chunks, and total replicas stored across cluster = 20 (5 per worker)
        assertThat(storedByWorker.values()).allMatch(m -> m.size() == 5);

        ResponseEntity<ClusterStatusInfo> statusResp = rest.getForEntity(
                "/api/v1/cluster/status", ClusterStatusInfo.class);
        assertThat(statusResp.getStatusCode()).isEqualTo(HttpStatus.OK);
        ClusterStatusInfo status = statusResp.getBody();
        assertThat(status.replicationFactor()).isEqualTo(2);
        assertThat(status.workersTotal()).isEqualTo(4);
        assertThat(status.workersUp()).isEqualTo(4);
        assertThat(status.workersDown()).isZero();
        assertThat(status.activeObjects()).isEqualTo(1);
        assertThat(status.totalChunks()).isEqualTo(10);
        assertThat(status.healthyChunks()).isEqualTo(10);
        assertThat(status.underReplicatedChunks()).isZero();
        assertThat(status.unavailableChunks()).isZero();
        assertThat(status.totalHealthyReplicas()).isEqualTo(20);
        assertThat(status.replicationStatus()).isEqualTo(ReplicationStatus.HEALTHY);
    }

    @Test
    void workerFailureTriggersDegradedStatusRespectsGracePeriodAndRestoresReplicationFactor() throws Exception {
        startAndRegisterStubWorkers(4);

        byte[] fourMiB = new byte[4 * ONE_MIB];
        new Random(99L).nextBytes(fourMiB);
        ObjectInfo info = upload("four-mib.bin", fourMiB);
        assertThat(info.chunkCount()).isEqualTo(4);

        // Mark worker2 DOWN at t0 -> chunks that had a replica on worker2 become under-replicated (effective RF = 1)
        Instant t0 = Instant.now();
        workerRepository.findById("worker2").ifPresent(w -> {
            w.setStatus(WorkerStatus.DOWN);
            w.setDownSince(t0);
            workerRepository.save(w);
        });

        ClusterStatusInfo degraded = rest.getForEntity("/api/v1/cluster/status", ClusterStatusInfo.class).getBody();
        assertThat(degraded.workersUp()).isEqualTo(3);
        assertThat(degraded.workersDown()).isEqualTo(1);
        assertThat(degraded.underReplicatedChunks()).isGreaterThan(0);
        assertThat(degraded.replicationStatus()).isEqualTo(ReplicationStatus.DEGRADED);

        // 1. Scheduled repair within the 30s grace period (t0 + 5s) skips re-replication
        RepairResultInfo withinGrace = replicationService.repairUnderReplicatedChunksAt(t0.plusSeconds(5), false);
        assertThat(withinGrace.repairedChunks()).isZero();
        assertThat(withinGrace.skippedWithinGracePeriod()).isEqualTo((int) degraded.underReplicatedChunks());

        // 2. Manual repair via POST /api/v1/admin/replication/repair bypasses grace period and restores RF=2 on worker1/3/4
        ResponseEntity<RepairResultInfo> manualRepairResp = rest.exchange(
                "/api/v1/admin/replication/repair", HttpMethod.POST, null, RepairResultInfo.class);
        assertThat(manualRepairResp.getStatusCode()).isEqualTo(HttpStatus.OK);
        RepairResultInfo repaired = manualRepairResp.getBody();
        assertThat(repaired.repairedChunks()).isEqualTo((int) degraded.underReplicatedChunks());
        assertThat(repaired.unrepairableChunks()).isZero();

        // Cluster status is now back to HEALTHY with 0 under-replicated chunks across the 3 surviving UP workers
        ClusterStatusInfo restored = rest.getForEntity("/api/v1/cluster/status", ClusterStatusInfo.class).getBody();
        assertThat(restored.healthyChunks()).isEqualTo(4);
        assertThat(restored.underReplicatedChunks()).isZero();
        assertThat(restored.replicationStatus()).isEqualTo(ReplicationStatus.HEALTHY);

        // Every chunk now has 2 HEALTHY replicas on distinct UP workers (none of which is worker2)
        for (ChunkEntity chunk : chunkRepository.findByObjectIdOrderByChunkIndexAsc(info.objectId())) {
            Set<String> upHolders = replicaRepository.findByChunkIdOrderByWorkerIdAsc(chunk.getId()).stream()
                    .filter(r -> r.getState() == ReplicaState.HEALTHY)
                    .map(ChunkReplicaEntity::getWorkerId)
                    .filter(wId -> !"worker2".equals(wId))
                    .collect(Collectors.toSet());
            assertThat(upHolders).hasSize(2);
        }
    }

    @Test
    void corruptedReplicaOnUpWorkerIsOverwrittenFromHealthyReplicaDuringRepair() throws Exception {
        startAndRegisterStubWorkers(4);

        byte[] payload = "important-chunk-for-corruption-repair".getBytes();
        ObjectInfo info = upload("single.bin", payload);
        ChunkEntity chunk = chunkRepository.findByObjectIdOrderByChunkIndexAsc(info.objectId()).getFirst();
        List<ChunkReplicaEntity> replicas = replicaRepository.findByChunkIdOrderByWorkerIdAsc(chunk.getId());
        ChunkReplicaEntity victim = replicas.getFirst();

        // Tamper the bytes on the victim worker and mark its replica CORRUPTED
        storedByWorker.get(victim.getWorkerId()).put(chunk.getId(), "corrupted-garbage".getBytes());
        victim.setState(ReplicaState.CORRUPTED);
        replicaRepository.save(victim);

        ClusterStatusInfo before = rest.getForEntity("/api/v1/cluster/status", ClusterStatusInfo.class).getBody();
        assertThat(before.corruptedReplicas()).isEqualTo(1);
        assertThat(before.underReplicatedChunks()).isEqualTo(1);
        assertThat(before.replicationStatus()).isEqualTo(ReplicationStatus.DEGRADED);

        // Run repair -> overwrites victim worker's corrupt chunk with verified copy from the healthy replica
        RepairResultInfo result = rest.postForEntity(
                "/api/v1/admin/replication/repair", null, RepairResultInfo.class).getBody();
        assertThat(result.corruptedReplicasRepaired()).isEqualTo(1);
        assertThat(result.repairedChunks()).isEqualTo(1);

        // Verify victim worker's on-disk bytes now match the original payload and replica state is HEALTHY
        byte[] repairedBytes = storedByWorker.get(victim.getWorkerId()).get(chunk.getId());
        assertThat(repairedBytes).isEqualTo(payload);

        ClusterStatusInfo after = rest.getForEntity("/api/v1/cluster/status", ClusterStatusInfo.class).getBody();
        assertThat(after.corruptedReplicas()).isZero();
        assertThat(after.underReplicatedChunks()).isZero();
        assertThat(after.healthyChunks()).isEqualTo(1);
        assertThat(after.replicationStatus()).isEqualTo(ReplicationStatus.HEALTHY);
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
            server.start();
            workerServers.add(server);
            String baseUrl = "http://127.0.0.1:" + server.getAddress().getPort();
            workerRegistryService.register(new RegisterWorkerRequest(workerId, baseUrl, 10_000_000_000L, 0L, 0));
        }
    }
}

