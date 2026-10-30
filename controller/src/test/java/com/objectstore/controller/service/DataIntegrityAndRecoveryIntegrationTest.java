package com.objectstore.controller.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.objectstore.common.dto.ClusterStatusInfo;
import com.objectstore.common.dto.HeartbeatRequest;
import com.objectstore.common.dto.ObjectInfo;
import com.objectstore.common.dto.RegisterWorkerRequest;
import com.objectstore.common.dto.RepairResultInfo;
import com.objectstore.common.dto.ReplicaState;
import com.objectstore.common.dto.ReplicationStatus;
import com.objectstore.common.dto.ScrubResultInfo;
import com.objectstore.common.dto.StoredChunkInfo;
import com.objectstore.common.dto.SystemEventInfo;
import com.objectstore.common.dto.WorkerChunksResponse;
import com.objectstore.common.error.ErrorCode;
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
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end integration tests for Phase 8 (Data Integrity Verification & Recovery, PDF §8 & §10.6):
 * <ul>
 *     <li>Corrupting a chunk on a worker via {@code POST /api/v1/admin/workers/{workerId}/chunks/{chunkId}/corrupt}:
 *     worker returns {@code 409 CHUNK_CORRUPTED} on read, controller marks the bad replica {@code CORRUPTED},
 *     serves the download byte-identically from the surviving replica, and {@code POST /api/v1/admin/integrity/scrub}
 *     (or repair) overwrites the bad replica from the verified copy so subsequent scrubs are 100% clean.</li>
 *     <li>Proactive integrity scrub via {@code POST /api/v1/admin/integrity/scrub} detects silent disk corruption
 *     across workers, marks affected replicas {@code CORRUPTED}, repairs them in-place from valid replicas,
 *     and logs {@code CHUNK_CORRUPTED}, {@code CHUNK_REPAIRED}, and {@code RF_RESTORED} events.</li>
 *     <li>Worker heartbeat carrying {@code corruptChunkIds} transitions the replica to {@code CORRUPTED} in the
 *     controller metadata store so the repair engine automatically heals it.</li>
 *     <li>When <b>all</b> replicas of a chunk are corrupted, download returns {@code 503 CHUNK_UNAVAILABLE}
 *     and never returns corrupted bytes to the client.</li>
 * </ul>
 */
@SpringBootTest(
        webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = {
                "spring.datasource.url=jdbc:h2:mem:phase8integrity;MODE=PostgreSQL;DATABASE_TO_LOWER=TRUE;DEFAULT_NULL_ORDERING=HIGH;DB_CLOSE_DELAY=-1",
                "spring.datasource.driver-class-name=org.h2.Driver",
                "spring.datasource.username=sa",
                "spring.datasource.password=",
                "spring.jpa.database-platform=org.hibernate.dialect.H2Dialect",
                "objectstore.internal.api-token=test-internal-token",
                "controller.scheduling.enabled=false"
        })
class DataIntegrityAndRecoveryIntegrationTest {

    private static final String TOKEN = "test-internal-token";
    private static final int ONE_MIB = 1_048_576;
    private static final ParameterizedTypeReference<List<SystemEventInfo>> EVENT_LIST = new ParameterizedTypeReference<>() {
    };
    private static final ParameterizedTypeReference<Map<String, Object>> JSON_MAP = new ParameterizedTypeReference<>() {
    };

    @Autowired
    TestRestTemplate rest;

    @Autowired
    WorkerRegistryService workerRegistryService;

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
    private final Map<String, Map<String, byte[]>> storedBytesByWorker = new ConcurrentHashMap<>();
    private final Map<String, Map<String, String>> expectedShaByWorker = new ConcurrentHashMap<>();

    @BeforeEach
    void setUp() {
        pendingDeletionRepository.deleteAll();
        replicaRepository.deleteAll();
        chunkRepository.deleteAll();
        metadataRepository.deleteAll();
        objectRepository.deleteAll();
        workerRepository.deleteAll();
        eventRepository.deleteAll();
        storedBytesByWorker.clear();
        expectedShaByWorker.clear();
    }

    @AfterEach
    void tearDown() {
        for (HttpServer s : workerServers.values()) {
            s.stop(0);
        }
        workerServers.clear();
    }

    @Test
    void corruptChunkDetectedOnDownloadServedFromValidReplicaAndRepairedOnDisk() throws Exception {
        startAndRegisterStubWorkers(4);

        byte[] original = new byte[2 * ONE_MIB + 123_456]; // 3 chunks
        new Random(801L).nextBytes(original);
        String originalSha = Sha256.hash(original);

        ObjectInfo uploaded = upload("integrity-demo.bin", original);
        assertThat(uploaded.chunkCount()).isEqualTo(3);

        ChunkEntity firstChunk = chunkRepository.findByObjectIdOrderByChunkIndexAsc(uploaded.objectId()).getFirst();
        List<ChunkReplicaEntity> replicas = replicaRepository.findByChunkIdOrderByWorkerIdAsc(firstChunk.getId());
        assertThat(replicas).hasSize(2);
        String victimWorker = replicas.getFirst().getWorkerId();

        // 1. Corrupt the first chunk on victimWorker via POST /api/v1/admin/workers/{workerId}/chunks/{chunkId}/corrupt
        ResponseEntity<Map<String, Object>> corruptResp = rest.exchange(
                "/api/v1/admin/workers/{workerId}/chunks/{chunkId}/corrupt",
                HttpMethod.POST,
                null,
                JSON_MAP,
                victimWorker,
                firstChunk.getId());
        assertThat(corruptResp.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(corruptResp.getBody()).containsEntry("corrupted", true);

        // 2. Download the object -> victimWorker returns 409 CHUNK_CORRUPTED on read, controller marks its replica
        // CORRUPTED, fails over to the surviving replica, and returns a byte-identical object!
        ResponseEntity<byte[]> downloadResp = rest.getForEntity(
                "/api/v1/objects/" + uploaded.objectId(), byte[].class);
        assertThat(downloadResp.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(Sha256.hash(downloadResp.getBody())).isEqualTo(originalSha);

        ChunkReplicaEntity victimReplicaAfterRead = replicaRepository
                .findByChunkIdAndWorkerId(firstChunk.getId(), victimWorker).orElseThrow();
        assertThat(victimReplicaAfterRead.getState()).isEqualTo(ReplicaState.CORRUPTED);

        // 3. Trigger repair (or scrub) -> overwrites victimWorker's corrupted chunk with verified copy from healthy replica
        ResponseEntity<RepairResultInfo> repairResp = rest.postForEntity(
                "/api/v1/admin/replication/repair", null, RepairResultInfo.class);
        assertThat(repairResp.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(repairResp.getBody().corruptedReplicasRepaired()).isEqualTo(1);

        // Verify victimWorker's on-disk bytes now match the expected chunk SHA-256 and replica is HEALTHY again
        byte[] restoredChunkBytes = storedBytesByWorker.get(victimWorker).get(firstChunk.getId());
        assertThat(Sha256.hash(restoredChunkBytes)).isEqualTo(firstChunk.getChecksumSha256());

        ChunkReplicaEntity victimReplicaAfterRepair = replicaRepository
                .findByChunkIdAndWorkerId(firstChunk.getId(), victimWorker).orElseThrow();
        assertThat(victimReplicaAfterRepair.getState()).isEqualTo(ReplicaState.HEALTHY);

        // 4. Subsequent full scrub reports 0 corrupted replicas
        ResponseEntity<ScrubResultInfo> scrubResp = rest.postForEntity(
                "/api/v1/admin/integrity/scrub", null, ScrubResultInfo.class);
        assertThat(scrubResp.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(scrubResp.getBody().corruptReplicasDetected()).isZero();
        assertThat(scrubResp.getBody().replicasValid()).isEqualTo(6);
    }

    @Test
    void integrityScrubEndpointDetectsSilentBitRotAndAutomaticallyRepairsCorruptedReplica() throws Exception {
        startAndRegisterStubWorkers(4);

        byte[] payload = new byte[2 * ONE_MIB];
        new Random(802L).nextBytes(payload);
        ObjectInfo uploaded = upload("scrub-target.bin", payload);

        ChunkEntity secondChunk = chunkRepository.findByObjectIdOrderByChunkIndexAsc(uploaded.objectId()).get(1);
        ChunkReplicaEntity targetReplica = replicaRepository.findByChunkIdOrderByWorkerIdAsc(secondChunk.getId()).getFirst();
        String targetWorker = targetReplica.getWorkerId();

        // Silently flip bytes in targetWorker's on-disk map (simulating bit-rot before any client read)
        byte[] corrupted = storedBytesByWorker.get(targetWorker).get(secondChunk.getId()).clone();
        corrupted[0] ^= (byte) 0xFF;
        corrupted[1] ^= (byte) 0xFF;
        storedBytesByWorker.get(targetWorker).put(secondChunk.getId(), corrupted);

        // Run POST /api/v1/admin/integrity/scrub?objectId=...&repair=true
        ResponseEntity<ScrubResultInfo> scrubResp = rest.postForEntity(
                "/api/v1/admin/integrity/scrub?objectId=" + uploaded.objectId() + "&repair=true",
                null,
                ScrubResultInfo.class);
        assertThat(scrubResp.getStatusCode()).isEqualTo(HttpStatus.OK);
        ScrubResultInfo scrub = scrubResp.getBody();
        assertThat(scrub.replicasExamined()).isEqualTo(4);
        assertThat(scrub.corruptReplicasDetected()).isEqualTo(1);
        assertThat(scrub.corruptedChunkIds()).containsExactly(secondChunk.getId());
        assertThat(scrub.replicasRepaired()).isGreaterThanOrEqualTo(1);

        // Verify the corrupted chunk on targetWorker was overwritten with the valid copy
        assertThat(Sha256.hash(storedBytesByWorker.get(targetWorker).get(secondChunk.getId())))
                .isEqualTo(secondChunk.getChecksumSha256());

        // Verify system_events recorded CHUNK_CORRUPTED and CHUNK_REPAIRED
        List<SystemEventInfo> events = rest.exchange(
                "/api/v1/admin/events?limit=50", HttpMethod.GET, null, EVENT_LIST).getBody();
        assertThat(events).extracting(SystemEventInfo::type)
                .contains(EventService.CHUNK_CORRUPTED, EventService.CHUNK_REPAIRED);
    }

    @Test
    void heartbeatReportingCorruptChunkMarksReplicaCorruptedAndAllReplicasCorruptReturns503() throws Exception {
        startAndRegisterStubWorkers(4);

        byte[] data = "critical-single-chunk-object".getBytes(StandardCharsets.UTF_8);
        ObjectInfo uploaded = upload("heartbeat-corrupt.bin", data);
        ChunkEntity chunk = chunkRepository.findByObjectIdOrderByChunkIndexAsc(uploaded.objectId()).getFirst();
        List<ChunkReplicaEntity> replicas = replicaRepository.findByChunkIdOrderByWorkerIdAsc(chunk.getId());
        assertThat(replicas).hasSize(2);

        ChunkReplicaEntity r1 = replicas.get(0);
        ChunkReplicaEntity r2 = replicas.get(1);

        // 1. Worker holding r1 reports chunk as corrupted via POST /internal/workers/heartbeat
        storedBytesByWorker.get(r1.getWorkerId()).put(chunk.getId(), "bad-bytes-1".getBytes(StandardCharsets.UTF_8));
        HttpHeaders authHeaders = new HttpHeaders();
        authHeaders.setContentType(MediaType.APPLICATION_JSON);
        authHeaders.set(InternalHeaders.INTERNAL_TOKEN, TOKEN);
        HeartbeatRequest hb = new HeartbeatRequest(r1.getWorkerId(), 100L, 9_000_000L, 1, List.of(chunk.getId()));
        rest.exchange("/internal/workers/heartbeat", HttpMethod.POST, new HttpEntity<>(hb, authHeaders), Map.class);

        // Replica r1 is now marked CORRUPTED in controller metadata
        assertThat(replicaRepository.findById(r1.getId()).orElseThrow().getState())
                .isEqualTo(ReplicaState.CORRUPTED);
        ClusterStatusInfo degraded = rest.getForEntity("/api/v1/cluster/status", ClusterStatusInfo.class).getBody();
        assertThat(degraded.corruptedReplicas()).isEqualTo(1);
        assertThat(degraded.replicationStatus()).isEqualTo(ReplicationStatus.DEGRADED);

        // 2. Now also corrupt the second replica r2 on disk so ALL replicas of the chunk are corrupt
        storedBytesByWorker.get(r2.getWorkerId()).put(chunk.getId(), "bad-bytes-2".getBytes(StandardCharsets.UTF_8));

        // Downloading the object must fail with 503 CHUNK_UNAVAILABLE and never return corrupted bytes
        ResponseEntity<Map<String, Object>> failedDownload = rest.exchange(
                "/api/v1/objects/" + uploaded.objectId(), HttpMethod.GET, null, JSON_MAP);
        assertThat(failedDownload.getStatusCode()).isEqualTo(HttpStatus.SERVICE_UNAVAILABLE);
        assertThat(failedDownload.getBody()).containsEntry("code", ErrorCode.CHUNK_UNAVAILABLE);
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
            Map<String, byte[]> workerBytes = new ConcurrentHashMap<>();
            Map<String, String> workerExpectedSha = new ConcurrentHashMap<>();
            storedBytesByWorker.put(workerId, workerBytes);
            expectedShaByWorker.put(workerId, workerExpectedSha);

            HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
            server.createContext("/internal/chunks/", exchange -> {
                String path = exchange.getRequestURI().getPath();
                String chunkId = path.substring("/internal/chunks/".length());
                if ("PUT".equals(exchange.getRequestMethod())) {
                    byte[] body = exchange.getRequestBody().readAllBytes();
                    String expectedSha = exchange.getRequestHeaders().getFirst(InternalHeaders.CHUNK_CHECKSUM);
                    assertThat(Sha256.hash(body)).isEqualTo(expectedSha);
                    workerBytes.put(chunkId, body);
                    workerExpectedSha.put(chunkId, expectedSha);
                    StoredChunkInfo dto = new StoredChunkInfo(chunkId, null, null, body.length, expectedSha, Instant.now(), true);
                    byte[] resp = objectMapper.writeValueAsBytes(dto);
                    exchange.getResponseHeaders().add("Content-Type", "application/json");
                    exchange.sendResponseHeaders(201, resp.length);
                    try (OutputStream os = exchange.getResponseBody()) {
                        os.write(resp);
                    }
                } else if ("GET".equals(exchange.getRequestMethod())) {
                    byte[] stored = workerBytes.get(chunkId);
                    if (stored == null) {
                        exchange.sendResponseHeaders(404, -1);
                        return;
                    }
                    String expectedSha = workerExpectedSha.get(chunkId);
                    String actualSha = Sha256.hash(stored);
                    if (expectedSha != null && !expectedSha.equalsIgnoreCase(actualSha)) {
                        // Mirror real worker ChunkStore.get: return 409 CHUNK_CORRUPTED and withhold bad bytes
                        byte[] err = """
                                {"status":409,"code":"CHUNK_CORRUPTED","message":"Integrity check failed"}
                                """.getBytes(StandardCharsets.UTF_8);
                        exchange.getResponseHeaders().add("Content-Type", "application/json");
                        exchange.sendResponseHeaders(409, err.length);
                        try (OutputStream os = exchange.getResponseBody()) {
                            os.write(err);
                        }
                        return;
                    }
                    exchange.getResponseHeaders().add("Content-Type", "application/octet-stream");
                    exchange.sendResponseHeaders(200, stored.length);
                    try (OutputStream os = exchange.getResponseBody()) {
                        os.write(stored);
                    }
                } else if ("DELETE".equals(exchange.getRequestMethod())) {
                    workerBytes.remove(chunkId);
                    workerExpectedSha.remove(chunkId);
                    exchange.sendResponseHeaders(204, -1);
                }
            });
            server.createContext("/internal/workers/" + workerId + "/chunks", exchange -> {
                List<StoredChunkInfo> list = workerBytes.entrySet().stream()
                        .sorted(Map.Entry.comparingByKey())
                        .map(e -> {
                            String chunkId = e.getKey();
                            byte[] bytes = e.getValue();
                            String expected = workerExpectedSha.get(chunkId);
                            String actual = Sha256.hash(bytes);
                            boolean valid = expected == null || expected.equalsIgnoreCase(actual);
                            return new StoredChunkInfo(
                                    chunkId,
                                    null,
                                    null,
                                    bytes.length,
                                    expected != null ? expected : actual,
                                    Instant.now(),
                                    valid);
                        })
                        .collect(Collectors.toList());
                long usedBytes = workerBytes.values().stream().mapToLong(b -> b.length).sum();
                WorkerChunksResponse inv = new WorkerChunksResponse(workerId, list.size(), usedBytes, list);
                byte[] resp = objectMapper.writeValueAsBytes(inv);
                exchange.getResponseHeaders().add("Content-Type", "application/json");
                exchange.sendResponseHeaders(200, resp.length);
                try (OutputStream os = exchange.getResponseBody()) {
                    os.write(resp);
                }
            });
            server.createContext("/internal/debug/chunks/", exchange -> {
                String path = exchange.getRequestURI().getPath();
                // /internal/debug/chunks/{chunkId}/corrupt
                String restPath = path.substring("/internal/debug/chunks/".length());
                String chunkId = restPath.substring(0, restPath.length() - "/corrupt".length());
                byte[] current = workerBytes.get(chunkId);
                if (current == null) {
                    exchange.sendResponseHeaders(404, -1);
                    return;
                }
                byte[] flipped = current.clone();
                flipped[0] ^= (byte) 0xFF;
                workerBytes.put(chunkId, flipped);
                byte[] resp = objectMapper.writeValueAsBytes(Map.of(
                        "workerId", workerId,
                        "chunkId", chunkId,
                        "corrupted", true));
                exchange.getResponseHeaders().add("Content-Type", "application/json");
                exchange.sendResponseHeaders(200, resp.length);
                try (OutputStream os = exchange.getResponseBody()) {
                    os.write(resp);
                }
            });
            server.start();
            workerServers.put(workerId, server);
            String baseUrl = "http://127.0.0.1:" + server.getAddress().getPort();
            workerRegistryService.register(new RegisterWorkerRequest(workerId, baseUrl, 10_000_000_000L, 0L, 0));
        }
    }
}

