package com.objectstore.controller.api;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.objectstore.common.dto.ObjectInfo;
import com.objectstore.common.dto.ObjectStatus;
import com.objectstore.common.dto.RegisterWorkerRequest;
import com.objectstore.common.dto.ReplicaState;
import com.objectstore.common.dto.StoredChunkInfo;
import com.objectstore.common.error.ErrorCode;
import com.objectstore.common.http.InternalHeaders;
import com.objectstore.common.util.Sha256;
import com.objectstore.controller.domain.ChunkEntity;
import com.objectstore.controller.domain.ChunkReplicaEntity;
import com.objectstore.controller.domain.ObjectMetadataEntity;
import com.objectstore.controller.domain.StoredObjectEntity;
import com.objectstore.controller.repository.ChunkReplicaRepository;
import com.objectstore.controller.repository.ChunkRepository;
import com.objectstore.controller.repository.ObjectMetadataRepository;
import com.objectstore.controller.repository.StoredObjectRepository;
import com.objectstore.controller.repository.SystemEventRepository;
import com.objectstore.controller.repository.WorkerRepository;
import com.objectstore.controller.service.WorkerRegistryService;
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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Full Spring Boot HTTP + DB integration tests for {@code POST /api/v1/objects}:
 * <ul>
 *     <li>Multi-chunk (~2.5 MiB) binary upload split into 3 chunks (1 MiB, 1 MiB, 0.5 MiB), distributed across 4 workers with RF=2 on distinct workers.</li>
 *     <li>0-byte empty file upload.</li>
 *     <li>Single-worker write failure mid-upload automatically failing over to a healthy replacement worker.</li>
 *     <li>Validation errors (400 missing file, 400 malformed metadata) and 503 when fewer than 2 workers are UP.</li>
 * </ul>
 */
@SpringBootTest(
        webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = {
                "spring.datasource.url=jdbc:h2:mem:phase3upload;MODE=PostgreSQL;DATABASE_TO_LOWER=TRUE;DEFAULT_NULL_ORDERING=HIGH;DB_CLOSE_DELAY=-1",
                "spring.datasource.driver-class-name=org.h2.Driver",
                "spring.datasource.username=sa",
                "spring.datasource.password=",
                "spring.jpa.database-platform=org.hibernate.dialect.H2Dialect",
                "objectstore.internal.api-token=test-internal-token",
                "controller.scheduling.enabled=false"
        })
class ObjectUploadIntegrationTest {

    private static final int ONE_MIB = 1_048_576;
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
    void uploadMultiMegabyteBinaryFileSplitsInto1MiBChunksAndDistributesTwoReplicasPerChunk() throws Exception {
        startAndRegisterStubWorkers(4, Set.of());

        int totalSize = (2 * ONE_MIB) + 500_000; // ~2.48 MiB -> 3 chunks (1 MiB, 1 MiB, 500,000 B)
        byte[] payload = new byte[totalSize];
        new Random(12345L).nextBytes(payload);
        String expectedObjectHash = "sha256:" + Sha256.hash(payload);

        ResponseEntity<ObjectInfo> resp = postMultipartUpload(
                "dataset.bin", MediaType.APPLICATION_OCTET_STREAM_VALUE, payload,
                "{\"owner\":\"alice\",\"env\":\"prod\"}");

        assertThat(resp.getStatusCode()).isEqualTo(HttpStatus.CREATED);
        assertThat(resp.getHeaders().getLocation()).hasPath("/api/v1/objects/" + resp.getBody().objectId());

        ObjectInfo info = resp.getBody();
        assertThat(info.name()).isEqualTo("dataset.bin");
        assertThat(info.size()).isEqualTo(totalSize);
        assertThat(info.chunkCount()).isEqualTo(3);
        assertThat(info.checksum()).isEqualTo(expectedObjectHash);
        assertThat(info.customMetadata())
                .containsEntry("owner", "alice")
                .containsEntry("env", "prod");

        // Verify database state
        StoredObjectEntity entity = objectRepository.findById(info.objectId()).orElseThrow();
        assertThat(entity.getStatus()).isEqualTo(ObjectStatus.ACTIVE);
        assertThat(entity.getChunkSize()).isEqualTo(ONE_MIB);

        List<ObjectMetadataEntity> metas = metadataRepository.findByIdObjectIdOrderByIdMetaKeyAsc(info.objectId());
        assertThat(metas).hasSize(2);

        List<ChunkEntity> chunks = chunkRepository.findByObjectIdOrderByChunkIndexAsc(info.objectId());
        assertThat(chunks).hasSize(3);
        assertThat(chunks.get(0).getSizeBytes()).isEqualTo(ONE_MIB);
        assertThat(chunks.get(1).getSizeBytes()).isEqualTo(ONE_MIB);
        assertThat(chunks.get(2).getSizeBytes()).isEqualTo(500_000);

        for (int i = 0; i < chunks.size(); i++) {
            ChunkEntity chunk = chunks.get(i);
            assertThat(chunk.getChunkIndex()).isEqualTo(i);
            assertThat(chunk.getId()).isEqualTo(String.format("%s_%05d", info.objectId(), i));

            List<ChunkReplicaEntity> replicas = replicaRepository.findByChunkIdOrderByWorkerIdAsc(chunk.getId());
            assertThat(replicas).hasSize(2);
            assertThat(replicas).allMatch(r -> r.getState() == ReplicaState.HEALTHY);
            // Each chunk's 2 replicas must reside on 2 distinct workers
            assertThat(replicas.get(0).getWorkerId()).isNotEqualTo(replicas.get(1).getWorkerId());

            // Both workers actually received the exact chunk bytes
            for (ChunkReplicaEntity r : replicas) {
                byte[] onWorker = storedByWorker.get(r.getWorkerId()).get(chunk.getId());
                assertThat(onWorker).isNotNull();
                assertThat(onWorker).hasSize(chunk.getSizeBytes());
                assertThat(Sha256.hash(onWorker)).isEqualTo(chunk.getChecksumSha256());
            }
        }

        // Verify all 4 workers participated in storing chunks (3 chunks * 2 replicas = 6 total replicas)
        int totalStoredOnStubs = storedByWorker.values().stream().mapToInt(Map::size).sum();
        assertThat(totalStoredOnStubs).isEqualTo(6);
        assertThat(storedByWorker.values()).allMatch(m -> !m.isEmpty());
    }

    @Test
    void uploadEmptyZeroByteFileSucceedsWithZeroChunks() throws Exception {
        startAndRegisterStubWorkers(4, Set.of());

        ResponseEntity<ObjectInfo> resp = postMultipartUpload(
                "empty.txt", MediaType.TEXT_PLAIN_VALUE, new byte[0], null);

        assertThat(resp.getStatusCode()).isEqualTo(HttpStatus.CREATED);
        assertThat(resp.getBody().size()).isZero();
        assertThat(resp.getBody().chunkCount()).isZero();
        assertThat(resp.getBody().checksum()).isEqualTo("sha256:" + Sha256.hash(new byte[0]));
    }

    @Test
    void uploadFailsOverToAnotherWorkerWhenOneTargetRejectsWrite() throws Exception {
        // worker2 will return HTTP 500 on PUT, while worker1, worker3, worker4 succeed
        startAndRegisterStubWorkers(4, Set.of("worker2"));

        byte[] payload = "failover-test-payload".getBytes(StandardCharsets.UTF_8);
        ResponseEntity<ObjectInfo> resp = postMultipartUpload(
                "resilient.txt", MediaType.TEXT_PLAIN_VALUE, payload, null);

        assertThat(resp.getStatusCode()).isEqualTo(HttpStatus.CREATED);
        List<ChunkEntity> chunks = chunkRepository.findByObjectIdOrderByChunkIndexAsc(resp.getBody().objectId());
        assertThat(chunks).hasSize(1);

        List<ChunkReplicaEntity> replicas = replicaRepository.findByChunkIdOrderByWorkerIdAsc(chunks.getFirst().getId());
        assertThat(replicas).hasSize(2);
        Set<String> replicaWorkers = replicas.stream().map(ChunkReplicaEntity::getWorkerId).collect(Collectors.toSet());
        assertThat(replicaWorkers).doesNotContain("worker2");
    }

    @Test
    void uploadReturns503WhenFewerThanTwoWorkersAreUp() throws Exception {
        startAndRegisterStubWorkers(1, Set.of());

        ResponseEntity<Map<String, Object>> resp = postMultipartExpectError(
                "file.bin", "data".getBytes(StandardCharsets.UTF_8), null);

        assertThat(resp.getStatusCode()).isEqualTo(HttpStatus.SERVICE_UNAVAILABLE);
        assertThat(resp.getBody()).containsEntry("code", ErrorCode.INSUFFICIENT_STORAGE_NODES);
        assertThat(objectRepository.count()).isZero();
    }

    @Test
    void uploadReturns400OnMissingFileOrInvalidMetadata() throws Exception {
        startAndRegisterStubWorkers(4, Set.of());

        // 1. Missing 'file' part
        MultiValueMap<String, Object> bodyWithoutFile = new LinkedMultiValueMap<>();
        bodyWithoutFile.add("metadata", "{\"k\":\"v\"}");
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.MULTIPART_FORM_DATA);
        ResponseEntity<Map<String, Object>> missingFileResp = rest.exchange(
                "/api/v1/objects", HttpMethod.POST, new HttpEntity<>(bodyWithoutFile, headers), JSON_MAP);
        assertThat(missingFileResp.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
        assertThat(missingFileResp.getBody()).containsEntry("code", ErrorCode.MISSING_FILE);

        // 2. Malformed JSON in 'metadata'
        ResponseEntity<Map<String, Object>> badJsonResp = postMultipartExpectError(
                "a.txt", "hello".getBytes(StandardCharsets.UTF_8), "{not-valid-json");
        assertThat(badJsonResp.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
        assertThat(badJsonResp.getBody()).containsEntry("code", ErrorCode.INVALID_METADATA);

        // 3. Invalid metadata key characters
        ResponseEntity<Map<String, Object>> badKeyResp = postMultipartExpectError(
                "a.txt", "hello".getBytes(StandardCharsets.UTF_8), "{\"bad key!\":\"val\"}");
        assertThat(badKeyResp.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
        assertThat(badKeyResp.getBody()).containsEntry("code", ErrorCode.INVALID_METADATA);
    }

    private ResponseEntity<ObjectInfo> postMultipartUpload(
            String filename, String contentType, byte[] data, String metadataJson) {
        HttpEntity<MultiValueMap<String, Object>> requestEntity = buildMultipartEntity(filename, contentType, data, metadataJson);
        return rest.exchange("/api/v1/objects", HttpMethod.POST, requestEntity, ObjectInfo.class);
    }

    private ResponseEntity<Map<String, Object>> postMultipartExpectError(
            String filename, byte[] data, String metadataJson) {
        HttpEntity<MultiValueMap<String, Object>> requestEntity = buildMultipartEntity(
                filename, MediaType.APPLICATION_OCTET_STREAM_VALUE, data, metadataJson);
        return rest.exchange("/api/v1/objects", HttpMethod.POST, requestEntity, JSON_MAP);
    }

    private static HttpEntity<MultiValueMap<String, Object>> buildMultipartEntity(
            String filename, String contentType, byte[] data, String metadataJson) {
        ByteArrayResource fileResource = new ByteArrayResource(data) {
            @Override
            public String getFilename() {
                return filename;
            }
        };
        HttpHeaders fileHeaders = new HttpHeaders();
        fileHeaders.setContentType(MediaType.parseMediaType(contentType));
        HttpEntity<ByteArrayResource> filePart = new HttpEntity<>(fileResource, fileHeaders);

        MultiValueMap<String, Object> multipart = new LinkedMultiValueMap<>();
        multipart.add("file", filePart);
        if (metadataJson != null) {
            HttpHeaders metaHeaders = new HttpHeaders();
            metaHeaders.setContentType(MediaType.TEXT_PLAIN);
            multipart.add("metadata", new HttpEntity<>(metadataJson, metaHeaders));
        }

        HttpHeaders topHeaders = new HttpHeaders();
        topHeaders.setContentType(MediaType.MULTIPART_FORM_DATA);
        return new HttpEntity<>(multipart, topHeaders);
    }

    private void startAndRegisterStubWorkers(int count, Set<String> failingWorkers) throws Exception {
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
                    if (failingWorkers.contains(workerId)) {
                        byte[] err = "{\"status\":500,\"code\":\"STORAGE_IO_ERROR\"}".getBytes(StandardCharsets.UTF_8);
                        exchange.getResponseHeaders().add("Content-Type", "application/json");
                        exchange.sendResponseHeaders(500, err.length);
                        try (OutputStream os = exchange.getResponseBody()) {
                            os.write(err);
                        }
                        return;
                    }
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

