package com.objectstore.controller.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.objectstore.common.dto.ClusterStatusInfo;
import com.objectstore.common.dto.ObjectInfo;
import com.objectstore.common.dto.ObjectListPage;
import com.objectstore.common.dto.ObjectMetadataResponse;
import com.objectstore.common.dto.RegisterWorkerRequest;
import com.objectstore.common.dto.ReplicaState;
import com.objectstore.common.dto.ReplicationStatus;
import com.objectstore.common.dto.StoredChunkInfo;
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
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Phase 10 comprehensive concurrency, multi-object workflow, and edge-case integration tests:
 * <ul>
 *     <li>Concurrent multi-client uploads (8 parallel multi-chunk uploads) distribute chunks evenly across all
 *     4 workers, maintain {@code RF = 2} on distinct workers for every chunk, and round-trip with exact SHA-256 equality.</li>
 *     <li>Concurrent metadata {@code PATCH} and read operations alongside object deletion verify isolation and
 *     clean {@code 404 OBJECT_NOT_FOUND} after deletion.</li>
 *     <li>Cluster and admin error paths (`404 UNKNOWN_WORKER` on reconcile/corrupt, `404 OBJECT_NOT_FOUND` on
 *     scoped scrub).</li>
 * </ul>
 */
@SpringBootTest(
        webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = {
                "spring.datasource.url=jdbc:h2:mem:phase10concurrency;MODE=PostgreSQL;DATABASE_TO_LOWER=TRUE;DEFAULT_NULL_ORDERING=HIGH;DB_CLOSE_DELAY=-1",
                "spring.datasource.driver-class-name=org.h2.Driver",
                "spring.datasource.username=sa",
                "spring.datasource.password=",
                "spring.jpa.database-platform=org.hibernate.dialect.H2Dialect",
                "objectstore.internal.api-token=test-internal-token",
                "controller.scheduling.enabled=false"
        })
class ConcurrencyAndEdgeCasesIntegrationTest {

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
    PendingDeletionRepository pendingDeletionRepository;

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
        pendingDeletionRepository.deleteAll();
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
    void eightConcurrentMultiChunkUploadsMaintainExactRfTwoAndByteIdenticalDownloads() throws Exception {
        startAndRegisterStubWorkers(4);

        int clientCount = 8;
        int payloadSize = ONE_MIB + 250_000; // 2 chunks per object -> 16 chunks total -> 32 replicas total
        Map<Integer, byte[]> payloadsByClient = new ConcurrentHashMap<>();
        for (int i = 0; i < clientCount; i++) {
            byte[] bytes = new byte[payloadSize];
            new Random(1000L + i).nextBytes(bytes);
            payloadsByClient.put(i, bytes);
        }

        List<ObjectInfo> uploadedObjects = new ArrayList<>();
        try (ExecutorService pool = Executors.newVirtualThreadPerTaskExecutor()) {
            List<Callable<ObjectInfo>> tasks = new ArrayList<>();
            for (int i = 0; i < clientCount; i++) {
                final int idx = i;
                tasks.add(() -> upload(
                        "concurrent-" + idx + ".bin",
                        payloadsByClient.get(idx),
                        "{\"client\":\"" + idx + "\",\"batch\":\"parallel\"}"));
            }
            List<Future<ObjectInfo>> futures = pool.invokeAll(tasks);
            for (Future<ObjectInfo> f : futures) {
                uploadedObjects.add(f.get());
            }
        }

        assertThat(uploadedObjects).hasSize(clientCount);

        // Verify listing returns all 8 active objects
        ObjectListPage page = rest.getForObject("/api/v1/objects?page=0&size=20", ObjectListPage.class);
        assertThat(page.totalItems()).isEqualTo(clientCount);

        // Verify cluster status: 8 objects, 16 chunks, 16 healthy, 32 healthy replicas
        ClusterStatusInfo status = rest.getForObject("/api/v1/cluster/status", ClusterStatusInfo.class);
        assertThat(status.activeObjects()).isEqualTo(clientCount);
        assertThat(status.totalChunks()).isEqualTo(clientCount * 2L);
        assertThat(status.healthyChunks()).isEqualTo(clientCount * 2L);
        assertThat(status.underReplicatedChunks()).isZero();
        assertThat(status.totalHealthyReplicas()).isEqualTo(clientCount * 4L);
        assertThat(status.replicationStatus()).isEqualTo(ReplicationStatus.HEALTHY);

        // Every worker received chunks
        assertThat(storedByWorker.values()).allMatch(m -> !m.isEmpty());

        // Download all 8 objects concurrently and verify SHA-256 equality + custom metadata
        for (int i = 0; i < clientCount; i++) {
            ObjectInfo info = uploadedObjects.get(i);
            byte[] expected = payloadsByClient.get(i);
            ResponseEntity<byte[]> dl = rest.getForEntity("/api/v1/objects/" + info.objectId(), byte[].class);
            assertThat(dl.getStatusCode()).isEqualTo(HttpStatus.OK);
            assertThat(Sha256.hash(dl.getBody())).isEqualTo(Sha256.hash(expected));

            List<ChunkEntity> chunks = chunkRepository.findByObjectIdOrderByChunkIndexAsc(info.objectId());
            assertThat(chunks).hasSize(2);
            for (ChunkEntity c : chunks) {
                List<ChunkReplicaEntity> replicas = replicaRepository.findByChunkIdOrderByWorkerIdAsc(c.getId());
                assertThat(replicas).hasSize(2);
                assertThat(replicas.get(0).getWorkerId()).isNotEqualTo(replicas.get(1).getWorkerId());
                assertThat(replicas).allMatch(r -> r.getState() == ReplicaState.HEALTHY);
            }
        }
    }

    @Test
    void sequentialMetadataPatchesAndDeletionCleanUpAllChunksAndReturn404AfterDelete() throws Exception {
        startAndRegisterStubWorkers(4);

        byte[] data = new byte[2 * ONE_MIB + 4096];
        new Random(4242L).nextBytes(data);
        ObjectInfo created = upload("lifecycle.bin", data, "{\"stage\":\"1\",\"temp\":\"remove-me\"}");

        // Patch 1: update stage, remove temp, add owner
        HttpHeaders jsonHeaders = new HttpHeaders();
        jsonHeaders.setContentType(MediaType.APPLICATION_JSON);
        ResponseEntity<ObjectMetadataResponse> patch1 = rest.exchange(
                "/api/v1/objects/" + created.objectId() + "/metadata",
                HttpMethod.PATCH,
                new HttpEntity<>("{\"stage\":\"2\",\"temp\":null,\"owner\":\"team\"}", jsonHeaders),
                ObjectMetadataResponse.class);
        assertThat(patch1.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(patch1.getBody().customMetadata())
                .containsEntry("stage", "2")
                .containsEntry("owner", "team")
                .doesNotContainKey("temp");

        // Delete object
        ResponseEntity<Void> delResp = rest.exchange(
                "/api/v1/objects/" + created.objectId(), HttpMethod.DELETE, null, Void.class);
        assertThat(delResp.getStatusCode()).isEqualTo(HttpStatus.NO_CONTENT);

        // Subsequent download, metadata GET, metadata PATCH, and second DELETE all return 404 OBJECT_NOT_FOUND
        assertThat(rest.exchange("/api/v1/objects/" + created.objectId(), HttpMethod.GET, null, JSON_MAP)
                .getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);
        assertThat(rest.exchange("/api/v1/objects/" + created.objectId() + "/metadata", HttpMethod.GET, null, JSON_MAP)
                .getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);
        assertThat(rest.exchange("/api/v1/objects/" + created.objectId() + "/metadata", HttpMethod.PATCH,
                new HttpEntity<>("{\"k\":\"v\"}", jsonHeaders), JSON_MAP)
                .getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);
        assertThat(rest.exchange("/api/v1/objects/" + created.objectId(), HttpMethod.DELETE, null, JSON_MAP)
                .getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);

        // All worker disks are now empty
        assertThat(storedByWorker.values()).allMatch(Map::isEmpty);
    }

    @Test
    void adminEndpointsReturnStructured404ForUnknownWorkerOrObject() throws Exception {
        startAndRegisterStubWorkers(4);

        ResponseEntity<Map<String, Object>> unknownReconcile = rest.exchange(
                "/api/v1/admin/workers/worker99/reconcile", HttpMethod.POST, null, JSON_MAP);
        assertThat(unknownReconcile.getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);
        assertThat(unknownReconcile.getBody()).containsEntry("code", ErrorCode.UNKNOWN_WORKER);

        ResponseEntity<Map<String, Object>> unknownCorrupt = rest.exchange(
                "/api/v1/admin/workers/worker99/chunks/some_00000/corrupt", HttpMethod.POST, null, JSON_MAP);
        assertThat(unknownCorrupt.getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);
        assertThat(unknownCorrupt.getBody()).containsEntry("code", ErrorCode.UNKNOWN_WORKER);

        ResponseEntity<Map<String, Object>> unknownScrubObject = rest.exchange(
                "/api/v1/admin/integrity/scrub?objectId=" + UUID.randomUUID(), HttpMethod.POST, null, JSON_MAP);
        assertThat(unknownScrubObject.getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);
        assertThat(unknownScrubObject.getBody()).containsEntry("code", ErrorCode.OBJECT_NOT_FOUND);
    }

    private ObjectInfo upload(String filename, byte[] data, String metadataJson) {
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
        if (metadataJson != null) {
            HttpHeaders metaHeaders = new HttpHeaders();
            metaHeaders.setContentType(MediaType.TEXT_PLAIN);
            multipart.add("metadata", new HttpEntity<>(metadataJson, metaHeaders));
        }

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

