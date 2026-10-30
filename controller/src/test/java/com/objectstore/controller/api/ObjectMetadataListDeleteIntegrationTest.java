package com.objectstore.controller.api;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.objectstore.common.dto.ChunkMetadataInfo;
import com.objectstore.common.dto.ObjectInfo;
import com.objectstore.common.dto.ObjectListPage;
import com.objectstore.common.dto.ObjectMetadataResponse;
import com.objectstore.common.dto.RegisterWorkerRequest;
import com.objectstore.common.dto.ReplicaState;
import com.objectstore.common.dto.ReplicationStatus;
import com.objectstore.common.dto.StoredChunkInfo;
import com.objectstore.common.dto.WorkerStatus;
import com.objectstore.common.error.ErrorCode;
import com.objectstore.common.http.InternalHeaders;
import com.objectstore.common.util.Sha256;
import com.objectstore.controller.domain.PendingDeletionEntity;
import com.objectstore.controller.repository.ChunkReplicaRepository;
import com.objectstore.controller.repository.ChunkRepository;
import com.objectstore.controller.repository.ObjectMetadataRepository;
import com.objectstore.controller.repository.PendingDeletionRepository;
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
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end integration tests for Phase 6 (Metadata GET/PATCH, Object Listing, and Object Deletion):
 * <ul>
 *     <li>{@code GET /api/v1/objects/{id}/metadata} returns full system + custom metadata + ordered chunk/replica map without reading worker chunks.</li>
 *     <li>{@code PATCH /api/v1/objects/{id}/metadata} adds, updates, and removes (via {@code null}) custom metadata keys without re-uploading or touching chunk data, and updates {@code updatedAt}.</li>
 *     <li>{@code GET /api/v1/objects} supports pagination and sorting ({@code createdAt}, {@code name}, {@code size}).</li>
 *     <li>{@code DELETE /api/v1/objects/{id}} removes chunk files from {@code UP} workers, queues {@code pending_deletions} for {@code DOWN} workers, removes DB rows, and returns {@code 204 No Content}.</li>
 * </ul>
 */
@SpringBootTest(
        webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = {
                "spring.datasource.url=jdbc:h2:mem:phase6metadata;MODE=PostgreSQL;DATABASE_TO_LOWER=TRUE;DEFAULT_NULL_ORDERING=HIGH;DB_CLOSE_DELAY=-1",
                "spring.datasource.driver-class-name=org.h2.Driver",
                "spring.datasource.username=sa",
                "spring.datasource.password=",
                "spring.jpa.database-platform=org.hibernate.dialect.H2Dialect",
                "objectstore.internal.api-token=test-internal-token",
                "controller.scheduling.enabled=false"
        })
class ObjectMetadataListDeleteIntegrationTest {

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
    private final AtomicInteger workerPutCalls = new AtomicInteger();
    private final AtomicInteger workerGetCalls = new AtomicInteger();

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
        workerPutCalls.set(0);
        workerGetCalls.set(0);
    }

    @AfterEach
    void tearDown() {
        for (HttpServer s : workerServers) {
            s.stop(0);
        }
        workerServers.clear();
    }

    @Test
    void getAndPatchMetadataWithoutReUploadingObjectData() throws Exception {
        startAndRegisterStubWorkers(4);

        byte[] payload = new byte[ONE_MIB + 250_000]; // 2 chunks
        new Random(101L).nextBytes(payload);
        ObjectInfo uploaded = upload("report.pdf", "application/pdf", payload,
                "{\"owner\":\"alice\",\"department\":\"research\",\"temp\":\"remove-me\"}");

        int putsAfterUpload = workerPutCalls.get();
        int getsAfterUpload = workerGetCalls.get();
        assertThat(putsAfterUpload).isEqualTo(4); // 2 chunks * 2 replicas

        // 1. GET /api/v1/objects/{id}/metadata returns system + custom metadata + chunk/replica placement
        ResponseEntity<ObjectMetadataResponse> getMetaResp = rest.getForEntity(
                "/api/v1/objects/{id}/metadata", ObjectMetadataResponse.class, uploaded.objectId());
        assertThat(getMetaResp.getStatusCode()).isEqualTo(HttpStatus.OK);
        ObjectMetadataResponse meta = getMetaResp.getBody();
        assertThat(meta.objectId()).isEqualTo(uploaded.objectId());
        assertThat(meta.name()).isEqualTo("report.pdf");
        assertThat(meta.size()).isEqualTo(payload.length);
        assertThat(meta.contentType()).isEqualTo("application/pdf");
        assertThat(meta.chunkCount()).isEqualTo(2);
        assertThat(meta.chunkSize()).isEqualTo(ONE_MIB);
        assertThat(meta.replicationFactor()).isEqualTo(2);
        assertThat(meta.replicationStatus()).isEqualTo(ReplicationStatus.HEALTHY);
        assertThat(meta.customMetadata())
                .containsEntry("owner", "alice")
                .containsEntry("department", "research")
                .containsEntry("temp", "remove-me");
        assertThat(meta.chunks()).hasSize(2);
        for (ChunkMetadataInfo c : meta.chunks()) {
            assertThat(c.replicationStatus()).isEqualTo(ReplicationStatus.HEALTHY);
            assertThat(c.replicas()).hasSize(2)
                    .allMatch(r -> r.state() == ReplicaState.HEALTHY && r.workerStatus() == WorkerStatus.UP);
        }

        // Neither GET nor PATCH metadata should touch worker PUT/GET endpoints
        assertThat(workerPutCalls.get()).isEqualTo(putsAfterUpload);
        assertThat(workerGetCalls.get()).isEqualTo(getsAfterUpload);

        // 2. PATCH /api/v1/objects/{id}/metadata: update 'owner', add 'tag', remove 'temp' via null
        HttpHeaders jsonHeaders = new HttpHeaders();
        jsonHeaders.setContentType(MediaType.APPLICATION_JSON);
        String patchJson = "{\"owner\":\"bob\",\"tag\":\"final\",\"temp\":null}";
        ResponseEntity<ObjectMetadataResponse> patchResp = rest.exchange(
                "/api/v1/objects/{id}/metadata",
                HttpMethod.PATCH,
                new HttpEntity<>(patchJson, jsonHeaders),
                ObjectMetadataResponse.class,
                uploaded.objectId());
        assertThat(patchResp.getStatusCode()).isEqualTo(HttpStatus.OK);
        ObjectMetadataResponse updated = patchResp.getBody();
        assertThat(updated.customMetadata())
                .containsEntry("owner", "bob")
                .containsEntry("department", "research")
                .containsEntry("tag", "final")
                .doesNotContainKey("temp");
        assertThat(updated.updatedAt()).isAfterOrEqualTo(meta.updatedAt());

        // Confirm zero worker PUT/GET calls occurred during PATCH
        assertThat(workerPutCalls.get()).isEqualTo(putsAfterUpload);
        assertThat(workerGetCalls.get()).isEqualTo(getsAfterUpload);

        // 3. Invalid PATCH (non-string value, invalid key, or 404 object)
        ResponseEntity<Map<String, Object>> badValResp = rest.exchange(
                "/api/v1/objects/{id}/metadata",
                HttpMethod.PATCH,
                new HttpEntity<>("{\"count\":123}", jsonHeaders),
                JSON_MAP,
                uploaded.objectId());
        assertThat(badValResp.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
        assertThat(badValResp.getBody()).containsEntry("code", ErrorCode.INVALID_METADATA);

        ResponseEntity<Map<String, Object>> notFoundPatch = rest.exchange(
                "/api/v1/objects/{id}/metadata",
                HttpMethod.PATCH,
                new HttpEntity<>("{\"k\":\"v\"}", jsonHeaders),
                JSON_MAP,
                UUID.randomUUID());
        assertThat(notFoundPatch.getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);
        assertThat(notFoundPatch.getBody()).containsEntry("code", ErrorCode.OBJECT_NOT_FOUND);
    }

    @Test
    void listObjectsSupportsPaginationAndSorting() throws Exception {
        startAndRegisterStubWorkers(4);

        upload("b-file.txt", MediaType.TEXT_PLAIN_VALUE, "12345".getBytes(StandardCharsets.UTF_8), "{\"k\":\"1\"}");
        upload("a-file.txt", MediaType.TEXT_PLAIN_VALUE, "12".getBytes(StandardCharsets.UTF_8), "{\"k\":\"2\"}");
        upload("c-file.txt", MediaType.TEXT_PLAIN_VALUE, "123456789".getBytes(StandardCharsets.UTF_8), "{\"k\":\"3\"}");

        // Page 0, size 2 sorted by name ascending
        ResponseEntity<ObjectListPage> page0 = rest.getForEntity(
                "/api/v1/objects?page=0&size=2&sort=name,asc", ObjectListPage.class);
        assertThat(page0.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(page0.getBody().totalItems()).isEqualTo(3);
        assertThat(page0.getBody().totalPages()).isEqualTo(2);
        assertThat(page0.getBody().items()).extracting(ObjectInfo::name)
                .containsExactly("a-file.txt", "b-file.txt");

        // Page 1, size 2 sorted by name ascending
        ResponseEntity<ObjectListPage> page1 = rest.getForEntity(
                "/api/v1/objects?page=1&size=2&sort=name,asc", ObjectListPage.class);
        assertThat(page1.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(page1.getBody().items()).extracting(ObjectInfo::name)
                .containsExactly("c-file.txt");

        // Sort by size descending
        ResponseEntity<ObjectListPage> bySize = rest.getForEntity(
                "/api/v1/objects?page=0&size=10&sort=size,desc", ObjectListPage.class);
        assertThat(bySize.getBody().items()).extracting(ObjectInfo::name)
                .containsExactly("c-file.txt", "b-file.txt", "a-file.txt");

        // Invalid page or sort field -> 400
        ResponseEntity<Map<String, Object>> badSort = rest.exchange(
                "/api/v1/objects?sort=unknownField,asc", HttpMethod.GET, null, JSON_MAP);
        assertThat(badSort.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
        assertThat(badSort.getBody()).containsEntry("code", ErrorCode.VALIDATION_ERROR);
    }

    @Test
    void deleteObjectRemovesStoredDataOnUpWorkersAndQueuesPendingDeletionsForDownWorkers() throws Exception {
        startAndRegisterStubWorkers(4);

        byte[] data = new byte[2 * ONE_MIB]; // 2 chunks -> 4 replicas across the 4 workers
        new Random(555L).nextBytes(data);
        ObjectInfo uploaded = upload("to-delete.bin", MediaType.APPLICATION_OCTET_STREAM_VALUE, data, "{\"env\":\"test\"}");

        // Total stored replicas across the 4 stub workers = 4 (1 per worker)
        assertThat(storedByWorker.values()).allMatch(m -> m.size() == 1);

        // Mark worker2 DOWN before deleting -> replicas on worker1/3/4 are deleted immediately;
        // the replica on worker2 is queued in pending_deletions (plan.md D15).
        workerRepository.findById("worker2").ifPresent(w -> {
            w.setStatus(WorkerStatus.DOWN);
            workerRepository.save(w);
        });

        ResponseEntity<Void> delResp = rest.exchange(
                "/api/v1/objects/{id}", HttpMethod.DELETE, null, Void.class, uploaded.objectId());
        assertThat(delResp.getStatusCode()).isEqualTo(HttpStatus.NO_CONTENT);

        // UP workers (worker1, worker3, worker4) had their chunk files deleted
        assertThat(storedByWorker.get("worker1")).isEmpty();
        assertThat(storedByWorker.get("worker3")).isEmpty();
        assertThat(storedByWorker.get("worker4")).isEmpty();

        // DOWN worker2 has 1 pending deletion queued in the DB
        List<PendingDeletionEntity> pendingForWorker2 = pendingDeletionRepository.findByWorkerIdOrderByIdAsc("worker2");
        assertThat(pendingForWorker2).hasSize(1);

        // Object, chunks, replicas, and custom metadata are gone from the DB
        assertThat(objectRepository.findById(uploaded.objectId())).isEmpty();
        assertThat(chunkRepository.findByObjectIdOrderByChunkIndexAsc(uploaded.objectId())).isEmpty();
        assertThat(metadataRepository.findByIdObjectIdOrderByIdMetaKeyAsc(uploaded.objectId())).isEmpty();

        // Repeating DELETE or GET on the deleted object returns 404 OBJECT_NOT_FOUND
        ResponseEntity<Map<String, Object>> secondDel = rest.exchange(
                "/api/v1/objects/{id}", HttpMethod.DELETE, null, JSON_MAP, uploaded.objectId());
        assertThat(secondDel.getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);
        assertThat(secondDel.getBody()).containsEntry("code", ErrorCode.OBJECT_NOT_FOUND);

        ResponseEntity<Map<String, Object>> getMetaAfterDel = rest.exchange(
                "/api/v1/objects/{id}/metadata", HttpMethod.GET, null, JSON_MAP, uploaded.objectId());
        assertThat(getMetaAfterDel.getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);
    }

    private ObjectInfo upload(String filename, String contentType, byte[] data, String metadataJson) {
        ByteArrayResource fileResource = new ByteArrayResource(data) {
            @Override
            public String getFilename() {
                return filename;
            }
        };
        HttpHeaders fileHeaders = new HttpHeaders();
        fileHeaders.setContentType(MediaType.parseMediaType(contentType));
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
                    workerPutCalls.incrementAndGet();
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
                    workerGetCalls.incrementAndGet();
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

