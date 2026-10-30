package com.objectstore.controller.api;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.objectstore.common.dto.ObjectInfo;
import com.objectstore.common.dto.ObjectStatus;
import com.objectstore.common.dto.RegisterWorkerRequest;
import com.objectstore.common.dto.ReplicaState;
import com.objectstore.common.dto.StoredChunkInfo;
import com.objectstore.common.dto.WorkerStatus;
import com.objectstore.common.error.ErrorCode;
import com.objectstore.common.http.InternalHeaders;
import com.objectstore.common.util.Sha256;
import com.objectstore.controller.domain.ChunkReplicaEntity;
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
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end integration tests for {@code GET /api/v1/objects/{objectId}} (Phase 4):
 * <ul>
 *     <li>Multi-chunk (~3.2 MiB) binary round-trip: byte-for-byte and SHA-256 equality, Content-Type, Content-Length, ETag, Content-Disposition.</li>
 *     <li>Zero-byte empty file download round-trip.</li>
 *     <li>Transparent read failover when one worker is DOWN or returns corrupted bytes (409 / bad hash), marking the bad replica CORRUPTED while serving the valid replica.</li>
 *     <li>Error handling: 404 OBJECT_NOT_FOUND, 400 invalid UUID, 409 OBJECT_NOT_READY, and 503 CHUNK_UNAVAILABLE when all replicas of a chunk are unreachable.</li>
 * </ul>
 */
@SpringBootTest(
        webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = {
                "spring.datasource.url=jdbc:h2:mem:phase4download;MODE=PostgreSQL;DATABASE_TO_LOWER=TRUE;DEFAULT_NULL_ORDERING=HIGH;DB_CLOSE_DELAY=-1",
                "spring.datasource.driver-class-name=org.h2.Driver",
                "spring.datasource.username=sa",
                "spring.datasource.password=",
                "spring.jpa.database-platform=org.hibernate.dialect.H2Dialect",
                "objectstore.internal.api-token=test-internal-token",
                "controller.scheduling.enabled=false"
        })
class ObjectDownloadIntegrationTest {

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
    private final Set<String> corruptOnReadWorkers = ConcurrentHashMap.newKeySet();

    @BeforeEach
    void setUp() {
        replicaRepository.deleteAll();
        chunkRepository.deleteAll();
        metadataRepository.deleteAll();
        objectRepository.deleteAll();
        workerRepository.deleteAll();
        eventRepository.deleteAll();
        storedByWorker.clear();
        corruptOnReadWorkers.clear();
    }

    @AfterEach
    void tearDown() {
        for (HttpServer s : workerServers) {
            s.stop(0);
        }
        workerServers.clear();
    }

    @Test
    void uploadAndDownloadMultiChunkBinaryObjectPreservesOrderAndExactBytes() throws Exception {
        startAndRegisterStubWorkers(4);

        int size = (3 * ONE_MIB) + 210_987; // 4 chunks (3 x 1 MiB + 210,987 B)
        byte[] original = new byte[size];
        new Random(98765L).nextBytes(original);
        String expectedSha = Sha256.hash(original);

        ObjectInfo uploaded = upload("archive.tar.gz", "application/gzip", original);
        assertThat(uploaded.chunkCount()).isEqualTo(4);

        ResponseEntity<byte[]> downloadResp = rest.getForEntity(
                "/api/v1/objects/{id}", byte[].class, uploaded.objectId());

        assertThat(downloadResp.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(downloadResp.getHeaders().getContentType()).isEqualTo(MediaType.parseMediaType("application/gzip"));
        assertThat(downloadResp.getHeaders().getContentLength()).isEqualTo(size);
        assertThat(downloadResp.getHeaders().getETag()).isEqualTo("\"" + expectedSha + "\"");
        assertThat(downloadResp.getHeaders().getFirst(ObjectController.HEADER_OBJECT_CHECKSUM_SHA256)).isEqualTo(expectedSha);
        assertThat(downloadResp.getHeaders().getFirst(HttpHeaders.CONTENT_DISPOSITION)).contains("archive.tar.gz");

        byte[] downloaded = downloadResp.getBody();
        assertThat(downloaded).isNotNull();
        assertThat(downloaded).hasSize(size);
        assertThat(Sha256.hash(downloaded)).isEqualTo(expectedSha);
        assertThat(downloaded).isEqualTo(original);
    }

    @Test
    void uploadAndDownloadEmptyZeroByteObjectRoundTrip() throws Exception {
        startAndRegisterStubWorkers(4);

        ObjectInfo uploaded = upload("empty.dat", MediaType.APPLICATION_OCTET_STREAM_VALUE, new byte[0]);
        ResponseEntity<byte[]> downloadResp = rest.getForEntity(
                "/api/v1/objects/{id}", byte[].class, uploaded.objectId());

        assertThat(downloadResp.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(downloadResp.getHeaders().getContentLength()).isZero();
        assertThat(downloadResp.getBody()).isNullOrEmpty();
    }

    @Test
    void downloadSucceedsWhenOneWorkerIsStoppedOrReturnsCorruptedChunk() throws Exception {
        startAndRegisterStubWorkers(4);

        int size = (2 * ONE_MIB) + 123_456; // 3 chunks
        byte[] original = new byte[size];
        new Random(777L).nextBytes(original);
        ObjectInfo uploaded = upload("video.mp4", "video/mp4", original);

        // 1. Mark worker2 DOWN in metadata -> download still succeeds from remaining replicas
        workerRepository.findById("worker2").ifPresent(w -> {
            w.setStatus(WorkerStatus.DOWN);
            workerRepository.save(w);
        });

        ResponseEntity<byte[]> respWithWorker2Down = rest.getForEntity(
                "/api/v1/objects/{id}", byte[].class, uploaded.objectId());
        assertThat(respWithWorker2Down.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(respWithWorker2Down.getBody()).isEqualTo(original);

        // Bring worker2 back UP, and make worker1 return tampered bytes on GET -> controller detects checksum mismatch,
        // marks worker1's replicas CORRUPTED, and fails over to the healthy second replica transparently
        workerRepository.findById("worker2").ifPresent(w -> {
            w.setStatus(WorkerStatus.UP);
            workerRepository.save(w);
        });
        corruptOnReadWorkers.add("worker1");

        ResponseEntity<byte[]> respWithWorker1Corrupt = rest.getForEntity(
                "/api/v1/objects/{id}", byte[].class, uploaded.objectId());
        assertThat(respWithWorker1Corrupt.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(respWithWorker1Corrupt.getBody()).isEqualTo(original);

        List<ChunkReplicaEntity> worker1Replicas = replicaRepository.findByWorkerId("worker1");
        assertThat(worker1Replicas).isNotEmpty()
                .allMatch(r -> r.getState() == ReplicaState.CORRUPTED);
    }

    @Test
    void downloadErrorPathsReturn404And400And409And503() throws Exception {
        startAndRegisterStubWorkers(4);

        // 1. 404 on non-existent UUID
        ResponseEntity<Map<String, Object>> notFound = rest.exchange(
                "/api/v1/objects/{id}", HttpMethod.GET, null, JSON_MAP, UUID.randomUUID());
        assertThat(notFound.getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);
        assertThat(notFound.getBody()).containsEntry("code", ErrorCode.OBJECT_NOT_FOUND);

        // 2. 400 on malformed UUID
        ResponseEntity<Map<String, Object>> badUuid = rest.exchange(
                "/api/v1/objects/not-a-uuid", HttpMethod.GET, null, JSON_MAP);
        assertThat(badUuid.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
        assertThat(badUuid.getBody()).containsEntry("code", ErrorCode.VALIDATION_ERROR);

        // 3. 409 OBJECT_NOT_READY when object status is UPLOADING
        UUID uploadingId = UUID.randomUUID();
        Instant now = Instant.now();
        objectRepository.save(new StoredObjectEntity(
                uploadingId, "in-progress.bin", 100L, "application/octet-stream",
                Sha256.hash(new byte[0]), 1, ONE_MIB, ObjectStatus.UPLOADING, now, now));
        ResponseEntity<Map<String, Object>> conflict = rest.exchange(
                "/api/v1/objects/{id}", HttpMethod.GET, null, JSON_MAP, uploadingId);
        assertThat(conflict.getStatusCode()).isEqualTo(HttpStatus.CONFLICT);
        assertThat(conflict.getBody()).containsEntry("code", ErrorCode.OBJECT_NOT_READY);

        // 4. 503 CHUNK_UNAVAILABLE when all workers holding a chunk are DOWN (pre-flight check)
        byte[] payload = "unavailable-when-all-down".getBytes(StandardCharsets.UTF_8);
        ObjectInfo uploaded = upload("doc.pdf", "application/pdf", payload);

        workerRepository.findAll().forEach(w -> {
            w.setStatus(WorkerStatus.DOWN);
            workerRepository.save(w);
        });

        ResponseEntity<Map<String, Object>> unavailable = rest.exchange(
                "/api/v1/objects/{id}", HttpMethod.GET, null, JSON_MAP, uploaded.objectId());
        assertThat(unavailable.getStatusCode()).isEqualTo(HttpStatus.SERVICE_UNAVAILABLE);
        assertThat(unavailable.getBody()).containsEntry("code", ErrorCode.CHUNK_UNAVAILABLE);
    }

    private ObjectInfo upload(String filename, String contentType, byte[] data) {
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
                    byte[] toSend = stored.clone();
                    if (corruptOnReadWorkers.contains(workerId) && toSend.length > 0) {
                        toSend[0] ^= 0x7F; // Intentionally corrupt first byte in transit
                    }
                    exchange.getResponseHeaders().add("Content-Type", "application/octet-stream");
                    exchange.sendResponseHeaders(200, toSend.length);
                    try (OutputStream os = exchange.getResponseBody()) {
                        os.write(toSend);
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
