package com.objectstore.controller.service;

import com.objectstore.common.dto.HeartbeatRequest;
import com.objectstore.common.dto.HeartbeatResponse;
import com.objectstore.common.dto.RegisterWorkerRequest;
import com.objectstore.common.dto.StoredChunkInfo;
import com.objectstore.common.dto.SystemEventInfo;
import com.objectstore.common.dto.WorkerChunksResponse;
import com.objectstore.common.dto.WorkerNodeInfo;
import com.objectstore.common.dto.WorkerStatus;
import com.objectstore.common.error.ErrorCode;
import com.objectstore.common.http.InternalHeaders;
import com.objectstore.common.util.Sha256;
import com.objectstore.controller.client.WorkerClient;
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
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;

import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests the controller's worker registration, heartbeat processing, UP/DOWN timeout detection,
 * event recording, and {@link WorkerClient} HTTP communication against an embedded database and stub worker server.
 */
@SpringBootTest(
        webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = {
                "spring.datasource.url=jdbc:h2:mem:phase2test;MODE=PostgreSQL;DATABASE_TO_LOWER=TRUE;DEFAULT_NULL_ORDERING=HIGH;DB_CLOSE_DELAY=-1",
                "spring.datasource.driver-class-name=org.h2.Driver",
                "spring.datasource.username=sa",
                "spring.datasource.password=",
                "spring.jpa.database-platform=org.hibernate.dialect.H2Dialect",
                "objectstore.internal.api-token=test-internal-token",
                "objectstore.cluster.heartbeat-timeout=5s",
                "controller.scheduling.enabled=false"
        })
class WorkerRegistryAndCommunicationTest {

    private static final String TOKEN = "test-internal-token";
    private static final ParameterizedTypeReference<List<WorkerNodeInfo>> WORKER_LIST = new ParameterizedTypeReference<>() {
    };
    private static final ParameterizedTypeReference<List<SystemEventInfo>> EVENT_LIST = new ParameterizedTypeReference<>() {
    };
    private static final ParameterizedTypeReference<Map<String, Object>> JSON_MAP = new ParameterizedTypeReference<>() {
    };

    @Autowired
    TestRestTemplate rest;

    @Autowired
    WorkerRegistryService registryService;

    @Autowired
    WorkerClient workerClient;

    @Autowired
    WorkerRepository workerRepository;

    @Autowired
    SystemEventRepository systemEventRepository;

    private HttpServer stubWorkerServer;

    @BeforeEach
    void cleanDb() {
        workerRepository.deleteAll();
        systemEventRepository.deleteAll();
    }

    @AfterEach
    void tearDown() {
        if (stubWorkerServer != null) {
            stubWorkerServer.stop(0);
        }
    }

    @Test
    void registerFourWorkersHeartbeatTimeoutAndRejoinLifecycle() {
        // 1. Register 4 workers via HTTP POST /internal/workers/register
        for (int i = 1; i <= 4; i++) {
            String id = "worker" + i;
            RegisterWorkerRequest req = new RegisterWorkerRequest(
                    id, "http://" + id + ":8081", 10_000_000_000L, i * 1024L, i);
            ResponseEntity<Map<String, Object>> resp = rest.exchange(
                    "/internal/workers/register",
                    HttpMethod.POST,
                    new HttpEntity<>(req, authJsonHeaders()),
                    JSON_MAP);
            assertThat(resp.getStatusCode()).isEqualTo(HttpStatus.CREATED);
            assertThat(resp.getBody()).containsEntry("workerId", id).containsEntry("status", "UP");
        }

        // 2. GET /api/v1/cluster/workers returns all 4 workers as UP
        ResponseEntity<List<WorkerNodeInfo>> listResp = rest.exchange(
                "/api/v1/cluster/workers", HttpMethod.GET, null, WORKER_LIST);
        assertThat(listResp.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(listResp.getBody()).hasSize(4)
                .allMatch(w -> w.status() == WorkerStatus.UP);

        // 3. Conflicting registration for live worker2 from a different URL returns 409 WORKER_ID_CONFLICT
        RegisterWorkerRequest conflictReq = new RegisterWorkerRequest(
                "worker2", "http://impostor:8081", 1000L, 0L, 0);
        ResponseEntity<Map<String, Object>> conflictResp = rest.exchange(
                "/internal/workers/register",
                HttpMethod.POST,
                new HttpEntity<>(conflictReq, authJsonHeaders()),
                JSON_MAP);
        assertThat(conflictResp.getStatusCode()).isEqualTo(HttpStatus.CONFLICT);
        assertThat(conflictResp.getBody()).containsEntry("code", ErrorCode.WORKER_ID_CONFLICT);

        // 4. Advance clock past heartbeatTimeout (5s) -> all 4 become DOWN unless they heartbeat
        // First send fresh heartbeats for worker1, worker3, worker4 only (simulating `docker stop worker2`)
        Instant future = Instant.now().plusSeconds(10);
        List<String> newlyDown = registryService.markStaleWorkersDownAt(future);
        assertThat(newlyDown).containsExactly("worker1", "worker2", "worker3", "worker4");

        // Now worker1, worker3, worker4 send heartbeats; worker2 stays silent
        for (String id : List.of("worker1", "worker3", "worker4")) {
            HeartbeatRequest hb = new HeartbeatRequest(id, 4096L, 9_000_000L, 4, List.of());
            ResponseEntity<HeartbeatResponse> hbResp = rest.exchange(
                    "/internal/workers/heartbeat",
                    HttpMethod.POST,
                    new HttpEntity<>(hb, authJsonHeaders()),
                    HeartbeatResponse.class);
            assertThat(hbResp.getStatusCode()).isEqualTo(HttpStatus.OK);
            assertThat(hbResp.getBody().status()).isEqualTo(WorkerStatus.UP);
        }

        Map<String, WorkerStatus> statusById = registryService.listWorkers().stream()
                .collect(java.util.stream.Collectors.toMap(WorkerNodeInfo::workerId, WorkerNodeInfo::status));
        assertThat(statusById)
                .containsEntry("worker1", WorkerStatus.UP)
                .containsEntry("worker2", WorkerStatus.DOWN)
                .containsEntry("worker3", WorkerStatus.UP)
                .containsEntry("worker4", WorkerStatus.UP);

        // 5. Heartbeat from unknown worker returns 404 UNKNOWN_WORKER
        HeartbeatRequest unknownHb = new HeartbeatRequest("worker99", 0L, 1000L, 0, List.of());
        ResponseEntity<Map<String, Object>> unknownResp = rest.exchange(
                "/internal/workers/heartbeat",
                HttpMethod.POST,
                new HttpEntity<>(unknownHb, authJsonHeaders()),
                JSON_MAP);
        assertThat(unknownResp.getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);
        assertThat(unknownResp.getBody()).containsEntry("code", ErrorCode.UNKNOWN_WORKER);

        // 6. Querying chunks of DOWN worker2 via controller returns 503 WORKER_UNAVAILABLE
        ResponseEntity<Map<String, Object>> downChunks = rest.exchange(
                "/internal/workers/worker2/chunks",
                HttpMethod.GET,
                new HttpEntity<>(authJsonHeaders()),
                JSON_MAP);
        assertThat(downChunks.getStatusCode()).isEqualTo(HttpStatus.SERVICE_UNAVAILABLE);
        assertThat(downChunks.getBody()).containsEntry("code", ErrorCode.WORKER_UNAVAILABLE);

        // 7. Restart worker2 (re-registers) -> status returns to UP
        RegisterWorkerRequest rejoinReq = new RegisterWorkerRequest(
                "worker2", "http://worker2:8081", 10_000_000_000L, 2048L, 2);
        ResponseEntity<Map<String, Object>> rejoinResp = rest.exchange(
                "/internal/workers/register",
                HttpMethod.POST,
                new HttpEntity<>(rejoinReq, authJsonHeaders()),
                JSON_MAP);
        assertThat(rejoinResp.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(rejoinResp.getBody()).containsEntry("status", "UP");

        // 8. Verify system_events recorded WORKER_REGISTERED, WORKER_DOWN, and WORKER_UP
        ResponseEntity<List<SystemEventInfo>> eventsResp = rest.exchange(
                "/api/v1/admin/events?limit=50", HttpMethod.GET, null, EVENT_LIST);
        assertThat(eventsResp.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(eventsResp.getBody()).extracting(SystemEventInfo::type)
                .contains(EventService.WORKER_REGISTERED, EventService.WORKER_DOWN, EventService.WORKER_UP);
    }

    @Test
    void controllerQueriesLiveWorkerChunksAndCommunicatesViaWorkerClient() throws Exception {
        byte[] sampleBytes = "hello-from-stub-worker".getBytes(StandardCharsets.UTF_8);
        String sampleSha = Sha256.hash(sampleBytes);

        stubWorkerServer = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        stubWorkerServer.createContext("/internal/workers/worker1/chunks", exchange -> {
            assertThat(exchange.getRequestHeaders().getFirst(InternalHeaders.INTERNAL_TOKEN)).isEqualTo(TOKEN);
            String json = """
                    {"workerId":"worker1","chunkCount":1,"usedBytes":22,
                     "chunks":[{"chunkId":"obj_00000","size":22,"checksum":"%s","valid":true}]}
                    """.formatted(sampleSha);
            byte[] body = json.getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().add("Content-Type", "application/json");
            exchange.sendResponseHeaders(200, body.length);
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(body);
            }
        });
        stubWorkerServer.createContext("/internal/chunks/obj_00000", exchange -> {
            assertThat(exchange.getRequestHeaders().getFirst(InternalHeaders.INTERNAL_TOKEN)).isEqualTo(TOKEN);
            if ("GET".equals(exchange.getRequestMethod())) {
                exchange.getResponseHeaders().add("Content-Type", "application/octet-stream");
                exchange.getResponseHeaders().add(InternalHeaders.CHUNK_CHECKSUM, sampleSha);
                exchange.sendResponseHeaders(200, sampleBytes.length);
                try (OutputStream os = exchange.getResponseBody()) {
                    os.write(sampleBytes);
                }
            } else {
                exchange.sendResponseHeaders(204, -1);
            }
        });
        stubWorkerServer.start();
        String baseUrl = "http://127.0.0.1:" + stubWorkerServer.getAddress().getPort();

        // Register worker1 pointing at the stub HTTP server
        registryService.register(new RegisterWorkerRequest("worker1", baseUrl, 1_000_000L, 22L, 1));

        // Controller's GET /internal/workers/worker1/chunks proxies to worker1 and returns the inventory
        ResponseEntity<WorkerChunksResponse> chunksResp = rest.exchange(
                "/internal/workers/worker1/chunks?verify=true",
                HttpMethod.GET,
                new HttpEntity<>(authJsonHeaders()),
                WorkerChunksResponse.class);
        assertThat(chunksResp.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(chunksResp.getBody().workerId()).isEqualTo("worker1");
        assertThat(chunksResp.getBody().chunkCount()).isEqualTo(1);
        assertThat(chunksResp.getBody().chunks().getFirst().chunkId()).isEqualTo("obj_00000");

        // WorkerClient GET and DELETE
        byte[] fetched = workerClient.getChunk("worker1", baseUrl, "obj_00000");
        assertThat(fetched).isEqualTo(sampleBytes);
        workerClient.deleteChunk("worker1", baseUrl, "obj_00000");
    }

    private static HttpHeaders authJsonHeaders() {
        HttpHeaders h = new HttpHeaders();
        h.setContentType(MediaType.APPLICATION_JSON);
        h.set(InternalHeaders.INTERNAL_TOKEN, TOKEN);
        return h;
    }
}
