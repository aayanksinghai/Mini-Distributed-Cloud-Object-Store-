package com.objectstore.worker.api;

import com.objectstore.common.dto.StoredChunkInfo;
import com.objectstore.common.dto.WorkerChunksResponse;
import com.objectstore.common.error.ErrorCode;
import com.objectstore.common.http.InternalHeaders;
import com.objectstore.common.util.Sha256;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
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
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class ChunkControllerTest {

    private static final String TOKEN = "test-internal-secret";
    private static final ParameterizedTypeReference<Map<String, Object>> JSON_MAP = new ParameterizedTypeReference<>() {
    };

    @TempDir
    static Path tempDir;

    @DynamicPropertySource
    static void props(DynamicPropertyRegistry registry) {
        registry.add("worker.id", () -> "worker2");
        registry.add("worker.storage-dir", () -> tempDir.resolve("chunks").toString());
        registry.add("worker.internal-api-token", () -> TOKEN);
        registry.add("worker.scheduling.enabled", () -> "false");
    }

    @Autowired
    TestRestTemplate rest;

    @Test
    void fullHttpLifecycleForChunkStorageAndListing() {
        byte[] data = "binary-chunk-content-for-http-test".getBytes(StandardCharsets.UTF_8);
        String sha = Sha256.hash(data);
        String chunkId = "11223344_00000";

        HttpHeaders putHeaders = authHeaders();
        putHeaders.setContentType(MediaType.APPLICATION_OCTET_STREAM);
        putHeaders.set(InternalHeaders.CHUNK_CHECKSUM, sha);
        putHeaders.set(InternalHeaders.OBJECT_ID, "11223344-0000-0000-0000-000000000000");
        putHeaders.set(InternalHeaders.CHUNK_INDEX, "0");

        ResponseEntity<StoredChunkInfo> putResp = rest.exchange(
                "/internal/chunks/{id}", HttpMethod.PUT, new HttpEntity<>(data, putHeaders),
                StoredChunkInfo.class, chunkId);
        assertThat(putResp.getStatusCode()).isEqualTo(HttpStatus.CREATED);
        assertThat(putResp.getBody().chunkId()).isEqualTo(chunkId);
        assertThat(putResp.getBody().size()).isEqualTo(data.length);

        // Idempotent second PUT returns 200 OK
        ResponseEntity<StoredChunkInfo> repeatResp = rest.exchange(
                "/internal/chunks/{id}", HttpMethod.PUT, new HttpEntity<>(data, putHeaders),
                StoredChunkInfo.class, chunkId);
        assertThat(repeatResp.getStatusCode()).isEqualTo(HttpStatus.OK);

        // GET chunk returns exact bytes + checksum header
        ResponseEntity<byte[]> getResp = rest.exchange(
                "/internal/chunks/{id}", HttpMethod.GET, new HttpEntity<>(authHeaders()),
                byte[].class, chunkId);
        assertThat(getResp.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(getResp.getBody()).isEqualTo(data);
        assertThat(getResp.getHeaders().getFirst(InternalHeaders.CHUNK_CHECKSUM)).isEqualTo(sha);

        // List chunks on worker2
        ResponseEntity<WorkerChunksResponse> listResp = rest.exchange(
                "/internal/workers/worker2/chunks?verify=true", HttpMethod.GET,
                new HttpEntity<>(authHeaders()), WorkerChunksResponse.class);
        assertThat(listResp.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(listResp.getBody().workerId()).isEqualTo("worker2");
        assertThat(listResp.getBody().chunkCount()).isEqualTo(1);
        assertThat(listResp.getBody().chunks().getFirst().valid()).isTrue();

        // Requesting another worker's ID from worker2 returns 404 UNKNOWN_WORKER
        ResponseEntity<Map<String, Object>> wrongWorker = rest.exchange(
                "/internal/workers/worker9/chunks", HttpMethod.GET,
                new HttpEntity<>(authHeaders()), JSON_MAP);
        assertThat(wrongWorker.getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);
        assertThat(wrongWorker.getBody()).containsEntry("code", ErrorCode.UNKNOWN_WORKER);

        // DELETE removes chunk
        ResponseEntity<Void> delResp = rest.exchange(
                "/internal/chunks/{id}", HttpMethod.DELETE, new HttpEntity<>(authHeaders()),
                Void.class, chunkId);
        assertThat(delResp.getStatusCode()).isEqualTo(HttpStatus.NO_CONTENT);

        // Subsequent GET returns 404
        ResponseEntity<Map<String, Object>> missingGet = rest.exchange(
                "/internal/chunks/{id}", HttpMethod.GET, new HttpEntity<>(authHeaders()),
                JSON_MAP, chunkId);
        assertThat(missingGet.getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);
        assertThat(missingGet.getBody()).containsEntry("code", ErrorCode.CHUNK_NOT_FOUND);
    }

    private static HttpHeaders authHeaders() {
        HttpHeaders h = new HttpHeaders();
        h.set(InternalHeaders.INTERNAL_TOKEN, TOKEN);
        return h;
    }
}

