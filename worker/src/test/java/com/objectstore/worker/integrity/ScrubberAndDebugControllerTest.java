package com.objectstore.worker.integrity;

import com.objectstore.common.dto.StoredChunkInfo;
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
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests the worker's opt-in debug corruption endpoint (`POST /internal/debug/chunks/{chunkId}/corrupt`),
 * verify-on-read (`409 CHUNK_CORRUPTED`), background {@link Scrubber} scan, and overwrite recovery.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class ScrubberAndDebugControllerTest {

    private static final String TOKEN = "test-internal-secret";
    private static final ParameterizedTypeReference<Map<String, Object>> JSON_MAP = new ParameterizedTypeReference<>() {
    };

    @TempDir
    static Path tempDir;

    @DynamicPropertySource
    static void props(DynamicPropertyRegistry registry) {
        registry.add("worker.id", () -> "worker1");
        registry.add("worker.storage-dir", () -> tempDir.resolve("chunks").toString());
        registry.add("worker.internal-api-token", () -> TOKEN);
        registry.add("worker.debug-endpoints", () -> "true");
        registry.add("worker.scheduling.enabled", () -> "false");
    }

    @Autowired
    TestRestTemplate rest;

    @Autowired
    Scrubber scrubber;

    @Test
    void debugCorruptEndpointFlipsBytesDetectsOnReadAndScrubAndClearsOnOverwrite() {
        byte[] validBytes = "clean-chunk-payload-for-scrubber-test".getBytes(StandardCharsets.UTF_8);
        String validSha = Sha256.hash(validBytes);
        String chunkId = "aabbccdd_00000";

        // 1. Store valid chunk
        HttpHeaders putHeaders = authHeaders();
        putHeaders.setContentType(MediaType.APPLICATION_OCTET_STREAM);
        putHeaders.set(InternalHeaders.CHUNK_CHECKSUM, validSha);
        ResponseEntity<StoredChunkInfo> putResp = rest.exchange(
                "/internal/chunks/{id}", HttpMethod.PUT, new HttpEntity<>(validBytes, putHeaders),
                StoredChunkInfo.class, chunkId);
        assertThat(putResp.getStatusCode()).isEqualTo(HttpStatus.CREATED);

        // Initial scrub finds 0 corrupted chunks
        assertThat(scrubber.scrubAll()).isEmpty();
        assertThat(scrubber.getCorruptChunkIds()).isEmpty();

        // 2. Intentionally corrupt chunk via POST /internal/debug/chunks/{id}/corrupt
        ResponseEntity<Map<String, Object>> corruptResp = rest.exchange(
                "/internal/debug/chunks/{id}/corrupt", HttpMethod.POST, new HttpEntity<>(authHeaders()),
                JSON_MAP, chunkId);
        assertThat(corruptResp.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(corruptResp.getBody()).containsEntry("chunkId", chunkId).containsEntry("corrupted", true);

        // 3. Reading the corrupted chunk returns 409 CHUNK_CORRUPTED and withholds corrupted bytes
        ResponseEntity<Map<String, Object>> getCorrupt = rest.exchange(
                "/internal/chunks/{id}", HttpMethod.GET, new HttpEntity<>(authHeaders()),
                JSON_MAP, chunkId);
        assertThat(getCorrupt.getStatusCode()).isEqualTo(HttpStatus.CONFLICT);
        assertThat(getCorrupt.getBody()).containsEntry("code", ErrorCode.CHUNK_CORRUPTED);

        // 4. Scrubber detects the corrupted chunk and tracks it for heartbeat reporting
        List<String> detected = scrubber.scrubAll();
        assertThat(detected).containsExactly(chunkId);
        assertThat(scrubber.getCorruptChunkIds()).containsExactly(chunkId);

        // 5. Overwriting the corrupted chunk with the verified copy restores integrity and clears Scrubber state
        ResponseEntity<StoredChunkInfo> repairPut = rest.exchange(
                "/internal/chunks/{id}?overwrite=true", HttpMethod.PUT, new HttpEntity<>(validBytes, putHeaders),
                StoredChunkInfo.class, chunkId);
        assertThat(repairPut.getStatusCode()).isEqualTo(HttpStatus.CREATED);
        assertThat(scrubber.getCorruptChunkIds()).isEmpty();
        assertThat(scrubber.scrubAll()).isEmpty();

        // Subsequent GET returns 200 OK with clean bytes
        ResponseEntity<byte[]> getClean = rest.exchange(
                "/internal/chunks/{id}", HttpMethod.GET, new HttpEntity<>(authHeaders()),
                byte[].class, chunkId);
        assertThat(getClean.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(getClean.getBody()).isEqualTo(validBytes);
    }

    private static HttpHeaders authHeaders() {
        HttpHeaders h = new HttpHeaders();
        h.set(InternalHeaders.INTERNAL_TOKEN, TOKEN);
        return h;
    }
}

