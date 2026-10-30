package com.objectstore.worker;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class WorkerApplicationTest {

    @TempDir
    static Path tempDir;

    @DynamicPropertySource
    static void props(DynamicPropertyRegistry registry) {
        registry.add("worker.id", () -> "worker-test");
        registry.add("worker.storage-dir", () -> tempDir.resolve("chunks").toString());
        registry.add("worker.internal-api-token", () -> "test-token");
    }

    private static final ParameterizedTypeReference<Map<String, Object>> JSON_MAP = new ParameterizedTypeReference<>() {
    };

    @Autowired
    TestRestTemplate rest;

    @Test
    void storageDirectoryIsCreatedOnStartup() {
        assertThat(Files.isDirectory(tempDir.resolve("chunks"))).isTrue();
    }

    @Test
    void healthIncludesChunkStorageComponent() {
        ResponseEntity<Map<String, Object>> resp = rest.exchange("/actuator/health", HttpMethod.GET, null, JSON_MAP);
        assertThat(resp.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(resp.getBody()).containsEntry("status", "UP");
        Map<?, ?> components = (Map<?, ?>) resp.getBody().get("components");
        Map<?, ?> storage = (Map<?, ?>) components.get("chunkStorage");
        assertThat(storage.get("status")).isEqualTo("UP");
        assertThat(((Map<?, ?>) storage.get("details")).get("workerId")).isEqualTo("worker-test");
    }

    @Test
    void unknownEndpointReturnsStructuredJson404() {
        ResponseEntity<Map<String, Object>> resp = rest.exchange("/internal/nope", HttpMethod.GET, null, JSON_MAP);
        assertThat(resp.getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);
        assertThat(resp.getBody()).containsEntry("code", "NOT_FOUND").containsEntry("status", 404);
    }
}

