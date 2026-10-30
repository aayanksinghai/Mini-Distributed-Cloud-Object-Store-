package com.objectstore.worker.cluster;

import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE)
class RegistrationClientTest {

    private static HttpServer stubController;
    private static final AtomicInteger registerCalls = new AtomicInteger();
    private static final AtomicInteger heartbeatCalls = new AtomicInteger();
    private static volatile int nextHeartbeatStatus = 200;

    @TempDir
    static Path tempDir;

    @DynamicPropertySource
    static void props(DynamicPropertyRegistry registry) throws Exception {
        registerCalls.set(0);
        heartbeatCalls.set(0);
        nextHeartbeatStatus = 200;
        stubController = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        stubController.createContext("/internal/workers/register", exchange -> {
            registerCalls.incrementAndGet();
            byte[] resp = """
                    {"workerId":"worker3","status":"UP","heartbeatIntervalMs":3000,"registeredAt":"2026-10-30T18:00:00Z"}
                    """.getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().add("Content-Type", "application/json");
            exchange.sendResponseHeaders(201, resp.length);
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(resp);
            }
        });
        stubController.createContext("/internal/workers/heartbeat", exchange -> {
            heartbeatCalls.incrementAndGet();
            int code = nextHeartbeatStatus;
            byte[] resp = code == 200
                    ? """
                      {"workerId":"worker3","status":"UP","receivedAt":"2026-10-30T18:00:03Z"}
                      """.getBytes(StandardCharsets.UTF_8)
                    : """
                      {"status":404,"code":"UNKNOWN_WORKER","message":"unknown"}
                      """.getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().add("Content-Type", "application/json");
            exchange.sendResponseHeaders(code, resp.length);
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(resp);
            }
        });
        stubController.start();
        String url = "http://127.0.0.1:" + stubController.getAddress().getPort();

        registry.add("worker.id", () -> "worker3");
        registry.add("worker.controller-url", () -> url);
        registry.add("worker.storage-dir", () -> tempDir.resolve("chunks").toString());
        registry.add("worker.internal-api-token", () -> "secret");
        registry.add("worker.scheduling.enabled", () -> "false");
    }

    @Autowired
    RegistrationClient client;

    @AfterEach
    void cleanup() {
        nextHeartbeatStatus = 200;
    }

    @Test
    void registersAndReRegistersOn404FromHeartbeat() {
        assertThat(client.register()).isTrue();
        assertThat(client.isRegistered()).isTrue();
        assertThat(registerCalls.get()).isEqualTo(1);

        assertThat(client.sendHeartbeat(List.of())).isTrue();
        assertThat(heartbeatCalls.get()).isEqualTo(1);

        // Simulate controller restart: heartbeat returns 404 -> client automatically re-registers
        nextHeartbeatStatus = 404;
        assertThat(client.sendHeartbeat(List.of())).isTrue();
        assertThat(registerCalls.get()).isEqualTo(2);
        assertThat(client.isRegistered()).isTrue();
    }
}

