package com.objectstore.worker.cluster;

import com.objectstore.common.dto.HeartbeatRequest;
import com.objectstore.common.dto.HeartbeatResponse;
import com.objectstore.common.dto.RegisterWorkerRequest;
import com.objectstore.common.dto.RegisterWorkerResponse;
import com.objectstore.worker.config.WorkerProperties;
import com.objectstore.worker.storage.ChunkStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RestClient;
import org.springframework.web.client.RestClientException;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Handles worker registration and periodic heartbeat reporting to the controller.
 * <p>
 * If the controller is unreachable at startup or restarts later (returning {@code 404 UNKNOWN_WORKER}
 * on a heartbeat), the worker automatically re-registers on the next heartbeat tick.
 */
@Component
public class RegistrationClient {

    private static final Logger log = LoggerFactory.getLogger(RegistrationClient.class);

    private final RestClient controllerRestClient;
    private final WorkerProperties props;
    private final ChunkStore chunkStore;
    private final AtomicBoolean registered = new AtomicBoolean(false);

    public RegistrationClient(RestClient controllerRestClient,
                              WorkerProperties props,
                              ChunkStore chunkStore) {
        this.controllerRestClient = controllerRestClient;
        this.props = props;
        this.chunkStore = chunkStore;
    }

    public boolean isRegistered() {
        return registered.get();
    }

    /**
     * Attempts to register this worker with the controller.
     *
     * @return true if registration succeeded, false if the controller could not be reached
     */
    public boolean register() {
        ChunkStore.StorageStats stats = chunkStore.stats();
        RegisterWorkerRequest request = new RegisterWorkerRequest(
                props.id(),
                props.advertisedUrl(),
                Math.max(0L, stats.capacityBytes()),
                Math.max(0L, stats.usedBytes()),
                stats.chunkCount());
        try {
            RegisterWorkerResponse resp = controllerRestClient.post()
                    .uri("/internal/workers/register")
                    .contentType(MediaType.APPLICATION_JSON)
                    .body(request)
                    .retrieve()
                    .body(RegisterWorkerResponse.class);
            registered.set(true);
            log.info("Registered worker '{}' with controller at {} (advertisedUrl={}, chunks={}, usedBytes={}, status={})",
                    props.id(), props.controllerUrl(), props.advertisedUrl(),
                    stats.chunkCount(), stats.usedBytes(), resp != null ? resp.status() : "UP");
            return true;
        } catch (RestClientException e) {
            registered.set(false);
            log.warn("Could not register worker '{}' with controller at {}: {}",
                    props.id(), props.controllerUrl(), e.getMessage());
            return false;
        }
    }

    /**
     * Sends a single heartbeat (or registers first if not yet registered).
     * Automatically re-registers when the controller responds with 404 (e.g. after controller DB reset).
     */
    public boolean sendHeartbeat(List<String> corruptChunkIds) {
        if (!registered.get() && !register()) {
            return false;
        }
        ChunkStore.StorageStats stats = chunkStore.stats();
        HeartbeatRequest request = new HeartbeatRequest(
                props.id(),
                Math.max(0L, stats.usedBytes()),
                Math.max(0L, stats.freeBytes()),
                stats.chunkCount(),
                corruptChunkIds);
        try {
            HeartbeatResponse resp = controllerRestClient.post()
                    .uri("/internal/workers/heartbeat")
                    .contentType(MediaType.APPLICATION_JSON)
                    .body(request)
                    .retrieve()
                    .body(HeartbeatResponse.class);
            log.debug("Heartbeat acknowledged for worker '{}' (status={})",
                    props.id(), resp != null ? resp.status() : "UP");
            return true;
        } catch (HttpClientErrorException.NotFound e) {
            log.info("Controller reported worker '{}' as unknown (404); re-registering...", props.id());
            registered.set(false);
            return register();
        } catch (RestClientException e) {
            log.warn("Heartbeat from worker '{}' to {} failed: {}",
                    props.id(), props.controllerUrl(), e.getMessage());
            return false;
        }
    }
}

