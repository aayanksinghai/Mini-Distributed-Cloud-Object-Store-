package com.objectstore.controller.api;

import com.objectstore.common.dto.HeartbeatRequest;
import com.objectstore.common.dto.HeartbeatResponse;
import com.objectstore.common.dto.RegisterWorkerRequest;
import com.objectstore.common.dto.RegisterWorkerResponse;
import com.objectstore.common.dto.WorkerChunksResponse;
import com.objectstore.common.dto.WorkerStatus;
import com.objectstore.common.error.ApiException;
import com.objectstore.common.error.ErrorCode;
import com.objectstore.controller.client.WorkerClient;
import com.objectstore.controller.domain.WorkerEntity;
import com.objectstore.controller.service.WorkerRegistryService;
import jakarta.validation.Valid;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

/**
 * Controller-hosted internal worker endpoints (PDF §6):
 * <ul>
 *     <li>{@code POST /internal/workers/register}</li>
 *     <li>{@code POST /internal/workers/heartbeat}</li>
 *     <li>{@code GET /internal/workers/{workerId}/chunks}</li>
 * </ul>
 */
@RestController
public class InternalWorkerController {

    private final WorkerRegistryService workerRegistryService;
    private final WorkerClient workerClient;

    public InternalWorkerController(WorkerRegistryService workerRegistryService,
                                    WorkerClient workerClient) {
        this.workerRegistryService = workerRegistryService;
        this.workerClient = workerClient;
    }

    @PostMapping(
            value = "/internal/workers/register",
            consumes = MediaType.APPLICATION_JSON_VALUE,
            produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<RegisterWorkerResponse> registerWorker(
            @Valid @RequestBody RegisterWorkerRequest request) {
        WorkerRegistryService.RegistrationOutcome outcome = workerRegistryService.register(request);
        HttpStatus status = outcome.created() ? HttpStatus.CREATED : HttpStatus.OK;
        return ResponseEntity.status(status).body(outcome.response());
    }

    @PostMapping(
            value = "/internal/workers/heartbeat",
            consumes = MediaType.APPLICATION_JSON_VALUE,
            produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<HeartbeatResponse> heartbeat(
            @Valid @RequestBody HeartbeatRequest request) {
        return ResponseEntity.ok(workerRegistryService.heartbeat(request));
    }

    @GetMapping(
            value = "/internal/workers/{workerId}/chunks",
            produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<WorkerChunksResponse> getWorkerChunks(
            @PathVariable String workerId,
            @RequestParam(value = "verify", defaultValue = "false") boolean verify) {
        WorkerEntity worker = workerRegistryService.getRequiredWorker(workerId);
        if (worker.getStatus() != WorkerStatus.UP) {
            throw new ApiException(503, ErrorCode.WORKER_UNAVAILABLE,
                    "Worker '" + workerId + "' is currently DOWN",
                    Map.of("workerId", workerId, "status", worker.getStatus()), null);
        }
        WorkerChunksResponse response = workerClient.listWorkerChunks(worker.getId(), worker.getBaseUrl(), verify);
        return ResponseEntity.ok(response);
    }
}

