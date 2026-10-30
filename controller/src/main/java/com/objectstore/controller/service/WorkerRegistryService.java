package com.objectstore.controller.service;

import com.objectstore.common.dto.EventSeverity;
import com.objectstore.common.dto.HeartbeatRequest;
import com.objectstore.common.dto.HeartbeatResponse;
import com.objectstore.common.dto.RegisterWorkerRequest;
import com.objectstore.common.dto.RegisterWorkerResponse;
import com.objectstore.common.dto.ReplicaState;
import com.objectstore.common.dto.WorkerNodeInfo;
import com.objectstore.common.dto.WorkerStatus;
import com.objectstore.common.error.ApiException;
import com.objectstore.common.error.ErrorCode;
import com.objectstore.controller.config.ControllerProperties;
import com.objectstore.controller.domain.WorkerEntity;
import com.objectstore.controller.repository.ChunkReplicaRepository;
import com.objectstore.controller.repository.WorkerRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Manages worker registration, heartbeat updates and UP/DOWN liveness transitions.
 */
@Service
public class WorkerRegistryService {

    private static final Logger log = LoggerFactory.getLogger(WorkerRegistryService.class);

    /** Default heartbeat interval suggested to workers upon registration (3 seconds). */
    private static final long DEFAULT_HEARTBEAT_INTERVAL_MS = 3000L;

    private final WorkerRepository workerRepository;
    private final ChunkReplicaRepository replicaRepository;
    private final EventService eventService;
    private final ReconciliationService reconciliationService;
    private final ControllerProperties properties;

    public WorkerRegistryService(WorkerRepository workerRepository,
                                 ChunkReplicaRepository replicaRepository,
                                 EventService eventService,
                                 ReconciliationService reconciliationService,
                                 ControllerProperties properties) {
        this.workerRepository = workerRepository;
        this.replicaRepository = replicaRepository;
        this.eventService = eventService;
        this.reconciliationService = reconciliationService;
        this.properties = properties;
    }

    @Transactional
    public RegistrationOutcome register(RegisterWorkerRequest request) {
        String workerId = request.workerId().trim();
        String baseUrl = normalizeBaseUrl(request.baseUrl());
        Instant now = Instant.now();
        long freeBytes = Math.max(0L, request.capacityBytes() - request.usedBytes());

        Optional<WorkerEntity> existingOpt = workerRepository.findById(workerId);
        if (existingOpt.isEmpty()) {
            WorkerEntity entity = new WorkerEntity(
                    workerId,
                    baseUrl,
                    WorkerStatus.UP,
                    request.capacityBytes(),
                    request.usedBytes(),
                    freeBytes,
                    request.chunkCount(),
                    now,
                    now);
            workerRepository.save(entity);
            eventService.record(EventService.WORKER_REGISTERED, EventSeverity.INFO, null, null, workerId,
                    "Worker " + workerId + " registered at " + baseUrl + " (chunks=" + request.chunkCount() + ")");
            return new RegistrationOutcome(true, new RegisterWorkerResponse(
                    workerId, WorkerStatus.UP, DEFAULT_HEARTBEAT_INTERVAL_MS, now));
        }

        WorkerEntity existing = existingOpt.get();
        boolean isLive = existing.getStatus() == WorkerStatus.UP
                && existing.getLastHeartbeatAt().plus(properties.cluster().heartbeatTimeout()).isAfter(now);
        if (isLive && !existing.getBaseUrl().equalsIgnoreCase(baseUrl)) {
            throw new ApiException(409, ErrorCode.WORKER_ID_CONFLICT,
                    "Worker ID '" + workerId + "' is already active at " + existing.getBaseUrl(),
                    Map.of("workerId", workerId, "existingBaseUrl", existing.getBaseUrl(), "requestedBaseUrl", baseUrl),
                    null);
        }

        boolean wasDown = existing.getStatus() == WorkerStatus.DOWN;
        existing.setBaseUrl(baseUrl);
        existing.setStatus(WorkerStatus.UP);
        existing.setCapacityBytes(request.capacityBytes());
        existing.setUsedBytes(request.usedBytes());
        existing.setFreeBytes(freeBytes);
        existing.setChunkCount(request.chunkCount());
        existing.setLastHeartbeatAt(now);
        existing.setDownSince(null);
        workerRepository.save(existing);

        if (wasDown) {
            eventService.record(EventService.WORKER_UP, EventSeverity.INFO, null, null, workerId,
                    "Worker " + workerId + " rejoined cluster and is UP (chunks=" + request.chunkCount() + ")");
        } else {
            log.info("Worker '{}' re-registered at {} (chunks={})", workerId, baseUrl, request.chunkCount());
        }

        reconciliationService.reconcileWorker(workerId);

        return new RegistrationOutcome(false, new RegisterWorkerResponse(
                workerId, WorkerStatus.UP, DEFAULT_HEARTBEAT_INTERVAL_MS, existing.getRegisteredAt()));
    }

    @Transactional
    public HeartbeatResponse heartbeat(HeartbeatRequest request) {
        String workerId = request.workerId().trim();
        WorkerEntity worker = workerRepository.findById(workerId)
                .orElseThrow(() -> new ApiException(404, ErrorCode.UNKNOWN_WORKER,
                        "Worker '" + workerId + "' is not registered; please register first",
                        Map.of("workerId", workerId), null));

        Instant now = Instant.now();
        boolean wasDown = worker.getStatus() == WorkerStatus.DOWN;
        worker.setStatus(WorkerStatus.UP);
        worker.setUsedBytes(request.usedBytes());
        worker.setFreeBytes(request.freeBytes());
        if (request.usedBytes() + request.freeBytes() > 0) {
            worker.setCapacityBytes(request.usedBytes() + request.freeBytes());
        }
        worker.setChunkCount(request.chunkCount());
        worker.setLastHeartbeatAt(now);
        worker.setDownSince(null);
        workerRepository.save(worker);

        if (wasDown) {
            eventService.record(EventService.WORKER_UP, EventSeverity.INFO, null, null, workerId,
                    "Worker " + workerId + " resumed heartbeats and is UP");
            reconciliationService.reconcileWorker(workerId);
        }

        for (String corruptChunkId : request.corruptChunkIds()) {
            if (corruptChunkId != null && !corruptChunkId.isBlank()) {
                String trimmedId = corruptChunkId.trim();
                replicaRepository.findByChunkIdAndWorkerId(trimmedId, workerId).ifPresent(replica -> {
                    if (replica.getState() != ReplicaState.CORRUPTED) {
                        replica.setState(ReplicaState.CORRUPTED);
                        replicaRepository.save(replica);
                        eventService.record(EventService.CHUNK_CORRUPTED, EventSeverity.WARN,
                                null, trimmedId, workerId,
                                "Worker " + workerId + " reported corrupted chunk " + trimmedId + " via heartbeat");
                    }
                });
                if (replicaRepository.findByChunkIdAndWorkerId(trimmedId, workerId).isEmpty()) {
                    eventService.record(EventService.CHUNK_CORRUPTED, EventSeverity.WARN,
                            null, trimmedId, workerId,
                            "Worker " + workerId + " reported corrupted chunk " + trimmedId + " via heartbeat");
                }
            }
        }

        return new HeartbeatResponse(workerId, WorkerStatus.UP, now);
    }

    /**
     * Marks any currently {@code UP} worker whose last heartbeat is older than {@code heartbeatTimeout}
     * as {@code DOWN}, recording a {@code WORKER_DOWN} event and marking its replicas {@code UNAVAILABLE}.
     *
     * @return list of worker IDs that transitioned from UP to DOWN during this check
     */
    @Transactional
    public List<String> markStaleWorkersDown() {
        return markStaleWorkersDownAt(Instant.now());
    }

    @Transactional
    public List<String> markStaleWorkersDownAt(Instant now) {
        Instant cutoff = now.minus(properties.cluster().heartbeatTimeout());
        List<WorkerEntity> upWorkers = workerRepository.findByStatusOrderByIdAsc(WorkerStatus.UP);
        List<String> newlyDown = new ArrayList<>();
        for (WorkerEntity worker : upWorkers) {
            if (worker.getLastHeartbeatAt().isBefore(cutoff)) {
                worker.setStatus(WorkerStatus.DOWN);
                worker.setDownSince(now);
                workerRepository.save(worker);
                newlyDown.add(worker.getId());
                eventService.record(EventService.WORKER_DOWN, EventSeverity.WARN, null, null, worker.getId(),
                        "Worker " + worker.getId() + " missed heartbeats since " + worker.getLastHeartbeatAt()
                                + " and is now marked DOWN");
                reconciliationService.handleWorkerDown(worker.getId());
            }
        }
        return newlyDown;
    }

    @Transactional(readOnly = true)
    public List<WorkerNodeInfo> listWorkers() {
        return workerRepository.findAllByOrderByIdAsc().stream()
                .map(WorkerRegistryService::toDto)
                .toList();
    }

    @Transactional(readOnly = true)
    public WorkerEntity getRequiredWorker(String workerId) {
        return workerRepository.findById(workerId)
                .orElseThrow(() -> new ApiException(404, ErrorCode.UNKNOWN_WORKER,
                        "Worker '" + workerId + "' does not exist",
                        Map.of("workerId", workerId), null));
    }

    public static WorkerNodeInfo toDto(WorkerEntity w) {
        return new WorkerNodeInfo(
                w.getId(),
                w.getBaseUrl(),
                w.getStatus(),
                w.getCapacityBytes(),
                w.getUsedBytes(),
                w.getFreeBytes(),
                w.getChunkCount(),
                w.getRegisteredAt(),
                w.getLastHeartbeatAt(),
                w.getDownSince());
    }

    private static String normalizeBaseUrl(String raw) {
        if (raw == null || raw.isBlank()) {
            throw new ApiException(400, ErrorCode.VALIDATION_ERROR, "Worker baseUrl must not be blank");
        }
        String trimmed = raw.trim();
        while (trimmed.endsWith("/")) {
            trimmed = trimmed.substring(0, trimmed.length() - 1);
        }
        if (!trimmed.startsWith("http://") && !trimmed.startsWith("https://")) {
            throw new ApiException(400, ErrorCode.VALIDATION_ERROR,
                    "Worker baseUrl must start with http:// or https://: " + raw);
        }
        return trimmed;
    }

    public record RegistrationOutcome(boolean created, RegisterWorkerResponse response) {
    }
}

