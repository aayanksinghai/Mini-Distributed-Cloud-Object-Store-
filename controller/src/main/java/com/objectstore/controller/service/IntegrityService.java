package com.objectstore.controller.service;

import com.objectstore.common.dto.EventSeverity;
import com.objectstore.common.dto.ObjectStatus;
import com.objectstore.common.dto.RepairResultInfo;
import com.objectstore.common.dto.ReplicaState;
import com.objectstore.common.dto.ScrubResultInfo;
import com.objectstore.common.dto.StoredChunkInfo;
import com.objectstore.common.dto.WorkerChunksResponse;
import com.objectstore.common.dto.WorkerStatus;
import com.objectstore.common.error.ApiException;
import com.objectstore.common.error.ErrorCode;
import com.objectstore.controller.client.WorkerClient;
import com.objectstore.controller.domain.ChunkEntity;
import com.objectstore.controller.domain.ChunkReplicaEntity;
import com.objectstore.controller.domain.StoredObjectEntity;
import com.objectstore.controller.domain.WorkerEntity;
import com.objectstore.controller.repository.ChunkReplicaRepository;
import com.objectstore.controller.repository.ChunkRepository;
import com.objectstore.controller.repository.StoredObjectRepository;
import com.objectstore.controller.repository.WorkerRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Coordinates cluster-wide and object-scoped SHA-256 data-integrity verification and automatic recovery
 * from valid replicas (PDF §8, plan.md D9, D10).
 */
@Service
public class IntegrityService {

    private static final Logger log = LoggerFactory.getLogger(IntegrityService.class);

    private final StoredObjectRepository objectRepository;
    private final ChunkRepository chunkRepository;
    private final ChunkReplicaRepository replicaRepository;
    private final WorkerRepository workerRepository;
    private final WorkerClient workerClient;
    private final ReplicationService replicationService;
    private final EventService eventService;
    private final TransactionTemplate txTemplate;

    public IntegrityService(StoredObjectRepository objectRepository,
                            ChunkRepository chunkRepository,
                            ChunkReplicaRepository replicaRepository,
                            WorkerRepository workerRepository,
                            WorkerClient workerClient,
                            ReplicationService replicationService,
                            EventService eventService,
                            TransactionTemplate txTemplate) {
        this.objectRepository = objectRepository;
        this.chunkRepository = chunkRepository;
        this.replicaRepository = replicaRepository;
        this.workerRepository = workerRepository;
        this.workerClient = workerClient;
        this.replicationService = replicationService;
        this.eventService = eventService;
        this.txTemplate = txTemplate;
    }

    /**
     * Verifies all stored chunk replicas (or those belonging to a specific {@code objectId}) across all
     * {@code UP} workers by querying each worker's verified chunk inventory (`?verify=true`), marks any
     * corrupted replicas as {@code CORRUPTED}, removes missing replica records, and optionally repairs
     * corrupted/under-replicated chunks from surviving healthy replicas.
     */
    public ScrubResultInfo scrub(UUID objectId, boolean repair) {
        Instant now = Instant.now();

        List<ChunkEntity> targetChunks;
        if (objectId != null) {
            StoredObjectEntity obj = objectRepository.findById(objectId)
                    .orElseThrow(() -> new ApiException(404, ErrorCode.OBJECT_NOT_FOUND,
                            "Object " + objectId + " does not exist",
                            Map.of("objectId", objectId.toString()), null));
            if (obj.getStatus() != ObjectStatus.ACTIVE) {
                throw new ApiException(409, ErrorCode.OBJECT_NOT_READY,
                        "Object " + objectId + " is in state " + obj.getStatus(),
                        Map.of("objectId", objectId.toString(), "status", obj.getStatus().name()), null);
            }
            targetChunks = chunkRepository.findByObjectIdOrderByChunkIndexAsc(objectId);
        } else {
            List<UUID> activeIds = objectRepository.findByStatusOrderByCreatedAtAsc(ObjectStatus.ACTIVE).stream()
                    .map(StoredObjectEntity::getId)
                    .toList();
            targetChunks = activeIds.isEmpty()
                    ? List.of()
                    : chunkRepository.findByObjectIdInOrderByObjectIdAscChunkIndexAsc(activeIds);
        }

        if (targetChunks.isEmpty()) {
            return new ScrubResultInfo(now, objectId, 0, 0, 0, 0, 0, List.of(), List.of());
        }

        Map<String, ChunkEntity> chunksById = targetChunks.stream()
                .collect(Collectors.toMap(ChunkEntity::getId, Function.identity()));
        List<String> chunkIds = new ArrayList<>(chunksById.keySet());
        List<ChunkReplicaEntity> replicas = replicaRepository.findByChunkIdInOrderByChunkIdAscWorkerIdAsc(chunkIds);

        Map<String, WorkerEntity> workersById = workerRepository.findAllByOrderByIdAsc().stream()
                .collect(Collectors.toMap(WorkerEntity::getId, Function.identity()));

        // Query each UP worker's verified on-disk inventory once.
        Map<String, Map<String, StoredChunkInfo>> verifiedByWorker = new HashMap<>();
        for (WorkerEntity worker : workersById.values()) {
            if (worker.getStatus() != WorkerStatus.UP) {
                continue;
            }
            try {
                WorkerChunksResponse inv = workerClient.listWorkerChunks(worker.getId(), worker.getBaseUrl(), true);
                Map<String, StoredChunkInfo> map = new HashMap<>();
                for (StoredChunkInfo c : inv.chunks()) {
                    map.put(c.chunkId(), c);
                }
                verifiedByWorker.put(worker.getId(), map);
            } catch (Exception e) {
                log.warn("Could not query verified chunk inventory from worker '{}' during scrub: {}",
                        worker.getId(), e.getMessage());
            }
        }

        int examined = 0;
        int validCount = 0;
        int corruptDetected = 0;
        int missingDetected = 0;
        Set<String> corruptedChunkIds = new LinkedHashSet<>();

        for (ChunkReplicaEntity replica : replicas) {
            WorkerEntity worker = workersById.get(replica.getWorkerId());
            if (worker == null || worker.getStatus() != WorkerStatus.UP) {
                continue;
            }
            Map<String, StoredChunkInfo> workerInventory = verifiedByWorker.get(worker.getId());
            if (workerInventory == null) {
                continue;
            }

            examined++;
            ChunkEntity chunk = chunksById.get(replica.getChunkId());
            StoredChunkInfo onDisk = workerInventory.get(replica.getChunkId());

            if (onDisk == null) {
                missingDetected++;
                corruptedChunkIds.add(replica.getChunkId());
                txTemplate.executeWithoutResult(s -> replicaRepository.deleteById(replica.getId()));
                eventService.record(EventService.CHUNK_CORRUPTED, EventSeverity.WARN,
                        chunk.getObjectId(), chunk.getId(), worker.getId(),
                        "Integrity scrub found missing replica for chunk " + chunk.getId()
                                + " on worker " + worker.getId());
                continue;
            }

            boolean validHash = !Boolean.FALSE.equals(onDisk.valid())
                    && onDisk.size() == chunk.getSizeBytes()
                    && (onDisk.checksum() == null || onDisk.checksum().equalsIgnoreCase(chunk.getChecksumSha256()));

            if (!validHash) {
                corruptDetected++;
                corruptedChunkIds.add(chunk.getId());
                txTemplate.executeWithoutResult(s ->
                        replicaRepository.findById(replica.getId()).ifPresent(r -> {
                            r.setState(ReplicaState.CORRUPTED);
                            replicaRepository.save(r);
                        }));
                eventService.record(EventService.CHUNK_CORRUPTED, EventSeverity.WARN,
                        chunk.getObjectId(), chunk.getId(), worker.getId(),
                        "Integrity scrub detected corrupted replica for chunk " + chunk.getId()
                                + " on worker " + worker.getId());
            } else {
                validCount++;
                txTemplate.executeWithoutResult(s ->
                        replicaRepository.findById(replica.getId()).ifPresent(r -> {
                            r.setState(ReplicaState.HEALTHY);
                            r.setLastVerifiedAt(now);
                            replicaRepository.save(r);
                        }));
            }
        }

        int repairedCount = 0;
        List<String> repairedChunkIds = List.of();
        if (repair && (corruptDetected > 0 || missingDetected > 0)) {
            RepairResultInfo repairResult = replicationService.repairUnderReplicatedChunks(true);
            repairedCount = repairResult.repairedChunks() + repairResult.corruptedReplicasRepaired();
            repairedChunkIds = repairResult.repairedChunkIds();
        }

        return new ScrubResultInfo(
                now,
                objectId,
                examined,
                validCount,
                corruptDetected,
                missingDetected,
                repairedCount,
                new ArrayList<>(corruptedChunkIds),
                repairedChunkIds);
    }

    /**
     * Proxies an opt-in debug corruption request to the specified worker (for demos and dashboard use when
     * {@code WORKER_DEBUG_ENDPOINTS=true}).
     */
    public Map<String, Object> corruptReplicaForDebug(String workerId, String chunkId) {
        WorkerEntity worker = workerRepository.findById(workerId)
                .orElseThrow(() -> new ApiException(404, ErrorCode.UNKNOWN_WORKER,
                        "Worker '" + workerId + "' does not exist",
                        Map.of("workerId", workerId), null));
        if (worker.getStatus() != WorkerStatus.UP) {
            throw new ApiException(503, ErrorCode.WORKER_UNAVAILABLE,
                    "Worker '" + workerId + "' is currently DOWN",
                    Map.of("workerId", workerId), null);
        }
        return workerClient.corruptChunkOnWorker(worker.getId(), worker.getBaseUrl(), chunkId);
    }
}

