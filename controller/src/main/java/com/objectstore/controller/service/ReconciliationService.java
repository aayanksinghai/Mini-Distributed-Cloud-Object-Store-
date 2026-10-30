package com.objectstore.controller.service;

import com.objectstore.common.dto.EventSeverity;
import com.objectstore.common.dto.ObjectStatus;
import com.objectstore.common.dto.ReconciliationResultInfo;
import com.objectstore.common.dto.ReplicaState;
import com.objectstore.common.dto.StoredChunkInfo;
import com.objectstore.common.dto.WorkerChunksResponse;
import com.objectstore.common.dto.WorkerStatus;
import com.objectstore.common.error.ApiException;
import com.objectstore.common.error.ErrorCode;
import com.objectstore.controller.client.WorkerClient;
import com.objectstore.controller.config.ControllerProperties;
import com.objectstore.controller.domain.ChunkEntity;
import com.objectstore.controller.domain.ChunkReplicaEntity;
import com.objectstore.controller.domain.PendingDeletionEntity;
import com.objectstore.controller.domain.StoredObjectEntity;
import com.objectstore.controller.domain.WorkerEntity;
import com.objectstore.controller.repository.ChunkReplicaRepository;
import com.objectstore.controller.repository.ChunkRepository;
import com.objectstore.controller.repository.PendingDeletionRepository;
import com.objectstore.controller.repository.StoredObjectRepository;
import com.objectstore.controller.repository.WorkerRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Reconciles worker on-disk chunk inventory against controller metadata on worker failure and rejoin
 * (PDF §7, plan.md D4, D8, D15):
 * <ul>
 *     <li>Transitions replicas on a failed worker to {@code UNAVAILABLE} and logs {@code UNDER_REPLICATED} events.</li>
 *     <li>When a worker rejoins: executes queued {@code pending_deletions}, garbage-collects orphan chunks,
 *     re-adopts valid replicas, removes missing replicas, marks corrupted replicas, and trims surplus replicas
 *     so every chunk settles back to {@code RF = 2} on distinct {@code UP} workers.</li>
 *     <li>Computes the expected-vs-actual inventory diff for {@code GET /internal/workers/{workerId}/chunks}.</li>
 * </ul>
 */
@Service
public class ReconciliationService {

    private static final Logger log = LoggerFactory.getLogger(ReconciliationService.class);

    private final WorkerRepository workerRepository;
    private final StoredObjectRepository objectRepository;
    private final ChunkRepository chunkRepository;
    private final ChunkReplicaRepository replicaRepository;
    private final PendingDeletionRepository pendingDeletionRepository;
    private final WorkerClient workerClient;
    private final EventService eventService;
    private final ControllerProperties properties;
    private final TransactionTemplate txTemplate;

    public ReconciliationService(WorkerRepository workerRepository,
                                 StoredObjectRepository objectRepository,
                                 ChunkRepository chunkRepository,
                                 ChunkReplicaRepository replicaRepository,
                                 PendingDeletionRepository pendingDeletionRepository,
                                 WorkerClient workerClient,
                                 EventService eventService,
                                 ControllerProperties properties,
                                 TransactionTemplate txTemplate) {
        this.workerRepository = workerRepository;
        this.objectRepository = objectRepository;
        this.chunkRepository = chunkRepository;
        this.replicaRepository = replicaRepository;
        this.pendingDeletionRepository = pendingDeletionRepository;
        this.workerClient = workerClient;
        this.eventService = eventService;
        this.properties = properties;
        this.txTemplate = txTemplate;
    }

    /**
     * Marks all {@code HEALTHY} replicas on a newly {@code DOWN} worker as {@code UNAVAILABLE} and
     * records an {@code UNDER_REPLICATED} warning event if any active chunks drop below {@code RF = 2}.
     */
    public void handleWorkerDown(String workerId) {
        int rf = properties.storage().replicationFactor();
        List<String> affectedChunkIds = txTemplate.execute(status -> {
            List<ChunkReplicaEntity> workerReplicas = replicaRepository.findByWorkerId(workerId);
            List<String> changedChunkIds = new ArrayList<>();
            for (ChunkReplicaEntity r : workerReplicas) {
                if (r.getState() == ReplicaState.HEALTHY) {
                    r.setState(ReplicaState.UNAVAILABLE);
                    replicaRepository.save(r);
                    changedChunkIds.add(r.getChunkId());
                }
            }
            return changedChunkIds;
        });

        if (affectedChunkIds == null || affectedChunkIds.isEmpty()) {
            return;
        }

        Map<String, WorkerEntity> workersById = workerRepository.findAllByOrderByIdAsc().stream()
                .collect(Collectors.toMap(WorkerEntity::getId, Function.identity()));
        Map<String, List<ChunkReplicaEntity>> replicasByChunk = replicaRepository
                .findByChunkIdInOrderByChunkIdAscWorkerIdAsc(affectedChunkIds).stream()
                .collect(Collectors.groupingBy(ChunkReplicaEntity::getChunkId));

        long underReplicatedCount = 0;
        for (String chunkId : affectedChunkIds) {
            long healthyOnUp = replicasByChunk.getOrDefault(chunkId, List.of()).stream()
                    .filter(r -> r.getState() == ReplicaState.HEALTHY)
                    .filter(r -> {
                        WorkerEntity w = workersById.get(r.getWorkerId());
                        return w != null && w.getStatus() == WorkerStatus.UP;
                    })
                    .count();
            if (healthyOnUp < rf) {
                underReplicatedCount++;
            }
        }

        if (underReplicatedCount > 0) {
            eventService.record(EventService.UNDER_REPLICATED, EventSeverity.WARN, null, null, workerId,
                    "Worker " + workerId + " went DOWN: " + underReplicatedCount
                            + " chunk(s) are now under-replicated (effective RF < " + rf + ")");
        }
    }

    /**
     * Reconciles a worker's on-disk chunk inventory against controller metadata (called automatically on worker
     * rejoin and available on-demand).
     */
    public ReconciliationResultInfo reconcileWorker(String workerId) {
        WorkerEntity worker = workerRepository.findById(workerId)
                .orElseThrow(() -> new ApiException(404, ErrorCode.UNKNOWN_WORKER,
                        "Worker '" + workerId + "' does not exist",
                        Map.of("workerId", workerId), null));

        Instant now = Instant.now();
        int rf = properties.storage().replicationFactor();

        // 1. Execute any queued pending_deletions for this worker first.
        List<PendingDeletionEntity> pendingList = pendingDeletionRepository.findByWorkerIdOrderByIdAsc(workerId);
        int pendingExecuted = 0;
        for (PendingDeletionEntity pending : pendingList) {
            try {
                workerClient.deleteChunk(workerId, worker.getBaseUrl(), pending.getChunkId());
                txTemplate.executeWithoutResult(status -> pendingDeletionRepository.deleteById(pending.getId()));
                pendingExecuted++;
            } catch (Exception e) {
                log.warn("Deferred delete of chunk {} on worker '{}' failed: {}",
                        pending.getChunkId(), workerId, e.getMessage());
            }
        }

        List<ChunkReplicaEntity> expectedReplicas = replicaRepository.findByWorkerId(workerId);
        Map<String, ChunkReplicaEntity> expectedByChunkId = expectedReplicas.stream()
                .collect(Collectors.toMap(ChunkReplicaEntity::getChunkId, Function.identity(), (a, b) -> a));
        boolean hasExpectedOrPendingWork = !pendingList.isEmpty() || !expectedReplicas.isEmpty();

        // 2. Fetch actual on-disk inventory from the worker (with checksum verification).
        final WorkerChunksResponse actualInventory;
        try {
            actualInventory = workerClient.listWorkerChunks(workerId, worker.getBaseUrl(), true);
        } catch (Exception e) {
            if (hasExpectedOrPendingWork) {
                log.warn("Could not fetch chunk inventory from worker '{}' during reconciliation: {}",
                        workerId, e.getMessage());
            } else {
                log.debug("Skipping live inventory query for worker '{}' (no expected replicas and unreachable: {})",
                        workerId, e.getMessage());
            }
            return new ReconciliationResultInfo(workerId, now, pendingExecuted, 0, 0, 0, 0, 0, List.of());
        }

        Map<String, StoredChunkInfo> actualByChunkId = new HashMap<>();
        for (StoredChunkInfo c : actualInventory.chunks()) {
            actualByChunkId.put(c.chunkId(), c);
        }

        Set<String> allReferencedChunkIds = new HashSet<>(expectedByChunkId.keySet());
        allReferencedChunkIds.addAll(actualByChunkId.keySet());
        Map<String, ChunkEntity> chunksById = allReferencedChunkIds.isEmpty()
                ? Map.of()
                : chunkRepository.findAllById(allReferencedChunkIds).stream()
                        .collect(Collectors.toMap(ChunkEntity::getId, Function.identity()));

        int orphansDeleted = 0;
        int replicasReadopted = 0;
        int missingRemoved = 0;
        int corruptDetected = 0;

        // 3. Garbage-collect orphan chunks on disk that are not expected on this worker.
        for (StoredChunkInfo actual : actualInventory.chunks()) {
            String chunkId = actual.chunkId();
            ChunkReplicaEntity expectedReplica = expectedByChunkId.get(chunkId);
            ChunkEntity chunkEntity = chunksById.get(chunkId);
            boolean objectStillExists = chunkEntity != null
                    && objectRepository.findById(chunkEntity.getObjectId())
                            .map(o -> o.getStatus() == ObjectStatus.ACTIVE || o.getStatus() == ObjectStatus.UPLOADING)
                            .orElse(false);

            if (expectedReplica == null || !objectStillExists) {
                try {
                    workerClient.deleteChunk(workerId, worker.getBaseUrl(), chunkId);
                    actualByChunkId.remove(chunkId);
                    orphansDeleted++;
                    if (expectedReplica != null) {
                        txTemplate.executeWithoutResult(s -> replicaRepository.deleteById(expectedReplica.getId()));
                    }
                } catch (Exception e) {
                    log.warn("Failed to garbage-collect orphan chunk {} on worker '{}': {}",
                            chunkId, workerId, e.getMessage());
                }
            }
        }

        // 4. Reconcile expected replicas on this worker against actual on-disk files.
        List<String> healthyAdoptedChunkIds = new ArrayList<>();
        for (ChunkReplicaEntity expected : expectedReplicas) {
            String chunkId = expected.getChunkId();
            ChunkEntity chunkEntity = chunksById.get(chunkId);
            if (chunkEntity == null) {
                continue;
            }
            StoredChunkInfo onDisk = actualByChunkId.get(chunkId);
            if (onDisk == null) {
                // Expected replica is missing from worker's disk -> remove stale replica record.
                txTemplate.executeWithoutResult(s -> replicaRepository.deleteById(expected.getId()));
                missingRemoved++;
                continue;
            }

            boolean validHash = !Boolean.FALSE.equals(onDisk.valid())
                    && onDisk.size() == chunkEntity.getSizeBytes()
                    && (onDisk.checksum() == null || onDisk.checksum().equalsIgnoreCase(chunkEntity.getChecksumSha256()));

            if (!validHash) {
                txTemplate.executeWithoutResult(s ->
                        replicaRepository.findById(expected.getId()).ifPresent(r -> {
                            r.setState(ReplicaState.CORRUPTED);
                            replicaRepository.save(r);
                        }));
                corruptDetected++;
                eventService.record(EventService.CHUNK_CORRUPTED, EventSeverity.WARN,
                        chunkEntity.getObjectId(), chunkId, workerId,
                        "Reconciliation detected corrupted chunk " + chunkId + " on worker " + workerId);
            } else {
                ReplicaState previousState = expected.getState();
                txTemplate.executeWithoutResult(s ->
                        replicaRepository.findById(expected.getId()).ifPresent(r -> {
                            r.setState(ReplicaState.HEALTHY);
                            r.setLastVerifiedAt(now);
                            replicaRepository.save(r);
                        }));
                if (previousState != ReplicaState.HEALTHY) {
                    replicasReadopted++;
                }
                healthyAdoptedChunkIds.add(chunkId);
            }
        }

        // 5. Trim surplus replicas (e.g. chunk was re-replicated while worker was DOWN, so now it has 3 HEALTHY copies).
        int surplusTrimmed = 0;
        List<String> trimmedChunkIds = new ArrayList<>();
        for (String chunkId : healthyAdoptedChunkIds) {
            ChunkEntity chunkEntity = chunksById.get(chunkId);
            if (chunkEntity == null) {
                continue;
            }
            List<ChunkReplicaEntity> allReplicas = replicaRepository.findByChunkIdOrderByWorkerIdAsc(chunkId);
            Map<String, WorkerEntity> workersById = workerRepository.findAllByOrderByIdAsc().stream()
                    .collect(Collectors.toMap(WorkerEntity::getId, Function.identity()));

            List<ChunkReplicaEntity> healthyOnUp = new ArrayList<>(allReplicas.stream()
                    .filter(r -> r.getState() == ReplicaState.HEALTHY)
                    .filter(r -> {
                        WorkerEntity w = workersById.get(r.getWorkerId());
                        return w != null && w.getStatus() == WorkerStatus.UP;
                    })
                    .toList());

            while (healthyOnUp.size() > rf) {
                // Pick the replica on the most heavily loaded worker (highest chunkCount, then usedBytes,
                // preferring the rejoining worker on ties) so cluster storage stays evenly balanced.
                healthyOnUp.sort(Comparator
                        .comparingInt((ChunkReplicaEntity r) -> {
                            WorkerEntity w = workersById.get(r.getWorkerId());
                            return w != null ? w.getChunkCount() : 0;
                        })
                        .thenComparingLong(r -> {
                            WorkerEntity w = workersById.get(r.getWorkerId());
                            return w != null ? w.getUsedBytes() : 0L;
                        })
                        .thenComparingInt(r -> r.getWorkerId().equals(workerId) ? 1 : 0)
                        .reversed());

                ChunkReplicaEntity surplus = healthyOnUp.removeFirst();
                WorkerEntity surplusWorker = workersById.get(surplus.getWorkerId());
                if (surplusWorker == null) {
                    continue;
                }
                try {
                    workerClient.deleteChunk(surplusWorker.getId(), surplusWorker.getBaseUrl(), chunkId);
                    txTemplate.executeWithoutResult(s -> {
                        replicaRepository.deleteById(surplus.getId());
                        workerRepository.findById(surplusWorker.getId()).ifPresent(w -> {
                            w.setChunkCount(Math.max(0, w.getChunkCount() - 1));
                            w.setUsedBytes(Math.max(0L, w.getUsedBytes() - chunkEntity.getSizeBytes()));
                            w.setFreeBytes(w.getFreeBytes() + chunkEntity.getSizeBytes());
                            workerRepository.save(w);
                        });
                    });
                    if (surplusWorker.getId().equals(workerId)) {
                        actualByChunkId.remove(chunkId);
                    }
                    surplusTrimmed++;
                    trimmedChunkIds.add(chunkId);
                } catch (Exception e) {
                    log.warn("Failed to trim surplus replica of chunk {} on worker '{}': {}",
                            chunkId, surplusWorker.getId(), e.getMessage());
                }
            }
        }

        // 6. Refresh the worker's chunkCount and usedBytes from its post-reconciliation state.
        List<ChunkReplicaEntity> remainingReplicas = replicaRepository.findByWorkerId(workerId);
        int finalChunkCount = remainingReplicas.size();
        long finalUsedBytes = remainingReplicas.stream()
                .map(r -> chunksById.get(r.getChunkId()))
                .filter(java.util.Objects::nonNull)
                .mapToLong(ChunkEntity::getSizeBytes)
                .sum();
        txTemplate.executeWithoutResult(s ->
                workerRepository.findById(workerId).ifPresent(w -> {
                    w.setChunkCount(finalChunkCount);
                    w.setUsedBytes(finalUsedBytes);
                    workerRepository.save(w);
                }));

        eventService.record(EventService.WORKER_RECONCILED, EventSeverity.INFO, null, null, workerId,
                "Reconciled worker " + workerId + ": pendingDeleted=" + pendingExecuted
                        + ", orphansDeleted=" + orphansDeleted
                        + ", readopted=" + replicasReadopted
                        + ", missingRemoved=" + missingRemoved
                        + ", corrupt=" + corruptDetected
                        + ", surplusTrimmed=" + surplusTrimmed);

        return new ReconciliationResultInfo(
                workerId,
                now,
                pendingExecuted,
                orphansDeleted,
                replicasReadopted,
                missingRemoved,
                corruptDetected,
                surplusTrimmed,
                trimmedChunkIds);
    }

    /**
     * Queries a worker's on-disk chunks and enriches the response with the controller's expected-vs-actual
     * reconciliation diff (plan.md D4).
     */
    public WorkerChunksResponse getWorkerChunksWithDiff(String workerId, boolean verify) {
        WorkerEntity worker = workerRepository.findById(workerId)
                .orElseThrow(() -> new ApiException(404, ErrorCode.UNKNOWN_WORKER,
                        "Worker '" + workerId + "' does not exist",
                        Map.of("workerId", workerId), null));
        if (worker.getStatus() != WorkerStatus.UP) {
            throw new ApiException(503, ErrorCode.WORKER_UNAVAILABLE,
                    "Worker '" + workerId + "' is currently DOWN",
                    Map.of("workerId", workerId, "status", worker.getStatus()), null);
        }

        WorkerChunksResponse raw = workerClient.listWorkerChunks(worker.getId(), worker.getBaseUrl(), verify);
        Set<String> actualIds = raw.chunks().stream()
                .map(StoredChunkInfo::chunkId)
                .collect(Collectors.toSet());

        List<String> expectedIds = replicaRepository.findByWorkerId(workerId).stream()
                .map(ChunkReplicaEntity::getChunkId)
                .sorted()
                .toList();
        Set<String> expectedSet = new HashSet<>(expectedIds);

        List<String> missingIds = expectedIds.stream()
                .filter(id -> !actualIds.contains(id))
                .toList();
        List<String> orphanIds = raw.chunks().stream()
                .map(StoredChunkInfo::chunkId)
                .filter(id -> !expectedSet.contains(id))
                .sorted()
                .toList();
        List<String> corruptIds = raw.chunks().stream()
                .filter(c -> Boolean.FALSE.equals(c.valid()))
                .map(StoredChunkInfo::chunkId)
                .sorted()
                .toList();

        return new WorkerChunksResponse(
                raw.workerId(),
                raw.chunkCount(),
                raw.usedBytes(),
                raw.chunks(),
                expectedIds,
                missingIds,
                orphanIds,
                corruptIds);
    }
}
