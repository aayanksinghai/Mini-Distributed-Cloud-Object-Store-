package com.objectstore.controller.service;

import com.objectstore.common.dto.ClusterStatusInfo;
import com.objectstore.common.dto.EventSeverity;
import com.objectstore.common.dto.ObjectStatus;
import com.objectstore.common.dto.RepairResultInfo;
import com.objectstore.common.dto.ReplicaState;
import com.objectstore.common.dto.ReplicationStatus;
import com.objectstore.common.dto.WorkerStatus;
import com.objectstore.common.util.Sha256;
import com.objectstore.controller.client.WorkerClient;
import com.objectstore.controller.config.ControllerProperties;
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
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Computes cluster/object replication status and repairs under-replicated or corrupted chunks (PDF §4, §7, §8).
 */
@Service
public class ReplicationService {

    private static final Logger log = LoggerFactory.getLogger(ReplicationService.class);

    private final StoredObjectRepository objectRepository;
    private final ChunkRepository chunkRepository;
    private final ChunkReplicaRepository replicaRepository;
    private final WorkerRepository workerRepository;
    private final PlacementService placementService;
    private final WorkerClient workerClient;
    private final EventService eventService;
    private final ControllerProperties properties;
    private final TransactionTemplate txTemplate;

    public ReplicationService(StoredObjectRepository objectRepository,
                              ChunkRepository chunkRepository,
                              ChunkReplicaRepository replicaRepository,
                              WorkerRepository workerRepository,
                              PlacementService placementService,
                              WorkerClient workerClient,
                              EventService eventService,
                              ControllerProperties properties,
                              TransactionTemplate txTemplate) {
        this.objectRepository = objectRepository;
        this.chunkRepository = chunkRepository;
        this.replicaRepository = replicaRepository;
        this.workerRepository = workerRepository;
        this.placementService = placementService;
        this.workerClient = workerClient;
        this.eventService = eventService;
        this.properties = properties;
        this.txTemplate = txTemplate;
    }

    /**
     * Computes the effective replication status for a single chunk given its replicas and the current worker states.
     */
    public ReplicationStatus evaluateChunkReplication(Collection<ChunkReplicaEntity> replicas,
                                                      Map<String, WorkerEntity> workersById) {
        int rf = properties.storage().replicationFactor();
        long healthyOnUpWorkers = replicas.stream()
                .filter(r -> r.getState() == ReplicaState.HEALTHY)
                .filter(r -> {
                    WorkerEntity w = workersById.get(r.getWorkerId());
                    return w != null && w.getStatus() == WorkerStatus.UP;
                })
                .count();
        if (healthyOnUpWorkers >= rf) {
            return ReplicationStatus.HEALTHY;
        }
        return healthyOnUpWorkers > 0 ? ReplicationStatus.DEGRADED : ReplicationStatus.UNAVAILABLE;
    }

    /**
     * Computes cluster-wide worker and chunk replication health (`GET /api/v1/cluster/status`).
     */
    @Transactional(readOnly = true)
    public ClusterStatusInfo getClusterStatus() {
        int rf = properties.storage().replicationFactor();
        List<WorkerEntity> workers = workerRepository.findAllByOrderByIdAsc();
        Map<String, WorkerEntity> workersById = workers.stream()
                .collect(Collectors.toMap(WorkerEntity::getId, Function.identity()));

        int workersTotal = workers.size();
        int workersUp = (int) workers.stream().filter(w -> w.getStatus() == WorkerStatus.UP).count();
        int workersDown = workersTotal - workersUp;

        List<StoredObjectEntity> activeObjects = objectRepository.findByStatusOrderByCreatedAtAsc(ObjectStatus.ACTIVE);
        List<UUID> activeObjectIds = activeObjects.stream().map(StoredObjectEntity::getId).toList();

        List<ChunkEntity> chunks = activeObjectIds.isEmpty()
                ? List.of()
                : chunkRepository.findByObjectIdInOrderByObjectIdAscChunkIndexAsc(activeObjectIds);
        List<String> chunkIds = chunks.stream().map(ChunkEntity::getId).toList();
        Map<String, List<ChunkReplicaEntity>> replicasByChunk = chunkIds.isEmpty()
                ? Map.of()
                : replicaRepository.findByChunkIdInOrderByChunkIdAscWorkerIdAsc(chunkIds).stream()
                        .collect(Collectors.groupingBy(ChunkReplicaEntity::getChunkId));

        long healthyChunks = 0;
        long underReplicatedChunks = 0;
        long unavailableChunks = 0;
        long totalHealthyReplicas = 0;
        long corruptedReplicas = 0;

        for (ChunkEntity chunk : chunks) {
            List<ChunkReplicaEntity> replicas = replicasByChunk.getOrDefault(chunk.getId(), List.of());
            long healthyOnUp = 0;
            for (ChunkReplicaEntity r : replicas) {
                if (r.getState() == ReplicaState.CORRUPTED) {
                    corruptedReplicas++;
                }
                WorkerEntity w = workersById.get(r.getWorkerId());
                if (r.getState() == ReplicaState.HEALTHY && w != null && w.getStatus() == WorkerStatus.UP) {
                    healthyOnUp++;
                    totalHealthyReplicas++;
                }
            }
            if (healthyOnUp >= rf) {
                healthyChunks++;
            } else if (healthyOnUp > 0) {
                underReplicatedChunks++;
            } else {
                unavailableChunks++;
            }
        }

        ReplicationStatus overallStatus;
        if (unavailableChunks > 0 || (chunks.isEmpty() && workersUp < rf)) {
            overallStatus = ReplicationStatus.UNAVAILABLE;
        } else if (underReplicatedChunks > 0 || corruptedReplicas > 0 || workersUp < rf) {
            overallStatus = ReplicationStatus.DEGRADED;
        } else {
            overallStatus = ReplicationStatus.HEALTHY;
        }

        return new ClusterStatusInfo(
                rf,
                workersTotal,
                workersUp,
                workersDown,
                activeObjects.size(),
                chunks.size(),
                healthyChunks,
                underReplicatedChunks,
                unavailableChunks,
                totalHealthyReplicas,
                corruptedReplicas,
                overallStatus);
    }

    /**
     * Scans all chunks of {@code ACTIVE} objects and restores their replication factor to {@code RF = 2}
     * on distinct {@code UP} workers whenever a valid replica exists. Also repairs any {@code CORRUPTED}
     * replica on an {@code UP} worker by overwriting it from a healthy replica.
     *
     * @param bypassGracePeriod when {@code true} (e.g. manual admin repair endpoint), immediately re-replicates
     *                          chunks even if the DOWN worker went down within {@code reReplicationGracePeriod}
     */
    public RepairResultInfo repairUnderReplicatedChunks(boolean bypassGracePeriod) {
        return repairUnderReplicatedChunksAt(Instant.now(), bypassGracePeriod);
    }

    public RepairResultInfo repairUnderReplicatedChunksAt(Instant now, boolean bypassGracePeriod) {
        int rf = properties.storage().replicationFactor();
        Instant graceCutoff = now.minus(properties.cluster().reReplicationGracePeriod());

        List<StoredObjectEntity> activeObjects = objectRepository.findByStatusOrderByCreatedAtAsc(ObjectStatus.ACTIVE);
        if (activeObjects.isEmpty()) {
            return new RepairResultInfo(now, 0, 0, 0, 0, 0, 0, List.of());
        }

        List<UUID> objectIds = activeObjects.stream().map(StoredObjectEntity::getId).toList();
        List<ChunkEntity> chunks = chunkRepository.findByObjectIdInOrderByObjectIdAscChunkIndexAsc(objectIds);

        int examined = chunks.size();
        int underReplicatedFound = 0;
        int repairedChunks = 0;
        int corruptedReplicasRepaired = 0;
        int skippedGrace = 0;
        int unrepairable = 0;
        List<String> repairedChunkIds = new ArrayList<>();

        for (ChunkEntity chunk : chunks) {
            Map<String, WorkerEntity> workersById = workerRepository.findAllByOrderByIdAsc().stream()
                    .collect(Collectors.toMap(WorkerEntity::getId, Function.identity()));
            List<ChunkReplicaEntity> replicas = replicaRepository.findByChunkIdOrderByWorkerIdAsc(chunk.getId());

            List<ChunkReplicaEntity> healthyOnUp = new ArrayList<>();
            List<ChunkReplicaEntity> corruptedOnUp = new ArrayList<>();
            List<WorkerEntity> downHolders = new ArrayList<>();

            for (ChunkReplicaEntity r : replicas) {
                WorkerEntity w = workersById.get(r.getWorkerId());
                if (w == null) {
                    continue;
                }
                if (w.getStatus() == WorkerStatus.UP) {
                    if (r.getState() == ReplicaState.HEALTHY) {
                        healthyOnUp.add(r);
                    } else if (r.getState() == ReplicaState.CORRUPTED) {
                        corruptedOnUp.add(r);
                    }
                } else {
                    downHolders.add(w);
                }
            }

            boolean needsNewReplicas = healthyOnUp.size() < rf;
            boolean hasCorruptedOnUp = !corruptedOnUp.isEmpty();
            if (!needsNewReplicas && !hasCorruptedOnUp) {
                continue;
            }

            if (needsNewReplicas) {
                underReplicatedFound++;
            }

            // If under-replication is strictly due to a recently-DOWN worker within the grace period, wait.
            if (!bypassGracePeriod && needsNewReplicas && !hasCorruptedOnUp && !downHolders.isEmpty()) {
                boolean allDownWithinGrace = downHolders.stream().allMatch(w ->
                        w.getDownSince() != null && w.getDownSince().isAfter(graceCutoff));
                if (allDownWithinGrace) {
                    skippedGrace++;
                    continue;
                }
            }

            // Find a verified source byte array from one of the healthy replicas on an UP worker.
            byte[] verifiedChunkBytes = fetchVerifiedSourceBytes(chunk, healthyOnUp, workersById);
            if (verifiedChunkBytes == null) {
                unrepairable++;
                continue;
            }

            boolean chunkModified = false;

            // 1. Repair any CORRUPTED replicas on UP workers in-place via overwrite=true.
            for (ChunkReplicaEntity corruptReplica : corruptedOnUp) {
                WorkerEntity targetWorker = workersById.get(corruptReplica.getWorkerId());
                if (targetWorker != null && overwriteCorruptedReplica(chunk, verifiedChunkBytes, corruptReplica, targetWorker)) {
                    corruptedReplicasRepaired++;
                    healthyOnUp.add(corruptReplica);
                    chunkModified = true;
                }
            }

            // 2. If effective healthy replicas on UP workers is still < RF, copy to new distinct UP worker(s).
            Set<String> excludedWorkerIds = replicas.stream()
                    .map(ChunkReplicaEntity::getWorkerId)
                    .collect(Collectors.toCollection(HashSet::new));

            while (healthyOnUp.size() < rf) {
                Optional<WorkerEntity> targetOpt = placementService.selectReplacementWorker(excludedWorkerIds);
                if (targetOpt.isEmpty()) {
                    break;
                }
                WorkerEntity target = targetOpt.get();
                excludedWorkerIds.add(target.getId());
                if (copyReplicaToNewWorker(chunk, verifiedChunkBytes, target)) {
                    healthyOnUp.add(new ChunkReplicaEntity(
                            chunk.getId(), target.getId(), ReplicaState.HEALTHY, Instant.now(), Instant.now()));
                    chunkModified = true;
                }
            }

            if (healthyOnUp.size() >= rf && chunkModified) {
                repairedChunks++;
                repairedChunkIds.add(chunk.getId());
            } else if (healthyOnUp.size() < rf) {
                unrepairable++;
            }
        }

        if (repairedChunks > 0 || corruptedReplicasRepaired > 0) {
            eventService.record(EventService.RF_RESTORED, EventSeverity.INFO, null, null, null,
                    "Replication repair completed: repairedChunks=" + repairedChunks
                            + ", corruptedReplicasRepaired=" + corruptedReplicasRepaired);
        }

        return new RepairResultInfo(
                now,
                examined,
                underReplicatedFound,
                repairedChunks,
                corruptedReplicasRepaired,
                skippedGrace,
                unrepairable,
                repairedChunkIds);
    }

    private byte[] fetchVerifiedSourceBytes(ChunkEntity chunk,
                                            List<ChunkReplicaEntity> healthyOnUp,
                                            Map<String, WorkerEntity> workersById) {
        for (ChunkReplicaEntity sourceReplica : healthyOnUp) {
            WorkerEntity sourceWorker = workersById.get(sourceReplica.getWorkerId());
            if (sourceWorker == null || sourceWorker.getStatus() != WorkerStatus.UP) {
                continue;
            }
            try {
                byte[] data = workerClient.getChunk(sourceWorker.getId(), sourceWorker.getBaseUrl(), chunk.getId());
                String actualSha = Sha256.hash(data);
                if (data.length == chunk.getSizeBytes() && actualSha.equalsIgnoreCase(chunk.getChecksumSha256())) {
                    return data;
                }
                log.warn("Source replica of chunk {} on worker '{}' failed checksum verification during repair",
                        chunk.getId(), sourceWorker.getId());
                txTemplate.executeWithoutResult(status ->
                        replicaRepository.findById(sourceReplica.getId()).ifPresent(r -> {
                            r.setState(ReplicaState.CORRUPTED);
                            replicaRepository.save(r);
                        }));
            } catch (Exception e) {
                log.warn("Could not read source chunk {} from worker '{}' during repair: {}",
                        chunk.getId(), sourceWorker.getId(), e.getMessage());
            }
        }
        return null;
    }

    private boolean overwriteCorruptedReplica(ChunkEntity chunk,
                                              byte[] verifiedBytes,
                                              ChunkReplicaEntity corruptReplica,
                                              WorkerEntity targetWorker) {
        try {
            workerClient.putChunk(
                    targetWorker.getId(),
                    targetWorker.getBaseUrl(),
                    chunk.getId(),
                    verifiedBytes,
                    chunk.getChecksumSha256(),
                    chunk.getObjectId().toString(),
                    chunk.getChunkIndex(),
                    true);
            Instant now = Instant.now();
            txTemplate.executeWithoutResult(status ->
                    replicaRepository.findById(corruptReplica.getId()).ifPresent(r -> {
                        r.setState(ReplicaState.HEALTHY);
                        r.setLastVerifiedAt(now);
                        replicaRepository.save(r);
                    }));
            eventService.record(EventService.CHUNK_REPAIRED, EventSeverity.INFO,
                    chunk.getObjectId(), chunk.getId(), targetWorker.getId(),
                    "Overwrote corrupted replica of chunk " + chunk.getId() + " on worker " + targetWorker.getId()
                            + " with verified copy");
            return true;
        } catch (Exception e) {
            log.warn("Failed to overwrite corrupted chunk {} on worker '{}': {}",
                    chunk.getId(), targetWorker.getId(), e.getMessage());
            return false;
        }
    }

    private boolean copyReplicaToNewWorker(ChunkEntity chunk,
                                           byte[] verifiedBytes,
                                           WorkerEntity targetWorker) {
        try {
            workerClient.putChunk(
                    targetWorker.getId(),
                    targetWorker.getBaseUrl(),
                    chunk.getId(),
                    verifiedBytes,
                    chunk.getChecksumSha256(),
                    chunk.getObjectId().toString(),
                    chunk.getChunkIndex(),
                    true);
            Instant now = Instant.now();
            txTemplate.executeWithoutResult(status -> {
                replicaRepository.save(new ChunkReplicaEntity(
                        chunk.getId(),
                        targetWorker.getId(),
                        ReplicaState.HEALTHY,
                        now,
                        now));
                workerRepository.findById(targetWorker.getId()).ifPresent(w -> {
                    w.setChunkCount(w.getChunkCount() + 1);
                    w.setUsedBytes(w.getUsedBytes() + chunk.getSizeBytes());
                    w.setFreeBytes(Math.max(0L, w.getFreeBytes() - chunk.getSizeBytes()));
                    workerRepository.save(w);
                });
            });
            eventService.record(EventService.CHUNK_REPAIRED, EventSeverity.INFO,
                    chunk.getObjectId(), chunk.getId(), targetWorker.getId(),
                    "Re-replicated chunk " + chunk.getId() + " to worker " + targetWorker.getId()
                            + " to restore RF=" + properties.storage().replicationFactor());
            return true;
        } catch (Exception e) {
            log.warn("Failed to re-replicate chunk {} to worker '{}': {}",
                    chunk.getId(), targetWorker.getId(), e.getMessage());
            return false;
        }
    }
}

