package com.objectstore.controller.service;

import com.objectstore.common.dto.WorkerStatus;
import com.objectstore.common.error.ApiException;
import com.objectstore.common.error.ErrorCode;
import com.objectstore.controller.config.ControllerProperties;
import com.objectstore.controller.domain.WorkerEntity;
import com.objectstore.controller.repository.WorkerRepository;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Selects distinct {@code UP} storage workers for chunk replica placement (plan.md D5, D6).
 * <p>
 * Policy: least-loaded workers first (by stored {@code chunkCount}, then {@code usedBytes}),
 * with a rotating tie-break offset across consecutive placements so chunks of an object spread
 * evenly across all 4 workers when load is equal.
 */
@Service
public class PlacementService {

    private final WorkerRepository workerRepository;
    private final int replicationFactor;
    private final AtomicInteger rotationCounter = new AtomicInteger(0);

    public PlacementService(WorkerRepository workerRepository, ControllerProperties properties) {
        this.workerRepository = workerRepository;
        this.replicationFactor = properties.storage().replicationFactor();
    }

    public int getReplicationFactor() {
        return replicationFactor;
    }

    /**
     * Verifies that at least {@code replicationFactor} workers are currently {@code UP}.
     *
     * @throws ApiException 503 {@link ErrorCode#INSUFFICIENT_STORAGE_NODES} if fewer than RF workers are UP
     */
    public List<WorkerEntity> requireSufficientUpWorkers() {
        List<WorkerEntity> upWorkers = workerRepository.findByStatusOrderByIdAsc(WorkerStatus.UP);
        if (upWorkers.size() < replicationFactor) {
            throw new ApiException(503, ErrorCode.INSUFFICIENT_STORAGE_NODES,
                    "Insufficient UP storage workers: required " + replicationFactor
                            + " for replication, but only " + upWorkers.size() + " available",
                    Map.of("requiredWorkers", replicationFactor, "availableWorkers", upWorkers.size()),
                    null);
        }
        return upWorkers;
    }

    /**
     * Selects {@code replicationFactor} distinct {@code UP} workers for placing a chunk.
     */
    public List<WorkerEntity> selectWorkersForChunk() {
        return selectWorkers(replicationFactor, Set.of());
    }

    /**
     * Selects {@code count} distinct {@code UP} workers excluding any worker IDs in {@code excludedWorkerIds}.
     */
    public List<WorkerEntity> selectWorkers(int count, Collection<String> excludedWorkerIds) {
        Set<String> excluded = excludedWorkerIds == null ? Set.of() : new HashSet<>(excludedWorkerIds);
        List<WorkerEntity> candidates = workerRepository.findByStatusOrderByIdAsc(WorkerStatus.UP).stream()
                .filter(w -> !excluded.contains(w.getId()))
                .toList();

        if (candidates.size() < count) {
            throw new ApiException(503, ErrorCode.INSUFFICIENT_STORAGE_NODES,
                    "Insufficient UP storage workers: needed " + count + " distinct worker(s), found " + candidates.size(),
                    Map.of("requiredWorkers", count, "availableWorkers", candidates.size()),
                    null);
        }

        int n = candidates.size();
        int rotation = Math.floorMod(rotationCounter.getAndIncrement(), n);

        List<IndexedWorker> indexed = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            int rotatedRank = Math.floorMod(i - rotation, n);
            indexed.add(new IndexedWorker(candidates.get(i), rotatedRank));
        }

        indexed.sort(Comparator
                .comparingInt((IndexedWorker iw) -> iw.worker().getChunkCount())
                .thenComparingLong(iw -> iw.worker().getUsedBytes())
                .thenComparingInt(IndexedWorker::rotatedRank));

        return indexed.subList(0, count).stream()
                .map(IndexedWorker::worker)
                .toList();
    }

    /**
     * Selects a single replacement {@code UP} worker not in {@code excludedWorkerIds}, if one exists.
     */
    public Optional<WorkerEntity> selectReplacementWorker(Collection<String> excludedWorkerIds) {
        Set<String> excluded = excludedWorkerIds == null ? Set.of() : new HashSet<>(excludedWorkerIds);
        List<WorkerEntity> candidates = workerRepository.findByStatusOrderByIdAsc(WorkerStatus.UP).stream()
                .filter(w -> !excluded.contains(w.getId()))
                .toList();
        if (candidates.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(selectWorkers(1, excluded).getFirst());
    }

    private record IndexedWorker(WorkerEntity worker, int rotatedRank) {
    }
}

