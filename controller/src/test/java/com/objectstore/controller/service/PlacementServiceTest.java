package com.objectstore.controller.service;

import com.objectstore.common.dto.WorkerStatus;
import com.objectstore.common.error.ApiException;
import com.objectstore.common.error.ErrorCode;
import com.objectstore.controller.config.ControllerProperties;
import com.objectstore.controller.domain.WorkerEntity;
import com.objectstore.controller.repository.WorkerRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.util.unit.DataSize;

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.when;

class PlacementServiceTest {

    private WorkerRepository workerRepository;
    private PlacementService placementService;

    @BeforeEach
    void setUp() {
        workerRepository = Mockito.mock(WorkerRepository.class);
        ControllerProperties props = new ControllerProperties(
                new ControllerProperties.Storage(DataSize.ofMegabytes(1), 2, DataSize.ofGigabytes(1)),
                new ControllerProperties.Cluster(Duration.ofSeconds(10), Duration.ofSeconds(2),
                        Duration.ofSeconds(30), Duration.ofSeconds(10), Duration.ofSeconds(5)),
                new ControllerProperties.Internal("secret"));
        placementService = new PlacementService(workerRepository, props);
    }

    @Test
    void rejectsWhenFewerThanTwoWorkersAreUp() {
        when(workerRepository.findByStatusOrderByIdAsc(WorkerStatus.UP))
                .thenReturn(List.of(worker("worker1", 0, 0)));

        assertThatThrownBy(() -> placementService.requireSufficientUpWorkers())
                .isInstanceOf(ApiException.class)
                .extracting("status", "code")
                .containsExactly(503, ErrorCode.INSUFFICIENT_STORAGE_NODES);

        assertThatThrownBy(() -> placementService.selectWorkersForChunk())
                .isInstanceOf(ApiException.class)
                .extracting("status", "code")
                .containsExactly(503, ErrorCode.INSUFFICIENT_STORAGE_NODES);
    }

    @Test
    void selectsDistinctWorkersAndSpreadsEvenlyAcrossFourNodes() {
        List<WorkerEntity> workers = List.of(
                worker("worker1", 0, 0),
                worker("worker2", 0, 0),
                worker("worker3", 0, 0),
                worker("worker4", 0, 0));
        when(workerRepository.findByStatusOrderByIdAsc(WorkerStatus.UP)).thenReturn(workers);

        Map<String, Integer> chosenCount = new HashMap<>();
        for (int i = 0; i < 8; i++) {
            List<WorkerEntity> pair = placementService.selectWorkersForChunk();
            assertThat(pair).hasSize(2);
            assertThat(pair.get(0).getId()).isNotEqualTo(pair.get(1).getId());
            for (WorkerEntity w : pair) {
                w.setChunkCount(w.getChunkCount() + 1);
                chosenCount.merge(w.getId(), 1, Integer::sum);
            }
        }

        // 8 chunks * 2 replicas = 16 replicas -> each of the 4 workers receives exactly 4 replicas
        assertThat(chosenCount).containsEntry("worker1", 4)
                .containsEntry("worker2", 4)
                .containsEntry("worker3", 4)
                .containsEntry("worker4", 4);
    }

    @Test
    void replacementWorkerExcludesAlreadyAttemptedNodes() {
        List<WorkerEntity> workers = List.of(
                worker("worker1", 5, 5000),
                worker("worker2", 5, 5000),
                worker("worker3", 1, 1000),
                worker("worker4", 3, 3000));
        when(workerRepository.findByStatusOrderByIdAsc(WorkerStatus.UP)).thenReturn(workers);

        WorkerEntity replacement = placementService.selectReplacementWorker(Set.of("worker1", "worker3")).orElseThrow();
        // Between worker2 (5 chunks) and worker4 (3 chunks), least-loaded worker4 is picked
        assertThat(replacement.getId()).isEqualTo("worker4");
    }

    private static WorkerEntity worker(String id, int chunks, long usedBytes) {
        Instant now = Instant.now();
        return new WorkerEntity(id, "http://" + id + ":8081", WorkerStatus.UP,
                10_000_000_000L, usedBytes, 10_000_000_000L - usedBytes, chunks, now, now);
    }
}

