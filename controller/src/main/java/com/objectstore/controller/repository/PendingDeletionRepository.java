package com.objectstore.controller.repository;

import com.objectstore.controller.domain.PendingDeletionEntity;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;
import java.util.Optional;

public interface PendingDeletionRepository extends JpaRepository<PendingDeletionEntity, Long> {

    List<PendingDeletionEntity> findByWorkerIdOrderByIdAsc(String workerId);

    Optional<PendingDeletionEntity> findByChunkIdAndWorkerId(String chunkId, String workerId);
}

