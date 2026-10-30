package com.objectstore.controller.repository;

import com.objectstore.controller.domain.ChunkReplicaEntity;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.Collection;
import java.util.List;

public interface ChunkReplicaRepository extends JpaRepository<ChunkReplicaEntity, Long> {

    List<ChunkReplicaEntity> findByChunkIdOrderByWorkerIdAsc(String chunkId);

    List<ChunkReplicaEntity> findByChunkIdInOrderByChunkIdAscWorkerIdAsc(Collection<String> chunkIds);

    List<ChunkReplicaEntity> findByWorkerId(String workerId);

    void deleteByChunkIdIn(Collection<String> chunkIds);
}

