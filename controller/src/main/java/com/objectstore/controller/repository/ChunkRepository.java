package com.objectstore.controller.repository;

import com.objectstore.controller.domain.ChunkEntity;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.Collection;
import java.util.List;
import java.util.UUID;

public interface ChunkRepository extends JpaRepository<ChunkEntity, String> {

    List<ChunkEntity> findByObjectIdOrderByChunkIndexAsc(UUID objectId);

    List<ChunkEntity> findByObjectIdInOrderByObjectIdAscChunkIndexAsc(Collection<UUID> objectIds);

    void deleteByObjectId(UUID objectId);
}

