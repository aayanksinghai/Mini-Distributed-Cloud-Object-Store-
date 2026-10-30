package com.objectstore.controller.repository;

import com.objectstore.controller.domain.ObjectMetadataEntity;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;
import java.util.UUID;

public interface ObjectMetadataRepository
        extends JpaRepository<ObjectMetadataEntity, ObjectMetadataEntity.ObjectMetadataId> {

    List<ObjectMetadataEntity> findByIdObjectIdOrderByIdMetaKeyAsc(UUID objectId);

    void deleteByIdObjectId(UUID objectId);
}

