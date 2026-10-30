package com.objectstore.controller.repository;

import com.objectstore.common.dto.ObjectStatus;
import com.objectstore.controller.domain.StoredObjectEntity;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;
import java.util.UUID;

public interface StoredObjectRepository extends JpaRepository<StoredObjectEntity, UUID> {

    Page<StoredObjectEntity> findByStatus(ObjectStatus status, Pageable pageable);

    List<StoredObjectEntity> findByStatusOrderByCreatedAtAsc(ObjectStatus status);

    long countByStatus(ObjectStatus status);
}

