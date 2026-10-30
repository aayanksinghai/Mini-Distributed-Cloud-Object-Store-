package com.objectstore.controller.repository;

import com.objectstore.common.dto.WorkerStatus;
import com.objectstore.controller.domain.WorkerEntity;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;

public interface WorkerRepository extends JpaRepository<WorkerEntity, String> {

    List<WorkerEntity> findAllByOrderByIdAsc();

    List<WorkerEntity> findByStatusOrderByIdAsc(WorkerStatus status);
}

