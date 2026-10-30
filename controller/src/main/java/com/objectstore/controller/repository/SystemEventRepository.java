package com.objectstore.controller.repository;

import com.objectstore.controller.domain.SystemEventEntity;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;

public interface SystemEventRepository extends JpaRepository<SystemEventEntity, Long> {

    List<SystemEventEntity> findByIdGreaterThanOrderByIdAsc(long sinceId, Pageable pageable);

    List<SystemEventEntity> findAllByOrderByIdDesc(Pageable pageable);
}

