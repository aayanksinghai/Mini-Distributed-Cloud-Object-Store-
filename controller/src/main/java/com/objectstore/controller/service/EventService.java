package com.objectstore.controller.service;

import com.objectstore.common.dto.EventSeverity;
import com.objectstore.common.dto.SystemEventInfo;
import com.objectstore.controller.domain.SystemEventEntity;
import com.objectstore.controller.repository.SystemEventRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.domain.PageRequest;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

/**
 * Records and queries cluster and storage lifecycle events in {@code system_events}.
 */
@Service
public class EventService {

    private static final Logger log = LoggerFactory.getLogger(EventService.class);

    public static final String WORKER_REGISTERED = "WORKER_REGISTERED";
    public static final String WORKER_UP = "WORKER_UP";
    public static final String WORKER_DOWN = "WORKER_DOWN";
    public static final String CHUNK_CORRUPTED = "CHUNK_CORRUPTED";
    public static final String OBJECT_UPLOADED = "OBJECT_UPLOADED";
    public static final String OBJECT_UPLOAD_FAILED = "OBJECT_UPLOAD_FAILED";
    public static final String OBJECT_METADATA_UPDATED = "OBJECT_METADATA_UPDATED";
    public static final String OBJECT_DELETED = "OBJECT_DELETED";
    public static final String CHUNK_REPAIRED = "CHUNK_REPAIRED";
    public static final String RF_RESTORED = "RF_RESTORED";
    public static final String UNDER_REPLICATED = "UNDER_REPLICATED";
    public static final String WORKER_RECONCILED = "WORKER_RECONCILED";

    private final SystemEventRepository repository;

    public EventService(SystemEventRepository repository) {
        this.repository = repository;
    }

    @Transactional
    public SystemEventEntity record(String type,
                                    EventSeverity severity,
                                    UUID objectId,
                                    String chunkId,
                                    String workerId,
                                    String message) {
        SystemEventEntity entity = new SystemEventEntity(
                Instant.now(), type, severity, objectId, chunkId, workerId, message);
        SystemEventEntity saved = repository.save(entity);
        log.info("Event [{}] ({}) worker={} object={} chunk={} - {}",
                type, severity, workerId, objectId, chunkId, message);
        return saved;
    }

    @Transactional(readOnly = true)
    public List<SystemEventInfo> listEvents(Long sinceId, int limit) {
        int clamped = Math.max(1, Math.min(limit, 500));
        List<SystemEventEntity> entities;
        if (sinceId != null && sinceId > 0) {
            entities = repository.findByIdGreaterThanOrderByIdAsc(sinceId, PageRequest.of(0, clamped));
        } else {
            List<SystemEventEntity> newestFirst = repository.findAllByOrderByIdDesc(PageRequest.of(0, clamped));
            entities = new ArrayList<>(newestFirst);
            Collections.reverse(entities);
        }
        return entities.stream().map(EventService::toDto).toList();
    }

    private static SystemEventInfo toDto(SystemEventEntity e) {
        return new SystemEventInfo(
                e.getId(),
                e.getOccurredAt(),
                e.getType(),
                e.getSeverity(),
                e.getObjectId(),
                e.getChunkId(),
                e.getWorkerId(),
                e.getMessage());
    }
}

