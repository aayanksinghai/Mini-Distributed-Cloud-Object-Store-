package com.objectstore.controller.service;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.objectstore.common.dto.ChunkMetadataInfo;
import com.objectstore.common.dto.ChunkReplicaInfo;
import com.objectstore.common.dto.EventSeverity;
import com.objectstore.common.dto.ObjectInfo;
import com.objectstore.common.dto.ObjectListPage;
import com.objectstore.common.dto.ObjectMetadataResponse;
import com.objectstore.common.dto.ObjectStatus;
import com.objectstore.common.dto.ReplicaState;
import com.objectstore.common.dto.ReplicationStatus;
import com.objectstore.common.dto.WorkerStatus;
import com.objectstore.common.error.ApiException;
import com.objectstore.common.error.ErrorCode;
import com.objectstore.common.util.Sha256;
import com.objectstore.controller.client.WorkerClient;
import com.objectstore.controller.config.ControllerProperties;
import com.objectstore.controller.domain.ChunkEntity;
import com.objectstore.controller.domain.ChunkReplicaEntity;
import com.objectstore.controller.domain.ObjectMetadataEntity;
import com.objectstore.controller.domain.PendingDeletionEntity;
import com.objectstore.controller.domain.StoredObjectEntity;
import com.objectstore.controller.domain.WorkerEntity;
import com.objectstore.controller.repository.ChunkReplicaRepository;
import com.objectstore.controller.repository.ChunkRepository;
import com.objectstore.controller.repository.ObjectMetadataRepository;
import com.objectstore.controller.repository.PendingDeletionRepository;
import com.objectstore.controller.repository.StoredObjectRepository;
import com.objectstore.controller.repository.WorkerRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Sort;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionTemplate;
import org.springframework.web.multipart.MultipartFile;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Coordinates object uploads, 1 MiB chunking, replica placement across workers, and metadata persistence.
 */
@Service
public class ObjectService {

    private static final Logger log = LoggerFactory.getLogger(ObjectService.class);

    public static final int MAX_METADATA_KEYS = 64;
    public static final int MAX_METADATA_VALUE_LENGTH = 1024;
    private static final Pattern VALID_METADATA_KEY = Pattern.compile("^[A-Za-z0-9._-]{1,128}$");
    private static final TypeReference<Map<String, Object>> RAW_MAP_TYPE = new TypeReference<>() {
    };
    private static final String EMPTY_SHA256 = Sha256.hash(new byte[0]);

    private final StoredObjectRepository objectRepository;
    private final ObjectMetadataRepository metadataRepository;
    private final ChunkRepository chunkRepository;
    private final ChunkReplicaRepository replicaRepository;
    private final PendingDeletionRepository pendingDeletionRepository;
    private final WorkerRepository workerRepository;
    private final Chunker chunker;
    private final PlacementService placementService;
    private final ReplicationService replicationService;
    private final WorkerClient workerClient;
    private final EventService eventService;
    private final ControllerProperties properties;
    private final ObjectMapper objectMapper;
    private final TransactionTemplate txTemplate;

    public ObjectService(StoredObjectRepository objectRepository,
                         ObjectMetadataRepository metadataRepository,
                         ChunkRepository chunkRepository,
                         ChunkReplicaRepository replicaRepository,
                         PendingDeletionRepository pendingDeletionRepository,
                         WorkerRepository workerRepository,
                         Chunker chunker,
                         PlacementService placementService,
                         ReplicationService replicationService,
                         WorkerClient workerClient,
                         EventService eventService,
                         ControllerProperties properties,
                         ObjectMapper objectMapper,
                         TransactionTemplate txTemplate) {
        this.objectRepository = objectRepository;
        this.metadataRepository = metadataRepository;
        this.chunkRepository = chunkRepository;
        this.replicaRepository = replicaRepository;
        this.pendingDeletionRepository = pendingDeletionRepository;
        this.workerRepository = workerRepository;
        this.chunker = chunker;
        this.placementService = placementService;
        this.replicationService = replicationService;
        this.workerClient = workerClient;
        this.eventService = eventService;
        this.properties = properties;
        this.objectMapper = objectMapper;
        this.txTemplate = txTemplate;
    }

    /**
     * Uploads an arbitrary binary object, splits it into 1 MiB chunks, stores {@code replicationFactor}
     * copies of each chunk on distinct {@code UP} workers, and records all metadata.
     */
    public ObjectInfo uploadObject(MultipartFile file, String rawMetadataJson) {
        if (file == null) {
            throw new ApiException(400, ErrorCode.MISSING_FILE,
                    "Required multipart part 'file' is missing");
        }
        long maxBytes = properties.storage().maxObjectSize().toBytes();
        if (file.getSize() > maxBytes) {
            throw new ApiException(400, ErrorCode.PAYLOAD_TOO_LARGE,
                    "Object size " + file.getSize() + " bytes exceeds maximum allowed " + maxBytes + " bytes");
        }

        Map<String, String> customMetadata = parseAndValidateCreateMetadata(rawMetadataJson);
        String objectName = resolveObjectName(file.getOriginalFilename());
        String contentType = resolveContentType(file.getContentType());

        // Pre-flight check: ensure at least RF workers are UP before creating metadata rows (plan.md D6).
        placementService.requireSufficientUpWorkers();

        UUID objectId = UUID.randomUUID();
        Instant createdAt = Instant.now();

        txTemplate.executeWithoutResult(status -> {
            StoredObjectEntity entity = new StoredObjectEntity(
                    objectId,
                    objectName,
                    0L,
                    contentType,
                    EMPTY_SHA256,
                    0,
                    chunker.getChunkSizeBytes(),
                    ObjectStatus.UPLOADING,
                    createdAt,
                    createdAt);
            objectRepository.save(entity);
            for (Map.Entry<String, String> entry : customMetadata.entrySet()) {
                metadataRepository.save(new ObjectMetadataEntity(objectId, entry.getKey(), entry.getValue()));
            }
        });

        List<WrittenReplica> writtenReplicas = java.util.Collections.synchronizedList(new ArrayList<>());
        try (InputStream in = file.getInputStream()) {
            Chunker.ChunkingResult chunkingResult = chunker.split(objectId, in, slice -> {
                List<WorkerEntity> confirmedWorkers = storeSliceWithFailover(objectId, slice, writtenReplicas);
                persistChunkAndReplicas(objectId, slice, confirmedWorkers);
            });

            Instant completedAt = Instant.now();
            txTemplate.executeWithoutResult(status -> {
                StoredObjectEntity entity = objectRepository.findById(objectId).orElseThrow();
                entity.setSizeBytes(chunkingResult.totalBytes());
                entity.setChecksumSha256(chunkingResult.objectSha256Hex());
                entity.setChunkCount(chunkingResult.chunkCount());
                entity.setStatus(ObjectStatus.ACTIVE);
                entity.setUpdatedAt(completedAt);
                objectRepository.save(entity);
            });

            eventService.record(EventService.OBJECT_UPLOADED, EventSeverity.INFO, objectId, null, null,
                    "Uploaded object '" + objectName + "' (" + chunkingResult.totalBytes() + " bytes, "
                            + chunkingResult.chunkCount() + " chunks, RF=" + placementService.getReplicationFactor() + ")");

            log.info("Completed upload of object {} ('{}', {} bytes, {} chunks, sha256={})",
                    objectId, objectName, chunkingResult.totalBytes(), chunkingResult.chunkCount(),
                    chunkingResult.objectSha256Hex());

            return new ObjectInfo(
                    objectId,
                    objectName,
                    chunkingResult.totalBytes(),
                    contentType,
                    "sha256:" + chunkingResult.objectSha256Hex(),
                    chunkingResult.chunkCount(),
                    createdAt,
                    completedAt,
                    customMetadata);
        } catch (Exception e) {
            rollbackPartialUpload(objectId, objectName, writtenReplicas, e);
            if (e instanceof ApiException apiEx) {
                throw apiEx;
            }
            throw new ApiException(500, ErrorCode.UPLOAD_FAILED,
                    "Failed to upload object '" + objectName + "': " + e.getMessage(),
                    Map.of("objectId", objectId.toString()), e);
        }
    }

    /**
     * Pre-validates an object download request (existence, {@code ACTIVE} status, and pre-flight check
     * that every chunk has at least one {@code HEALTHY} replica on an {@code UP} worker) before any
     * HTTP 200 response headers or bytes are committed to the client (plan.md D16).
     */
    public PreparedDownload prepareDownload(UUID objectId) {
        StoredObjectEntity object = objectRepository.findById(objectId)
                .orElseThrow(() -> new ApiException(404, ErrorCode.OBJECT_NOT_FOUND,
                        "Object " + objectId + " does not exist",
                        Map.of("objectId", objectId.toString()), null));

        if (object.getStatus() != ObjectStatus.ACTIVE) {
            throw new ApiException(409, ErrorCode.OBJECT_NOT_READY,
                    "Object " + objectId + " is currently in state " + object.getStatus() + " and cannot be downloaded",
                    Map.of("objectId", objectId.toString(), "status", object.getStatus().name()), null);
        }

        List<ChunkEntity> chunks = chunkRepository.findByObjectIdOrderByChunkIndexAsc(objectId);
        if (chunks.isEmpty()) {
            return new PreparedDownload(object, List.of());
        }

        List<String> chunkIds = chunks.stream().map(ChunkEntity::getId).toList();
        Map<String, List<ChunkReplicaEntity>> replicasByChunk = replicaRepository
                .findByChunkIdInOrderByChunkIdAscWorkerIdAsc(chunkIds)
                .stream()
                .collect(Collectors.groupingBy(ChunkReplicaEntity::getChunkId));
        Map<String, WorkerEntity> workersById = workerRepository.findAllByOrderByIdAsc().stream()
                .collect(Collectors.toMap(WorkerEntity::getId, Function.identity()));

        List<String> unavailableChunkIds = new ArrayList<>();
        for (ChunkEntity chunk : chunks) {
            List<ChunkReplicaEntity> replicas = replicasByChunk.getOrDefault(chunk.getId(), List.of());
            boolean hasAvailableHealthyReplica = replicas.stream().anyMatch(r -> {
                WorkerEntity w = workersById.get(r.getWorkerId());
                return r.getState() == ReplicaState.HEALTHY && w != null && w.getStatus() == WorkerStatus.UP;
            });
            if (!hasAvailableHealthyReplica) {
                unavailableChunkIds.add(chunk.getId());
            }
        }

        if (!unavailableChunkIds.isEmpty()) {
            throw new ApiException(503, ErrorCode.CHUNK_UNAVAILABLE,
                    "Cannot download object " + objectId + ": " + unavailableChunkIds.size()
                            + " chunk(s) have no healthy replica on an available worker",
                    Map.of("objectId", objectId.toString(), "unavailableChunkIds", unavailableChunkIds),
                    null);
        }

        return new PreparedDownload(object, chunks);
    }

    /**
     * Streams all chunks of a prepared download in {@code chunkIndex} order to {@code outputStream},
     * verifying each chunk's SHA-256 checksum and automatically failing over to surviving replicas
     * if a worker fails or returns corrupted data.
     */
    public void streamDownload(PreparedDownload prepared, OutputStream outputStream) throws IOException {
        for (ChunkEntity chunk : prepared.chunks()) {
            byte[] verifiedBytes = fetchVerifiedChunkWithFailover(prepared.object().getId(), chunk);
            outputStream.write(verifiedBytes);
        }
        outputStream.flush();
    }

    private byte[] fetchVerifiedChunkWithFailover(UUID objectId, ChunkEntity chunk) {
        List<ChunkReplicaEntity> replicas = replicaRepository.findByChunkIdOrderByWorkerIdAsc(chunk.getId());
        Map<String, WorkerEntity> workersById = workerRepository.findAllByOrderByIdAsc().stream()
                .collect(Collectors.toMap(WorkerEntity::getId, Function.identity()));

        List<CandidateReplica> candidates = replicas.stream()
                .filter(r -> r.getState() == ReplicaState.HEALTHY)
                .map(r -> new CandidateReplica(r, workersById.get(r.getWorkerId())))
                .filter(cr -> cr.worker() != null && cr.worker().getStatus() == WorkerStatus.UP)
                .sorted(Comparator
                        .comparingInt((CandidateReplica cr) -> cr.worker().getChunkCount())
                        .thenComparing(cr -> cr.worker().getId()))
                .toList();

        Exception lastError = null;
        for (CandidateReplica candidate : candidates) {
            String workerId = candidate.worker().getId();
            String baseUrl = candidate.worker().getBaseUrl();
            try {
                byte[] bytes = workerClient.getChunk(workerId, baseUrl, chunk.getId());
                String actualHash = Sha256.hash(bytes);
                if (bytes.length != chunk.getSizeBytes()
                        || !actualHash.equalsIgnoreCase(chunk.getChecksumSha256())) {
                    markReplicaCorrupted(objectId, chunk.getId(), candidate.replica(), workerId,
                            "Controller checksum/size verification failed on read (expected sha256="
                                    + chunk.getChecksumSha256() + ", actual=" + actualHash + ")");
                    continue;
                }
                touchReplicaVerified(candidate.replica());
                return bytes;
            } catch (ApiException e) {
                lastError = e;
                if (ErrorCode.CHUNK_CORRUPTED.equals(e.getCode()) || e.getStatus() == 409) {
                    markReplicaCorrupted(objectId, chunk.getId(), candidate.replica(), workerId,
                            "Worker '" + workerId + "' reported chunk " + chunk.getId() + " as corrupted (HTTP 409)");
                } else {
                    log.warn("Failed to read chunk {} from worker '{}' ({}): [{}] {}; trying next replica",
                            chunk.getId(), workerId, baseUrl, e.getCode(), e.getMessage());
                }
            }
        }

        throw new ApiException(503, ErrorCode.CHUNK_UNAVAILABLE,
                "All replicas failed or were corrupted for chunk " + chunk.getId(),
                Map.of("objectId", objectId.toString(), "chunkId", chunk.getId()),
                lastError);
    }

    private void markReplicaCorrupted(UUID objectId,
                                      String chunkId,
                                      ChunkReplicaEntity replica,
                                      String workerId,
                                      String reason) {
        log.warn("Marking replica of chunk {} on worker '{}' as CORRUPTED: {}", chunkId, workerId, reason);
        txTemplate.executeWithoutResult(status ->
                replicaRepository.findById(replica.getId()).ifPresent(entity -> {
                    entity.setState(ReplicaState.CORRUPTED);
                    replicaRepository.save(entity);
                }));
        eventService.record(EventService.CHUNK_CORRUPTED, EventSeverity.WARN, objectId, chunkId, workerId, reason);
    }

    private void touchReplicaVerified(ChunkReplicaEntity replica) {
        Instant now = Instant.now();
        txTemplate.executeWithoutResult(status ->
                replicaRepository.findById(replica.getId()).ifPresent(entity -> {
                    entity.setLastVerifiedAt(now);
                    replicaRepository.save(entity);
                }));
    }

    /**
     * §5.3 Get Object Metadata — retrieves complete system and custom metadata (including ordered chunks
     * and replica locations) without downloading chunk data.
     */
    @Transactional(readOnly = true)
    public ObjectMetadataResponse getObjectMetadata(UUID objectId) {
        StoredObjectEntity object = getRequiredActiveObject(objectId);
        return buildObjectMetadataResponse(object);
    }

    /**
     * §5.4 Update Object Metadata — merges custom metadata (JSON Merge Patch style, plan.md D12):
     * non-null string values add or update keys, {@code null} values delete keys, and omitted keys
     * remain unchanged. Does not re-upload or touch chunk data.
     */
    @Transactional
    public ObjectMetadataResponse updateCustomMetadata(UUID objectId, Map<String, Object> patch) {
        if (patch == null) {
            throw new ApiException(400, ErrorCode.INVALID_METADATA, "Metadata patch body must be a JSON object");
        }
        StoredObjectEntity object = getRequiredActiveObject(objectId);

        Map<String, ObjectMetadataEntity> existingByKey = metadataRepository
                .findByIdObjectIdOrderByIdMetaKeyAsc(objectId)
                .stream()
                .collect(Collectors.toMap(ObjectMetadataEntity::getMetaKey, Function.identity(),
                        (a, b) -> a, LinkedHashMap::new));

        for (Map.Entry<String, Object> entry : patch.entrySet()) {
            String key = entry.getKey();
            Object rawValue = entry.getValue();
            validateMetadataKey(key);
            if (rawValue == null) {
                ObjectMetadataEntity removed = existingByKey.remove(key);
                if (removed != null) {
                    metadataRepository.delete(removed);
                }
            } else if (rawValue instanceof String strVal) {
                validateMetadataValue(key, strVal);
                ObjectMetadataEntity existing = existingByKey.get(key);
                if (existing != null) {
                    existing.setMetaValue(strVal);
                    metadataRepository.save(existing);
                } else {
                    ObjectMetadataEntity created = new ObjectMetadataEntity(objectId, key, strVal);
                    metadataRepository.save(created);
                    existingByKey.put(key, created);
                }
            } else {
                throw new ApiException(400, ErrorCode.INVALID_METADATA,
                        "Metadata value for key '" + key + "' must be a string or null");
            }
        }

        if (existingByKey.size() > MAX_METADATA_KEYS) {
            throw new ApiException(400, ErrorCode.INVALID_METADATA,
                    "Total custom metadata keys (" + existingByKey.size() + ") exceeds maximum " + MAX_METADATA_KEYS);
        }

        object.setUpdatedAt(Instant.now());
        objectRepository.save(object);

        eventService.record(EventService.OBJECT_METADATA_UPDATED, EventSeverity.INFO, objectId, null, null,
                "Updated custom metadata for object '" + object.getName() + "' (" + existingByKey.size() + " keys)");

        return buildObjectMetadataResponse(object);
    }

    /**
     * §5.5 Delete Object — deletes an object, its metadata, and all stored chunk replicas across workers.
     * Replicas residing on currently {@code DOWN} or unreachable workers are queued in {@code pending_deletions}
     * for deferred removal upon worker rejoin (plan.md D15).
     */
    public void deleteObject(UUID objectId) {
        StoredObjectEntity object = txTemplate.execute(status -> {
            StoredObjectEntity found = objectRepository.findById(objectId)
                    .orElseThrow(() -> new ApiException(404, ErrorCode.OBJECT_NOT_FOUND,
                            "Object " + objectId + " does not exist",
                            Map.of("objectId", objectId.toString()), null));
            if (found.getStatus() != ObjectStatus.ACTIVE) {
                throw new ApiException(409, ErrorCode.OBJECT_NOT_READY,
                        "Object " + objectId + " is in state " + found.getStatus() + " and cannot be deleted",
                        Map.of("objectId", objectId.toString(), "status", found.getStatus().name()), null);
            }
            found.setStatus(ObjectStatus.DELETING);
            found.setUpdatedAt(Instant.now());
            return objectRepository.save(found);
        });

        List<ChunkEntity> chunks = chunkRepository.findByObjectIdOrderByChunkIndexAsc(objectId);
        List<String> chunkIds = chunks.stream().map(ChunkEntity::getId).toList();
        Map<String, ChunkEntity> chunksById = chunks.stream()
                .collect(Collectors.toMap(ChunkEntity::getId, Function.identity()));
        List<ChunkReplicaEntity> replicas = chunkIds.isEmpty()
                ? List.of()
                : replicaRepository.findByChunkIdInOrderByChunkIdAscWorkerIdAsc(chunkIds);
        Map<String, WorkerEntity> workersById = workerRepository.findAllByOrderByIdAsc().stream()
                .collect(Collectors.toMap(WorkerEntity::getId, Function.identity()));

        for (ChunkReplicaEntity replica : replicas) {
            WorkerEntity worker = workersById.get(replica.getWorkerId());
            ChunkEntity chunk = chunksById.get(replica.getChunkId());
            int chunkBytes = chunk != null ? chunk.getSizeBytes() : 0;

            if (worker != null && worker.getStatus() == WorkerStatus.UP) {
                try {
                    workerClient.deleteChunk(worker.getId(), worker.getBaseUrl(), replica.getChunkId());
                    decrementWorkerCounters(worker.getId(), chunkBytes);
                } catch (Exception e) {
                    log.warn("Could not delete chunk {} from UP worker '{}'; queueing pending deletion: {}",
                            replica.getChunkId(), worker.getId(), e.getMessage());
                    queuePendingDeletion(replica.getChunkId(), worker.getId());
                }
            } else if (worker != null) {
                queuePendingDeletion(replica.getChunkId(), worker.getId());
            }
        }

        txTemplate.executeWithoutResult(status -> {
            if (!chunkIds.isEmpty()) {
                replicaRepository.deleteByChunkIdIn(chunkIds);
            }
            chunkRepository.deleteByObjectId(objectId);
            metadataRepository.deleteByIdObjectId(objectId);
            objectRepository.deleteById(objectId);
        });

        eventService.record(EventService.OBJECT_DELETED, EventSeverity.INFO, objectId, null, null,
                "Deleted object '" + object.getName() + "' (" + chunks.size() + " chunks)");
    }

    /**
     * §5.6 List Objects — returns a paginated list of currently stored ({@code ACTIVE}) objects.
     */
    @Transactional(readOnly = true)
    public ObjectListPage listObjects(int page, int size, String sortSpec) {
        if (page < 0) {
            throw new ApiException(400, ErrorCode.VALIDATION_ERROR, "Page index must not be negative: " + page);
        }
        if (size < 1 || size > 200) {
            throw new ApiException(400, ErrorCode.VALIDATION_ERROR, "Page size must be between 1 and 200: " + size);
        }

        Sort sort = parseSort(sortSpec);
        Page<StoredObjectEntity> objectPage = objectRepository.findByStatus(
                ObjectStatus.ACTIVE, PageRequest.of(page, size, sort));

        List<StoredObjectEntity> entities = objectPage.getContent();
        List<UUID> objectIds = entities.stream().map(StoredObjectEntity::getId).toList();
        Map<UUID, Map<String, String>> metadataByObject = new LinkedHashMap<>();
        if (!objectIds.isEmpty()) {
            for (ObjectMetadataEntity meta : metadataRepository.findByIdObjectIdInOrderByIdObjectIdAscIdMetaKeyAsc(objectIds)) {
                metadataByObject
                        .computeIfAbsent(meta.getObjectId(), k -> new LinkedHashMap<>())
                        .put(meta.getMetaKey(), meta.getMetaValue());
            }
        }

        List<ObjectInfo> items = entities.stream()
                .map(obj -> new ObjectInfo(
                        obj.getId(),
                        obj.getName(),
                        obj.getSizeBytes(),
                        obj.getContentType(),
                        "sha256:" + obj.getChecksumSha256(),
                        obj.getChunkCount(),
                        obj.getCreatedAt(),
                        obj.getUpdatedAt(),
                        metadataByObject.getOrDefault(obj.getId(), Map.of())))
                .toList();

        return new ObjectListPage(
                items,
                objectPage.getNumber(),
                objectPage.getSize(),
                objectPage.getTotalElements(),
                objectPage.getTotalPages());
    }

    private StoredObjectEntity getRequiredActiveObject(UUID objectId) {
        StoredObjectEntity object = objectRepository.findById(objectId)
                .orElseThrow(() -> new ApiException(404, ErrorCode.OBJECT_NOT_FOUND,
                        "Object " + objectId + " does not exist",
                        Map.of("objectId", objectId.toString()), null));
        if (object.getStatus() != ObjectStatus.ACTIVE) {
            throw new ApiException(409, ErrorCode.OBJECT_NOT_READY,
                    "Object " + objectId + " is currently in state " + object.getStatus(),
                    Map.of("objectId", objectId.toString(), "status", object.getStatus().name()), null);
        }
        return object;
    }

    private ObjectMetadataResponse buildObjectMetadataResponse(StoredObjectEntity object) {
        UUID objectId = object.getId();
        Map<String, String> customMeta = new LinkedHashMap<>();
        for (ObjectMetadataEntity meta : metadataRepository.findByIdObjectIdOrderByIdMetaKeyAsc(objectId)) {
            customMeta.put(meta.getMetaKey(), meta.getMetaValue());
        }

        List<ChunkEntity> chunks = chunkRepository.findByObjectIdOrderByChunkIndexAsc(objectId);
        List<String> chunkIds = chunks.stream().map(ChunkEntity::getId).toList();
        Map<String, List<ChunkReplicaEntity>> replicasByChunk = chunkIds.isEmpty()
                ? Map.of()
                : replicaRepository.findByChunkIdInOrderByChunkIdAscWorkerIdAsc(chunkIds).stream()
                        .collect(Collectors.groupingBy(ChunkReplicaEntity::getChunkId));
        Map<String, WorkerEntity> workersById = workerRepository.findAllByOrderByIdAsc().stream()
                .collect(Collectors.toMap(WorkerEntity::getId, Function.identity()));

        List<ChunkMetadataInfo> chunkDtos = new ArrayList<>(chunks.size());
        boolean anyUnavailable = false;
        boolean anyDegraded = false;

        for (ChunkEntity chunk : chunks) {
            List<ChunkReplicaEntity> replicas = replicasByChunk.getOrDefault(chunk.getId(), List.of());
            ReplicationStatus chunkStatus = replicationService.evaluateChunkReplication(replicas, workersById);
            if (chunkStatus == ReplicationStatus.UNAVAILABLE) {
                anyUnavailable = true;
            } else if (chunkStatus == ReplicationStatus.DEGRADED) {
                anyDegraded = true;
            }

            List<ChunkReplicaInfo> replicaDtos = replicas.stream()
                    .map(r -> {
                        WorkerEntity w = workersById.get(r.getWorkerId());
                        WorkerStatus workerStatus = w != null ? w.getStatus() : WorkerStatus.DOWN;
                        return new ChunkReplicaInfo(r.getWorkerId(), r.getState(), workerStatus, r.getLastVerifiedAt());
                    })
                    .toList();

            chunkDtos.add(new ChunkMetadataInfo(
                    chunk.getId(),
                    chunk.getChunkIndex(),
                    chunk.getSizeBytes(),
                    "sha256:" + chunk.getChecksumSha256(),
                    chunkStatus,
                    replicaDtos));
        }

        ReplicationStatus objectReplicationStatus;
        if (anyUnavailable) {
            objectReplicationStatus = ReplicationStatus.UNAVAILABLE;
        } else if (anyDegraded) {
            objectReplicationStatus = ReplicationStatus.DEGRADED;
        } else {
            objectReplicationStatus = ReplicationStatus.HEALTHY;
        }

        return new ObjectMetadataResponse(
                object.getId(),
                object.getName(),
                object.getSizeBytes(),
                object.getContentType(),
                "sha256:" + object.getChecksumSha256(),
                object.getChunkCount(),
                object.getChunkSize(),
                properties.storage().replicationFactor(),
                objectReplicationStatus,
                object.getCreatedAt(),
                object.getUpdatedAt(),
                customMeta,
                chunkDtos);
    }

    private void decrementWorkerCounters(String workerId, int chunkBytes) {
        txTemplate.executeWithoutResult(status ->
                workerRepository.findById(workerId).ifPresent(w -> {
                    w.setChunkCount(Math.max(0, w.getChunkCount() - 1));
                    w.setUsedBytes(Math.max(0L, w.getUsedBytes() - chunkBytes));
                    w.setFreeBytes(w.getFreeBytes() + chunkBytes);
                    workerRepository.save(w);
                }));
    }

    private void queuePendingDeletion(String chunkId, String workerId) {
        txTemplate.executeWithoutResult(status -> {
            if (pendingDeletionRepository.findByChunkIdAndWorkerId(chunkId, workerId).isEmpty()) {
                pendingDeletionRepository.save(new PendingDeletionEntity(chunkId, workerId, Instant.now()));
            }
        });
    }

    private static Sort parseSort(String sortSpec) {
        if (sortSpec == null || sortSpec.isBlank()) {
            return Sort.by(Sort.Direction.DESC, "createdAt");
        }
        String[] parts = sortSpec.split(",", 2);
        String rawField = parts[0].trim();
        String property = switch (rawField) {
            case "createdAt" -> "createdAt";
            case "updatedAt" -> "updatedAt";
            case "name" -> "name";
            case "size", "sizeBytes" -> "sizeBytes";
            default -> throw new ApiException(400, ErrorCode.VALIDATION_ERROR,
                    "Unsupported sort field '" + rawField + "'; allowed: createdAt, updatedAt, name, size");
        };
        Sort.Direction direction = Sort.Direction.DESC;
        if (parts.length > 1 && !parts[1].isBlank()) {
            String dir = parts[1].trim();
            if ("asc".equalsIgnoreCase(dir)) {
                direction = Sort.Direction.ASC;
            } else if ("desc".equalsIgnoreCase(dir)) {
                direction = Sort.Direction.DESC;
            } else {
                throw new ApiException(400, ErrorCode.VALIDATION_ERROR,
                        "Unsupported sort direction '" + dir + "'; allowed: asc, desc");
            }
        }
        return Sort.by(direction, property);
    }

    /**
     * Writes a chunk slice in parallel to {@code replicationFactor} distinct {@code UP} workers.
     * If a target worker fails during the PUT, tries replacement {@code UP} workers not yet attempted.
     */
    private List<WorkerEntity> storeSliceWithFailover(UUID objectId,
                                                      Chunker.ChunkSlice slice,
                                                      List<WrittenReplica> writtenReplicas) {
        int rf = placementService.getReplicationFactor();
        List<WorkerEntity> initialTargets = placementService.selectWorkersForChunk();
        Set<String> attemptedWorkerIds = new HashSet<>();
        for (WorkerEntity target : initialTargets) {
            attemptedWorkerIds.add(target.getId());
        }

        List<WorkerEntity> confirmed = new ArrayList<>(rf);
        Exception lastFailure = null;

        try (ExecutorService vExecutor = Executors.newVirtualThreadPerTaskExecutor()) {
            List<CompletableFuture<Boolean>> futures = initialTargets.stream()
                    .map(target -> CompletableFuture.supplyAsync(
                            () -> tryWriteReplica(objectId, slice, target, writtenReplicas), vExecutor))
                    .toList();
            for (int i = 0; i < initialTargets.size(); i++) {
                if (Boolean.TRUE.equals(futures.get(i).join())) {
                    confirmed.add(initialTargets.get(i));
                }
            }
        }

        while (confirmed.size() < rf) {
            Optional<WorkerEntity> replacementOpt = placementService.selectReplacementWorker(attemptedWorkerIds);
            if (replacementOpt.isEmpty()) {
                break;
            }
            WorkerEntity replacement = replacementOpt.get();
            attemptedWorkerIds.add(replacement.getId());
            try {
                if (tryWriteReplica(objectId, slice, replacement, writtenReplicas)) {
                    confirmed.add(replacement);
                }
            } catch (Exception ex) {
                lastFailure = ex;
            }
        }

        if (confirmed.size() < rf) {
            throw new ApiException(503, ErrorCode.INSUFFICIENT_STORAGE_NODES,
                    "Could not store " + rf + " replicas for chunk " + slice.chunkId()
                            + " (only " + confirmed.size() + " succeeded)",
                    Map.of("chunkId", slice.chunkId(), "requiredReplicas", rf, "storedReplicas", confirmed.size()),
                    lastFailure);
        }
        return confirmed;
    }

    private boolean tryWriteReplica(UUID objectId,
                                    Chunker.ChunkSlice slice,
                                    WorkerEntity target,
                                    List<WrittenReplica> writtenReplicas) {
        try {
            workerClient.putChunk(
                    target.getId(),
                    target.getBaseUrl(),
                    slice.chunkId(),
                    slice.data(),
                    slice.sha256Hex(),
                    objectId.toString(),
                    slice.index(),
                    false);
            writtenReplicas.add(new WrittenReplica(slice.chunkId(), target.getId(), target.getBaseUrl()));
            return true;
        } catch (ApiException e) {
            log.warn("Replica write of chunk {} to worker '{}' ({}) failed: [{}] {}",
                    slice.chunkId(), target.getId(), target.getBaseUrl(), e.getCode(), e.getMessage());
            return false;
        }
    }

    private void persistChunkAndReplicas(UUID objectId,
                                         Chunker.ChunkSlice slice,
                                         List<WorkerEntity> confirmedWorkers) {
        Instant now = Instant.now();
        txTemplate.executeWithoutResult(status -> {
            chunkRepository.save(new ChunkEntity(
                    slice.chunkId(),
                    objectId,
                    slice.index(),
                    slice.size(),
                    slice.sha256Hex()));
            for (WorkerEntity w : confirmedWorkers) {
                replicaRepository.save(new ChunkReplicaEntity(
                        slice.chunkId(),
                        w.getId(),
                        ReplicaState.HEALTHY,
                        now,
                        now));
                workerRepository.findById(w.getId()).ifPresent(workerEntity -> {
                    workerEntity.setChunkCount(workerEntity.getChunkCount() + 1);
                    workerEntity.setUsedBytes(workerEntity.getUsedBytes() + slice.size());
                    workerEntity.setFreeBytes(Math.max(0L, workerEntity.getFreeBytes() - slice.size()));
                    workerRepository.save(workerEntity);
                });
            }
        });
    }

    private void rollbackPartialUpload(UUID objectId,
                                       String objectName,
                                       List<WrittenReplica> writtenReplicas,
                                       Exception cause) {
        log.warn("Rolling back partial upload for object {} ('{}') after failure: {}",
                objectId, objectName, cause.getMessage());
        for (WrittenReplica replica : writtenReplicas) {
            try {
                workerClient.deleteChunk(replica.workerId(), replica.workerBaseUrl(), replica.chunkId());
            } catch (Exception cleanupEx) {
                log.debug("Best-effort cleanup of chunk {} on worker {} failed: {}",
                        replica.chunkId(), replica.workerId(), cleanupEx.getMessage());
            }
        }
        try {
            txTemplate.executeWithoutResult(status -> {
                List<String> chunkIds = chunkRepository.findByObjectIdOrderByChunkIndexAsc(objectId).stream()
                        .map(ChunkEntity::getId)
                        .toList();
                if (!chunkIds.isEmpty()) {
                    replicaRepository.deleteByChunkIdIn(chunkIds);
                }
                chunkRepository.deleteByObjectId(objectId);
                metadataRepository.deleteByIdObjectId(objectId);
                objectRepository.deleteById(objectId);
            });
            eventService.record(EventService.OBJECT_UPLOAD_FAILED, EventSeverity.WARN, objectId, null, null,
                    "Upload of object '" + objectName + "' failed and was rolled back: " + cause.getMessage());
        } catch (Exception dbEx) {
            log.error("Failed to clean up metadata for aborted upload {}", objectId, dbEx);
        }
    }

    /**
     * Parses and validates custom metadata provided at upload time (all values must be non-null strings).
     */
    public Map<String, String> parseAndValidateCreateMetadata(String rawMetadataJson) {
        if (rawMetadataJson == null || rawMetadataJson.isBlank()) {
            return Map.of();
        }
        final Map<String, Object> parsed;
        try {
            parsed = objectMapper.readValue(rawMetadataJson, RAW_MAP_TYPE);
        } catch (IOException e) {
            throw new ApiException(400, ErrorCode.INVALID_METADATA,
                    "Invalid 'metadata' JSON: must be a JSON object of string key-value pairs");
        }
        if (parsed == null) {
            return Map.of();
        }
        if (parsed.size() > MAX_METADATA_KEYS) {
            throw new ApiException(400, ErrorCode.INVALID_METADATA,
                    "Too many metadata keys: maximum is " + MAX_METADATA_KEYS);
        }
        Map<String, String> validated = new LinkedHashMap<>();
        for (Map.Entry<String, Object> entry : parsed.entrySet()) {
            String key = entry.getKey();
            Object rawVal = entry.getValue();
            validateMetadataKey(key);
            if (!(rawVal instanceof String strVal)) {
                throw new ApiException(400, ErrorCode.INVALID_METADATA,
                        "Metadata value for key '" + key + "' must be a string");
            }
            validateMetadataValue(key, strVal);
            validated.put(key, strVal);
        }
        return validated;
    }

    public static void validateMetadataKey(String key) {
        if (key == null || !VALID_METADATA_KEY.matcher(key).matches()) {
            throw new ApiException(400, ErrorCode.INVALID_METADATA,
                    "Invalid metadata key '" + key + "': must be 1-128 chars of [A-Za-z0-9._-]");
        }
    }

    public static void validateMetadataValue(String key, String value) {
        if (value.length() > MAX_METADATA_VALUE_LENGTH) {
            throw new ApiException(400, ErrorCode.INVALID_METADATA,
                    "Metadata value for key '" + key + "' exceeds " + MAX_METADATA_VALUE_LENGTH + " characters");
        }
    }

    private static String resolveObjectName(String originalFilename) {
        if (originalFilename == null || originalFilename.isBlank()) {
            return "unnamed-object";
        }
        String trimmed = originalFilename.trim().replace('\\', '/');
        int lastSlash = trimmed.lastIndexOf('/');
        String base = lastSlash >= 0 ? trimmed.substring(lastSlash + 1) : trimmed;
        if (base.isBlank()) {
            return "unnamed-object";
        }
        return base.length() > 512 ? base.substring(0, 512) : base;
    }

    private static String resolveContentType(String rawContentType) {
        if (rawContentType == null || rawContentType.isBlank()) {
            return MediaType.APPLICATION_OCTET_STREAM_VALUE;
        }
        return rawContentType.trim();
    }

    public record PreparedDownload(StoredObjectEntity object, List<ChunkEntity> chunks) {
    }

    private record CandidateReplica(ChunkReplicaEntity replica, WorkerEntity worker) {
    }

    private record WrittenReplica(String chunkId, String workerId, String workerBaseUrl) {
    }
}

