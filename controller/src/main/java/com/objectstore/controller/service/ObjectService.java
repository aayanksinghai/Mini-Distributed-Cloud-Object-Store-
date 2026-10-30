package com.objectstore.controller.service;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.objectstore.common.dto.EventSeverity;
import com.objectstore.common.dto.ObjectInfo;
import com.objectstore.common.dto.ObjectStatus;
import com.objectstore.common.dto.ReplicaState;
import com.objectstore.common.dto.WorkerStatus;
import com.objectstore.common.error.ApiException;
import com.objectstore.common.error.ErrorCode;
import com.objectstore.common.util.Sha256;
import com.objectstore.controller.client.WorkerClient;
import com.objectstore.controller.config.ControllerProperties;
import com.objectstore.controller.domain.ChunkEntity;
import com.objectstore.controller.domain.ChunkReplicaEntity;
import com.objectstore.controller.domain.ObjectMetadataEntity;
import com.objectstore.controller.domain.StoredObjectEntity;
import com.objectstore.controller.domain.WorkerEntity;
import com.objectstore.controller.repository.ChunkReplicaRepository;
import com.objectstore.controller.repository.ChunkRepository;
import com.objectstore.controller.repository.ObjectMetadataRepository;
import com.objectstore.controller.repository.StoredObjectRepository;
import com.objectstore.controller.repository.WorkerRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Service;
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
    private final WorkerRepository workerRepository;
    private final Chunker chunker;
    private final PlacementService placementService;
    private final WorkerClient workerClient;
    private final EventService eventService;
    private final ControllerProperties properties;
    private final ObjectMapper objectMapper;
    private final TransactionTemplate txTemplate;

    public ObjectService(StoredObjectRepository objectRepository,
                         ObjectMetadataRepository metadataRepository,
                         ChunkRepository chunkRepository,
                         ChunkReplicaRepository replicaRepository,
                         WorkerRepository workerRepository,
                         Chunker chunker,
                         PlacementService placementService,
                         WorkerClient workerClient,
                         EventService eventService,
                         ControllerProperties properties,
                         ObjectMapper objectMapper,
                         TransactionTemplate txTemplate) {
        this.objectRepository = objectRepository;
        this.metadataRepository = metadataRepository;
        this.chunkRepository = chunkRepository;
        this.replicaRepository = replicaRepository;
        this.workerRepository = workerRepository;
        this.chunker = chunker;
        this.placementService = placementService;
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

        List<WrittenReplica> writtenReplicas = new ArrayList<>();
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
     * Writes a chunk slice to {@code replicationFactor} distinct {@code UP} workers.
     * If a target worker fails during the PUT, tries replacement {@code UP} workers not yet attempted.
     */
    private List<WorkerEntity> storeSliceWithFailover(UUID objectId,
                                                      Chunker.ChunkSlice slice,
                                                      List<WrittenReplica> writtenReplicas) {
        int rf = placementService.getReplicationFactor();
        List<WorkerEntity> initialTargets = placementService.selectWorkersForChunk();
        Set<String> attemptedWorkerIds = new HashSet<>();
        List<WorkerEntity> confirmed = new ArrayList<>(rf);
        Exception lastFailure = null;

        for (WorkerEntity target : initialTargets) {
            attemptedWorkerIds.add(target.getId());
            if (tryWriteReplica(objectId, slice, target, writtenReplicas)) {
                confirmed.add(target);
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

