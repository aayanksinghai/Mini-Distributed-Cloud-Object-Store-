package com.objectstore.worker.api;

import com.objectstore.common.dto.StoredChunkInfo;
import com.objectstore.common.dto.WorkerChunksResponse;
import com.objectstore.common.error.ApiException;
import com.objectstore.common.error.ErrorCode;
import com.objectstore.common.http.InternalHeaders;
import com.objectstore.worker.config.WorkerProperties;
import com.objectstore.worker.storage.ChunkMetadata;
import com.objectstore.worker.storage.ChunkStore;
import jakarta.servlet.http.HttpServletRequest;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;

/**
 * Internal worker REST endpoints for storing, retrieving, deleting and listing chunks (PDF §6).
 */
@RestController
public class ChunkController {

    private final ChunkStore chunkStore;
    private final WorkerProperties workerProperties;

    public ChunkController(ChunkStore chunkStore, WorkerProperties workerProperties) {
        this.chunkStore = chunkStore;
        this.workerProperties = workerProperties;
    }

    @PutMapping(value = "/internal/chunks/{chunkId}", produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<StoredChunkInfo> putChunk(
            @PathVariable String chunkId,
            @RequestHeader(InternalHeaders.CHUNK_CHECKSUM) String expectedChecksum,
            @RequestHeader(value = InternalHeaders.OBJECT_ID, required = false) String objectId,
            @RequestHeader(value = InternalHeaders.CHUNK_INDEX, required = false) Integer chunkIndex,
            @RequestParam(value = "overwrite", defaultValue = "false") boolean overwrite,
            HttpServletRequest request) throws IOException {

        try (InputStream body = request.getInputStream()) {
            ChunkStore.WriteResult writeResult = chunkStore.put(
                    chunkId, body, expectedChecksum, objectId, chunkIndex, overwrite);
            ChunkMetadata meta = writeResult.metadata();
            StoredChunkInfo dto = new StoredChunkInfo(
                    meta.chunkId(),
                    meta.objectId(),
                    meta.chunkIndex(),
                    meta.size(),
                    meta.sha256(),
                    meta.storedAt(),
                    true);
            HttpStatus status = writeResult.created() ? HttpStatus.CREATED : HttpStatus.OK;
            return ResponseEntity.status(status)
                    .header(InternalHeaders.CHUNK_CHECKSUM, meta.sha256())
                    .body(dto);
        }
    }

    @GetMapping(value = "/internal/chunks/{chunkId}")
    public ResponseEntity<byte[]> getChunk(@PathVariable String chunkId) {
        ChunkStore.ReadResult result = chunkStore.get(chunkId);
        return ResponseEntity.ok()
                .contentType(MediaType.APPLICATION_OCTET_STREAM)
                .contentLength(result.data().length)
                .header(InternalHeaders.CHUNK_CHECKSUM, result.metadata().sha256())
                .body(result.data());
    }

    @DeleteMapping(value = "/internal/chunks/{chunkId}")
    public ResponseEntity<Void> deleteChunk(@PathVariable String chunkId) {
        chunkStore.delete(chunkId);
        return ResponseEntity.noContent().build();
    }

    @GetMapping(value = "/internal/workers/{workerId}/chunks", produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<WorkerChunksResponse> listWorkerChunks(
            @PathVariable String workerId,
            @RequestParam(value = "verify", defaultValue = "false") boolean verify) {
        if (!workerProperties.id().equals(workerId)) {
            throw new ApiException(404, ErrorCode.UNKNOWN_WORKER,
                    "This worker is '" + workerProperties.id() + "', not '" + workerId + "'",
                    Map.of("workerId", workerId, "selfWorkerId", workerProperties.id()), null);
        }
        List<StoredChunkInfo> chunks = chunkStore.listChunks(verify);
        long usedBytes = chunks.stream().mapToLong(StoredChunkInfo::size).sum();
        return ResponseEntity.ok(new WorkerChunksResponse(workerProperties.id(), chunks.size(), usedBytes, chunks));
    }
}

