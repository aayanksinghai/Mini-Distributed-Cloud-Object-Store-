package com.objectstore.worker.api;

import com.objectstore.common.error.ApiException;
import com.objectstore.common.error.ErrorCode;
import com.objectstore.common.util.Sha256;
import com.objectstore.worker.config.WorkerProperties;
import com.objectstore.worker.integrity.Scrubber;
import com.objectstore.worker.storage.ChunkStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Map;

/**
 * Opt-in debug endpoint (`POST /internal/debug/chunks/{chunkId}/corrupt`) for intentionally corrupting
 * a stored chunk file on disk during demonstrations (PDF §8 & §10.6, plan.md D10).
 * <p>
 * Disabled by default ({@code WORKER_DEBUG_ENDPOINTS=false}); returns 404 when disabled.
 */
@RestController
public class DebugController {

    private static final Logger log = LoggerFactory.getLogger(DebugController.class);

    private final ChunkStore chunkStore;
    private final WorkerProperties workerProperties;
    private final Scrubber scrubber;

    public DebugController(ChunkStore chunkStore, WorkerProperties workerProperties, Scrubber scrubber) {
        this.chunkStore = chunkStore;
        this.workerProperties = workerProperties;
        this.scrubber = scrubber;
    }

    @PostMapping(value = "/internal/debug/chunks/{chunkId}/corrupt", produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<Map<String, Object>> corruptChunk(@PathVariable String chunkId) {
        if (!workerProperties.debugEndpoints()) {
            throw new ApiException(404, ErrorCode.VALIDATION_ERROR,
                    "Worker debug endpoints are disabled (set WORKER_DEBUG_ENDPOINTS=true to enable)");
        }
        Path chunkFile = chunkStore.chunkFilePath(chunkId);
        if (!Files.exists(chunkFile)) {
            throw new ApiException(404, ErrorCode.CHUNK_NOT_FOUND, "Chunk " + chunkId + " does not exist on " + workerProperties.id());
        }
        try {
            byte[] bytes = Files.readAllBytes(chunkFile);
            String beforeSha = Sha256.hash(bytes);
            if (bytes.length == 0) {
                bytes = new byte[]{(byte) 0xFF};
            } else {
                // Flip bits in the first up-to-16 bytes so SHA-256 is guaranteed to mismatch
                int flipCount = Math.min(16, bytes.length);
                for (int i = 0; i < flipCount; i++) {
                    bytes[i] = (byte) (bytes[i] ^ 0xFF);
                }
            }
            Files.write(chunkFile, bytes, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING);
            String afterSha = Sha256.hash(bytes);
            scrubber.recordCorruptChunk(chunkId);
            log.warn("DEBUG: Intentionally corrupted chunk {} on worker '{}' (beforeSha={}, afterSha={})",
                    chunkId, workerProperties.id(), beforeSha, afterSha);
            return ResponseEntity.ok(Map.of(
                    "workerId", workerProperties.id(),
                    "chunkId", chunkId,
                    "corrupted", true,
                    "previousChecksum", beforeSha,
                    "corruptedChecksum", afterSha));
        } catch (IOException e) {
            throw new ApiException(500, ErrorCode.STORAGE_IO_ERROR,
                    "Failed to corrupt chunk " + chunkId + ": " + e.getMessage(),
                    Map.of("chunkId", chunkId), e);
        }
    }
}

