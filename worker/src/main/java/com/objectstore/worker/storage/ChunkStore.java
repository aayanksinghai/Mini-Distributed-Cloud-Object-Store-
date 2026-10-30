package com.objectstore.worker.storage;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.objectstore.common.dto.StoredChunkInfo;
import com.objectstore.common.error.ApiException;
import com.objectstore.common.error.ErrorCode;
import com.objectstore.common.util.Sha256;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.channels.FileChannel;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Stream;

/**
 * Filesystem-backed store for chunk data and sidecar metadata on the worker's persistent volume.
 * <p>
 * Layout: {@code <root>/<shard>/<chunkId>.chunk} and {@code <root>/<shard>/<chunkId>.meta},
 * where {@code <shard>} is the first 2 characters of {@code chunkId} (lowercase).
 */
@Component
public class ChunkStore {

    private static final Logger log = LoggerFactory.getLogger(ChunkStore.class);

    /**
     * Strict whitelist for chunk identifiers (prevents path traversal).
     * Allows deterministic IDs like {@code <uuid>_00000} as well as alphanumeric/hyphen/underscore IDs.
     */
    private static final Pattern VALID_CHUNK_ID = Pattern.compile("^[A-Za-z0-9][A-Za-z0-9_-]{1,127}$");

    private final StorageDirectory storageDirectory;
    private final ObjectMapper objectMapper;

    public ChunkStore(StorageDirectory storageDirectory, ObjectMapper objectMapper) {
        this.storageDirectory = storageDirectory;
        this.objectMapper = objectMapper;
    }

    public static void validateChunkId(String chunkId) {
        if (chunkId == null || !VALID_CHUNK_ID.matcher(chunkId).matches()) {
            throw new ApiException(400, ErrorCode.INVALID_CHUNK_ID,
                    "Invalid chunkId '" + chunkId + "': must be 2-128 chars of [A-Za-z0-9_-]");
        }
    }

    /**
     * Stores a chunk atomically, verifying its SHA-256 checksum on write.
     *
     * @param chunkId          unique chunk identifier
     * @param dataStream       raw chunk bytes
     * @param expectedChecksum expected SHA-256 hex digest (from {@code X-Chunk-Checksum})
     * @param objectId         optional owning object ID
     * @param chunkIndex       optional zero-based index within the object
     * @param overwrite        when true, replaces an existing chunk even if its content/checksum differs
     */
    public WriteResult put(String chunkId,
                           InputStream dataStream,
                           String expectedChecksum,
                           String objectId,
                           Integer chunkIndex,
                           boolean overwrite) {
        validateChunkId(chunkId);
        final String expectedHex;
        try {
            expectedHex = Sha256.normalizeHex(expectedChecksum);
        } catch (IllegalArgumentException e) {
            throw new ApiException(400, ErrorCode.VALIDATION_ERROR,
                    "Missing or invalid X-Chunk-Checksum header: " + e.getMessage());
        }

        Path shardDir = shardDir(chunkId);
        Path chunkFile = shardDir.resolve(chunkId + ".chunk");
        Path metaFile = shardDir.resolve(chunkId + ".meta");
        Path tempChunk = null;
        Path tempMeta = null;

        try {
            Files.createDirectories(shardDir);
            tempChunk = Files.createTempFile(shardDir, chunkId + ".", ".chunk.tmp");

            Sha256.StreamHashResult hashResult;
            try (OutputStream out = Files.newOutputStream(tempChunk,
                    StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING)) {
                hashResult = Sha256.copyAndHash(dataStream, out);
            }
            fsyncFile(tempChunk);

            if (!expectedHex.equals(hashResult.sha256Hex())) {
                deleteQuietly(tempChunk);
                throw new ApiException(400, ErrorCode.CHECKSUM_MISMATCH,
                        "Chunk checksum mismatch for " + chunkId + ": expected " + expectedHex
                                + " but computed " + hashResult.sha256Hex(),
                        Map.of("chunkId", chunkId, "expected", expectedHex, "actual", hashResult.sha256Hex()), null);
            }

            // Check if the chunk already exists on disk.
            if (Files.exists(chunkFile) && !overwrite) {
                ChunkMetadata existingMeta = readMetadataOrNull(metaFile);
                String existingHash = existingMeta != null ? existingMeta.sha256() : safeHashFile(chunkFile);
                if (expectedHex.equalsIgnoreCase(existingHash)) {
                    deleteQuietly(tempChunk);
                    ChunkMetadata finalMeta = existingMeta != null ? existingMeta : new ChunkMetadata(
                            chunkId, objectId, chunkIndex, hashResult.bytesWritten(), expectedHex, Instant.now());
                    if (existingMeta == null) {
                        writeMetadataAtomically(shardDir, chunkId, metaFile, finalMeta);
                    }
                    log.debug("Chunk {} already exists with identical checksum {}; idempotent 200", chunkId, expectedHex);
                    return new WriteResult(false, finalMeta);
                } else {
                    deleteQuietly(tempChunk);
                    throw new ApiException(409, ErrorCode.CHUNK_ALREADY_EXISTS,
                            "Chunk " + chunkId + " already exists with a different checksum",
                            Map.of("chunkId", chunkId), null);
                }
            }

            ChunkMetadata metadata = new ChunkMetadata(
                    chunkId,
                    objectId,
                    chunkIndex,
                    hashResult.bytesWritten(),
                    expectedHex,
                    Instant.now());

            tempMeta = writeMetadataTemp(shardDir, chunkId, metadata);
            moveAtomically(tempChunk, chunkFile);
            moveAtomically(tempMeta, metaFile);

            log.info("Stored chunk {} ({} bytes, sha256={})", chunkId, metadata.size(), metadata.sha256());
            return new WriteResult(true, metadata);
        } catch (ApiException e) {
            throw e;
        } catch (IOException e) {
            deleteQuietly(tempChunk);
            deleteQuietly(tempMeta);
            throw new ApiException(503, ErrorCode.STORAGE_IO_ERROR,
                    "Failed to persist chunk " + chunkId + ": " + e.getMessage(),
                    Map.of("chunkId", chunkId), e);
        }
    }

    public WriteResult put(String chunkId, byte[] bytes, String expectedChecksum,
                           String objectId, Integer chunkIndex, boolean overwrite) {
        return put(chunkId, new ByteArrayInputStream(bytes), expectedChecksum, objectId, chunkIndex, overwrite);
    }

    /**
     * Reads a chunk and verifies its SHA-256 against its stored metadata before returning any bytes.
     * If corruption is detected on disk, throws 409 {@link ErrorCode#CHUNK_CORRUPTED} and never returns the bad bytes.
     */
    public ReadResult get(String chunkId) {
        validateChunkId(chunkId);
        Path shardDir = shardDir(chunkId);
        Path chunkFile = shardDir.resolve(chunkId + ".chunk");
        Path metaFile = shardDir.resolve(chunkId + ".meta");

        if (!Files.exists(chunkFile)) {
            throw new ApiException(404, ErrorCode.CHUNK_NOT_FOUND, "Chunk " + chunkId + " does not exist");
        }

        try {
            byte[] bytes = Files.readAllBytes(chunkFile);
            String actualHash = Sha256.hash(bytes);
            ChunkMetadata meta = readMetadataOrNull(metaFile);

            if (meta == null) {
                throw new ApiException(409, ErrorCode.CHUNK_CORRUPTED,
                        "Chunk " + chunkId + " is missing its metadata sidecar",
                        Map.of("chunkId", chunkId), null);
            }

            if (bytes.length != meta.size() || !actualHash.equalsIgnoreCase(meta.sha256())) {
                log.warn("Corruption detected on read of chunk {}: expected (size={}, sha256={}), actual (size={}, sha256={})",
                        chunkId, meta.size(), meta.sha256(), bytes.length, actualHash);
                throw new ApiException(409, ErrorCode.CHUNK_CORRUPTED,
                        "Integrity check failed for chunk " + chunkId,
                        Map.of("chunkId", chunkId, "expectedChecksum", meta.sha256(), "actualChecksum", actualHash),
                        null);
            }

            return new ReadResult(bytes, meta);
        } catch (ApiException e) {
            throw e;
        } catch (IOException e) {
            throw new ApiException(500, ErrorCode.STORAGE_IO_ERROR,
                    "I/O error reading chunk " + chunkId + ": " + e.getMessage(),
                    Map.of("chunkId", chunkId), e);
        }
    }

    /**
     * Deletes a chunk and its sidecar metadata.
     *
     * @throws ApiException 404 if the chunk does not exist
     */
    public void delete(String chunkId) {
        validateChunkId(chunkId);
        Path shardDir = shardDir(chunkId);
        Path chunkFile = shardDir.resolve(chunkId + ".chunk");
        Path metaFile = shardDir.resolve(chunkId + ".meta");

        if (!Files.exists(chunkFile) && !Files.exists(metaFile)) {
            throw new ApiException(404, ErrorCode.CHUNK_NOT_FOUND, "Chunk " + chunkId + " does not exist");
        }
        try {
            Files.deleteIfExists(chunkFile);
            Files.deleteIfExists(metaFile);
            log.info("Deleted chunk {}", chunkId);
        } catch (IOException e) {
            throw new ApiException(500, ErrorCode.STORAGE_IO_ERROR,
                    "Failed to delete chunk " + chunkId + ": " + e.getMessage(),
                    Map.of("chunkId", chunkId), e);
        }
    }

    /**
     * Lists all chunks stored on disk.
     *
     * @param verifyChecksums when true, re-hashes each chunk file and populates {@link StoredChunkInfo#valid()}
     */
    public List<StoredChunkInfo> listChunks(boolean verifyChecksums) {
        Path root = storageDirectory.root();
        if (!Files.exists(root)) {
            return List.of();
        }
        List<StoredChunkInfo> result = new ArrayList<>();
        try (Stream<Path> paths = Files.walk(root, 2)) {
            List<Path> chunkFiles = paths
                    .filter(Files::isRegularFile)
                    .filter(p -> p.getFileName().toString().endsWith(".chunk"))
                    .toList();
            for (Path chunkFile : chunkFiles) {
                String fileName = chunkFile.getFileName().toString();
                String chunkId = fileName.substring(0, fileName.length() - ".chunk".length());
                Path metaFile = chunkFile.resolveSibling(chunkId + ".meta");
                ChunkMetadata meta = readMetadataOrNull(metaFile);
                long fileSize = Files.size(chunkFile);
                String checksum = meta != null ? meta.sha256() : null;
                Instant storedAt = meta != null ? meta.storedAt() : Files.getLastModifiedTime(chunkFile).toInstant();
                Boolean valid = null;
                if (verifyChecksums) {
                    if (meta == null) {
                        valid = false;
                    } else {
                        String actualHash = safeHashFile(chunkFile);
                        valid = (fileSize == meta.size()) && meta.sha256().equalsIgnoreCase(actualHash);
                    }
                }
                result.add(new StoredChunkInfo(
                        chunkId,
                        meta != null ? meta.objectId() : null,
                        meta != null ? meta.chunkIndex() : null,
                        fileSize,
                        checksum,
                        storedAt,
                        valid));
            }
        } catch (IOException e) {
            throw new ApiException(500, ErrorCode.STORAGE_IO_ERROR,
                    "Failed to list stored chunks: " + e.getMessage(), Map.of(), e);
        }
        result.sort(Comparator.comparing(StoredChunkInfo::chunkId));
        return result;
    }

    /**
     * Fast summary of chunk count and total chunk bytes currently stored on disk.
     */
    public StorageStats stats() {
        List<StoredChunkInfo> chunks = listChunks(false);
        long used = 0;
        for (StoredChunkInfo c : chunks) {
            used += c.size();
        }
        return new StorageStats(chunks.size(), used, storageDirectory.usableBytes(), storageDirectory.totalBytes());
    }

    public Path chunkFilePath(String chunkId) {
        validateChunkId(chunkId);
        return shardDir(chunkId).resolve(chunkId + ".chunk");
    }

    private Path shardDir(String chunkId) {
        String shard = chunkId.substring(0, 2).toLowerCase(Locale.ROOT);
        Path dir = storageDirectory.root().resolve(shard).normalize();
        if (!dir.startsWith(storageDirectory.root())) {
            throw new ApiException(400, ErrorCode.INVALID_CHUNK_ID, "Invalid chunkId path: " + chunkId);
        }
        return dir;
    }

    private ChunkMetadata readMetadataOrNull(Path metaFile) {
        if (!Files.exists(metaFile)) {
            return null;
        }
        try {
            return objectMapper.readValue(metaFile.toFile(), ChunkMetadata.class);
        } catch (IOException e) {
            log.warn("Failed to parse metadata file {}: {}", metaFile, e.getMessage());
            return null;
        }
    }

    private String safeHashFile(Path file) {
        try (InputStream in = Files.newInputStream(file)) {
            return Sha256.hash(in);
        } catch (IOException e) {
            return "";
        }
    }

    private Path writeMetadataTemp(Path shardDir, String chunkId, ChunkMetadata metadata) throws IOException {
        Path tempMeta = Files.createTempFile(shardDir, chunkId + ".", ".meta.tmp");
        byte[] json = objectMapper.writeValueAsBytes(metadata);
        Files.write(tempMeta, json, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING);
        fsyncFile(tempMeta);
        return tempMeta;
    }

    private void writeMetadataAtomically(Path shardDir, String chunkId, Path metaFile, ChunkMetadata metadata) throws IOException {
        Path tempMeta = writeMetadataTemp(shardDir, chunkId, metadata);
        moveAtomically(tempMeta, metaFile);
    }

    private static void moveAtomically(Path source, Path target) throws IOException {
        try {
            Files.move(source, target, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
        } catch (AtomicMoveNotSupportedException e) {
            Files.move(source, target, StandardCopyOption.REPLACE_EXISTING);
        }
    }

    private static void fsyncFile(Path file) throws IOException {
        try (FileChannel channel = FileChannel.open(file, StandardOpenOption.WRITE)) {
            channel.force(true);
        }
    }

    private static void deleteQuietly(Path path) {
        if (path != null) {
            try {
                Files.deleteIfExists(path);
            } catch (IOException ignored) {
            }
        }
    }

    public record WriteResult(boolean created, ChunkMetadata metadata) {
    }

    public record ReadResult(byte[] data, ChunkMetadata metadata) {
    }

    public record StorageStats(int chunkCount, long usedBytes, long freeBytes, long capacityBytes) {
    }
}

