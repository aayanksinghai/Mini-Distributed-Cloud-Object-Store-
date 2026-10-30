package com.objectstore.worker.storage;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.objectstore.common.dto.StoredChunkInfo;
import com.objectstore.common.error.ApiException;
import com.objectstore.common.error.ErrorCode;
import com.objectstore.common.util.Sha256;
import com.objectstore.worker.config.WorkerProperties;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ChunkStoreTest {

    @TempDir
    Path tempDir;

    private ChunkStore store;

    @BeforeEach
    void setUp() {
        WorkerProperties props = new WorkerProperties(
                "worker1",
                "http://worker1:8081",
                "http://controller:8080",
                tempDir.resolve("chunks"),
                Duration.ofSeconds(3),
                Duration.ofMinutes(5),
                false,
                "token");
        StorageDirectory dir = new StorageDirectory(props);
        ObjectMapper mapper = new ObjectMapper().findAndRegisterModules();
        store = new ChunkStore(dir, mapper);
    }

    @Test
    void putGetListAndDeleteRoundTrip() {
        byte[] payload = "chunk-payload-bytes-000".getBytes(StandardCharsets.UTF_8);
        String sha = Sha256.hash(payload);
        String chunkId = "3f2a9c00-0000-0000-0000-000000000001_00000";

        ChunkStore.WriteResult write = store.put(chunkId, payload, sha, "3f2a9c00-0000-0000-0000-000000000001", 0, false);
        assertThat(write.created()).isTrue();
        assertThat(write.metadata().size()).isEqualTo(payload.length);
        assertThat(write.metadata().sha256()).isEqualTo(sha);

        // Idempotent repeat write with same content returns created=false
        ChunkStore.WriteResult repeat = store.put(chunkId, payload, sha, "3f2a9c00-0000-0000-0000-000000000001", 0, false);
        assertThat(repeat.created()).isFalse();

        ChunkStore.ReadResult read = store.get(chunkId);
        assertThat(read.data()).isEqualTo(payload);
        assertThat(read.metadata().sha256()).isEqualTo(sha);

        List<StoredChunkInfo> list = store.listChunks(true);
        assertThat(list).hasSize(1);
        assertThat(list.getFirst().chunkId()).isEqualTo(chunkId);
        assertThat(list.getFirst().valid()).isTrue();

        store.delete(chunkId);
        assertThat(store.listChunks(false)).isEmpty();
        assertThatThrownBy(() -> store.get(chunkId))
                .isInstanceOf(ApiException.class)
                .extracting("status", "code")
                .containsExactly(404, ErrorCode.CHUNK_NOT_FOUND);
    }

    @Test
    void putRejectsChecksumMismatchAndCleansTempFile() {
        byte[] payload = "actual-bytes".getBytes(StandardCharsets.UTF_8);
        String wrongSha = Sha256.hash("different-bytes".getBytes(StandardCharsets.UTF_8));
        String chunkId = "ab12_00001";

        assertThatThrownBy(() -> store.put(chunkId, payload, wrongSha, null, null, false))
                .isInstanceOf(ApiException.class)
                .extracting("status", "code")
                .containsExactly(400, ErrorCode.CHECKSUM_MISMATCH);

        assertThat(store.listChunks(false)).isEmpty();
    }

    @Test
    void putRejectsConflictingContentUnlessOverwriteEnabled() {
        String chunkId = "cd34_00000";
        byte[] v1 = "version-1".getBytes(StandardCharsets.UTF_8);
        byte[] v2 = "version-2-different".getBytes(StandardCharsets.UTF_8);

        store.put(chunkId, v1, Sha256.hash(v1), null, 0, false);

        assertThatThrownBy(() -> store.put(chunkId, v2, Sha256.hash(v2), null, 0, false))
                .isInstanceOf(ApiException.class)
                .extracting("status", "code")
                .containsExactly(409, ErrorCode.CHUNK_ALREADY_EXISTS);

        // Overwrite=true replaces the chunk cleanly
        ChunkStore.WriteResult overwritten = store.put(chunkId, v2, Sha256.hash(v2), null, 0, true);
        assertThat(overwritten.created()).isTrue();
        assertThat(store.get(chunkId).data()).isEqualTo(v2);
    }

    @Test
    void getDetectsOnDiskCorruptionAndWithholdsBytes() throws Exception {
        String chunkId = "ef56_00002";
        byte[] original = "clean-chunk-bytes".getBytes(StandardCharsets.UTF_8);
        store.put(chunkId, original, Sha256.hash(original), null, 2, false);

        // Flip a byte directly on disk
        Path file = store.chunkFilePath(chunkId);
        byte[] tampered = Files.readAllBytes(file);
        tampered[0] ^= 0x5A;
        Files.write(file, tampered);

        assertThatThrownBy(() -> store.get(chunkId))
                .isInstanceOf(ApiException.class)
                .extracting("status", "code")
                .containsExactly(409, ErrorCode.CHUNK_CORRUPTED);

        List<StoredChunkInfo> verifiedList = store.listChunks(true);
        assertThat(verifiedList).hasSize(1);
        assertThat(verifiedList.getFirst().valid()).isFalse();
    }

    @Test
    void invalidChunkIdsAreRejected() {
        byte[] data = "x".getBytes(StandardCharsets.UTF_8);
        String sha = Sha256.hash(data);
        for (String badId : List.of("", "a", "../escape", "chunk/name", "chunk..id", "chunk id")) {
            assertThatThrownBy(() -> store.put(badId, data, sha, null, null, false))
                    .isInstanceOf(ApiException.class)
                    .extracting("status", "code")
                    .containsExactly(400, ErrorCode.INVALID_CHUNK_ID);
        }
    }
}

