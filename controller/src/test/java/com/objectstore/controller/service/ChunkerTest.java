package com.objectstore.controller.service;

import com.objectstore.common.util.Sha256;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

class ChunkerTest {

    private static final int ONE_MIB = 1_048_576;

    private final Chunker chunker = new Chunker(ONE_MIB);

    @Test
    void emptyStreamProducesZeroChunksAndValidEmptyHash() throws Exception {
        UUID objectId = UUID.randomUUID();
        List<Chunker.ChunkSlice> slices = new ArrayList<>();

        Chunker.ChunkingResult res = chunker.split(objectId, new ByteArrayInputStream(new byte[0]), slices::add);

        assertThat(res.totalBytes()).isZero();
        assertThat(res.chunkCount()).isZero();
        assertThat(res.chunkSize()).isEqualTo(ONE_MIB);
        assertThat(res.objectSha256Hex()).isEqualTo(Sha256.hash(new byte[0]));
        assertThat(slices).isEmpty();
    }

    @Test
    void boundarySizesSplitIntoExact1MiBChunksWithSmallerFinalChunk() throws Exception {
        // Test 1 B, 1 MiB - 1, exact 1 MiB, 1 MiB + 1, and ~2.3 MiB
        int[] sizes = {1, ONE_MIB - 1, ONE_MIB, ONE_MIB + 1, (2 * ONE_MIB) + 345_678};
        Random rng = new Random(42L);

        for (int size : sizes) {
            byte[] original = new byte[size];
            rng.nextBytes(original);
            UUID objectId = UUID.randomUUID();
            List<Chunker.ChunkSlice> slices = new ArrayList<>();

            Chunker.ChunkingResult res = chunker.split(objectId, new ByteArrayInputStream(original), slices::add);

            int expectedChunks = (size + ONE_MIB - 1) / ONE_MIB;
            assertThat(res.totalBytes()).isEqualTo(size);
            assertThat(res.chunkCount()).isEqualTo(expectedChunks);
            assertThat(res.objectSha256Hex()).isEqualTo(Sha256.hash(original));
            assertThat(slices).hasSize(expectedChunks);

            ByteArrayOutputStream reconstructed = new ByteArrayOutputStream(size);
            for (int i = 0; i < slices.size(); i++) {
                Chunker.ChunkSlice slice = slices.get(i);
                assertThat(slice.index()).isEqualTo(i);
                assertThat(slice.chunkId()).isEqualTo(String.format("%s_%05d", objectId, i));
                assertThat(slice.sha256Hex()).isEqualTo(Sha256.hash(slice.data()));
                if (i < slices.size() - 1) {
                    assertThat(slice.size()).isEqualTo(ONE_MIB);
                } else {
                    int expectedLast = size - (i * ONE_MIB);
                    assertThat(slice.size()).isEqualTo(expectedLast);
                }
                reconstructed.write(slice.data());
            }
            assertThat(reconstructed.toByteArray()).isEqualTo(original);
        }
    }
}

