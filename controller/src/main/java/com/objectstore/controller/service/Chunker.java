package com.objectstore.controller.service;

import com.objectstore.common.util.Sha256;
import com.objectstore.controller.config.ControllerProperties;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.io.InputStream;
import java.security.MessageDigest;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.UUID;

/**
 * Splits an arbitrary binary stream into ordered fixed-size chunks (1 MiB = 1,048,576 bytes by default,
 * with a smaller final chunk when applicable), computing SHA-256 checksums for each chunk and for the
 * complete object in a single pass.
 */
@Component
public class Chunker {

    private static final HexFormat HEX = HexFormat.of();

    private final int chunkSizeBytes;

    @Autowired
    public Chunker(ControllerProperties properties) {
        long configured = properties.storage().chunkSize().toBytes();
        if (configured <= 0 || configured > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("Invalid chunk size bytes: " + configured);
        }
        this.chunkSizeBytes = (int) configured;
    }

    public Chunker(int chunkSizeBytes) {
        if (chunkSizeBytes <= 0) {
            throw new IllegalArgumentException("chunkSizeBytes must be positive: " + chunkSizeBytes);
        }
        this.chunkSizeBytes = chunkSizeBytes;
    }

    public int getChunkSizeBytes() {
        return chunkSizeBytes;
    }

    /**
     * Formats the deterministic chunk identifier {@code {objectId}_{index:05d}} (plan.md D3).
     */
    public static String formatChunkId(UUID objectId, int chunkIndex) {
        return String.format("%s_%05d", objectId, chunkIndex);
    }

    /**
     * Reads {@code inputStream} until EOF, invoking {@code consumer} for every chunk in order.
     * For a 0-byte input stream, {@code consumer} is not invoked and {@code chunkCount} is 0.
     */
    public ChunkingResult split(UUID objectId, InputStream inputStream, ChunkConsumer consumer) throws IOException {
        MessageDigest objectDigest = Sha256.newDigest();
        byte[] buffer = new byte[chunkSizeBytes];
        long totalBytes = 0L;
        int chunkIndex = 0;

        while (true) {
            int filled = readUpToChunkSize(inputStream, buffer);
            if (filled == 0) {
                break;
            }
            byte[] chunkBytes = filled == buffer.length ? buffer.clone() : Arrays.copyOf(buffer, filled);
            objectDigest.update(chunkBytes, 0, filled);
            String chunkSha256 = Sha256.hash(chunkBytes);
            String chunkId = formatChunkId(objectId, chunkIndex);

            consumer.accept(new ChunkSlice(chunkId, chunkIndex, chunkBytes, chunkSha256));

            totalBytes += filled;
            chunkIndex++;
        }

        String objectSha256 = HEX.formatHex(objectDigest.digest());
        return new ChunkingResult(totalBytes, chunkIndex, chunkSizeBytes, objectSha256);
    }

    private static int readUpToChunkSize(InputStream in, byte[] buffer) throws IOException {
        int offset = 0;
        while (offset < buffer.length) {
            int read = in.read(buffer, offset, buffer.length - offset);
            if (read == -1) {
                break;
            }
            offset += read;
        }
        return offset;
    }

    @FunctionalInterface
    public interface ChunkConsumer {
        void accept(ChunkSlice slice) throws IOException;
    }

    public record ChunkSlice(String chunkId, int index, byte[] data, String sha256Hex) {
        public int size() {
            return data.length;
        }
    }

    public record ChunkingResult(long totalBytes, int chunkCount, int chunkSize, String objectSha256Hex) {
    }
}
