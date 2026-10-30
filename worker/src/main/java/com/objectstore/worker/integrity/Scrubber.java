package com.objectstore.worker.integrity;

import com.objectstore.common.dto.StoredChunkInfo;
import com.objectstore.worker.storage.ChunkStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Background integrity scrubber that periodically re-computes SHA-256 checksums of all stored chunks on disk
 * and maintains the set of currently corrupted chunk IDs to report on the next heartbeat (PDF §8, plan.md D9).
 */
@Component
public class Scrubber {

    private static final Logger log = LoggerFactory.getLogger(Scrubber.class);

    private final ChunkStore chunkStore;
    private final Set<String> corruptChunkIds = ConcurrentHashMap.newKeySet();

    public Scrubber(ChunkStore chunkStore) {
        this.chunkStore = chunkStore;
    }

    /**
     * Scans all chunks on disk, verifying each file's size and SHA-256 against its sidecar metadata.
     *
     * @return sorted list of chunk IDs found to be corrupted during this scan
     */
    public List<String> scrubAll() {
        List<StoredChunkInfo> chunks = chunkStore.listChunks(true);
        List<String> newlyDetected = new ArrayList<>();
        Set<String> stillPresent = ConcurrentHashMap.newKeySet();

        for (StoredChunkInfo chunk : chunks) {
            stillPresent.add(chunk.chunkId());
            if (Boolean.FALSE.equals(chunk.valid())) {
                corruptChunkIds.add(chunk.chunkId());
                newlyDetected.add(chunk.chunkId());
            } else if (Boolean.TRUE.equals(chunk.valid())) {
                corruptChunkIds.remove(chunk.chunkId());
            }
        }
        corruptChunkIds.retainAll(stillPresent);

        if (!newlyDetected.isEmpty()) {
            log.warn("Background integrity scrub detected {} corrupted chunk(s): {}",
                    newlyDetected.size(), newlyDetected);
        } else {
            log.debug("Background integrity scrub verified {} chunk(s); 0 corrupted", chunks.size());
        }
        return List.copyOf(newlyDetected);
    }

    public void recordCorruptChunk(String chunkId) {
        if (chunkId != null && !chunkId.isBlank()) {
            corruptChunkIds.add(chunkId);
        }
    }

    public void clearCorruptChunk(String chunkId) {
        if (chunkId != null) {
            corruptChunkIds.remove(chunkId);
        }
    }

    public List<String> getCorruptChunkIds() {
        return corruptChunkIds.stream().sorted().toList();
    }

    @Component
    @ConditionalOnProperty(name = "worker.scheduling.enabled", havingValue = "true", matchIfMissing = true)
    public static class ScrubberScheduler {

        private final Scrubber scrubber;

        public ScrubberScheduler(Scrubber scrubber) {
            this.scrubber = scrubber;
        }

        @Scheduled(
                fixedDelayString = "${worker.scrub-interval:5m}",
                initialDelayString = "${worker.scrub-interval:5m}")
        public void scheduledScrub() {
            scrubber.scrubAll();
        }
    }
}

