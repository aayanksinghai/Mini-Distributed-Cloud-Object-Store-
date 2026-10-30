package com.objectstore.worker.storage;

import com.objectstore.worker.config.WorkerProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Owns the worker's chunk directory on its persistent volume. The directory is created (if needed)
 * and probed for writability at start-up so that a worker with unusable storage fails fast instead
 * of registering with the controller and then failing every write.
 */
@Component
public class StorageDirectory {

    private static final Logger log = LoggerFactory.getLogger(StorageDirectory.class);

    private final Path root;

    public StorageDirectory(WorkerProperties props) {
        this.root = props.storageDir().toAbsolutePath().normalize();
        initialise();
        log.info("Worker '{}' using chunk storage at {} (usable={} bytes)", props.id(), root, usableBytes());
    }

    private void initialise() {
        try {
            Files.createDirectories(root);
            Path probe = Files.createTempFile(root, ".write-probe", ".tmp");
            Files.delete(probe);
        } catch (IOException e) {
            throw new UncheckedIOException("Chunk storage directory " + root + " is not usable", e);
        }
    }

    public Path root() {
        return root;
    }

    public boolean isWritable() {
        return Files.isDirectory(root) && Files.isWritable(root);
    }

    public long usableBytes() {
        try {
            return Files.getFileStore(root).getUsableSpace();
        } catch (IOException e) {
            return -1;
        }
    }

    public long totalBytes() {
        try {
            return Files.getFileStore(root).getTotalSpace();
        } catch (IOException e) {
            return -1;
        }
    }
}

