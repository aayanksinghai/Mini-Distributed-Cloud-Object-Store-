package com.objectstore.worker.storage;

import com.objectstore.worker.config.WorkerProperties;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.stereotype.Component;

/**
 * Exposes chunk-storage health under {@code /actuator/health} (component {@code chunkStorage});
 * used by the Docker healthcheck.
 */
@Component("chunkStorage")
class StorageHealthIndicator implements HealthIndicator {

    private final StorageDirectory storage;
    private final WorkerProperties props;

    StorageHealthIndicator(StorageDirectory storage, WorkerProperties props) {
        this.storage = storage;
        this.props = props;
    }

    @Override
    public Health health() {
        Health.Builder builder = storage.isWritable() ? Health.up() : Health.down();
        return builder
                .withDetail("workerId", props.id())
                .withDetail("path", storage.root().toString())
                .withDetail("usableBytes", storage.usableBytes())
                .withDetail("totalBytes", storage.totalBytes())
                .build();
    }
}

