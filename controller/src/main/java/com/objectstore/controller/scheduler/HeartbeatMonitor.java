package com.objectstore.controller.scheduler;

import com.objectstore.controller.service.WorkerRegistryService;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * Periodically evaluates worker heartbeat timestamps and marks unresponsive workers DOWN.
 */
@Component
@ConditionalOnProperty(name = "controller.scheduling.enabled", havingValue = "true", matchIfMissing = true)
public class HeartbeatMonitor {

    private final WorkerRegistryService workerRegistryService;

    public HeartbeatMonitor(WorkerRegistryService workerRegistryService) {
        this.workerRegistryService = workerRegistryService;
    }

    @Scheduled(
            fixedDelayString = "${objectstore.cluster.heartbeat-check-interval:2s}",
            initialDelayString = "${objectstore.cluster.heartbeat-check-interval:2s}")
    public void checkHeartbeats() {
        workerRegistryService.markStaleWorkersDown();
    }
}

