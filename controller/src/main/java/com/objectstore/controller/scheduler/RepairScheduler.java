package com.objectstore.controller.scheduler;

import com.objectstore.controller.service.ReplicationService;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * Periodically scans for under-replicated or corrupted chunks and restores the replication factor.
 */
@Component
@ConditionalOnProperty(name = "controller.scheduling.enabled", havingValue = "true", matchIfMissing = true)
public class RepairScheduler {

    private final ReplicationService replicationService;

    public RepairScheduler(ReplicationService replicationService) {
        this.replicationService = replicationService;
    }

    @Scheduled(
            fixedDelayString = "${objectstore.cluster.repair-interval:10s}",
            initialDelayString = "${objectstore.cluster.repair-interval:10s}")
    public void runScheduledRepair() {
        replicationService.repairUnderReplicatedChunks(false);
    }
}

