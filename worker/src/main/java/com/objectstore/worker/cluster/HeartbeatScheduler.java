package com.objectstore.worker.cluster;

import com.objectstore.worker.integrity.Scrubber;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * Periodically drives worker registration and heartbeats.
 */
@Component
@ConditionalOnProperty(
        name = "worker.scheduling.enabled", havingValue = "true", matchIfMissing = true)
public class HeartbeatScheduler {

    private final RegistrationClient registrationClient;
    private final Scrubber scrubber;

    public HeartbeatScheduler(RegistrationClient registrationClient, Scrubber scrubber) {
        this.registrationClient = registrationClient;
        this.scrubber = scrubber;
    }

    @EventListener(ApplicationReadyEvent.class)
    public void onReady() {
        registrationClient.register();
    }

    @Scheduled(
            fixedDelayString = "${worker.heartbeat-interval:3s}",
            initialDelayString = "${worker.heartbeat-interval:3s}")
    public void tick() {
        registrationClient.sendHeartbeat(scrubber.getCorruptChunkIds());
    }
}
