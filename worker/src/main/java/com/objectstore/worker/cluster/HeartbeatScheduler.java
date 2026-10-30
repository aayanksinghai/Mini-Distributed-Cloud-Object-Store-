package com.objectstore.worker.cluster;

import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * Periodically drives worker registration and heartbeats.
 */
@Component
@ org.springframework.boot.autoconfigure.condition.ConditionalOnProperty(
        name = "worker.scheduling.enabled", havingValue = "true", matchIfMissing = true)
public class HeartbeatScheduler {

    private final RegistrationClient registrationClient;

    public HeartbeatScheduler(RegistrationClient registrationClient) {
        this.registrationClient = registrationClient;
    }

    @EventListener(ApplicationReadyEvent.class)
    public void onReady() {
        registrationClient.register();
    }

    @Scheduled(
            fixedDelayString = "${worker.heartbeat-interval:3s}",
            initialDelayString = "${worker.heartbeat-interval:3s}")
    public void tick() {
        registrationClient.sendHeartbeat(List.of());
    }
}
