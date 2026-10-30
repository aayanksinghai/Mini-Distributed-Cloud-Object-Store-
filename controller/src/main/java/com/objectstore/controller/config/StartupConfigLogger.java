package com.objectstore.controller.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

/**
 * Logs the effective (non-secret) configuration once the controller is ready, which makes
 * container logs self-describing during demos and troubleshooting.
 */
@Component
class StartupConfigLogger {

    private static final Logger log = LoggerFactory.getLogger(StartupConfigLogger.class);

    private final ControllerProperties props;

    StartupConfigLogger(ControllerProperties props) {
        this.props = props;
    }

    @EventListener(ApplicationReadyEvent.class)
    void logConfiguration() {
        ControllerProperties.Storage s = props.storage();
        ControllerProperties.Cluster c = props.cluster();
        log.info("Controller ready: chunkSize={} bytes, replicationFactor={}, maxObjectSize={} bytes",
                s.chunkSize().toBytes(), s.replicationFactor(), s.maxObjectSize().toBytes());
        log.info("Cluster timing: heartbeatTimeout={}, checkInterval={}, reReplicationGrace={}, repairInterval={}, workerRequestTimeout={}",
                c.heartbeatTimeout(), c.heartbeatCheckInterval(), c.reReplicationGracePeriod(),
                c.repairInterval(), c.workerRequestTimeout());
    }
}

