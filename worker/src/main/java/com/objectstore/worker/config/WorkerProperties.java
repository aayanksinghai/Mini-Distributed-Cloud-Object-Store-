package com.objectstore.worker.config;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;
import org.springframework.validation.annotation.Validated;

import java.nio.file.Path;
import java.time.Duration;

/**
 * Typed, validated worker configuration bound from {@code worker.*}. A missing or malformed
 * {@code WORKER_ID} fails start-up so a misconfigured node never joins the cluster.
 *
 * @param id                identity of this worker (e.g. {@code worker2})
 * @param advertisedUrl     base URL the controller uses to reach this worker
 * @param controllerUrl     base URL of the controller (registration / heartbeats)
 * @param storageDir        directory (on the persistent volume) holding chunk files
 * @param heartbeatInterval interval between heartbeats
 * @param scrubInterval     interval between background integrity scans
 * @param debugEndpoints    whether opt-in debug endpoints (e.g. corruption injection) are enabled
 * @param internalApiToken  shared secret for the internal API
 */
@Validated
@ConfigurationProperties(prefix = "worker")
public record WorkerProperties(
        @NotBlank @Pattern(regexp = "[A-Za-z0-9][A-Za-z0-9_-]{0,62}",
                message = "must be 1-63 chars of letters, digits, '-' or '_'") String id,
        @NotBlank String advertisedUrl,
        @NotBlank String controllerUrl,
        @NotNull Path storageDir,
        @NotNull @DefaultValue("3s") Duration heartbeatInterval,
        @NotNull @DefaultValue("5m") Duration scrubInterval,
        @DefaultValue("false") boolean debugEndpoints,
        @NotBlank String internalApiToken) {
}

