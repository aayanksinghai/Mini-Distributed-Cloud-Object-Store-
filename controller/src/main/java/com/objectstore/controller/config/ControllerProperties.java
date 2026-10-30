package com.objectstore.controller.config;

import jakarta.validation.Valid;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;
import org.springframework.util.unit.DataSize;
import org.springframework.validation.annotation.Validated;

import java.time.Duration;

/**
 * Typed, validated controller configuration bound from {@code objectstore.*} (environment variables
 * are mapped in {@code application.yml}). Invalid values fail application start-up.
 */
@Validated
@ConfigurationProperties(prefix = "objectstore")
public record ControllerProperties(
        @Valid @NotNull @DefaultValue Storage storage,
        @Valid @NotNull @DefaultValue Cluster cluster,
        @Valid @NotNull Internal internal) {

    /**
     * @param chunkSize         fixed chunk size (1 MiB per specification)
     * @param replicationFactor number of copies per chunk (2 per specification)
     * @param maxObjectSize     maximum accepted upload size
     */
    public record Storage(
            @NotNull @DefaultValue("1MB") DataSize chunkSize,
            @Min(1) @DefaultValue("2") int replicationFactor,
            @NotNull @DefaultValue("1GB") DataSize maxObjectSize) {
    }

    /**
     * @param heartbeatTimeout         worker considered DOWN when no heartbeat for this long
     * @param heartbeatCheckInterval   how often liveness is evaluated
     * @param reReplicationGracePeriod wait before re-replicating chunks of a DOWN worker
     * @param repairInterval           how often the repair scheduler runs
     * @param workerRequestTimeout     timeout for controller to worker HTTP calls
     */
    public record Cluster(
            @NotNull @DefaultValue("10s") Duration heartbeatTimeout,
            @NotNull @DefaultValue("2s") Duration heartbeatCheckInterval,
            @NotNull @DefaultValue("30s") Duration reReplicationGracePeriod,
            @NotNull @DefaultValue("10s") Duration repairInterval,
            @NotNull @DefaultValue("5s") Duration workerRequestTimeout) {
    }

    /**
     * @param apiToken shared secret for the internal controller &lt;-&gt; worker API
     */
    public record Internal(@NotBlank String apiToken) {
    }
}

