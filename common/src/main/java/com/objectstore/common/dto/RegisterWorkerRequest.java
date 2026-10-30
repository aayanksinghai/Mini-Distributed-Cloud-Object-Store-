package com.objectstore.common.dto;

import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Pattern;

/**
 * Payload sent by a worker to {@code POST /internal/workers/register}.
 */
public record RegisterWorkerRequest(
        @NotBlank @Pattern(regexp = "[A-Za-z0-9][A-Za-z0-9_-]{0,62}") String workerId,
        @NotBlank String baseUrl,
        @Min(0) long capacityBytes,
        @Min(0) long usedBytes,
        @Min(0) int chunkCount) {
}

