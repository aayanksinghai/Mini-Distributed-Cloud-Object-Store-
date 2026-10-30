package com.objectstore.controller.client;

import com.objectstore.common.dto.StoredChunkInfo;
import com.objectstore.common.dto.WorkerChunksResponse;
import com.objectstore.common.error.ApiException;
import com.objectstore.common.error.ErrorCode;
import com.objectstore.common.http.InternalHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;
import org.springframework.web.client.HttpStatusCodeException;
import org.springframework.web.client.RestClient;
import org.springframework.web.client.RestClientException;

import java.util.Map;

/**
 * HTTP client used by the controller to invoke internal endpoints on storage workers.
 */
@Component
public class WorkerClient {

    private final RestClient restClient;

    public WorkerClient(RestClient workerRestClient) {
        this.restClient = workerRestClient;
    }

    public StoredChunkInfo putChunk(String workerId,
                                    String workerBaseUrl,
                                    String chunkId,
                                    byte[] data,
                                    String expectedSha256Hex,
                                    String objectId,
                                    Integer chunkIndex,
                                    boolean overwrite) {
        try {
            RestClient.RequestBodySpec spec = restClient.put()
                    .uri(workerBaseUrl + "/internal/chunks/{chunkId}?overwrite={overwrite}", chunkId, overwrite)
                    .contentType(MediaType.APPLICATION_OCTET_STREAM)
                    .header(InternalHeaders.CHUNK_CHECKSUM, expectedSha256Hex);
            if (objectId != null) {
                spec.header(InternalHeaders.OBJECT_ID, objectId);
            }
            if (chunkIndex != null) {
                spec.header(InternalHeaders.CHUNK_INDEX, String.valueOf(chunkIndex));
            }
            return spec.body(data)
                    .retrieve()
                    .body(StoredChunkInfo.class);
        } catch (HttpStatusCodeException e) {
            throw translateWorkerHttpError(workerId, "PUT chunk " + chunkId, e);
        } catch (RestClientException e) {
            throw new ApiException(503, ErrorCode.WORKER_UNAVAILABLE,
                    "Failed to reach worker '" + workerId + "' during PUT chunk " + chunkId + ": " + e.getMessage(),
                    Map.of("workerId", workerId, "chunkId", chunkId), e);
        }
    }

    public byte[] getChunk(String workerId, String workerBaseUrl, String chunkId) {
        try {
            ResponseEntity<byte[]> resp = restClient.get()
                    .uri(workerBaseUrl + "/internal/chunks/{chunkId}", chunkId)
                    .accept(MediaType.APPLICATION_OCTET_STREAM)
                    .retrieve()
                    .toEntity(byte[].class);
            return resp.getBody() != null ? resp.getBody() : new byte[0];
        } catch (HttpStatusCodeException e) {
            throw translateWorkerHttpError(workerId, "GET chunk " + chunkId, e);
        } catch (RestClientException e) {
            throw new ApiException(503, ErrorCode.WORKER_UNAVAILABLE,
                    "Failed to reach worker '" + workerId + "' during GET chunk " + chunkId + ": " + e.getMessage(),
                    Map.of("workerId", workerId, "chunkId", chunkId), e);
        }
    }

    public void deleteChunk(String workerId, String workerBaseUrl, String chunkId) {
        try {
            restClient.delete()
                    .uri(workerBaseUrl + "/internal/chunks/{chunkId}", chunkId)
                    .retrieve()
                    .toBodilessEntity();
        } catch (HttpStatusCodeException e) {
            if (e.getStatusCode().value() == 404) {
                return;
            }
            throw translateWorkerHttpError(workerId, "DELETE chunk " + chunkId, e);
        } catch (RestClientException e) {
            throw new ApiException(503, ErrorCode.WORKER_UNAVAILABLE,
                    "Failed to reach worker '" + workerId + "' during DELETE chunk " + chunkId + ": " + e.getMessage(),
                    Map.of("workerId", workerId, "chunkId", chunkId), e);
        }
    }

    public WorkerChunksResponse listWorkerChunks(String workerId, String workerBaseUrl, boolean verify) {
        try {
            return restClient.get()
                    .uri(workerBaseUrl + "/internal/workers/{workerId}/chunks?verify={verify}", workerId, verify)
                    .accept(MediaType.APPLICATION_JSON)
                    .retrieve()
                    .body(WorkerChunksResponse.class);
        } catch (HttpStatusCodeException e) {
            throw translateWorkerHttpError(workerId, "list chunks", e);
        } catch (RestClientException e) {
            throw new ApiException(503, ErrorCode.WORKER_UNAVAILABLE,
                    "Failed to reach worker '" + workerId + "' to list chunks: " + e.getMessage(),
                    Map.of("workerId", workerId), e);
        }
    }

    private static ApiException translateWorkerHttpError(String workerId, String action, HttpStatusCodeException e) {
        int status = e.getStatusCode().value();
        String code = switch (status) {
            case 404 -> ErrorCode.CHUNK_NOT_FOUND;
            case 409 -> ErrorCode.CHUNK_CORRUPTED;
            case 400 -> ErrorCode.VALIDATION_ERROR;
            default -> ErrorCode.WORKER_UNAVAILABLE;
        };
        return new ApiException(status, code,
                "Worker '" + workerId + "' returned HTTP " + status + " during " + action,
                Map.of("workerId", workerId, "workerStatus", status), e);
    }
}

