package com.objectstore.controller.api;

import com.objectstore.common.dto.ObjectInfo;
import com.objectstore.controller.domain.StoredObjectEntity;
import com.objectstore.controller.service.ObjectService;
import org.springframework.http.ContentDisposition;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.InvalidMediaTypeException;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestPart;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;
import org.springframework.web.servlet.mvc.method.annotation.StreamingResponseBody;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.UUID;

/**
 * Client-facing REST API for object operations (PDF §5).
 */
@RestController
@RequestMapping("/api/v1/objects")
public class ObjectController {

    public static final String HEADER_OBJECT_CHECKSUM_SHA256 = "X-Object-Checksum-SHA256";

    private final ObjectService objectService;

    public ObjectController(ObjectService objectService) {
        this.objectService = objectService;
    }

    /**
     * §5.1 Upload Object — {@code POST /api/v1/objects} ({@code multipart/form-data}).
     * Required part: {@code file}. Optional part: {@code metadata} (JSON string of custom key/value metadata).
     */
    @PostMapping(
            consumes = MediaType.MULTIPART_FORM_DATA_VALUE,
            produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<ObjectInfo> uploadObject(
            @RequestPart(value = "file", required = false) MultipartFile file,
            @RequestPart(value = "metadata", required = false) String metadata) {
        ObjectInfo created = objectService.uploadObject(file, metadata);
        URI location = URI.create("/api/v1/objects/" + created.objectId());
        return ResponseEntity.status(HttpStatus.CREATED).location(location).body(created);
    }

    /**
     * §5.2 Download Object — {@code GET /api/v1/objects/{objectId}}.
     * Streams the reconstructed original object in chunk order with its stored {@code Content-Type}.
     */
    @GetMapping("/{objectId}")
    public ResponseEntity<StreamingResponseBody> downloadObject(@PathVariable UUID objectId) {
        ObjectService.PreparedDownload prepared = objectService.prepareDownload(objectId);
        StoredObjectEntity obj = prepared.object();

        MediaType mediaType = parseMediaTypeOrDefault(obj.getContentType());
        ContentDisposition disposition = ContentDisposition.attachment()
                .filename(obj.getName(), StandardCharsets.UTF_8)
                .build();

        StreamingResponseBody body = outputStream -> objectService.streamDownload(prepared, outputStream);

        return ResponseEntity.ok()
                .contentType(mediaType)
                .contentLength(obj.getSizeBytes())
                .header(HttpHeaders.CONTENT_DISPOSITION, disposition.toString())
                .eTag("\"" + obj.getChecksumSha256() + "\"")
                .header(HEADER_OBJECT_CHECKSUM_SHA256, obj.getChecksumSha256())
                .body(body);
    }

    private static MediaType parseMediaTypeOrDefault(String raw) {
        if (raw == null || raw.isBlank()) {
            return MediaType.APPLICATION_OCTET_STREAM;
        }
        try {
            return MediaType.parseMediaType(raw);
        } catch (InvalidMediaTypeException e) {
            return MediaType.APPLICATION_OCTET_STREAM;
        }
    }
}
