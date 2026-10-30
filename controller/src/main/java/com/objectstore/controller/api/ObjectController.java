package com.objectstore.controller.api;

import com.objectstore.common.dto.ObjectInfo;
import com.objectstore.controller.service.ObjectService;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestPart;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;

import java.net.URI;

/**
 * Client-facing REST API for object operations (PDF §5).
 */
@RestController
@RequestMapping("/api/v1/objects")
public class ObjectController {

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
}

