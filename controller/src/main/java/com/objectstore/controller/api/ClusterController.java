package com.objectstore.controller.api;

import com.objectstore.common.dto.SystemEventInfo;
import com.objectstore.common.dto.WorkerNodeInfo;
import com.objectstore.controller.service.EventService;
import com.objectstore.controller.service.WorkerRegistryService;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

/**
 * Public cluster and operational event endpoints used by demos and the dashboard.
 */
@RestController
public class ClusterController {

    private final WorkerRegistryService workerRegistryService;
    private final EventService eventService;

    public ClusterController(WorkerRegistryService workerRegistryService, EventService eventService) {
        this.workerRegistryService = workerRegistryService;
        this.eventService = eventService;
    }

    @GetMapping(value = "/api/v1/cluster/workers", produces = MediaType.APPLICATION_JSON_VALUE)
    public List<WorkerNodeInfo> listWorkers() {
        return workerRegistryService.listWorkers();
    }

    @GetMapping(value = "/api/v1/admin/events", produces = MediaType.APPLICATION_JSON_VALUE)
    public List<SystemEventInfo> listEvents(
            @RequestParam(value = "since", required = false) Long since,
            @RequestParam(value = "limit", defaultValue = "100") int limit) {
        return eventService.listEvents(since, limit);
    }
}

