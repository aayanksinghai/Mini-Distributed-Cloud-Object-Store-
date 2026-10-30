package com.objectstore.controller.api;

import com.objectstore.common.dto.ClusterStatusInfo;
import com.objectstore.common.dto.ReconciliationResultInfo;
import com.objectstore.common.dto.RepairResultInfo;
import com.objectstore.common.dto.ScrubResultInfo;
import com.objectstore.common.dto.SystemEventInfo;
import com.objectstore.common.dto.WorkerNodeInfo;
import com.objectstore.controller.service.EventService;
import com.objectstore.controller.service.IntegrityService;
import com.objectstore.controller.service.ReconciliationService;
import com.objectstore.controller.service.ReplicationService;
import com.objectstore.controller.service.WorkerRegistryService;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Public cluster status, replication repair, worker reconciliation, data-integrity scrub, and operational event
 * endpoints used by demos and the dashboard.
 */
@RestController
public class ClusterController {

    private final WorkerRegistryService workerRegistryService;
    private final ReplicationService replicationService;
    private final ReconciliationService reconciliationService;
    private final IntegrityService integrityService;
    private final EventService eventService;

    public ClusterController(WorkerRegistryService workerRegistryService,
                             ReplicationService replicationService,
                             ReconciliationService reconciliationService,
                             IntegrityService integrityService,
                             EventService eventService) {
        this.workerRegistryService = workerRegistryService;
        this.replicationService = replicationService;
        this.reconciliationService = reconciliationService;
        this.integrityService = integrityService;
        this.eventService = eventService;
    }

    @GetMapping(value = "/api/v1/cluster/workers", produces = MediaType.APPLICATION_JSON_VALUE)
    public List<WorkerNodeInfo> listWorkers() {
        return workerRegistryService.listWorkers();
    }

    @GetMapping(value = "/api/v1/cluster/status", produces = MediaType.APPLICATION_JSON_VALUE)
    public ClusterStatusInfo getClusterStatus() {
        return replicationService.getClusterStatus();
    }

    @PostMapping(value = "/api/v1/admin/replication/repair", produces = MediaType.APPLICATION_JSON_VALUE)
    public RepairResultInfo triggerRepair() {
        return replicationService.repairUnderReplicatedChunks(true);
    }

    @PostMapping(value = "/api/v1/admin/workers/{workerId}/reconcile", produces = MediaType.APPLICATION_JSON_VALUE)
    public ReconciliationResultInfo reconcileWorker(@PathVariable String workerId) {
        return reconciliationService.reconcileWorker(workerId);
    }

    @PostMapping(value = "/api/v1/admin/integrity/scrub", produces = MediaType.APPLICATION_JSON_VALUE)
    public ScrubResultInfo triggerIntegrityScrub(
            @RequestParam(value = "objectId", required = false) UUID objectId,
            @RequestParam(value = "repair", defaultValue = "true") boolean repair) {
        return integrityService.scrub(objectId, repair);
    }

    @PostMapping(
            value = "/api/v1/admin/workers/{workerId}/chunks/{chunkId}/corrupt",
            produces = MediaType.APPLICATION_JSON_VALUE)
    public Map<String, Object> corruptReplicaForDemo(
            @PathVariable String workerId,
            @PathVariable String chunkId) {
        return integrityService.corruptReplicaForDebug(workerId, chunkId);
    }

    @GetMapping(value = "/api/v1/admin/events", produces = MediaType.APPLICATION_JSON_VALUE)
    public List<SystemEventInfo> listEvents(
            @RequestParam(value = "since", required = false) Long since,
            @RequestParam(value = "limit", defaultValue = "100") int limit) {
        return eventService.listEvents(since, limit);
    }
}
