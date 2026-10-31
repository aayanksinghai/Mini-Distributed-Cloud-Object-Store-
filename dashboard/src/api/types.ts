// TypeScript mirrors of the controller's JSON DTOs (common/src/main/java/com/objectstore/common/dto).
// Timestamps are ISO-8601 strings; UUIDs are strings.

export type ReplicationStatus = 'HEALTHY' | 'DEGRADED' | 'UNAVAILABLE';
export type ReplicaState = 'PENDING' | 'HEALTHY' | 'CORRUPTED' | 'UNAVAILABLE';
export type WorkerStatus = 'UP' | 'DOWN';
export type EventSeverity = 'INFO' | 'WARN' | 'ERROR';

export interface ObjectInfo {
  objectId: string;
  name: string;
  size: number;
  contentType: string;
  checksum: string; // "sha256:<hex>"
  chunkCount: number;
  createdAt: string;
  updatedAt: string;
  customMetadata: Record<string, string>;
}

export interface ObjectListPage {
  items: ObjectInfo[];
  page: number;
  size: number;
  totalItems: number;
  totalPages: number;
}

export interface ChunkReplicaInfo {
  workerId: string;
  state: ReplicaState;
  workerStatus: WorkerStatus;
  lastVerifiedAt: string | null;
}

export interface ChunkMetadataInfo {
  chunkId: string;
  index: number;
  size: number;
  checksum: string;
  replicationStatus: ReplicationStatus;
  replicas: ChunkReplicaInfo[];
}

export interface ObjectMetadataResponse {
  objectId: string;
  name: string;
  size: number;
  contentType: string;
  checksum: string;
  chunkCount: number;
  chunkSize: number;
  replicationFactor: number;
  replicationStatus: ReplicationStatus;
  createdAt: string;
  updatedAt: string;
  customMetadata: Record<string, string>;
  chunks: ChunkMetadataInfo[];
}

export interface WorkerNodeInfo {
  workerId: string;
  baseUrl: string;
  status: WorkerStatus;
  capacityBytes: number;
  usedBytes: number;
  freeBytes: number;
  chunkCount: number;
  registeredAt: string;
  lastHeartbeatAt: string;
  downSince: string | null;
}

export interface ClusterStatusInfo {
  replicationFactor: number;
  workersTotal: number;
  workersUp: number;
  workersDown: number;
  activeObjects: number;
  totalChunks: number;
  healthyChunks: number;
  underReplicatedChunks: number;
  unavailableChunks: number;
  totalHealthyReplicas: number;
  corruptedReplicas: number;
  replicationStatus: ReplicationStatus;
}

export interface SystemEventInfo {
  id: number;
  occurredAt: string;
  type: string;
  severity: EventSeverity;
  objectId: string | null;
  chunkId: string | null;
  workerId: string | null;
  message: string;
}

export interface RepairResultInfo {
  executedAt: string;
  examinedChunks: number;
  underReplicatedChunksFound: number;
  repairedChunks: number;
  corruptedReplicasRepaired: number;
  skippedWithinGracePeriod: number;
  unrepairableChunks: number;
  repairedChunkIds: string[];
}

export interface ScrubResultInfo {
  scrubbedAt: string;
  objectId: string | null;
  replicasExamined: number;
  replicasValid: number;
  corruptReplicasDetected: number;
  missingReplicasDetected: number;
  replicasRepaired: number;
  corruptedChunkIds: string[];
  repairedChunkIds: string[];
}

export interface ReconciliationResultInfo {
  workerId: string;
  reconciledAt: string;
  pendingDeletionsExecuted: number;
  orphanChunksDeleted: number;
  replicasReadopted: number;
  missingReplicasRemoved: number;
  corruptReplicasDetected: number;
  surplusReplicasTrimmed: number;
  trimmedChunkIds: string[];
}

export interface ErrorResponse {
  timestamp: string;
  status: number;
  error: string;
  code: string;
  message: string;
  path: string;
  details?: Record<string, unknown>;
}

/** JSON Merge Patch body for PATCH /api/v1/objects/{id}/metadata (string = set, null = remove). */
export type MetadataPatch = Record<string, string | null>;

