import type {
  ClusterStatusInfo,
  ErrorResponse,
  MetadataPatch,
  ObjectInfo,
  ObjectListPage,
  ObjectMetadataResponse,
  ReconciliationResultInfo,
  RepairResultInfo,
  ScrubResultInfo,
  SystemEventInfo,
  WorkerNodeInfo,
} from './types';

/**
 * Typed client for the controller's real REST API. All paths are same-origin (`/api/v1/...`): in Docker
 * Compose Nginx proxies them to `controller:8080`; in `npm run dev` the Vite dev server proxies them.
 */
export const API_BASE = '/api/v1';

/** Error carrying the controller's structured JSON error body (`ErrorResponse`). */
export class ApiError extends Error {
  readonly status: number;
  readonly code: string;
  readonly details: Record<string, unknown>;

  constructor(status: number, code: string, message: string, details: Record<string, unknown> = {}) {
    super(message);
    this.name = 'ApiError';
    this.status = status;
    this.code = code;
    this.details = details;
  }
}

function isErrorResponse(value: unknown): value is ErrorResponse {
  return typeof value === 'object' && value !== null && 'code' in value && 'message' in value;
}

/** Converts a non-2xx Response into an ApiError, preferring the controller's JSON error body. */
export async function toApiError(resp: Response): Promise<ApiError> {
  let body: unknown = null;
  const text = await resp.text().catch(() => '');
  if (text) {
    try {
      body = JSON.parse(text);
    } catch {
      body = null;
    }
  }
  if (isErrorResponse(body)) {
    return new ApiError(resp.status, body.code, body.message, body.details ?? {});
  }
  const fallback = text && text.length < 300 ? text : resp.statusText || 'Request failed';
  return new ApiError(resp.status, `HTTP_${resp.status}`, `HTTP ${resp.status}: ${fallback}`);
}

async function request<T>(path: string, init?: RequestInit): Promise<T> {
  let resp: Response;
  try {
    resp = await fetch(`${API_BASE}${path}`, {
      ...init,
      headers: { Accept: 'application/json', ...(init?.headers ?? {}) },
    });
  } catch (e) {
    const reason = e instanceof Error ? e.message : String(e);
    throw new ApiError(0, 'NETWORK_ERROR', `Cannot reach the controller: ${reason}`);
  }
  if (!resp.ok) {
    throw await toApiError(resp);
  }
  if (resp.status === 204) {
    return undefined as T;
  }
  return (await resp.json()) as T;
}

const enc = encodeURIComponent;

// ---- Client object API (PDF §5) ----

export function listObjects(page = 0, size = 20, sort = 'createdAt,desc'): Promise<ObjectListPage> {
  return request(`/objects?page=${page}&size=${size}&sort=${enc(sort)}`);
}

export function getObjectMetadata(objectId: string): Promise<ObjectMetadataResponse> {
  return request(`/objects/${enc(objectId)}/metadata`);
}

export function updateObjectMetadata(objectId: string, patch: MetadataPatch): Promise<ObjectMetadataResponse> {
  return request(`/objects/${enc(objectId)}/metadata`, {
    method: 'PATCH',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(patch),
  });
}

export function deleteObject(objectId: string): Promise<void> {
  return request(`/objects/${enc(objectId)}`, { method: 'DELETE' });
}

/** URL of the streaming download endpoint (used directly by <a download> so the browser streams to disk). */
export function objectDownloadUrl(objectId: string): string {
  return `${API_BASE}/objects/${enc(objectId)}`;
}

/** Downloads the full object into memory (used for in-browser SHA-256 verification of the round trip). */
export async function downloadObjectBytes(objectId: string): Promise<ArrayBuffer> {
  let resp: Response;
  try {
    resp = await fetch(objectDownloadUrl(objectId));
  } catch (e) {
    const reason = e instanceof Error ? e.message : String(e);
    throw new ApiError(0, 'NETWORK_ERROR', `Cannot reach the controller: ${reason}`);
  }
  if (!resp.ok) {
    throw await toApiError(resp);
  }
  return resp.arrayBuffer();
}

/**
 * Uploads a file via multipart/form-data (`file` + optional `metadata` JSON string). Uses XMLHttpRequest
 * so upload progress can be shown for large objects.
 */
export function uploadObject(
  file: File,
  metadata: Record<string, string>,
  onProgress?: (fraction: number) => void,
): Promise<ObjectInfo> {
  const form = new FormData();
  form.append('file', file, file.name);
  if (Object.keys(metadata).length > 0) {
    form.append('metadata', JSON.stringify(metadata));
  }
  return new Promise((resolve, reject) => {
    const xhr = new XMLHttpRequest();
    xhr.open('POST', `${API_BASE}/objects`);
    xhr.setRequestHeader('Accept', 'application/json');
    xhr.upload.onprogress = (ev) => {
      if (ev.lengthComputable && onProgress) {
        onProgress(ev.loaded / ev.total);
      }
    };
    xhr.onerror = () => reject(new ApiError(0, 'NETWORK_ERROR', 'Upload failed: cannot reach the controller'));
    xhr.onload = () => {
      let body: unknown = null;
      try {
        body = xhr.responseText ? JSON.parse(xhr.responseText) : null;
      } catch {
        body = null;
      }
      if (xhr.status === 201) {
        resolve(body as ObjectInfo);
      } else if (isErrorResponse(body)) {
        reject(new ApiError(xhr.status, body.code, body.message, body.details ?? {}));
      } else {
        reject(new ApiError(xhr.status, `HTTP_${xhr.status}`, `Upload failed with HTTP ${xhr.status}`));
      }
    };
    xhr.send(form);
  });
}

// ---- Cluster & admin API (plan.md §8.3) ----

export function listWorkers(): Promise<WorkerNodeInfo[]> {
  return request('/cluster/workers');
}

export function getClusterStatus(): Promise<ClusterStatusInfo> {
  return request('/cluster/status');
}

export function listEvents(sinceId?: number, limit = 200): Promise<SystemEventInfo[]> {
  const since = sinceId && sinceId > 0 ? `&since=${sinceId}` : '';
  return request(`/admin/events?limit=${limit}${since}`);
}

export function triggerRepair(): Promise<RepairResultInfo> {
  return request('/admin/replication/repair', { method: 'POST' });
}

export function triggerScrub(objectId?: string, repair = true): Promise<ScrubResultInfo> {
  const obj = objectId ? `&objectId=${enc(objectId)}` : '';
  return request(`/admin/integrity/scrub?repair=${repair}${obj}`, { method: 'POST' });
}

export function reconcileWorker(workerId: string): Promise<ReconciliationResultInfo> {
  return request(`/admin/workers/${enc(workerId)}/reconcile`, { method: 'POST' });
}

/** Opt-in demo corruption (requires WORKER_DEBUG_ENDPOINTS=true on the workers). */
export function corruptReplica(workerId: string, chunkId: string): Promise<Record<string, unknown>> {
  return request(`/admin/workers/${enc(workerId)}/chunks/${enc(chunkId)}/corrupt`, { method: 'POST' });
}

/** Human-friendly message for any thrown value. */
export function errorMessage(e: unknown): string {
  if (e instanceof ApiError) {
    return e.code && !e.code.startsWith('HTTP_') && e.code !== 'NETWORK_ERROR'
      ? `${e.message} [${e.code}]`
      : e.message;
  }
  return e instanceof Error ? e.message : String(e);
}

