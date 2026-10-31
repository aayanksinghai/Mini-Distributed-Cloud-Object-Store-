import type { ClusterStatusInfo, WorkerNodeInfo } from '../api/types';
import { formatAge, formatBytes } from '../lib/format';
import { StatusBadge } from './StatusBadge';

/** Health card for one storage worker, with its share of the cluster's stored chunk bytes. */
export function WorkerCard({
  worker,
  totalUsedBytes,
  now,
  onReconcile,
  busy = false,
}: {
  worker: WorkerNodeInfo;
  totalUsedBytes: number;
  now: number;
  onReconcile?: (workerId: string) => void;
  busy?: boolean;
}) {
  const share = totalUsedBytes > 0 ? worker.usedBytes / totalUsedBytes : 0;
  const isUp = worker.status === 'UP';
  return (
    <div className={`card worker-card ${isUp ? '' : 'worker-down'}`} data-testid={`worker-${worker.workerId}`}>
      <div className="row space-between">
        <h3>{worker.workerId}</h3>
        <StatusBadge value={worker.status} />
      </div>
      <dl className="kv">
        <dt>Chunks stored</dt>
        <dd>{worker.chunkCount}</dd>
        <dt>Chunk bytes</dt>
        <dd>{formatBytes(worker.usedBytes)}</dd>
        <dt>Free disk</dt>
        <dd>{formatBytes(worker.freeBytes)}</dd>
        <dt>Last heartbeat</dt>
        <dd>{formatAge(worker.lastHeartbeatAt, now)}</dd>
        {!isUp && (
          <>
            <dt>Down since</dt>
            <dd>{formatAge(worker.downSince, now)}</dd>
          </>
        )}
      </dl>
      <div className="bar" title={`${(share * 100).toFixed(1)}% of all stored chunk bytes`}>
        <div className="bar-fill" style={{ width: `${(share * 100).toFixed(1)}%` }} />
      </div>
      <div className="row space-between small muted">
        <span>{(share * 100).toFixed(1)}% of cluster data</span>
        <span className="mono">{worker.baseUrl}</span>
      </div>
      {onReconcile && (
        <button className="btn btn-ghost small" disabled={!isUp || busy} onClick={() => onReconcile(worker.workerId)}>
          Reconcile inventory
        </button>
      )}
    </div>
  );
}

/** Cluster-wide replication summary tiles (from GET /api/v1/cluster/status). */
export function ClusterSummary({ status }: { status: ClusterStatusInfo }) {
  const tiles: { label: string; value: number | string; tone?: string }[] = [
    { label: 'Workers UP', value: `${status.workersUp} / ${status.workersTotal}`, tone: status.workersDown > 0 ? 'warn' : 'ok' },
    { label: 'Objects', value: status.activeObjects },
    { label: 'Chunks', value: status.totalChunks },
    { label: `Healthy (RF ≥ ${status.replicationFactor})`, value: status.healthyChunks, tone: 'ok' },
    { label: 'Under-replicated', value: status.underReplicatedChunks, tone: status.underReplicatedChunks > 0 ? 'warn' : undefined },
    { label: 'Unavailable', value: status.unavailableChunks, tone: status.unavailableChunks > 0 ? 'bad' : undefined },
    { label: 'Corrupted replicas', value: status.corruptedReplicas, tone: status.corruptedReplicas > 0 ? 'bad' : undefined },
    { label: 'Healthy replicas', value: status.totalHealthyReplicas },
  ];
  return (
    <div className="tiles" data-testid="cluster-summary">
      {tiles.map((t) => (
        <div key={t.label} className={`tile ${t.tone ? `tile-${t.tone}` : ''}`}>
          <div className="tile-value">{t.value}</div>
          <div className="tile-label">{t.label}</div>
        </div>
      ))}
    </div>
  );
}

