import type { ChunkMetadataInfo, ChunkReplicaInfo } from '../api/types';
import { formatBytes, shortId } from '../lib/format';
import { StatusBadge } from './StatusBadge';

export interface SelectedReplica {
  chunk: ChunkMetadataInfo;
  replica: ChunkReplicaInfo;
}

/** Returns the ordered list of worker columns: all known workers plus any referenced only by replicas. */
export function matrixWorkerIds(chunks: ChunkMetadataInfo[], knownWorkerIds: string[]): string[] {
  const ids = new Set(knownWorkerIds);
  for (const c of chunks) for (const r of c.replicas) ids.add(r.workerId);
  return Array.from(ids).sort((a, b) => a.localeCompare(b, undefined, { numeric: true }));
}

/** Cell CSS class for a replica (or empty cell when the worker holds no replica of the chunk). */
export function replicaCellClass(replica: ChunkReplicaInfo | undefined): string {
  if (!replica) return 'cell-empty';
  if (replica.state === 'CORRUPTED') return 'cell-corrupted';
  if (replica.workerStatus === 'DOWN' || replica.state === 'UNAVAILABLE') return 'cell-unavailable';
  if (replica.state === 'HEALTHY') return 'cell-healthy';
  return 'cell-pending';
}

/**
 * Chunks × workers grid showing exactly where each replica lives and its state. Clicking a replica
 * cell selects it (used by the object detail page to offer the demo corruption action).
 */
export function ReplicaMatrix({
  chunks,
  workerIds,
  selected,
  onSelect,
}: {
  chunks: ChunkMetadataInfo[];
  workerIds: string[];
  selected?: SelectedReplica | null;
  onSelect?: (sel: SelectedReplica) => void;
}) {
  const columns = matrixWorkerIds(chunks, workerIds);
  if (chunks.length === 0) return <div className="muted small">No chunks to display.</div>;

  return (
    <div className="table-wrap">
      <table className="matrix" data-testid="replica-matrix">
        <thead>
          <tr>
            <th>Chunk</th>
            <th>Size</th>
            <th>Status</th>
            {columns.map((w) => (
              <th key={w} className="center">
                {w}
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {chunks.map((chunk) => (
            <tr key={chunk.chunkId}>
              <td className="mono" title={chunk.chunkId}>
                #{chunk.index} <span className="muted">{shortId(chunk.chunkId, 6, 6)}</span>
              </td>
              <td>{formatBytes(chunk.size)}</td>
              <td>
                <StatusBadge value={chunk.replicationStatus} />
              </td>
              {columns.map((w) => {
                const replica = chunk.replicas.find((r) => r.workerId === w);
                const isSelected =
                  !!replica && selected?.chunk.chunkId === chunk.chunkId && selected.replica.workerId === w;
                return (
                  <td
                    key={w}
                    data-testid={`cell-${chunk.index}-${w}`}
                    className={`matrix-cell ${replicaCellClass(replica)}${isSelected ? ' cell-selected' : ''}${
                      replica && onSelect ? ' clickable' : ''
                    }`}
                    title={
                      replica
                        ? `${w}: replica ${replica.state} (worker ${replica.workerStatus})`
                        : `${w}: no replica`
                    }
                    onClick={() => replica && onSelect?.({ chunk, replica })}
                  >
                    {replica ? (replica.state === 'HEALTHY' && replica.workerStatus === 'UP' ? '●' : replica.state === 'CORRUPTED' ? '✕' : '○') : ''}
                  </td>
                );
              })}
            </tr>
          ))}
        </tbody>
      </table>
      <div className="legend small">
        <span><i className="dot cell-healthy" /> healthy replica</span>
        <span><i className="dot cell-unavailable" /> unavailable (worker DOWN)</span>
        <span><i className="dot cell-corrupted" /> corrupted</span>
        <span><i className="dot cell-empty" /> no replica</span>
      </div>
    </div>
  );
}

