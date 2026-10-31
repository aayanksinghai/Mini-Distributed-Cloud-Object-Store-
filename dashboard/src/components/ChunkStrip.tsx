import type { ChunkMetadataInfo } from '../api/types';
import { formatBytes } from '../lib/format';
import { toneFor } from './StatusBadge';

/**
 * Visualises how an object was split: one segment per chunk in index order, width proportional to
 * chunk size (exactly 1 MiB except a smaller final chunk), coloured by the chunk's replication status.
 */
export function ChunkStrip({ chunks }: { chunks: ChunkMetadataInfo[] }) {
  if (chunks.length === 0) {
    return <div className="muted small">Empty object — 0 chunks.</div>;
  }
  const total = chunks.reduce((sum, c) => sum + c.size, 0) || 1;
  return (
    <div className="chunk-strip" data-testid="chunk-strip">
      {chunks.map((c) => (
        <div
          key={c.chunkId}
          className={`chunk-seg tone-${toneFor(c.replicationStatus)}`}
          style={{ flexGrow: Math.max(c.size, 1) / total }}
          title={`Chunk #${c.index} • ${formatBytes(c.size)} • ${c.replicationStatus}\n${c.chunkId}`}
        >
          <span>#{c.index}</span>
        </div>
      ))}
    </div>
  );
}

