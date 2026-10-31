import type { EventSeverity, ReplicaState, ReplicationStatus, WorkerStatus } from '../api/types';

type Tone = 'ok' | 'warn' | 'bad' | 'muted' | 'info';

const TONES: Record<string, Tone> = {
  HEALTHY: 'ok',
  UP: 'ok',
  INFO: 'info',
  DEGRADED: 'warn',
  WARN: 'warn',
  PENDING: 'muted',
  UNAVAILABLE: 'bad',
  CORRUPTED: 'bad',
  DOWN: 'bad',
  ERROR: 'bad',
};

export function toneFor(value: string): Tone {
  return TONES[value] ?? 'muted';
}

export function StatusBadge({
  value,
  title,
}: {
  value: ReplicationStatus | ReplicaState | WorkerStatus | EventSeverity | string;
  title?: string;
}) {
  return (
    <span className={`badge badge-${toneFor(value)}`} title={title}>
      {value}
    </span>
  );
}

