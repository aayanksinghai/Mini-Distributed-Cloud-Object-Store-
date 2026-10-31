import type { SystemEventInfo } from '../api/types';
import { formatTime, shortId } from '../lib/format';
import { navigate } from '../router';
import { StatusBadge } from './StatusBadge';

/** Renders system events (newest first) from GET /api/v1/admin/events. */
export function EventList({ events, compact = false }: { events: SystemEventInfo[]; compact?: boolean }) {
  if (events.length === 0) {
    return <div className="muted small">No events yet.</div>;
  }
  const ordered = [...events].sort((a, b) => b.id - a.id);
  return (
    <ul className={`events ${compact ? 'events-compact' : ''}`} data-testid="event-list">
      {ordered.map((e) => (
        <li key={e.id} className={`event event-${e.severity.toLowerCase()}`}>
          <span className="event-time mono">{formatTime(e.occurredAt)}</span>
          <StatusBadge value={e.severity} />
          <span className="event-type mono">{e.type}</span>
          <span className="event-msg">{e.message}</span>
          {!compact && (
            <span className="event-refs small muted">
              {e.workerId && <span>worker={e.workerId} </span>}
              {e.objectId && (
                <a href={`#/objects/${e.objectId}`} onClick={(ev) => { ev.preventDefault(); navigate(`/objects/${e.objectId}`); }}>
                  object={shortId(e.objectId)}
                </a>
              )}
              {e.chunkId && <span title={e.chunkId}> chunk={shortId(e.chunkId, 6, 6)}</span>}
            </span>
          )}
        </li>
      ))}
    </ul>
  );
}

