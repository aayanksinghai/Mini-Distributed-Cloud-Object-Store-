import { useEffect, useRef, useState } from 'react';
import { errorMessage, listEvents } from '../api/client';
import type { EventSeverity, SystemEventInfo } from '../api/types';
import { EventList } from '../components/EventList';

const MAX_EVENTS = 1000;

/** Merges newly fetched events into the existing list (dedup by id, keep newest MAX_EVENTS). */
export function mergeEvents(existing: SystemEventInfo[], incoming: SystemEventInfo[]): SystemEventInfo[] {
  const byId = new Map<number, SystemEventInfo>();
  for (const e of existing) byId.set(e.id, e);
  for (const e of incoming) byId.set(e.id, e);
  return Array.from(byId.values())
    .sort((a, b) => a.id - b.id)
    .slice(-MAX_EVENTS);
}

/** Live operations log: polls GET /api/v1/admin/events with a `since` cursor and filters client-side. */
export function EventsPage() {
  const [events, setEvents] = useState<SystemEventInfo[]>([]);
  const [error, setError] = useState<string | null>(null);
  const [paused, setPaused] = useState(false);
  const [severities, setSeverities] = useState<Record<EventSeverity, boolean>>({ INFO: true, WARN: true, ERROR: true });
  const [typeFilter, setTypeFilter] = useState('ALL');
  const [search, setSearch] = useState('');
  const cursor = useRef(0);

  useEffect(() => {
    let cancelled = false;
    const poll = async () => {
      if (paused) return;
      try {
        const batch = await listEvents(cursor.current || undefined, 500);
        if (cancelled) return;
        if (batch.length > 0) {
          cursor.current = Math.max(cursor.current, ...batch.map((e) => e.id));
          setEvents((prev) => mergeEvents(prev, batch));
        }
        setError(null);
      } catch (e) {
        if (!cancelled) setError(errorMessage(e));
      }
    };
    void poll();
    const id = window.setInterval(poll, 2000);
    return () => {
      cancelled = true;
      window.clearInterval(id);
    };
  }, [paused]);

  const types = Array.from(new Set(events.map((e) => e.type))).sort();
  const needle = search.trim().toLowerCase();
  const visible = events.filter(
    (e) =>
      severities[e.severity] &&
      (typeFilter === 'ALL' || e.type === typeFilter) &&
      (needle === '' ||
        e.message.toLowerCase().includes(needle) ||
        (e.workerId ?? '').toLowerCase().includes(needle) ||
        (e.objectId ?? '').toLowerCase().includes(needle) ||
        (e.chunkId ?? '').toLowerCase().includes(needle)),
  );

  return (
    <div className="card">
      <div className="row space-between wrap">
        <h2>Operations log <span className="muted small">({visible.length} of {events.length})</span></h2>
        <div className="row wrap small">
          {(['INFO', 'WARN', 'ERROR'] as EventSeverity[]).map((s) => (
            <label key={s}>
              <input
                type="checkbox"
                checked={severities[s]}
                onChange={(e) => setSeverities({ ...severities, [s]: e.target.checked })}
              />{' '}
              {s}
            </label>
          ))}
          <select value={typeFilter} onChange={(e) => setTypeFilter(e.target.value)} aria-label="event type filter">
            <option value="ALL">All types</option>
            {types.map((t) => <option key={t} value={t}>{t}</option>)}
          </select>
          <input placeholder="Search worker / object / message" value={search} onChange={(e) => setSearch(e.target.value)} />
          <button className="btn btn-ghost small" onClick={() => setPaused(!paused)}>{paused ? 'Resume' : 'Pause'}</button>
        </div>
      </div>
      {error && <div className="alert alert-error">{error}</div>}
      <EventList events={visible} />
    </div>
  );
}

