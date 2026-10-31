import { useState } from 'react';
import {
  ApiError,
  corruptReplica,
  deleteObject,
  downloadObjectBytes,
  errorMessage,
  getObjectMetadata,
  listEvents,
  listWorkers,
  objectDownloadUrl,
  triggerRepair,
  triggerScrub,
  updateObjectMetadata,
} from '../api/client';
import type { ObjectMetadataResponse } from '../api/types';
import { ChunkStrip } from '../components/ChunkStrip';
import { EventList } from '../components/EventList';
import { MetadataRowsEditor } from '../components/MetadataRowsEditor';
import { ReplicaMatrix, type SelectedReplica } from '../components/ReplicaMatrix';
import { StatusBadge } from '../components/StatusBadge';
import { useToast } from '../components/Toasts';
import { usePolling } from '../hooks/usePolling';
import { formatBytes, formatDateTime, stripShaPrefix } from '../lib/format';
import { buildMergePatch, mapToRows, validateMetadataRows, type MetadataRow } from '../lib/metadata';
import { canHashInBrowser, sha256Hex } from '../lib/sha256';
import { navigate } from '../router';

type VerifyState = { status: 'idle' } | { status: 'running' } | { status: 'done'; match: boolean; actual: string; bytes: number };

function MetadataEditorCard({ meta, onSaved }: { meta: ObjectMetadataResponse; onSaved: (m: ObjectMetadataResponse) => void }) {
  const toast = useToast();
  const [editing, setEditing] = useState(false);
  const [rows, setRows] = useState<MetadataRow[]>([]);
  const [saving, setSaving] = useState(false);
  const errors = editing ? validateMetadataRows(rows) : [];
  const patch = editing ? buildMergePatch(meta.customMetadata, rows) : {};
  const changeCount = Object.keys(patch).length;

  const save = async () => {
    setSaving(true);
    try {
      const updated = await updateObjectMetadata(meta.objectId, patch);
      toast.success(`Metadata updated (${changeCount} change(s)) — no object data was re-uploaded`);
      setEditing(false);
      onSaved(updated);
    } catch (e) {
      toast.error(`Metadata update failed: ${errorMessage(e)}`);
    } finally {
      setSaving(false);
    }
  };

  return (
    <div className="card">
      <div className="row space-between">
        <h2>Custom metadata</h2>
        {!editing && (
          <button className="btn btn-ghost small" onClick={() => { setRows(mapToRows(meta.customMetadata)); setEditing(true); }}>
            Edit
          </button>
        )}
      </div>
      {editing ? (
        <>
          <MetadataRowsEditor rows={rows} onChange={setRows} disabled={saving} />
          {errors.length > 0 && (
            <ul className="errors small">{errors.map((e) => <li key={e}>{e}</li>)}</ul>
          )}
          <div className="small muted">
            PATCH body (JSON Merge Patch): <code className="mono">{JSON.stringify(patch)}</code>
          </div>
          <div className="row">
            <button className="btn btn-primary" disabled={saving || errors.length > 0 || changeCount === 0} onClick={save}>
              {saving ? 'Saving…' : 'Save'}
            </button>
            <button className="btn btn-ghost" disabled={saving} onClick={() => setEditing(false)}>Cancel</button>
          </div>
        </>
      ) : Object.keys(meta.customMetadata).length === 0 ? (
        <div className="muted small">No custom metadata.</div>
      ) : (
        <dl className="kv">
          {Object.entries(meta.customMetadata).sort(([a], [b]) => a.localeCompare(b)).map(([k, v]) => (
            <div key={k} className="kv-pair"><dt className="mono">{k}</dt><dd>{v}</dd></div>
          ))}
        </dl>
      )}
    </div>
  );
}

export function ObjectDetailPage({ objectId }: { objectId: string }) {
  const toast = useToast();
  const meta = usePolling(() => getObjectMetadata(objectId), 3000, [objectId]);
  const workers = usePolling(listWorkers, 5000);
  const events = usePolling(() => listEvents(undefined, 300), 4000);
  const [selected, setSelected] = useState<SelectedReplica | null>(null);
  const [busy, setBusy] = useState<string | null>(null);
  const [verify, setVerify] = useState<VerifyState>({ status: 'idle' });

  const notFound = meta.error && !meta.data && /OBJECT_NOT_FOUND/.test(meta.error);
  if (notFound) {
    return (
      <div className="card">
        <h2>Object not found</h2>
        <p className="muted">{meta.error}</p>
        <button className="btn btn-ghost" onClick={() => navigate('/objects')}>← Back to objects</button>
      </div>
    );
  }
  if (!meta.data) {
    return <div className="card">{meta.error ? <div className="alert alert-error">{meta.error}</div> : 'Loading…'}</div>;
  }
  const m = meta.data;
  const objectEvents = (events.data ?? []).filter((e) => e.objectId === objectId || (e.chunkId && e.chunkId.startsWith(objectId)));

  const run = async (label: string, fn: () => Promise<void>) => {
    setBusy(label);
    try {
      await fn();
    } catch (e) {
      toast.error(`${label} failed: ${errorMessage(e)}`);
    } finally {
      setBusy(null);
      void meta.refresh();
      void events.refresh();
    }
  };

  const verifyDownload = () =>
    run('Verify download', async () => {
      setVerify({ status: 'running' });
      try {
        const bytes = await downloadObjectBytes(objectId);
        const actual = await sha256Hex(bytes);
        const match = actual === stripShaPrefix(m.checksum);
        setVerify({ status: 'done', match, actual, bytes: bytes.byteLength });
        if (match) toast.success('Downloaded object matches its stored SHA-256 checksum');
        else toast.error('Downloaded bytes do NOT match the stored checksum!');
      } catch (e) {
        setVerify({ status: 'idle' });
        throw e;
      }
    });

  const onDelete = () =>
    run('Delete', async () => {
      if (!window.confirm(`Delete "${m.name}" and all its chunk replicas?`)) return;
      await deleteObject(objectId);
      toast.success(`Deleted "${m.name}"`);
      navigate('/objects');
    });

  const onScrub = () =>
    run('Integrity scrub', async () => {
      const r = await triggerScrub(objectId, true);
      toast.info(`Scrub: ${r.replicasValid}/${r.replicasExamined} replicas valid, ${r.corruptReplicasDetected} corrupted, ${r.missingReplicasDetected} missing, ${r.replicasRepaired} repaired`);
    });

  const onRepair = () =>
    run('Repair', async () => {
      const r = await triggerRepair();
      toast.info(`Repair: ${r.repairedChunks} chunk(s) re-replicated, ${r.corruptedReplicasRepaired} corrupted replica(s) overwritten, ${r.unrepairableChunks} unrepairable`);
    });

  const onCorrupt = (sel: SelectedReplica) =>
    run('Corrupt replica', async () => {
      try {
        await corruptReplica(sel.replica.workerId, sel.chunk.chunkId);
        toast.info(`Corrupted chunk #${sel.chunk.index} on ${sel.replica.workerId} (on disk). Download or scrub to detect & repair it.`);
      } catch (e) {
        if (e instanceof ApiError && e.status === 404) {
          throw new ApiError(404, e.code, 'Worker debug endpoint is disabled — set WORKER_DEBUG_ENDPOINTS=true, or use scripts/demo/corrupt-chunk.sh');
        }
        throw e;
      }
    });

  const currentSelected = selected
    ? (() => {
        const chunk = m.chunks.find((c) => c.chunkId === selected.chunk.chunkId);
        const replica = chunk?.replicas.find((r) => r.workerId === selected.replica.workerId);
        return chunk && replica ? { chunk, replica } : null;
      })()
    : null;

  return (
    <div className="stack">
      <div className="row">
        <button className="btn btn-ghost small" onClick={() => navigate('/objects')}>← Objects</button>
        {meta.error && <span className="alert alert-error small">Refresh failed: {meta.error}</span>}
      </div>

      <div className="card">
        <div className="row space-between">
          <h2>{m.name}</h2>
          <StatusBadge value={m.replicationStatus} title="Object replication status" />
        </div>
        <dl className="kv kv-grid">
          <div className="kv-pair"><dt>Object ID</dt><dd className="mono">{m.objectId}</dd></div>
          <div className="kv-pair"><dt>Size</dt><dd>{formatBytes(m.size)} ({m.size.toLocaleString()} bytes)</dd></div>
          <div className="kv-pair"><dt>Content type</dt><dd>{m.contentType}</dd></div>
          <div className="kv-pair"><dt>Chunks</dt><dd>{m.chunkCount} × ≤ {formatBytes(m.chunkSize)}</dd></div>
          <div className="kv-pair"><dt>Replication factor</dt><dd>{m.replicationFactor}</dd></div>
          <div className="kv-pair"><dt>Created</dt><dd>{formatDateTime(m.createdAt)}</dd></div>
          <div className="kv-pair"><dt>Updated</dt><dd>{formatDateTime(m.updatedAt)}</dd></div>
          <div className="kv-pair"><dt>SHA-256</dt><dd className="mono small break">{stripShaPrefix(m.checksum)}</dd></div>
        </dl>
        <div className="row wrap">
          <a className="btn btn-primary" href={objectDownloadUrl(objectId)} download={m.name}>Download</a>
          <button
            className="btn btn-ghost"
            disabled={!!busy || !canHashInBrowser()}
            title={canHashInBrowser() ? 'Download in the browser and compare SHA-256' : 'WebCrypto requires localhost or HTTPS'}
            onClick={verifyDownload}
          >
            {verify.status === 'running' ? 'Verifying…' : 'Download & verify SHA-256'}
          </button>
          <button className="btn btn-ghost" disabled={!!busy} onClick={onScrub}>Scrub this object</button>
          <button className="btn btn-ghost" disabled={!!busy} onClick={onRepair}>Run repair</button>
          <button className="btn btn-danger" disabled={!!busy} onClick={onDelete}>Delete</button>
        </div>
        {verify.status === 'done' && (
          <div className={`alert ${verify.match ? 'alert-ok' : 'alert-error'} small`} data-testid="verify-result">
            {verify.match ? '✔ Byte-identical:' : '✘ Mismatch:'} downloaded {verify.bytes.toLocaleString()} bytes, SHA-256{' '}
            <span className="mono">{verify.actual}</span>
          </div>
        )}
      </div>

      <MetadataEditorCard meta={m} onSaved={() => void meta.refresh()} />

      <div className="card">
        <h2>Chunking</h2>
        <p className="small muted">Ordered chunks; width is proportional to chunk size (1 MiB, final chunk may be smaller).</p>
        <ChunkStrip chunks={m.chunks} />
      </div>

      <div className="card">
        <h2>Replica placement</h2>
        <p className="small muted">Each chunk should have {m.replicationFactor} healthy replicas on distinct workers. Click a replica cell to select it.</p>
        <ReplicaMatrix
          chunks={m.chunks}
          workerIds={(workers.data ?? []).map((w) => w.workerId)}
          selected={currentSelected}
          onSelect={setSelected}
        />
        {currentSelected && (
          <div className="selected-panel" data-testid="selected-replica">
            <div>
              <strong>Chunk #{currentSelected.chunk.index}</strong> on <strong>{currentSelected.replica.workerId}</strong>{' '}
              <StatusBadge value={currentSelected.replica.state} /> worker <StatusBadge value={currentSelected.replica.workerStatus} />
            </div>
            <div className="small mono break">{currentSelected.chunk.chunkId}</div>
            <div className="small">Expected SHA-256: <span className="mono break">{stripShaPrefix(currentSelected.chunk.checksum)}</span></div>
            <div className="small">Last verified: {formatDateTime(currentSelected.replica.lastVerifiedAt)}</div>
            <div className="row">
              <button
                className="btn btn-danger small"
                disabled={!!busy || currentSelected.replica.workerStatus !== 'UP'}
                onClick={() => onCorrupt(currentSelected)}
                title="Demo only: flips bytes in the chunk file on the worker's disk (requires WORKER_DEBUG_ENDPOINTS=true)"
              >
                Corrupt this replica (demo)
              </button>
              <button className="btn btn-ghost small" onClick={() => setSelected(null)}>Close</button>
            </div>
          </div>
        )}
      </div>

      <div className="card">
        <h2>Events for this object</h2>
        <EventList events={objectEvents} />
      </div>
    </div>
  );
}

