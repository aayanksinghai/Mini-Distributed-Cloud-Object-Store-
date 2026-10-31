import { useRef, useState, type DragEvent } from 'react';
import { deleteObject, errorMessage, listObjects, objectDownloadUrl, uploadObject } from '../api/client';
import type { ObjectInfo } from '../api/types';
import { MetadataRowsEditor } from '../components/MetadataRowsEditor';
import { useToast } from '../components/Toasts';
import { usePolling } from '../hooks/usePolling';
import { formatBytes, formatDateTime, shortId } from '../lib/format';
import { rowsToMap, validateMetadataRows, type MetadataRow } from '../lib/metadata';
import { navigate } from '../router';

const PAGE_SIZE = 10;
const ONE_MIB = 1024 * 1024;

function UploadPanel({ onUploaded }: { onUploaded: (info: ObjectInfo) => void }) {
  const toast = useToast();
  const [file, setFile] = useState<File | null>(null);
  const [rows, setRows] = useState<MetadataRow[]>([]);
  const [progress, setProgress] = useState<number | null>(null);
  const [dragging, setDragging] = useState(false);
  const inputRef = useRef<HTMLInputElement>(null);
  const errors = validateMetadataRows(rows);
  const uploading = progress !== null;

  const onDrop = (e: DragEvent) => {
    e.preventDefault();
    setDragging(false);
    const dropped = e.dataTransfer.files?.[0];
    if (dropped) setFile(dropped);
  };

  const submit = async () => {
    if (!file || errors.length > 0) return;
    setProgress(0);
    try {
      const info = await uploadObject(file, rowsToMap(rows), setProgress);
      toast.success(`Uploaded "${info.name}" — ${formatBytes(info.size)} in ${info.chunkCount} chunk(s), RF=2`);
      setFile(null);
      setRows([]);
      if (inputRef.current) inputRef.current.value = '';
      onUploaded(info);
    } catch (e) {
      toast.error(`Upload failed: ${errorMessage(e)}`);
    } finally {
      setProgress(null);
    }
  };

  return (
    <div className="card">
      <h2>Upload object</h2>
      <div
        className={`dropzone ${dragging ? 'dropzone-active' : ''}`}
        onDragOver={(e) => {
          e.preventDefault();
          setDragging(true);
        }}
        onDragLeave={() => setDragging(false)}
        onDrop={onDrop}
        onClick={() => !uploading && inputRef.current?.click()}
        data-testid="dropzone"
      >
        <input
          ref={inputRef}
          type="file"
          hidden
          aria-label="file to upload"
          onChange={(e) => setFile(e.target.files?.[0] ?? null)}
        />
        {file ? (
          <div>
            <strong>{file.name}</strong> — {formatBytes(file.size)} →{' '}
            <span className="muted">
              will be split into {Math.max(1, Math.ceil(file.size / ONE_MIB))} chunk(s) of ≤ 1 MiB
              {file.size === 0 ? ' (empty object: 0 chunks)' : ''}
            </span>
          </div>
        ) : (
          <div className="muted">Drag &amp; drop any binary file here, or click to choose</div>
        )}
      </div>
      <h4>Custom metadata (optional)</h4>
      <MetadataRowsEditor rows={rows} onChange={setRows} disabled={uploading} />
      {errors.length > 0 && (
        <ul className="errors small">
          {errors.map((err) => (
            <li key={err}>{err}</li>
          ))}
        </ul>
      )}
      {uploading && (
        <div className="bar" aria-label="upload progress">
          <div className="bar-fill" style={{ width: `${Math.round((progress ?? 0) * 100)}%` }} />
        </div>
      )}
      <div className="row">
        <button className="btn btn-primary" disabled={!file || uploading || errors.length > 0} onClick={submit}>
          {uploading
            ? (progress ?? 0) >= 1
              ? 'Chunking & replicating…'
              : `Uploading ${Math.round((progress ?? 0) * 100)}%`
            : 'Upload'}
        </button>
      </div>
    </div>
  );
}

export function ObjectsPage() {
  const toast = useToast();
  const [page, setPage] = useState(0);
  const [sort, setSort] = useState('createdAt,desc');
  const [deleting, setDeleting] = useState<string | null>(null);
  const { data, error, loading, refresh } = usePolling(() => listObjects(page, PAGE_SIZE, sort), 5000, [page, sort]);

  const onDelete = async (obj: ObjectInfo) => {
    if (!window.confirm(`Delete "${obj.name}" and all ${obj.chunkCount * 2} chunk replicas? This cannot be undone.`)) return;
    setDeleting(obj.objectId);
    try {
      await deleteObject(obj.objectId);
      toast.success(`Deleted "${obj.name}"`);
      await refresh();
    } catch (e) {
      toast.error(`Delete failed: ${errorMessage(e)}`);
    } finally {
      setDeleting(null);
    }
  };

  return (
    <div className="stack">
      <UploadPanel onUploaded={() => void refresh()} />
      <div className="card">
        <div className="row space-between">
          <h2>Stored objects {data && <span className="muted">({data.totalItems})</span>}</h2>
          <label className="small">
            Sort{' '}
            <select value={sort} onChange={(e) => { setSort(e.target.value); setPage(0); }} aria-label="sort objects">
              <option value="createdAt,desc">Newest first</option>
              <option value="createdAt,asc">Oldest first</option>
              <option value="name,asc">Name A→Z</option>
              <option value="size,desc">Largest first</option>
              <option value="updatedAt,desc">Recently updated</option>
            </select>
          </label>
        </div>
        {error && <div className="alert alert-error">{error}</div>}
        {loading && !data ? (
          <div className="muted">Loading…</div>
        ) : data && data.items.length === 0 ? (
          <div className="muted">No objects stored yet. Upload one above.</div>
        ) : (
          data && (
            <div className="table-wrap">
              <table data-testid="objects-table">
                <thead>
                  <tr>
                    <th>Name</th>
                    <th>Size</th>
                    <th>Chunks</th>
                    <th>Content type</th>
                    <th>Metadata</th>
                    <th>Created</th>
                    <th>SHA-256</th>
                    <th />
                  </tr>
                </thead>
                <tbody>
                  {data.items.map((obj) => (
                    <tr key={obj.objectId}>
                      <td>
                        <a href={`#/objects/${obj.objectId}`}>{obj.name}</a>
                        <div className="muted small mono">{shortId(obj.objectId)}</div>
                      </td>
                      <td>{formatBytes(obj.size)}</td>
                      <td>{obj.chunkCount}</td>
                      <td className="small">{obj.contentType}</td>
                      <td className="small">
                        {Object.entries(obj.customMetadata).map(([k, v]) => (
                          <span key={k} className="chip">{k}={v}</span>
                        ))}
                      </td>
                      <td className="small">{formatDateTime(obj.createdAt)}</td>
                      <td className="mono small" title={obj.checksum}>{shortId(obj.checksum.replace('sha256:', ''), 10, 4)}</td>
                      <td className="actions">
                        <button className="btn btn-ghost small" onClick={() => navigate(`/objects/${obj.objectId}`)}>Inspect</button>
                        <a className="btn btn-ghost small" href={objectDownloadUrl(obj.objectId)} download={obj.name}>Download</a>
                        <button className="btn btn-danger small" disabled={deleting === obj.objectId} onClick={() => onDelete(obj)}>
                          {deleting === obj.objectId ? 'Deleting…' : 'Delete'}
                        </button>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )
        )}
        {data && data.totalPages > 1 && (
          <div className="row pager">
            <button className="btn btn-ghost small" disabled={page === 0} onClick={() => setPage(page - 1)}>← Prev</button>
            <span className="small">Page {data.page + 1} of {data.totalPages}</span>
            <button className="btn btn-ghost small" disabled={page + 1 >= data.totalPages} onClick={() => setPage(page + 1)}>Next →</button>
          </div>
        )}
      </div>
    </div>
  );
}

