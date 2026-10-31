import type { MetadataRow } from '../lib/metadata';

/** Editable list of custom-metadata key/value rows. */
export function MetadataRowsEditor({
  rows,
  onChange,
  disabled = false,
}: {
  rows: MetadataRow[];
  onChange: (rows: MetadataRow[]) => void;
  disabled?: boolean;
}) {
  const update = (i: number, patch: Partial<MetadataRow>) =>
    onChange(rows.map((r, idx) => (idx === i ? { ...r, ...patch } : r)));
  const remove = (i: number) => onChange(rows.filter((_, idx) => idx !== i));
  const add = () => onChange([...rows, { key: '', value: '' }]);

  return (
    <div className="meta-rows">
      {rows.length === 0 && <div className="muted small">No custom metadata.</div>}
      {rows.map((row, i) => (
        <div className="meta-row" key={i}>
          <input
            aria-label={`metadata key ${i + 1}`}
            placeholder="key"
            value={row.key}
            disabled={disabled}
            onChange={(e) => update(i, { key: e.target.value })}
          />
          <input
            aria-label={`metadata value ${i + 1}`}
            placeholder="value"
            value={row.value}
            disabled={disabled}
            onChange={(e) => update(i, { value: e.target.value })}
          />
          <button
            type="button"
            className="btn btn-ghost"
            disabled={disabled}
            onClick={() => remove(i)}
            aria-label={`remove metadata row ${i + 1}`}
          >
            ✕
          </button>
        </div>
      ))}
      <button type="button" className="btn btn-ghost small" disabled={disabled} onClick={add}>
        + Add key
      </button>
    </div>
  );
}

