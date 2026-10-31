import type { MetadataPatch } from '../api/types';

/** Editable key/value row used by the upload form and the metadata editor. */
export interface MetadataRow {
  key: string;
  value: string;
}

// Mirrors server-side validation in ObjectService (keys: 1-128 of [A-Za-z0-9._-], values ≤ 1024, ≤ 64 keys).
export const METADATA_KEY_PATTERN = /^[A-Za-z0-9._-]{1,128}$/;
export const MAX_METADATA_VALUE_LENGTH = 1024;
export const MAX_METADATA_KEYS = 64;

/** Returns a list of human-readable validation errors (empty list = valid). Blank rows are ignored. */
export function validateMetadataRows(rows: MetadataRow[]): string[] {
  const errors: string[] = [];
  const seen = new Set<string>();
  const filled = rows.filter((r) => r.key.trim() !== '' || r.value !== '');
  for (const row of filled) {
    const key = row.key.trim();
    if (!METADATA_KEY_PATTERN.test(key)) {
      errors.push(`Invalid key "${key}": use 1-128 characters of letters, digits, '.', '_' or '-'`);
    } else if (seen.has(key)) {
      errors.push(`Duplicate key "${key}"`);
    }
    seen.add(key);
    if (row.value.length > MAX_METADATA_VALUE_LENGTH) {
      errors.push(`Value for "${key}" exceeds ${MAX_METADATA_VALUE_LENGTH} characters`);
    }
  }
  if (seen.size > MAX_METADATA_KEYS) {
    errors.push(`At most ${MAX_METADATA_KEYS} metadata keys are allowed`);
  }
  return errors;
}

/** Converts rows (ignoring blank ones) into a plain key → value map. */
export function rowsToMap(rows: MetadataRow[]): Record<string, string> {
  const out: Record<string, string> = {};
  for (const row of rows) {
    const key = row.key.trim();
    if (key !== '') out[key] = row.value;
  }
  return out;
}

export function mapToRows(map: Record<string, string>): MetadataRow[] {
  return Object.keys(map)
    .sort()
    .map((key) => ({ key, value: map[key] }));
}

/**
 * Builds a JSON Merge Patch (plan.md D12) that transforms `original` into the edited rows:
 * new or changed keys → string value, removed keys → null, unchanged keys omitted.
 */
export function buildMergePatch(original: Record<string, string>, rows: MetadataRow[]): MetadataPatch {
  const edited = rowsToMap(rows);
  const patch: MetadataPatch = {};
  for (const [key, value] of Object.entries(edited)) {
    if (original[key] !== value) patch[key] = value;
  }
  for (const key of Object.keys(original)) {
    if (!(key in edited)) patch[key] = null;
  }
  return patch;
}

