/** Returns true when the browser exposes WebCrypto SHA-256 (requires a secure context such as localhost/https). */
export function canHashInBrowser(): boolean {
  return typeof globalThis.crypto !== 'undefined' && typeof globalThis.crypto.subtle?.digest === 'function';
}

/** Computes the lowercase hex SHA-256 digest of `data` using WebCrypto. */
export async function sha256Hex(data: ArrayBuffer): Promise<string> {
  const digest = await globalThis.crypto.subtle.digest('SHA-256', data);
  return Array.from(new Uint8Array(digest))
    .map((b) => b.toString(16).padStart(2, '0'))
    .join('');
}

