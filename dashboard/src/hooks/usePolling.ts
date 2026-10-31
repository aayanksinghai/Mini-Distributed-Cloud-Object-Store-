import { useCallback, useEffect, useRef, useState } from 'react';
import { errorMessage } from '../api/client';

export interface PollingState<T> {
  data: T | null;
  error: string | null;
  loading: boolean;
  lastUpdated: number | null;
  refresh: () => Promise<void>;
}

/**
 * Repeatedly invokes `fetcher` every `intervalMs` (and immediately on mount / when `deps` change).
 * Keeps the last successful data visible while surfacing the latest error, so a briefly unreachable
 * controller does not blank the UI. Pauses while the browser tab is hidden.
 */
export function usePolling<T>(fetcher: () => Promise<T>, intervalMs: number, deps: unknown[] = []): PollingState<T> {
  const [data, setData] = useState<T | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);
  const [lastUpdated, setLastUpdated] = useState<number | null>(null);
  const fetcherRef = useRef(fetcher);
  fetcherRef.current = fetcher;
  const mounted = useRef(true);

  const refresh = useCallback(async () => {
    try {
      const result = await fetcherRef.current();
      if (!mounted.current) return;
      setData(result);
      setError(null);
      setLastUpdated(Date.now());
    } catch (e) {
      if (!mounted.current) return;
      setError(errorMessage(e));
    } finally {
      if (mounted.current) setLoading(false);
    }
  }, []);

  useEffect(() => {
    mounted.current = true;
    setLoading(true);
    void refresh();
    const id = window.setInterval(() => {
      if (typeof document === 'undefined' || document.visibilityState !== 'hidden') {
        void refresh();
      }
    }, intervalMs);
    return () => {
      mounted.current = false;
      window.clearInterval(id);
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [intervalMs, refresh, ...deps]);

  return { data, error, loading, lastUpdated, refresh };
}

