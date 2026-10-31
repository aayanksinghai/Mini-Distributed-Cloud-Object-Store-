import { useEffect, useState } from 'react';

/** Minimal hash-based routing (no router dependency): #/objects, #/objects/:id, #/cluster, #/events. */
export type Route =
  | { name: 'objects' }
  | { name: 'object'; objectId: string }
  | { name: 'cluster' }
  | { name: 'events' };

export function parseRoute(hash: string): Route {
  const path = hash.replace(/^#/, '').replace(/\/+$/, '');
  const parts = path.split('/').filter(Boolean);
  if (parts[0] === 'objects' && parts[1]) return { name: 'object', objectId: decodeURIComponent(parts[1]) };
  if (parts[0] === 'cluster') return { name: 'cluster' };
  if (parts[0] === 'events') return { name: 'events' };
  return { name: 'objects' };
}

export function navigate(path: string): void {
  window.location.hash = path;
}

export function useRoute(): Route {
  const [route, setRoute] = useState<Route>(() => parseRoute(window.location.hash));
  useEffect(() => {
    const onChange = () => setRoute(parseRoute(window.location.hash));
    window.addEventListener('hashchange', onChange);
    return () => window.removeEventListener('hashchange', onChange);
  }, []);
  return route;
}

