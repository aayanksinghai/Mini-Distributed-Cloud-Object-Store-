import { getClusterStatus } from './api/client';
import { StatusBadge } from './components/StatusBadge';
import { ToastProvider } from './components/Toasts';
import { usePolling } from './hooks/usePolling';
import { ClusterPage } from './pages/ClusterPage';
import { EventsPage } from './pages/EventsPage';
import { ObjectDetailPage } from './pages/ObjectDetailPage';
import { ObjectsPage } from './pages/ObjectsPage';
import { useRoute } from './router';

function Header() {
  const route = useRoute();
  const status = usePolling(getClusterStatus, 3000);
  const active = (name: string) => (route.name === name || (name === 'objects' && route.name === 'object') ? 'active' : '');
  return (
    <header className="header">
      <div className="brand">
        <span className="logo">◆</span> Mini Distributed Cloud Object Store
      </div>
      <nav>
        <a className={active('objects')} href="#/objects">Objects</a>
        <a className={active('cluster')} href="#/cluster">Cluster</a>
        <a className={active('events')} href="#/events">Operations log</a>
      </nav>
      <div className="header-status" data-testid="header-status">
        {status.error ? (
          <span className="badge badge-bad" title={status.error}>Controller unreachable</span>
        ) : status.data ? (
          <>
            <span className="small">
              Workers {status.data.workersUp}/{status.data.workersTotal} UP
            </span>
            <StatusBadge value={status.data.replicationStatus} title="Cluster replication status" />
          </>
        ) : (
          <span className="small muted">Connecting…</span>
        )}
      </div>
    </header>
  );
}

export function App() {
  const route = useRoute();
  return (
    <ToastProvider>
      <Header />
      <main className="main">
        {route.name === 'objects' && <ObjectsPage />}
        {route.name === 'object' && <ObjectDetailPage key={route.objectId} objectId={route.objectId} />}
        {route.name === 'cluster' && <ClusterPage />}
        {route.name === 'events' && <EventsPage />}
      </main>
      <footer className="footer small muted">
        1 controller · 4 workers · 1 MiB chunks · RF = 2 · SHA-256 integrity — all data from the live REST API
      </footer>
    </ToastProvider>
  );
}

