import { useEffect, useState } from 'react';
import { errorMessage, getClusterStatus, listEvents, listWorkers, reconcileWorker, triggerRepair, triggerScrub } from '../api/client';
import type { ReconciliationResultInfo, RepairResultInfo, ScrubResultInfo } from '../api/types';
import { EventList } from '../components/EventList';
import { StatusBadge } from '../components/StatusBadge';
import { useToast } from '../components/Toasts';
import { ClusterSummary, WorkerCard } from '../components/WorkerCard';
import { usePolling } from '../hooks/usePolling';
import { formatTime } from '../lib/format';

type LastAction =
  | { kind: 'repair'; result: RepairResultInfo }
  | { kind: 'scrub'; result: ScrubResultInfo }
  | { kind: 'reconcile'; result: ReconciliationResultInfo };

function useNow(intervalMs = 1000): number {
  const [now, setNow] = useState(Date.now());
  useEffect(() => {
    const id = window.setInterval(() => setNow(Date.now()), intervalMs);
    return () => window.clearInterval(id);
  }, [intervalMs]);
  return now;
}

export function ClusterPage() {
  const toast = useToast();
  const now = useNow();
  const status = usePolling(getClusterStatus, 2000);
  const workers = usePolling(listWorkers, 2000);
  const events = usePolling(() => listEvents(undefined, 25), 3000);
  const [busy, setBusy] = useState<string | null>(null);
  const [last, setLast] = useState<LastAction | null>(null);

  const run = async (label: string, fn: () => Promise<void>) => {
    setBusy(label);
    try {
      await fn();
    } catch (e) {
      toast.error(`${label} failed: ${errorMessage(e)}`);
    } finally {
      setBusy(null);
      void status.refresh();
      void workers.refresh();
      void events.refresh();
    }
  };

  const workerList = workers.data ?? [];
  const totalUsed = workerList.reduce((s, w) => s + w.usedBytes, 0);

  return (
    <div className="stack">
      <div className="card">
        <div className="row space-between">
          <h2>Cluster replication health</h2>
          {status.data && <StatusBadge value={status.data.replicationStatus} />}
        </div>
        {status.error && <div className="alert alert-error">{status.error}</div>}
        {status.data ? <ClusterSummary status={status.data} /> : !status.error && <div className="muted">Loading…</div>}
        <div className="row wrap">
          <button
            className="btn btn-primary"
            disabled={!!busy}
            onClick={() => run('Repair', async () => {
              const result = await triggerRepair();
              setLast({ kind: 'repair', result });
              toast.success(`Repair finished: ${result.repairedChunks} chunk(s) restored to RF=2`);
            })}
          >
            {busy === 'Repair' ? 'Repairing…' : 'Run replication repair now'}
          </button>
          <button
            className="btn btn-ghost"
            disabled={!!busy}
            onClick={() => run('Integrity scrub', async () => {
              const result = await triggerScrub(undefined, true);
              setLast({ kind: 'scrub', result });
              toast.success(`Scrub finished: ${result.corruptReplicasDetected} corrupted replica(s) found, ${result.replicasRepaired} repaired`);
            })}
          >
            {busy === 'Integrity scrub' ? 'Scrubbing…' : 'Run integrity scrub (SHA-256)'}
          </button>
        </div>
        {last && <LastActionResult last={last} />}
      </div>

      <div className="card">
        <h2>Storage workers</h2>
        <p className="small muted">
          Stop a worker (<code>docker stop worker2</code>) to watch it go DOWN after the heartbeat timeout; restart it
          (<code>docker start worker2</code>) to watch it rejoin and reconcile.
        </p>
        {workers.error && <div className="alert alert-error">{workers.error}</div>}
        <div className="workers-grid">
          {workerList.map((w) => (
            <WorkerCard
              key={w.workerId}
              worker={w}
              totalUsedBytes={totalUsed}
              now={now}
              busy={!!busy}
              onReconcile={(id) => run('Reconcile', async () => {
                const result = await reconcileWorker(id);
                setLast({ kind: 'reconcile', result });
                toast.success(`Reconciled ${id}`);
              })}
            />
          ))}
          {workerList.length === 0 && !workers.loading && <div className="muted">No workers registered yet.</div>}
        </div>
      </div>

      <div className="card">
        <div className="row space-between">
          <h2>Recent events</h2>
          <a href="#/events" className="small">Full operations log →</a>
        </div>
        <EventList events={events.data ?? []} compact />
      </div>
    </div>
  );
}

function LastActionResult({ last }: { last: LastAction }) {
  if (last.kind === 'repair') {
    const r = last.result;
    return (
      <div className="alert alert-info small" data-testid="last-action">
        <strong>Repair @ {formatTime(r.executedAt)}:</strong> examined {r.examinedChunks} chunks, {r.underReplicatedChunksFound} under-replicated,
        {' '}{r.repairedChunks} repaired, {r.corruptedReplicasRepaired} corrupted replica(s) overwritten, {r.unrepairableChunks} unrepairable.
      </div>
    );
  }
  if (last.kind === 'scrub') {
    const r = last.result;
    return (
      <div className="alert alert-info small" data-testid="last-action">
        <strong>Scrub @ {formatTime(r.scrubbedAt)}:</strong> {r.replicasValid}/{r.replicasExamined} replicas valid,
        {' '}{r.corruptReplicasDetected} corrupted, {r.missingReplicasDetected} missing, {r.replicasRepaired} repaired.
      </div>
    );
  }
  const r = last.result;
  return (
    <div className="alert alert-info small" data-testid="last-action">
      <strong>Reconcile {r.workerId} @ {formatTime(r.reconciledAt)}:</strong> {r.pendingDeletionsExecuted} pending deletes, {r.orphanChunksDeleted} orphans removed,
      {' '}{r.replicasReadopted} re-adopted, {r.missingReplicasRemoved} missing, {r.corruptReplicasDetected} corrupt, {r.surplusReplicasTrimmed} surplus trimmed.
    </div>
  );
}

