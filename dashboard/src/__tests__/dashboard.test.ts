import assert from 'node:assert/strict';
import { afterEach, describe, it } from 'node:test';
import {
  ApiError,
  deleteObject,
  errorMessage,
  getClusterStatus,
  getObjectMetadata,
  listObjects,
  objectDownloadUrl,
  triggerRepair,
  triggerScrub,
  updateObjectMetadata,
} from '../api/client.ts';
import { matrixWorkerIds, replicaCellClass } from '../components/ReplicaMatrix.tsx';
import { toneFor } from '../components/StatusBadge.tsx';
import { formatAge, formatBytes, shortId, stripShaPrefix } from '../lib/format.ts';
import { buildMergePatch, mapToRows, rowsToMap, validateMetadataRows } from '../lib/metadata.ts';
import { mergeEvents } from '../pages/EventsPage.tsx';
import { parseRoute } from '../router.ts';

describe('format helpers', () => {
  it('formats byte sizes cleanly', () => {
    assert.equal(formatBytes(0), '0 B');
    assert.equal(formatBytes(1023), '1023 B');
    assert.equal(formatBytes(1024), '1.00 KiB');
    assert.equal(formatBytes(1_048_576), '1.00 MiB');
    assert.equal(formatBytes(-1), '—');
  });

  it('formats relative ages and strips sha256 prefix', () => {
    const now = Date.parse('2026-10-31T10:00:30Z');
    assert.equal(formatAge('2026-10-31T10:00:25Z', now), '5s ago');
    assert.equal(formatAge('2026-10-31T09:57:30Z', now), '3m ago');
    assert.equal(stripShaPrefix('sha256:abcdef'), 'abcdef');
    assert.equal(shortId('3f2a9c10-4b2e-4f1a-8e2d-91c2b3a4d5e6'), '3f2a9c10…d5e6');
  });
});

describe('metadata validation & JSON Merge Patch builder (plan.md D12)', () => {
  it('validates keys and value lengths', () => {
    assert.deepEqual(validateMetadataRows([{ key: 'owner', value: 'alice' }]), []);
    assert.equal(validateMetadataRows([{ key: 'bad key!', value: 'x' }]).length, 1);
    assert.ok(
      validateMetadataRows([
        { key: 'dup', value: '1' },
        { key: 'dup', value: '2' },
      ]).includes('Duplicate key "dup"'),
    );
  });

  it('builds merge patch adding, updating, and removing keys with null', () => {
    const original = { owner: 'alice', env: 'dev', keep: 'same' };
    const edited = [
      { key: 'owner', value: 'bob' }, // updated
      { key: 'keep', value: 'same' }, // unchanged -> omitted
      { key: 'tag', value: 'v2' }, // added
      // 'env' removed -> null
    ];
    const patch = buildMergePatch(original, edited);
    assert.deepEqual(patch, {
      owner: 'bob',
      tag: 'v2',
      env: null,
    });
    assert.deepEqual(rowsToMap(mapToRows({ b: '2', a: '1' })), { a: '1', b: '2' });
  });
});

describe('ReplicaMatrix & StatusBadge helpers', () => {
  it('computes sorted worker columns and cell CSS classes for all replica states', () => {
    const chunks = [
      {
        chunkId: 'obj_00000',
        index: 0,
        size: 1_048_576,
        checksum: 'sha256:11',
        replicationStatus: 'HEALTHY' as const,
        replicas: [
          { workerId: 'worker3', state: 'HEALTHY' as const, workerStatus: 'UP' as const, lastVerifiedAt: null },
          { workerId: 'worker1', state: 'CORRUPTED' as const, workerStatus: 'UP' as const, lastVerifiedAt: null },
        ],
      },
    ];
    assert.deepEqual(matrixWorkerIds(chunks, ['worker4', 'worker2', 'worker1']), [
      'worker1',
      'worker2',
      'worker3',
      'worker4',
    ]);
    assert.equal(replicaCellClass(undefined), 'cell-empty');
    assert.equal(
      replicaCellClass({ workerId: 'worker1', state: 'HEALTHY', workerStatus: 'UP', lastVerifiedAt: null }),
      'cell-healthy',
    );
    assert.equal(
      replicaCellClass({ workerId: 'worker2', state: 'UNAVAILABLE', workerStatus: 'DOWN', lastVerifiedAt: null }),
      'cell-unavailable',
    );
    assert.equal(
      replicaCellClass({ workerId: 'worker3', state: 'CORRUPTED', workerStatus: 'UP', lastVerifiedAt: null }),
      'cell-corrupted',
    );
    assert.equal(toneFor('HEALTHY'), 'ok');
    assert.equal(toneFor('DEGRADED'), 'warn');
    assert.equal(toneFor('CORRUPTED'), 'bad');
  });
});

describe('router and event merging', () => {
  it('parses hash routes accurately', () => {
    assert.deepEqual(parseRoute(''), { name: 'objects' });
    assert.deepEqual(parseRoute('#/objects'), { name: 'objects' });
    assert.deepEqual(parseRoute('#/objects/abc-123'), { name: 'object', objectId: 'abc-123' });
    assert.deepEqual(parseRoute('#/cluster'), { name: 'cluster' });
    assert.deepEqual(parseRoute('#/events'), { name: 'events' });
  });

  it('deduplicates and orders system events by id', () => {
    const e1 = { id: 1, occurredAt: 't1', type: 'A', severity: 'INFO' as const, objectId: null, chunkId: null, workerId: null, message: 'm1' };
    const e2 = { id: 2, occurredAt: 't2', type: 'B', severity: 'WARN' as const, objectId: null, chunkId: null, workerId: null, message: 'm2' };
    const e3 = { id: 3, occurredAt: 't3', type: 'C', severity: 'ERROR' as const, objectId: null, chunkId: null, workerId: null, message: 'm3' };
    assert.deepEqual(
      mergeEvents([e1, e2], [e2, e3]).map((e) => e.id),
      [1, 2, 3],
    );
  });
});

describe('API client against controller REST endpoints', () => {
  const originalFetch = globalThis.fetch;

  afterEach(() => {
    globalThis.fetch = originalFetch;
  });

  it('calls real REST paths for listObjects, getObjectMetadata, PATCH metadata, DELETE, cluster status, repair & scrub', async () => {
    globalThis.fetch = async (input: RequestInfo | URL, init?: RequestInit) => {
      const url = String(input);
      if (url.startsWith('/api/v1/objects?')) {
        return new Response(
          JSON.stringify({ items: [], page: 0, size: 10, totalItems: 0, totalPages: 0 }),
          { status: 200, headers: { 'Content-Type': 'application/json' } },
        );
      }
      if (url === '/api/v1/objects/obj-1/metadata' && (!init?.method || init.method === 'GET')) {
        return new Response(
          JSON.stringify({
            objectId: 'obj-1',
            name: 'report.pdf',
            size: 1024,
            contentType: 'application/pdf',
            checksum: 'sha256:aa',
            chunkCount: 1,
            chunkSize: 1048576,
            replicationFactor: 2,
            replicationStatus: 'HEALTHY',
            createdAt: '2026-10-31T00:00:00Z',
            updatedAt: '2026-10-31T00:00:00Z',
            customMetadata: { owner: 'alice' },
            chunks: [],
          }),
          { status: 200, headers: { 'Content-Type': 'application/json' } },
        );
      }
      if (url === '/api/v1/objects/obj-1/metadata' && init?.method === 'PATCH') {
        assert.deepEqual(JSON.parse(String(init.body)), { owner: 'bob', oldKey: null });
        return new Response(
          JSON.stringify({
            objectId: 'obj-1',
            name: 'report.pdf',
            size: 1024,
            contentType: 'application/pdf',
            checksum: 'sha256:aa',
            chunkCount: 1,
            chunkSize: 1048576,
            replicationFactor: 2,
            replicationStatus: 'HEALTHY',
            createdAt: '2026-10-31T00:00:00Z',
            updatedAt: '2026-10-31T00:01:00Z',
            customMetadata: { owner: 'bob' },
            chunks: [],
          }),
          { status: 200, headers: { 'Content-Type': 'application/json' } },
        );
      }
      if (url === '/api/v1/objects/obj-1' && init?.method === 'DELETE') {
        return new Response(null, { status: 204 });
      }
      if (url === '/api/v1/cluster/status') {
        return new Response(
          JSON.stringify({
            replicationFactor: 2,
            workersTotal: 4,
            workersUp: 4,
            workersDown: 0,
            activeObjects: 1,
            totalChunks: 2,
            healthyChunks: 2,
            underReplicatedChunks: 0,
            unavailableChunks: 0,
            totalHealthyReplicas: 4,
            corruptedReplicas: 0,
            replicationStatus: 'HEALTHY',
          }),
          { status: 200, headers: { 'Content-Type': 'application/json' } },
        );
      }
      if (url === '/api/v1/admin/replication/repair' && init?.method === 'POST') {
        return new Response(
          JSON.stringify({
            executedAt: '2026-10-31T00:00:00Z',
            examinedChunks: 2,
            underReplicatedChunksFound: 1,
            repairedChunks: 1,
            corruptedReplicasRepaired: 0,
            skippedWithinGracePeriod: 0,
            unrepairableChunks: 0,
            repairedChunkIds: ['obj-1_00000'],
          }),
          { status: 200, headers: { 'Content-Type': 'application/json' } },
        );
      }
      if (url.startsWith('/api/v1/admin/integrity/scrub') && init?.method === 'POST') {
        return new Response(
          JSON.stringify({
            scrubbedAt: '2026-10-31T00:00:00Z',
            objectId: null,
            replicasExamined: 4,
            replicasValid: 4,
            corruptReplicasDetected: 0,
            missingReplicasDetected: 0,
            replicasRepaired: 0,
            corruptedChunkIds: [],
            repairedChunkIds: [],
          }),
          { status: 200, headers: { 'Content-Type': 'application/json' } },
        );
      }
      return new Response(
        JSON.stringify({
          timestamp: '2026-10-31T00:00:00Z',
          status: 404,
          error: 'Not Found',
          code: 'OBJECT_NOT_FOUND',
          message: 'Object missing does not exist',
          path: url,
        }),
        { status: 404, headers: { 'Content-Type': 'application/json' } },
      );
    };

    const page = await listObjects(0, 10, 'createdAt,desc');
    assert.equal(page.totalItems, 0);

    const meta = await getObjectMetadata('obj-1');
    assert.equal(meta.customMetadata.owner, 'alice');

    const patched = await updateObjectMetadata('obj-1', { owner: 'bob', oldKey: null });
    assert.equal(patched.customMetadata.owner, 'bob');

    await deleteObject('obj-1');
    assert.equal(objectDownloadUrl('obj-1'), '/api/v1/objects/obj-1');

    const status = await getClusterStatus();
    assert.equal(status.workersUp, 4);

    const repair = await triggerRepair();
    assert.equal(repair.repairedChunks, 1);

    const scrub = await triggerScrub(undefined, true);
    assert.equal(scrub.replicasValid, 4);

    await assert.rejects(() => getObjectMetadata('missing'), ApiError);
    try {
      await getObjectMetadata('missing');
    } catch (e) {
      assert.ok(errorMessage(e).includes('OBJECT_NOT_FOUND'));
    }
  });
});
