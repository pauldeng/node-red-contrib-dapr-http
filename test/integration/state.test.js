'use strict';

// Real daprd 1.18.2 + a real Redis state.redis component -- state management
// never touches pub/sub broker semantics, so this needs no NATS/JetStream
// and no pubsub component (test/helpers/integration.js's stateComponentYaml
// is passed through startDaprd's broker-agnostic `components` array, not its
// `redisPort` shortcut, which would also write a pub/sub component this
// suite has no use for). Proves: a real save/get round trip carries a real
// ETag, delete actually removes the key, stale save/delete etags are genuine
// 409s from the Redis component, a transactional conflict remains daprd's
// generic 500, bulk get resolves a mix of present/absent keys in one call,
// and a transaction applies its upsert/delete atomically.

const { test } = require('node:test');
const assert = require('node:assert/strict');

const { freePort } = require('../helpers/node-red');
const { ContainerNodeRed } = require('../helpers/node-red-container');
const { httpRequest } = require('../helpers/http');
const { startRedis, startDaprd, stateComponentYaml } = require('../helpers/integration');
const { waitFor } = require('../helpers/wait-for');

const STORE = 'statestore';

function flow({ appPort, daprHttpPort }) {
  return [
    { id: 'tab', type: 'tab', label: 'it-state' },
    {
      id: 'c1',
      type: 'dapr-connection',
      daprHost: '127.0.0.1',
      daprPort: String(daprHttpPort),
      bindAddress: '127.0.0.1',
      appPort: String(appPort),
    },
    { id: 'in', type: 'http in', z: 'tab', url: '/state', method: 'post', wires: [['before']] },
    {
      id: 'before',
      type: 'function',
      z: 'tab',
      func: 'msg.dapr = msg.payload.dapr; msg.payload = msg.payload.payload; return msg;',
      outputs: 1,
      wires: [['state']],
    },
    {
      id: 'state',
      type: 'dapr-state',
      z: 'tab',
      connection: 'c1',
      operation: 'get',
      storeName: STORE,
      key: '',
      consistency: '',
      concurrency: '',
      ttlSeconds: '',
      metadata: '{}',
      wires: [['success']],
    },
    {
      id: 'success',
      type: 'function',
      z: 'tab',
      func: 'msg.statusCode = 200; msg.payload = { dapr: msg.dapr, payload: msg.payload }; return msg;',
      outputs: 1,
      wires: [['res']],
    },
    {
      id: 'errors',
      type: 'catch',
      z: 'tab',
      scope: ['state'],
      uncaught: false,
      wires: [['failure']],
    },
    {
      id: 'failure',
      type: 'function',
      z: 'tab',
      func: 'msg.statusCode = 503; msg.payload = { message: msg.error.message, code: msg.error.code }; return msg;',
      outputs: 1,
      wires: [['res']],
    },
    { id: 'res', type: 'http response', z: 'tab' },
  ];
}

test(
  'a real state.redis component supports save/get/delete round trip, etag conflicts, bulk get, and transactions',
  { timeout: 60000 },
  async (t) => {
    const appPort = await freePort();
    const daprHttpPort = await freePort();

    const nr = new ContainerNodeRed();
    await nr.start({ flows: flow({ appPort, daprHttpPort }) });
    t.after(() => nr.stop());
    await waitFor(async () => {
      const r = await httpRequest(`http://127.0.0.1:${appPort}/healthz`, { timeoutMs: 1000 });
      return r.status === 204 ? true : null;
    });

    const redis = await startRedis();
    t.after(() => redis.stop());
    const daprd = await startDaprd({
      appId: 'it-state-app',
      appPort,
      httpPort: daprHttpPort,
      components: [{ filename: 'statestore.yaml', yaml: stateComponentYaml(redis.port) }],
    });
    t.after(() => daprd.stop());

    // startDaprd() resolving only proves daprd's OWN /healthz answers; the
    // dapr-connection node polls independently on its own bounded-backoff
    // schedule and may not have caught up yet.
    await waitFor(() => (/Dapr sidecar is available/.test(nr.logText()) ? true : null));

    const call = (dapr, payload) =>
      waitFor(async () => {
        const r = await httpRequest(nr.nodeUrl('/state'), {
          method: 'POST',
          headers: { 'content-type': 'application/json' },
          body: JSON.stringify({ dapr, payload }),
          timeoutMs: 4000,
        });
        return r.status ? r : null;
      });

    // save -> get round trip carries a real ETag from the Redis component.
    const saved = await call({ operation: 'save', key: 'order-1' }, { total: 42 });
    assert.equal(saved.status, 200);

    const got = await call({ operation: 'get', key: 'order-1' });
    assert.equal(got.status, 200);
    const gotBody = JSON.parse(got.text);
    assert.deepEqual(gotBody.payload, { total: 42 });
    assert.ok(gotBody.dapr.etag, 'a real Redis component must return a real ETag');

    // A first-write save with a stale etag is a genuine 409 from the Redis
    // component, not only a fake-sidecar status-code classification.
    const staleSave = await call(
      {
        operation: 'save',
        key: 'order-1',
        etag: String(Number(gotBody.dapr.etag) + 1),
        concurrency: 'first-write',
      },
      { total: 99 }
    );
    assert.equal(staleSave.status, 503);
    assert.equal(JSON.parse(staleSave.text).code, 'STATE_ETAG_MISMATCH', staleSave.text);

    // get on a key that was never saved is a normal 204/null, not an error.
    const missing = await call({ operation: 'get', key: 'never-saved' });
    assert.equal(missing.status, 200);
    assert.equal(JSON.parse(missing.text).payload, null);

    // delete with a stale etag is a genuine 409 from the Redis component.
    const staleDelete = await call({
      operation: 'delete',
      key: 'order-1',
      etag: String(Number(gotBody.dapr.etag) + 1),
    });
    assert.equal(staleDelete.status, 503);
    assert.equal(JSON.parse(staleDelete.text).code, 'STATE_ETAG_MISMATCH');

    // delete with the correct etag actually removes the key.
    const realDelete = await call({ operation: 'delete', key: 'order-1', etag: gotBody.dapr.etag });
    assert.equal(realDelete.status, 200);
    const afterDelete = await call({ operation: 'get', key: 'order-1' });
    assert.equal(JSON.parse(afterDelete.text).payload, null);

    // bulk get resolves a mix of present and absent keys in one call.
    await call({ operation: 'save', key: 'bulk-a' }, { n: 1 });
    await call({ operation: 'save', key: 'bulk-b' }, { n: 2 });
    const bulkB = await call({ operation: 'get', key: 'bulk-b' });
    const bulk = await call({ operation: 'bulkGet' }, ['bulk-a', 'bulk-b', 'bulk-missing']);
    assert.equal(bulk.status, 200);
    const bulkItems = JSON.parse(bulk.text).payload;
    const byKey = new Map(bulkItems.map((item) => [item.key, item]));
    assert.deepEqual(byKey.get('bulk-a').data, { n: 1 });
    assert.deepEqual(byKey.get('bulk-b').data, { n: 2 });
    assert.equal(byKey.get('bulk-missing').data, undefined);

    // daprd 1.18.2 returns a generic 500 for a transactional Redis ETag
    // conflict, so the client must not promise the save/delete 409 contract.
    const staleTransaction = await call({ operation: 'transaction' }, [
      {
        operation: 'upsert',
        key: 'bulk-b',
        value: { n: 3 },
        etag: String(Number(JSON.parse(bulkB.text).dapr.etag) + 1),
        concurrency: 'first-write',
      },
    ]);
    assert.equal(staleTransaction.status, 503);
    assert.equal(JSON.parse(staleTransaction.text).code, 'STATE_OPERATION_FAILED');

    // a transaction applies its upsert/delete atomically.
    const transaction = await call({ operation: 'transaction' }, [
      { operation: 'upsert', key: 'tx-upsert', value: { done: true } },
      { operation: 'delete', key: 'bulk-a' },
    ]);
    assert.equal(transaction.status, 200);
    const txUpsert = await call({ operation: 'get', key: 'tx-upsert' });
    assert.deepEqual(JSON.parse(txUpsert.text).payload, { done: true });
    const txDeleted = await call({ operation: 'get', key: 'bulk-a' });
    assert.equal(JSON.parse(txDeleted.text).payload, null);
  }
);
