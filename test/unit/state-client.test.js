'use strict';

const { test } = require('node:test');
const assert = require('node:assert/strict');
const http = require('node:http');

const {
  buildStatePath,
  stateGet,
  stateSave,
  stateDelete,
  stateBulkGet,
  stateTransaction,
} = require('../../lib/state-client');
const { DaprError, ErrorCodes } = require('../../lib/errors');

async function fakeSidecar(handler) {
  const server = http.createServer(handler);
  await new Promise((resolve) => server.listen(0, '127.0.0.1', resolve));
  return {
    baseUrl: `http://127.0.0.1:${server.address().port}`,
    stop: () =>
      new Promise((resolve) => {
        server.closeAllConnections();
        server.close(resolve);
      }),
  };
}

// Records every request and answers with the queued status/body/headers.
async function recordingSidecar(status = 204, body = '', extraHeaders = {}) {
  const requests = [];
  const state = { status, body, extraHeaders };
  const sidecar = await fakeSidecar((req, res) => {
    const chunks = [];
    req.on('data', (c) => chunks.push(c));
    req.on('end', () => {
      const url = new URL(req.url, 'http://localhost');
      requests.push({
        method: req.method,
        path: url.pathname,
        query: Object.fromEntries(url.searchParams),
        headers: req.headers,
        body: Buffer.concat(chunks),
      });
      res
        .writeHead(state.status, { 'content-type': 'application/json', ...state.extraHeaders })
        .end(state.body);
    });
  });
  return { ...sidecar, requests, state };
}

// --- buildStatePath ---

test('buildStatePath encodes the store name and an optional key segment', () => {
  assert.equal(buildStatePath('mystore'), '/v1.0/state/mystore');
  assert.equal(buildStatePath('mystore', 'my key'), '/v1.0/state/mystore/my%20key');
  assert.equal(buildStatePath('my store', 'a/b'), '/v1.0/state/my%20store/a%2Fb');
});

test('buildStatePath rejects an empty, ".", or ".." store name', () => {
  for (const bad of ['', '.', '..']) {
    assert.throws(
      () => buildStatePath(bad, 'key'),
      (err) => err instanceof DaprError && err.code === ErrorCodes.INVALID_MESSAGE
    );
  }
});

test('buildStatePath rejects a store name containing a slash', () => {
  assert.throws(
    () => buildStatePath('a/b', 'key'),
    (err) => err instanceof DaprError && err.code === ErrorCodes.INVALID_MESSAGE
  );
});

test('buildStatePath rejects an empty, ".", or ".." key', () => {
  for (const bad of ['', '.', '..']) {
    assert.throws(
      () => buildStatePath('store', bad),
      (err) => err instanceof DaprError && err.code === ErrorCodes.INVALID_MESSAGE
    );
  }
});

// --- stateGet ---

test('stateGet issues GET /v1.0/state/<store>/<key> with consistency and metadata as query params', async (t) => {
  const sidecar = await recordingSidecar(200, '{"hello":"world"}');
  t.after(() => sidecar.stop());

  const result = await stateGet(
    { baseUrl: sidecar.baseUrl, token: 'realtoken' },
    { storeName: 'orders', key: 'order-1', consistency: 'strong', metadata: { partition: 'p1' } }
  );

  const [received] = sidecar.requests;
  assert.equal(received.method, 'GET');
  assert.equal(received.path, '/v1.0/state/orders/order-1');
  assert.deepEqual(received.query, { consistency: 'strong', 'metadata.partition': 'p1' });
  assert.equal(received.headers['dapr-api-token'], 'realtoken');
  assert.deepEqual(result.value, { hello: 'world' });
  assert.equal(result.status, 200);
});

test('stateGet reads the etag from the response ETag header', async (t) => {
  const sidecar = await recordingSidecar(200, '"v"', { ETag: 'abc123' });
  t.after(() => sidecar.stop());

  const result = await stateGet({ baseUrl: sidecar.baseUrl }, { storeName: 's', key: 'k' });
  assert.equal(result.etag, 'abc123');
});

test('stateGet on 204 (key not found) resolves a null value with no etag, not an error', async (t) => {
  const sidecar = await recordingSidecar(204, '');
  t.after(() => sidecar.stop());

  const result = await stateGet({ baseUrl: sidecar.baseUrl }, { storeName: 's', key: 'missing' });
  assert.equal(result.value, null);
  assert.equal(result.etag, undefined);
  assert.equal(result.status, 204);
});

test('stateGet decodes the value by response content-type, reusing decodeBody', async (t) => {
  const sidecar = await fakeSidecar((req, res) => {
    res.writeHead(200, { 'content-type': 'text/plain' }).end('plain text');
  });
  t.after(() => sidecar.stop());

  const result = await stateGet({ baseUrl: sidecar.baseUrl }, { storeName: 's', key: 'k' });
  assert.equal(result.value, 'plain text');
});

test('a non-2xx, non-204 stateGet response is STATE_OPERATION_FAILED', async (t) => {
  const sidecar = await recordingSidecar(400, '{"errorCode":"ERR_STATE_STORE_NOT_FOUND"}');
  t.after(() => sidecar.stop());

  await assert.rejects(
    stateGet({ baseUrl: sidecar.baseUrl }, { storeName: 'missing', key: 'k' }),
    (err) =>
      err instanceof DaprError &&
      err.code === ErrorCodes.STATE_OPERATION_FAILED &&
      /400/.test(err.message) &&
      /ERR_STATE_STORE_NOT_FOUND/.test(err.message)
  );
});

test('a 409 stateGet response is STATE_ETAG_MISMATCH', async (t) => {
  const sidecar = await recordingSidecar(409, '{"errorCode":"ERR_ETAG_MISMATCH"}');
  t.after(() => sidecar.stop());

  await assert.rejects(
    stateGet({ baseUrl: sidecar.baseUrl }, { storeName: 's', key: 'k' }),
    (err) => err instanceof DaprError && err.code === ErrorCodes.STATE_ETAG_MISMATCH
  );
});

test('a stateGet transport failure is SIDECAR_UNAVAILABLE', async () => {
  await assert.rejects(
    stateGet({ baseUrl: 'http://127.0.0.1:1', timeoutMs: 500 }, { storeName: 's', key: 'k' }),
    (err) => err instanceof DaprError && err.code === ErrorCodes.SIDECAR_UNAVAILABLE
  );
});

test('an over-size stateGet response is RESPONSE_TOO_LARGE', async (t) => {
  const sidecar = await recordingSidecar(200, JSON.stringify('x'.repeat(4096)));
  t.after(() => sidecar.stop());

  await assert.rejects(
    stateGet({ baseUrl: sidecar.baseUrl, maxResponseBytes: 1024 }, { storeName: 's', key: 'k' }),
    (err) => err instanceof DaprError && err.code === ErrorCodes.RESPONSE_TOO_LARGE
  );
});

// --- stateSave ---

test('stateSave POSTs a single-item array to /v1.0/state/<store>', async (t) => {
  const sidecar = await recordingSidecar(204);
  t.after(() => sidecar.stop());

  await stateSave(
    { baseUrl: sidecar.baseUrl, token: 'realtoken' },
    { storeName: 'orders', item: { key: 'order-1', value: { total: 42 } } }
  );

  const [received] = sidecar.requests;
  assert.equal(received.method, 'POST');
  assert.equal(received.path, '/v1.0/state/orders');
  assert.equal(received.headers['dapr-api-token'], 'realtoken');
  assert.deepEqual(JSON.parse(received.body.toString()), [
    { key: 'order-1', value: { total: 42 } },
  ]);
});

test('stateSave forwards etag, metadata, and options exactly as shaped by the caller', async (t) => {
  const sidecar = await recordingSidecar(204);
  t.after(() => sidecar.stop());

  const item = {
    key: 'order-1',
    value: { total: 42 },
    etag: 'v1',
    metadata: { ttlInSeconds: '60' },
    options: { concurrency: 'first-write', consistency: 'strong' },
  };
  await stateSave({ baseUrl: sidecar.baseUrl }, { storeName: 'orders', item });
  assert.deepEqual(JSON.parse(sidecar.requests[0].body.toString()), [item]);
});

test('a non-2xx stateSave response is STATE_OPERATION_FAILED', async (t) => {
  const sidecar = await recordingSidecar(400, '{"errorCode":"ERR_MALFORMED_REQUEST"}');
  t.after(() => sidecar.stop());

  await assert.rejects(
    stateSave({ baseUrl: sidecar.baseUrl }, { storeName: 's', item: { key: 'k', value: 1 } }),
    (err) => err instanceof DaprError && err.code === ErrorCodes.STATE_OPERATION_FAILED
  );
});

test('an unserializable stateSave value is INVALID_MESSAGE, not a sidecar outage', async (t) => {
  const sidecar = await recordingSidecar(204);
  t.after(() => sidecar.stop());

  const circular = {};
  circular.self = circular;
  await assert.rejects(
    stateSave(
      { baseUrl: sidecar.baseUrl },
      { storeName: 's', item: { key: 'k', value: circular } }
    ),
    (err) => err instanceof DaprError && err.code === ErrorCodes.INVALID_MESSAGE
  );
  assert.equal(sidecar.requests.length, 0);
});

test('a stateSave transport failure is SIDECAR_UNAVAILABLE', async () => {
  await assert.rejects(
    stateSave(
      { baseUrl: 'http://127.0.0.1:1', timeoutMs: 500 },
      { storeName: 's', item: { key: 'k', value: 1 } }
    ),
    (err) => err instanceof DaprError && err.code === ErrorCodes.SIDECAR_UNAVAILABLE
  );
});

// --- stateDelete ---

test('stateDelete issues DELETE with the etag as an If-Match header, not a query param', async (t) => {
  const sidecar = await recordingSidecar(204);
  t.after(() => sidecar.stop());

  await stateDelete(
    { baseUrl: sidecar.baseUrl },
    {
      storeName: 'orders',
      key: 'order-1',
      etag: 'v1',
      consistency: 'eventual',
      concurrency: 'last-write',
    }
  );

  const [received] = sidecar.requests;
  assert.equal(received.method, 'DELETE');
  assert.equal(received.path, '/v1.0/state/orders/order-1');
  assert.equal(received.headers['if-match'], 'v1');
  assert.deepEqual(received.query, { consistency: 'eventual', concurrency: 'last-write' });
});

test('stateDelete without an etag sends no If-Match header', async (t) => {
  const sidecar = await recordingSidecar(204);
  t.after(() => sidecar.stop());

  await stateDelete({ baseUrl: sidecar.baseUrl }, { storeName: 's', key: 'k' });
  assert.equal('if-match' in sidecar.requests[0].headers, false);
});

test('a 409 stateDelete response is STATE_ETAG_MISMATCH', async (t) => {
  const sidecar = await recordingSidecar(409, '{"errorCode":"ERR_ETAG_MISMATCH"}');
  t.after(() => sidecar.stop());

  await assert.rejects(
    stateDelete({ baseUrl: sidecar.baseUrl }, { storeName: 's', key: 'k', etag: 'stale' }),
    (err) => err instanceof DaprError && err.code === ErrorCodes.STATE_ETAG_MISMATCH
  );
});

test('a stateDelete transport failure is SIDECAR_UNAVAILABLE', async () => {
  await assert.rejects(
    stateDelete({ baseUrl: 'http://127.0.0.1:1', timeoutMs: 500 }, { storeName: 's', key: 'k' }),
    (err) => err instanceof DaprError && err.code === ErrorCodes.SIDECAR_UNAVAILABLE
  );
});

// --- stateBulkGet ---

test('stateBulkGet POSTs keys/parallelism/metadata to the bulk endpoint', async (t) => {
  const sidecar = await recordingSidecar(
    200,
    JSON.stringify([
      { key: 'a', data: 1, etag: 'e1' },
      { key: 'b', error: 'not found' },
    ])
  );
  t.after(() => sidecar.stop());

  const result = await stateBulkGet(
    { baseUrl: sidecar.baseUrl, token: 'realtoken' },
    { storeName: 'orders', keys: ['a', 'b'], parallelism: 4, metadata: {} }
  );

  const [received] = sidecar.requests;
  assert.equal(received.method, 'POST');
  assert.equal(received.path, '/v1.0/state/orders/bulk');
  assert.equal(received.headers['dapr-api-token'], 'realtoken');
  assert.deepEqual(JSON.parse(received.body.toString()), {
    keys: ['a', 'b'],
    parallelism: 4,
    metadata: {},
  });
  assert.deepEqual(result.items, [
    { key: 'a', data: 1, etag: 'e1' },
    { key: 'b', error: 'not found' },
  ]);
});

test('a per-item error in a 200 stateBulkGet response does not fail the call', async (t) => {
  const sidecar = await recordingSidecar(200, JSON.stringify([{ key: 'a', error: 'boom' }]));
  t.after(() => sidecar.stop());

  const result = await stateBulkGet(
    { baseUrl: sidecar.baseUrl },
    { storeName: 's', keys: ['a'], metadata: {} }
  );
  assert.equal(result.items[0].error, 'boom');
});

test('a 2xx stateBulkGet response must contain the documented result array', async (t) => {
  const sidecar = await recordingSidecar(200, '{"items":[]}');
  t.after(() => sidecar.stop());

  await assert.rejects(
    stateBulkGet({ baseUrl: sidecar.baseUrl }, { storeName: 's', keys: ['a'], metadata: {} }),
    (err) => err instanceof DaprError && err.code === ErrorCodes.STATE_OPERATION_FAILED
  );
});

test('a whole-call non-2xx stateBulkGet response is STATE_OPERATION_FAILED', async (t) => {
  const sidecar = await recordingSidecar(500, '{"errorCode":"ERR_STATE_BULK_GET"}');
  t.after(() => sidecar.stop());

  await assert.rejects(
    stateBulkGet({ baseUrl: sidecar.baseUrl }, { storeName: 's', keys: ['a'], metadata: {} }),
    (err) => err instanceof DaprError && err.code === ErrorCodes.STATE_OPERATION_FAILED
  );
});

test('a stateBulkGet transport failure is SIDECAR_UNAVAILABLE', async () => {
  await assert.rejects(
    stateBulkGet(
      { baseUrl: 'http://127.0.0.1:1', timeoutMs: 500 },
      { storeName: 's', keys: ['a'], metadata: {} }
    ),
    (err) => err instanceof DaprError && err.code === ErrorCodes.SIDECAR_UNAVAILABLE
  );
});

// --- stateTransaction ---

test('stateTransaction POSTs operations/metadata to the transaction endpoint', async (t) => {
  const sidecar = await recordingSidecar(204);
  t.after(() => sidecar.stop());

  const operations = [
    { operation: 'upsert', request: { key: 'a', value: 1 } },
    { operation: 'delete', request: { key: 'b' } },
  ];
  await stateTransaction(
    { baseUrl: sidecar.baseUrl, token: 'realtoken' },
    { storeName: 'orders', operations, metadata: {} }
  );

  const [received] = sidecar.requests;
  assert.equal(received.method, 'POST');
  assert.equal(received.path, '/v1.0/state/orders/transaction');
  assert.equal(received.headers['dapr-api-token'], 'realtoken');
  assert.deepEqual(JSON.parse(received.body.toString()), { operations, metadata: {} });
});

test('a non-2xx stateTransaction response is STATE_OPERATION_FAILED', async (t) => {
  const sidecar = await recordingSidecar(400, '{"errorCode":"ERR_NOT_SUPPORTED_STATE_OPERATION"}');
  t.after(() => sidecar.stop());

  await assert.rejects(
    stateTransaction(
      { baseUrl: sidecar.baseUrl },
      {
        storeName: 's',
        operations: [{ operation: 'upsert', request: { key: 'a', value: 1 } }],
        metadata: {},
      }
    ),
    (err) => err instanceof DaprError && err.code === ErrorCodes.STATE_OPERATION_FAILED
  );
});

test('a 409 stateTransaction response is STATE_ETAG_MISMATCH', async (t) => {
  // Real daprd's transaction handler has no confirmed 409 branch, but a
  // store-dependent one is possible -- the shared status-branch in
  // lib/state-client.js applies identically across all 5 operations, so
  // this stays correct regardless of which stores ever take this path.
  const sidecar = await recordingSidecar(409, '{"errorCode":"ERR_ETAG_MISMATCH"}');
  t.after(() => sidecar.stop());

  await assert.rejects(
    stateTransaction(
      { baseUrl: sidecar.baseUrl },
      {
        storeName: 's',
        operations: [{ operation: 'upsert', request: { key: 'a', value: 1 } }],
        metadata: {},
      }
    ),
    (err) => err instanceof DaprError && err.code === ErrorCodes.STATE_ETAG_MISMATCH
  );
});

test('a stateTransaction transport failure is SIDECAR_UNAVAILABLE', async () => {
  await assert.rejects(
    stateTransaction(
      { baseUrl: 'http://127.0.0.1:1', timeoutMs: 500 },
      { storeName: 's', operations: [{ operation: 'delete', request: { key: 'a' } }], metadata: {} }
    ),
    (err) => err instanceof DaprError && err.code === ErrorCodes.SIDECAR_UNAVAILABLE
  );
});
