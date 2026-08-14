'use strict';

const { test } = require('node:test');
const assert = require('node:assert/strict');
const http = require('node:http');

const {
  buildConfigurationPath,
  buildConfigurationCallbackPath,
  getConfiguration,
  subscribeConfiguration,
  unsubscribeConfiguration,
} = require('../../lib/configuration-client');
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

// Records every request and answers with the queued status/body.
async function recordingSidecar(status = 200, body = '{}') {
  const requests = [];
  const state = { status, body };
  const sidecar = await fakeSidecar((req, res) => {
    const chunks = [];
    req.on('data', (c) => chunks.push(c));
    req.on('end', () => {
      const url = new URL(req.url, 'http://localhost');
      requests.push({
        method: req.method,
        path: url.pathname,
        query: [...url.searchParams.entries()],
        headers: req.headers,
        body: Buffer.concat(chunks),
      });
      res.writeHead(state.status, { 'content-type': 'application/json' }).end(state.body);
    });
  });
  return { ...sidecar, requests, state };
}

// --- buildConfigurationPath ---

test('buildConfigurationPath encodes the store name', () => {
  assert.equal(buildConfigurationPath('mystore'), '/v1.0/configuration/mystore');
  assert.equal(buildConfigurationPath('my store'), '/v1.0/configuration/my%20store');
});

test('buildConfigurationPath rejects an empty, ".", or ".." store name', () => {
  for (const bad of ['', '.', '..']) {
    assert.throws(
      () => buildConfigurationPath(bad),
      (err) => err instanceof DaprError && err.code === ErrorCodes.INVALID_MESSAGE
    );
  }
});

test('buildConfigurationPath rejects a store name containing a slash', () => {
  assert.throws(
    () => buildConfigurationPath('a/b'),
    (err) => err instanceof DaprError && err.code === ErrorCodes.INVALID_MESSAGE
  );
});

test('buildConfigurationCallbackPath preserves legal path characters and encodes spaces', () => {
  assert.equal(
    buildConfigurationCallbackPath('configstore', 'customer:feature flag'),
    '/configuration/configstore/customer:feature%20flag'
  );
});

test('buildConfigurationCallbackPath rejects keys that cannot be represented as one callback route', () => {
  for (const bad of ['a?b', 'a#b', 'a/../b', 'a//b']) {
    assert.throws(
      () => buildConfigurationCallbackPath('configstore', bad),
      (err) => err instanceof DaprError && err.code === ErrorCodes.INVALID_MESSAGE
    );
  }
});

// --- getConfiguration ---

test('getConfiguration issues GET /v1.0/configuration/<store> with repeated key and metadata.* params', async (t) => {
  const sidecar = await recordingSidecar(200, JSON.stringify({ a: { value: '1', version: 'v1' } }));
  t.after(() => sidecar.stop());

  const result = await getConfiguration(
    { baseUrl: sidecar.baseUrl, token: 'realtoken' },
    { storeName: 'orders', keys: ['a', 'b'], metadata: { partition: 'p1' } }
  );

  const [received] = sidecar.requests;
  assert.equal(received.method, 'GET');
  assert.equal(received.path, '/v1.0/configuration/orders');
  assert.deepEqual(received.query, [
    ['key', 'a'],
    ['key', 'b'],
    ['metadata.partition', 'p1'],
  ]);
  assert.equal(received.headers['dapr-api-token'], 'realtoken');
  assert.deepEqual(result.items, { a: { value: '1', version: 'v1' } });
  assert.equal(result.status, 200);
});

test('getConfiguration with no keys sends no key query params', async (t) => {
  const sidecar = await recordingSidecar(200, '{}');
  t.after(() => sidecar.stop());

  await getConfiguration({ baseUrl: sidecar.baseUrl }, { storeName: 'orders' });
  assert.deepEqual(sidecar.requests[0].query, []);
});

test('getConfiguration on 204 (no items) resolves an empty items object, not an error', async (t) => {
  const sidecar = await recordingSidecar(204, '');
  t.after(() => sidecar.stop());

  const result = await getConfiguration({ baseUrl: sidecar.baseUrl }, { storeName: 'orders' });
  assert.deepEqual(result.items, {});
  assert.equal(result.status, 204);
});

test('a malformed (non-object) getConfiguration response is CONFIGURATION_OPERATION_FAILED', async (t) => {
  const sidecar = await recordingSidecar(200, '[1,2,3]');
  t.after(() => sidecar.stop());

  await assert.rejects(
    getConfiguration({ baseUrl: sidecar.baseUrl }, { storeName: 'orders' }),
    (err) => err instanceof DaprError && err.code === ErrorCodes.CONFIGURATION_OPERATION_FAILED
  );
});

test('a non-2xx, non-204 getConfiguration response is CONFIGURATION_OPERATION_FAILED', async (t) => {
  const sidecar = await recordingSidecar(400, '{"errorCode":"ERR_CONFIGURATION_STORE_NOT_FOUND"}');
  t.after(() => sidecar.stop());

  await assert.rejects(
    getConfiguration({ baseUrl: sidecar.baseUrl }, { storeName: 'missing' }),
    (err) =>
      err instanceof DaprError &&
      err.code === ErrorCodes.CONFIGURATION_OPERATION_FAILED &&
      /400/.test(err.message) &&
      /ERR_CONFIGURATION_STORE_NOT_FOUND/.test(err.message)
  );
});

test('a getConfiguration transport failure is SIDECAR_UNAVAILABLE', async () => {
  await assert.rejects(
    getConfiguration({ baseUrl: 'http://127.0.0.1:1', timeoutMs: 500 }, { storeName: 'orders' }),
    (err) => err instanceof DaprError && err.code === ErrorCodes.SIDECAR_UNAVAILABLE
  );
});

test('an over-size getConfiguration response is RESPONSE_TOO_LARGE', async (t) => {
  const sidecar = await recordingSidecar(200, JSON.stringify({ a: { value: 'x'.repeat(4096) } }));
  t.after(() => sidecar.stop());

  await assert.rejects(
    getConfiguration({ baseUrl: sidecar.baseUrl, maxResponseBytes: 1024 }, { storeName: 'orders' }),
    (err) => err instanceof DaprError && err.code === ErrorCodes.RESPONSE_TOO_LARGE
  );
});

// --- subscribeConfiguration ---

test('subscribeConfiguration issues GET .../subscribe with repeated key params and returns the subscription id', async (t) => {
  const sidecar = await recordingSidecar(200, JSON.stringify({ id: 'sub-1' }));
  t.after(() => sidecar.stop());

  const result = await subscribeConfiguration(
    { baseUrl: sidecar.baseUrl, token: 'realtoken' },
    { storeName: 'orders', keys: ['a', 'b'], metadata: {} }
  );

  const [received] = sidecar.requests;
  assert.equal(received.method, 'GET');
  assert.equal(received.path, '/v1.0/configuration/orders/subscribe');
  assert.deepEqual(received.query, [
    ['key', 'a'],
    ['key', 'b'],
  ]);
  assert.equal(received.headers['dapr-api-token'], 'realtoken');
  assert.equal(result.id, 'sub-1');
  assert.equal(result.status, 200);
});

test('a subscribeConfiguration response with no id is CONFIGURATION_OPERATION_FAILED', async (t) => {
  const sidecar = await recordingSidecar(200, '{}');
  t.after(() => sidecar.stop());

  await assert.rejects(
    subscribeConfiguration({ baseUrl: sidecar.baseUrl }, { storeName: 'orders', keys: ['a'] }),
    (err) => err instanceof DaprError && err.code === ErrorCodes.CONFIGURATION_OPERATION_FAILED
  );
});

test('a non-2xx subscribeConfiguration response is CONFIGURATION_OPERATION_FAILED', async (t) => {
  const sidecar = await recordingSidecar(500, '{"errorCode":"ERR_CONFIGURATION_SUBSCRIBE"}');
  t.after(() => sidecar.stop());

  await assert.rejects(
    subscribeConfiguration({ baseUrl: sidecar.baseUrl }, { storeName: 'orders', keys: ['a'] }),
    (err) => err instanceof DaprError && err.code === ErrorCodes.CONFIGURATION_OPERATION_FAILED
  );
});

test('a subscribeConfiguration transport failure is SIDECAR_UNAVAILABLE', async () => {
  await assert.rejects(
    subscribeConfiguration(
      { baseUrl: 'http://127.0.0.1:1', timeoutMs: 500 },
      { storeName: 'orders', keys: ['a'] }
    ),
    (err) => err instanceof DaprError && err.code === ErrorCodes.SIDECAR_UNAVAILABLE
  );
});

// --- unsubscribeConfiguration ---

test('unsubscribeConfiguration issues GET .../<subId>/unsubscribe', async (t) => {
  const sidecar = await recordingSidecar(200, '{"ok":true}');
  t.after(() => sidecar.stop());

  const result = await unsubscribeConfiguration(
    { baseUrl: sidecar.baseUrl, token: 'realtoken' },
    { storeName: 'orders', subscriptionId: 'sub-1' }
  );

  const [received] = sidecar.requests;
  assert.equal(received.method, 'GET');
  assert.equal(received.path, '/v1.0/configuration/orders/sub-1/unsubscribe');
  assert.equal(received.headers['dapr-api-token'], 'realtoken');
  assert.equal(result.status, 200);
});

test('a non-2xx unsubscribeConfiguration response is CONFIGURATION_OPERATION_FAILED, even without an errorCode field', async (t) => {
  // Real daprd's unsubscribe failure body is {"ok":false,"message":"..."} --
  // no errorCode field, unlike the generic envelope get/subscribe use.
  const sidecar = await recordingSidecar(500, '{"ok":false,"message":"failed to unsubscribe"}');
  t.after(() => sidecar.stop());

  await assert.rejects(
    unsubscribeConfiguration(
      { baseUrl: sidecar.baseUrl },
      { storeName: 'orders', subscriptionId: 'sub-1' }
    ),
    (err) =>
      err instanceof DaprError &&
      err.code === ErrorCodes.CONFIGURATION_OPERATION_FAILED &&
      /failed to unsubscribe/.test(err.message)
  );
});

test('unsubscribeConfiguration rejects an invalid subscriptionId before any request', async (t) => {
  const sidecar = await recordingSidecar(200, '{"ok":true}');
  t.after(() => sidecar.stop());

  for (const bad of ['', '.', '..']) {
    await assert.rejects(
      unsubscribeConfiguration(
        { baseUrl: sidecar.baseUrl },
        { storeName: 'orders', subscriptionId: bad }
      ),
      (err) => err instanceof DaprError && err.code === ErrorCodes.INVALID_MESSAGE
    );
  }
  assert.equal(sidecar.requests.length, 0);
});

test('an unsubscribeConfiguration transport failure is SIDECAR_UNAVAILABLE', async () => {
  await assert.rejects(
    unsubscribeConfiguration(
      { baseUrl: 'http://127.0.0.1:1', timeoutMs: 500 },
      { storeName: 'orders', subscriptionId: 'sub-1' }
    ),
    (err) => err instanceof DaprError && err.code === ErrorCodes.SIDECAR_UNAVAILABLE
  );
});
