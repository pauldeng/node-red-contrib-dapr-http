'use strict';

const { test } = require('node:test');
const assert = require('node:assert/strict');
const http = require('node:http');

const { getMetadata } = require('../../lib/metadata-client');
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

async function recordingSidecar(status = 200, body = '{}') {
  const requests = [];
  const sidecar = await fakeSidecar((req, res) => {
    requests.push({ method: req.method, path: req.url, headers: req.headers });
    res.writeHead(status, { 'content-type': 'application/json' }).end(body);
  });
  return { ...sidecar, requests };
}

test('getMetadata issues GET /v1.0/metadata', async (t) => {
  const sidecar = await recordingSidecar(
    200,
    JSON.stringify({
      id: 'my-app',
      runtimeVersion: '1.18.2',
      components: [
        { name: 'pubsub', type: 'pubsub.redis', version: 'v1', capabilities: ['BULK_PUBLISH'] },
      ],
      subscriptions: [{ topic: 'orders' }],
      extended: { arbitrary: 'must not escape' },
    })
  );
  t.after(() => sidecar.stop());

  const result = await getMetadata({ baseUrl: sidecar.baseUrl, token: 'realtoken' });

  const [received] = sidecar.requests;
  assert.equal(received.method, 'GET');
  assert.equal(received.path, '/v1.0/metadata');
  assert.equal(received.headers['dapr-api-token'], 'realtoken');
  assert.deepEqual(result.data, {
    id: 'my-app',
    runtimeVersion: '1.18.2',
    components: [{ name: 'pubsub', type: 'pubsub.redis' }],
    componentCount: 1,
    subscriptionCount: 1,
  });
  assert.equal(result.status, 200);
});

test('getMetadata bounds the curated admin response', async (t) => {
  const longText = 'x'.repeat(300);
  const sidecar = await recordingSidecar(
    200,
    JSON.stringify({
      id: longText,
      runtimeVersion: longText,
      components: Array.from({ length: 25 }, (_, i) => ({
        name: `${i}-${longText}`,
        type: longText,
      })),
    })
  );
  t.after(() => sidecar.stop());

  const result = await getMetadata({ baseUrl: sidecar.baseUrl });

  assert.equal(result.data.id.length, 256);
  assert.equal(result.data.runtimeVersion.length, 256);
  assert.equal(result.data.componentCount, 25);
  assert.equal(result.data.components.length, 20);
  assert.ok(result.data.components.every((component) => component.name.length <= 256));
  assert.ok(result.data.components.every((component) => component.type.length <= 256));
});

test('getMetadata omits the token header when none is configured', async (t) => {
  const sidecar = await recordingSidecar(200, '{}');
  t.after(() => sidecar.stop());

  await getMetadata({ baseUrl: sidecar.baseUrl });
  assert.equal(sidecar.requests[0].headers['dapr-api-token'], undefined);
});

test('a non-2xx getMetadata response is METADATA_OPERATION_FAILED', async (t) => {
  const sidecar = await recordingSidecar(401, JSON.stringify({ errorCode: 'ERR_API_TOKEN' }));
  t.after(() => sidecar.stop());

  await assert.rejects(
    getMetadata({ baseUrl: sidecar.baseUrl }),
    (err) =>
      err instanceof DaprError &&
      err.code === ErrorCodes.METADATA_OPERATION_FAILED &&
      /401/.test(err.message)
  );
});

test('a malformed (non-JSON) 200 getMetadata response is METADATA_OPERATION_FAILED', async (t) => {
  const sidecar = await recordingSidecar(200, 'not json');
  t.after(() => sidecar.stop());

  await assert.rejects(
    getMetadata({ baseUrl: sidecar.baseUrl }),
    (err) => err instanceof DaprError && err.code === ErrorCodes.METADATA_OPERATION_FAILED
  );
});

test('a getMetadata transport failure is SIDECAR_UNAVAILABLE', async () => {
  await assert.rejects(
    getMetadata({ baseUrl: 'http://127.0.0.1:1', timeoutMs: 500 }),
    (err) => err instanceof DaprError && err.code === ErrorCodes.SIDECAR_UNAVAILABLE
  );
});

test('an over-size getMetadata response is RESPONSE_TOO_LARGE', async (t) => {
  const sidecar = await recordingSidecar(200, JSON.stringify({ id: 'x'.repeat(4096) }));
  t.after(() => sidecar.stop());

  await assert.rejects(
    getMetadata({ baseUrl: sidecar.baseUrl, maxResponseBytes: 1024 }),
    (err) => err instanceof DaprError && err.code === ErrorCodes.RESPONSE_TOO_LARGE
  );
});
