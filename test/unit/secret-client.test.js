'use strict';

const { test } = require('node:test');
const assert = require('node:assert/strict');
const http = require('node:http');

const { buildSecretPath, getSecret } = require('../../lib/secret-client');
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
    const url = new URL(req.url, 'http://localhost');
    requests.push({
      method: req.method,
      path: url.pathname,
      query: Object.fromEntries(url.searchParams),
      headers: req.headers,
    });
    res.writeHead(status, { 'content-type': 'application/json' }).end(body);
  });
  return { ...sidecar, requests };
}

// A distinctive string standing in for a real secret's key/name -- a leak
// would be this exact substring surviving into a thrown error's message.
const MARKER = 'sk-live-marker-should-never-leak';

// --- buildSecretPath ---

test('buildSecretPath encodes the store name and key', () => {
  assert.equal(buildSecretPath('mystore', 'apiKey'), '/v1.0/secrets/mystore/apiKey');
  assert.equal(buildSecretPath('my store', 'a key'), '/v1.0/secrets/my%20store/a%20key');
});

test('buildSecretPath rejects an empty, ".", or ".." store name or key', () => {
  for (const bad of ['', '.', '..']) {
    assert.throws(
      () => buildSecretPath(bad, 'k'),
      (err) => err instanceof DaprError && err.code === ErrorCodes.INVALID_MESSAGE
    );
    assert.throws(
      () => buildSecretPath('s', bad),
      (err) => err instanceof DaprError && err.code === ErrorCodes.INVALID_MESSAGE
    );
  }
});

test('buildSecretPath rejects a store name containing a slash', () => {
  assert.throws(
    () => buildSecretPath('a/b', 'k'),
    (err) => err instanceof DaprError && err.code === ErrorCodes.INVALID_MESSAGE
  );
});

// --- getSecret ---

test('getSecret issues GET /v1.0/secrets/<store>/<key> with metadata.* query params', async (t) => {
  const sidecar = await recordingSidecar(200, JSON.stringify({ apiKey: 'real-value' }));
  t.after(() => sidecar.stop());

  const result = await getSecret(
    { baseUrl: sidecar.baseUrl, token: 'realtoken' },
    { storeName: 'vault', key: 'apiKey', metadata: { version_id: 'v2' } }
  );

  const [received] = sidecar.requests;
  assert.equal(received.method, 'GET');
  assert.equal(received.path, '/v1.0/secrets/vault/apiKey');
  assert.deepEqual(received.query, { 'metadata.version_id': 'v2' });
  assert.equal(received.headers['dapr-api-token'], 'realtoken');
  assert.deepEqual(result.data, { apiKey: 'real-value' });
  assert.equal(result.status, 200);
});

test('getSecret with no metadata sends no query params', async (t) => {
  const sidecar = await recordingSidecar(200, '{}');
  t.after(() => sidecar.stop());

  await getSecret({ baseUrl: sidecar.baseUrl }, { storeName: 'vault', key: 'apiKey' });
  assert.deepEqual(sidecar.requests[0].query, {});
});

test('getSecret on 204 resolves null data, not an error', async (t) => {
  const sidecar = await recordingSidecar(204, '');
  t.after(() => sidecar.stop());

  const result = await getSecret({ baseUrl: sidecar.baseUrl }, { storeName: 'vault', key: 'k' });
  assert.equal(result.data, null);
  assert.equal(result.status, 204);
});

test("a 403 response is SECRET_ACCESS_DENIED, and never includes daprd's own body", async (t) => {
  const sidecar = await recordingSidecar(
    403,
    JSON.stringify({
      errorCode: 'ERR_PERMISSION_DENIED',
      message: `access denied by policy to get "${MARKER}" from "vault"`,
    })
  );
  t.after(() => sidecar.stop());

  await assert.rejects(
    getSecret({ baseUrl: sidecar.baseUrl }, { storeName: 'vault', key: MARKER }),
    (err) => {
      assert.ok(err instanceof DaprError);
      assert.equal(err.code, ErrorCodes.SECRET_ACCESS_DENIED);
      assert.ok(!err.message.includes(MARKER), 'error message must never include the marker');
      return true;
    }
  );
});

test("a 401 (store not found) response is SECRET_OPERATION_FAILED, and never includes daprd's own body", async (t) => {
  const sidecar = await recordingSidecar(
    401,
    JSON.stringify({
      errorCode: 'ERR_SECRET_STORE_NOT_FOUND',
      message: `failed finding secret store with key ${MARKER}`,
    })
  );
  t.after(() => sidecar.stop());

  await assert.rejects(
    getSecret({ baseUrl: sidecar.baseUrl }, { storeName: MARKER, key: 'k' }),
    (err) => {
      assert.ok(err instanceof DaprError);
      assert.equal(err.code, ErrorCodes.SECRET_OPERATION_FAILED);
      assert.ok(!err.message.includes(MARKER), 'error message must never include the marker');
      return true;
    }
  );
});

test("a 500 (not found or component error) response is SECRET_OPERATION_FAILED, and never includes daprd's own body", async (t) => {
  const sidecar = await recordingSidecar(
    500,
    JSON.stringify({
      errorCode: 'ERR_SECRET_GET',
      message: `failed getting secret with key ${MARKER} from secret store vault: secret ${MARKER} not found`,
    })
  );
  t.after(() => sidecar.stop());

  await assert.rejects(
    getSecret({ baseUrl: sidecar.baseUrl }, { storeName: 'vault', key: MARKER }),
    (err) => {
      assert.ok(err instanceof DaprError);
      assert.equal(err.code, ErrorCodes.SECRET_OPERATION_FAILED);
      assert.ok(!err.message.includes(MARKER), 'error message must never include the marker');
      return true;
    }
  );
});

test('a getSecret transport failure is SIDECAR_UNAVAILABLE', async () => {
  await assert.rejects(
    getSecret({ baseUrl: 'http://127.0.0.1:1', timeoutMs: 500 }, { storeName: 'vault', key: 'k' }),
    (err) => err instanceof DaprError && err.code === ErrorCodes.SIDECAR_UNAVAILABLE
  );
});

test('an over-size getSecret response is RESPONSE_TOO_LARGE', async (t) => {
  const sidecar = await recordingSidecar(200, JSON.stringify({ k: 'x'.repeat(4096) }));
  t.after(() => sidecar.stop());

  await assert.rejects(
    getSecret(
      { baseUrl: sidecar.baseUrl, maxResponseBytes: 1024 },
      { storeName: 'vault', key: 'k' }
    ),
    (err) => err instanceof DaprError && err.code === ErrorCodes.RESPONSE_TOO_LARGE
  );
});

test('a malformed (non-JSON) 200 getSecret response is SECRET_OPERATION_FAILED, and never includes the raw body', async (t) => {
  const sidecar = await recordingSidecar(200, `not json, contains ${MARKER}`);
  t.after(() => sidecar.stop());

  await assert.rejects(
    getSecret({ baseUrl: sidecar.baseUrl }, { storeName: 'vault', key: 'k' }),
    (err) => {
      assert.ok(err instanceof DaprError);
      assert.equal(err.code, ErrorCodes.SECRET_OPERATION_FAILED);
      assert.ok(!err.message.includes(MARKER), 'error message must never include the marker');
      return true;
    }
  );
});
