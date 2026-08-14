'use strict';

const { test } = require('node:test');
const assert = require('node:assert/strict');
const http = require('node:http');

const { buildBindingPath, invokeBinding } = require('../../lib/binding-client');
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

// Records every request and answers with the configured status/body/headers.
async function recordingSidecar(status = 200, body = '', headers = {}) {
  const requests = [];
  const sidecar = await fakeSidecar((req, res) => {
    const chunks = [];
    req.on('data', (c) => chunks.push(c));
    req.on('end', () => {
      const url = new URL(req.url, 'http://localhost');
      requests.push({
        method: req.method,
        path: url.pathname,
        headers: req.headers,
        body: Buffer.concat(chunks),
      });
      res.writeHead(status, headers).end(body);
    });
  });
  return { ...sidecar, requests };
}

// --- buildBindingPath ---

test('buildBindingPath encodes the binding name', () => {
  assert.equal(buildBindingPath('mybinding'), '/v1.0/bindings/mybinding');
  assert.equal(buildBindingPath('my binding'), '/v1.0/bindings/my%20binding');
});

test('buildBindingPath rejects an empty, ".", or ".." binding name', () => {
  for (const bad of ['', '.', '..']) {
    assert.throws(
      () => buildBindingPath(bad),
      (err) => err instanceof DaprError && err.code === ErrorCodes.INVALID_MESSAGE
    );
  }
});

test('buildBindingPath rejects a binding name containing a slash', () => {
  assert.throws(
    () => buildBindingPath('a/b'),
    (err) => err instanceof DaprError && err.code === ErrorCodes.INVALID_MESSAGE
  );
});

// --- invokeBinding ---

test('invokeBinding POSTs the data/metadata/operation envelope to /v1.0/bindings/<name>', async (t) => {
  const sidecar = await recordingSidecar(200, 'ok', { 'content-type': 'text/plain' });
  t.after(() => sidecar.stop());

  const result = await invokeBinding(
    { baseUrl: sidecar.baseUrl, token: 'realtoken' },
    { bindingName: 'orders', operation: 'create', data: { id: 1 }, metadata: { partition: 'p1' } }
  );

  const [received] = sidecar.requests;
  assert.equal(received.method, 'POST');
  assert.equal(received.path, '/v1.0/bindings/orders');
  assert.equal(received.headers['dapr-api-token'], 'realtoken');
  assert.deepEqual(JSON.parse(received.body.toString()), {
    data: { id: 1 },
    metadata: { partition: 'p1' },
    operation: 'create',
  });
  assert.equal(result.status, 200);
  assert.equal(result.body.toString(), 'ok');
});

test('invokeBinding on 204 resolves an empty body, not an error', async (t) => {
  const sidecar = await recordingSidecar(204, '');
  t.after(() => sidecar.stop());

  const result = await invokeBinding(
    { baseUrl: sidecar.baseUrl },
    { bindingName: 'orders', operation: 'create', data: null, metadata: {} }
  );
  assert.equal(result.status, 204);
  assert.equal(result.body.length, 0);
});

test('invokeBinding surfaces metadata.* response headers verbatim', async (t) => {
  const sidecar = await recordingSidecar(200, 'body', { 'metadata.statusCode': '201' });
  t.after(() => sidecar.stop());

  const result = await invokeBinding(
    { baseUrl: sidecar.baseUrl },
    { bindingName: 'orders', operation: 'create', data: null, metadata: {} }
  );
  // Node's http client lower-cases every incoming header name.
  assert.equal(result.headers['metadata.statuscode'], '201');
});

test('a non-2xx, non-204 invokeBinding response is BINDING_INVOKE_FAILED', async (t) => {
  const sidecar = await recordingSidecar(
    500,
    '{"errorCode":"ERR_INVOKE_OUTPUT_BINDING","message":"error invoking output binding orders: couldn\'t find output binding orders"}'
  );
  t.after(() => sidecar.stop());

  await assert.rejects(
    invokeBinding(
      { baseUrl: sidecar.baseUrl },
      { bindingName: 'orders', operation: 'create', data: null, metadata: {} }
    ),
    (err) =>
      err instanceof DaprError &&
      err.code === ErrorCodes.BINDING_INVOKE_FAILED &&
      /500/.test(err.message) &&
      /ERR_INVOKE_OUTPUT_BINDING/.test(err.message)
  );
});

test('an invokeBinding transport failure is SIDECAR_UNAVAILABLE', async () => {
  await assert.rejects(
    invokeBinding(
      { baseUrl: 'http://127.0.0.1:1', timeoutMs: 500 },
      { bindingName: 'orders', operation: 'create', data: null, metadata: {} }
    ),
    (err) => err instanceof DaprError && err.code === ErrorCodes.SIDECAR_UNAVAILABLE
  );
});

test('an over-size invokeBinding response is RESPONSE_TOO_LARGE', async (t) => {
  const sidecar = await recordingSidecar(200, 'x'.repeat(4096));
  t.after(() => sidecar.stop());

  await assert.rejects(
    invokeBinding(
      { baseUrl: sidecar.baseUrl, maxResponseBytes: 1024 },
      { bindingName: 'orders', operation: 'create', data: null, metadata: {} }
    ),
    (err) => err instanceof DaprError && err.code === ErrorCodes.RESPONSE_TOO_LARGE
  );
});

test('invokeBinding serializes an unrepresentable data value as INVALID_MESSAGE before contacting daprd', async (t) => {
  const sidecar = await recordingSidecar(200, '');
  t.after(() => sidecar.stop());

  const circular = {};
  circular.self = circular;

  await assert.rejects(
    invokeBinding(
      { baseUrl: sidecar.baseUrl },
      { bindingName: 'orders', operation: 'create', data: circular, metadata: {} }
    ),
    (err) => err instanceof DaprError && err.code === ErrorCodes.INVALID_MESSAGE
  );
  assert.equal(sidecar.requests.length, 0);
});
