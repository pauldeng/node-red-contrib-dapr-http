'use strict';

const { test } = require('node:test');
const assert = require('node:assert/strict');
const http = require('node:http');

const { publish, publishBulk } = require('../../lib/dapr-client');
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

// Records every request and answers with the queued status (204 by default).
async function recordingSidecar(status = 204, body = '') {
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
        query: Object.fromEntries(url.searchParams),
        headers: req.headers,
        body: Buffer.concat(chunks),
      });
      res.writeHead(state.status, { 'content-type': 'application/json' }).end(state.body);
    });
  });
  return { ...sidecar, requests, state };
}

const request = (over = {}) => ({
  pubsubName: 'pubsub',
  topic: 'orders',
  data: { id: 1 },
  options: { contentType: 'application/json', metadata: {} },
  ...over,
});

test('publish POSTs to the sidecar pub/sub API with metadata as query parameters', async (t) => {
  const sidecar = await recordingSidecar();
  t.after(() => sidecar.stop());

  await publish(
    { baseUrl: sidecar.baseUrl, token: 'realtoken', timeoutMs: 5000 },
    request({ options: { contentType: 'application/json', metadata: { ttlInSeconds: '10' } } })
  );

  const [received] = sidecar.requests;
  assert.equal(received.method, 'POST');
  assert.equal(received.path, '/v1.0/publish/pubsub/orders');
  assert.deepEqual(received.query, { 'metadata.ttlInSeconds': '10' });
  assert.equal(received.headers['content-type'], 'application/json');
  assert.equal(received.headers['dapr-api-token'], 'realtoken');
  assert.equal(received.headers['content-length'], '8');
  assert.equal(received.body.toString(), '{"id":1}');
});

test('publish sends no dapr-api-token header when no token is configured', async (t) => {
  const sidecar = await recordingSidecar();
  t.after(() => sidecar.stop());

  await publish({ baseUrl: sidecar.baseUrl }, request());
  assert.equal('dapr-api-token' in sidecar.requests[0].headers, false);
});

test('publish serializes falsy payloads as themselves, never as an omitted body', async (t) => {
  const sidecar = await recordingSidecar();
  t.after(() => sidecar.stop());

  for (const data of [0, false, null]) {
    await publish(
      { baseUrl: sidecar.baseUrl },
      request({ data, options: { contentType: 'application/json', metadata: {} } })
    );
    assert.equal(sidecar.requests.at(-1).body.toString(), JSON.stringify(data));
  }

  await publish(
    { baseUrl: sidecar.baseUrl },
    request({ data: '', options: { contentType: 'text/plain', metadata: {} } })
  );
  assert.equal(sidecar.requests.at(-1).body.toString(), '');
  assert.equal(sidecar.requests.at(-1).headers['content-length'], '0');
});

test('publish sends a Buffer payload byte for byte and a text payload verbatim', async (t) => {
  const sidecar = await recordingSidecar();
  t.after(() => sidecar.stop());

  const bytes = Buffer.from([0, 1, 255]);
  await publish(
    { baseUrl: sidecar.baseUrl },
    request({ data: bytes, options: { contentType: 'application/octet-stream', metadata: {} } })
  );
  assert.deepEqual(sidecar.requests.at(-1).body, bytes);

  await publish(
    { baseUrl: sidecar.baseUrl },
    request({ data: 'plain text', options: { contentType: 'text/plain', metadata: {} } })
  );
  assert.equal(sidecar.requests.at(-1).body.toString(), 'plain text');
});

test('publish encodes pubsub and topic so neither can escape the publish route', async (t) => {
  const sidecar = await recordingSidecar();
  t.after(() => sidecar.stop());

  await publish(
    { baseUrl: sidecar.baseUrl },
    request({ pubsubName: 'pub sub', topic: '../../v1.0/metadata' })
  );
  assert.equal(sidecar.requests[0].path, '/v1.0/publish/pub%20sub/..%2F..%2Fv1.0%2Fmetadata');
});

test('publish forwards caller headers, dropping hop-by-hop and token overrides', async (t) => {
  const sidecar = await recordingSidecar();
  t.after(() => sidecar.stop());

  await publish(
    { baseUrl: sidecar.baseUrl, token: 'realtoken' },
    request({
      headers: {
        traceparent: '00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01',
        'transfer-encoding': 'chunked',
        'dapr-api-token': 'HACK',
        'content-type': 'text/plain',
      },
    })
  );

  const { headers } = sidecar.requests[0];
  assert.equal(headers.traceparent, '00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01');
  assert.equal(headers['transfer-encoding'], undefined);
  assert.equal(headers['dapr-api-token'], 'realtoken');
  assert.equal(headers['content-type'], 'application/json', 'the resolved content type wins');
});

test('a non-2xx sidecar response becomes PUBLISH_FAILED carrying the status', async (t) => {
  const sidecar = await recordingSidecar(500, '{"errorCode":"ERR_PUBSUB_PUBLISH_MESSAGE"}');
  t.after(() => sidecar.stop());

  await assert.rejects(
    publish({ baseUrl: sidecar.baseUrl }, request()),
    (err) =>
      err instanceof DaprError &&
      err.code === ErrorCodes.PUBLISH_FAILED &&
      /500/.test(err.message) &&
      /ERR_PUBSUB_PUBLISH_MESSAGE/.test(err.message)
  );
});

test('a transport failure becomes SIDECAR_UNAVAILABLE, not PUBLISH_FAILED', async () => {
  const sidecar = await recordingSidecar();
  await sidecar.stop(); // nothing is listening any more

  await assert.rejects(
    publish({ baseUrl: sidecar.baseUrl }, request()),
    (err) => err instanceof DaprError && err.code === ErrorCodes.SIDECAR_UNAVAILABLE
  );
});

test('an over-size error body is RESPONSE_TOO_LARGE, not a sidecar outage', async (t) => {
  // The sidecar answered — it just answered with more than the configured limit.
  // Reporting that as SIDECAR_UNAVAILABLE would send an operator looking for a
  // sidecar that is up, and would let a Catch node retry a body that cannot
  // shrink.
  const sidecar = await recordingSidecar(500, 'x'.repeat(4096));
  t.after(() => sidecar.stop());

  await assert.rejects(
    publish({ baseUrl: sidecar.baseUrl, maxResponseBytes: 1024 }, request()),
    (err) => err instanceof DaprError && err.code === ErrorCodes.RESPONSE_TOO_LARGE
  );
});

test('publish rejects when it exceeds its deadline', async (t) => {
  const sidecar = await fakeSidecar((req) => req.resume()); // never responds
  t.after(() => sidecar.stop());

  await assert.rejects(
    publish({ baseUrl: sidecar.baseUrl, timeoutMs: 200 }, request()),
    (err) => err instanceof DaprError && err.code === ErrorCodes.SIDECAR_UNAVAILABLE
  );
});

test('publish aborts in flight when its signal is aborted', async (t) => {
  const sidecar = await fakeSidecar((req) => req.resume()); // never responds
  t.after(() => sidecar.stop());

  const controller = new AbortController();
  const pending = publish(
    { baseUrl: sidecar.baseUrl, timeoutMs: 5000, signal: controller.signal },
    request()
  );
  controller.abort();
  await assert.rejects(pending, (err) => err instanceof DaprError);
});

test('publish reuses a pooled keep-alive socket across sequential calls', async (t) => {
  const sidecar = await recordingSidecar();
  t.after(() => sidecar.stop());

  await publish({ baseUrl: sidecar.baseUrl }, request());
  await publish({ baseUrl: sidecar.baseUrl }, request());
  assert.equal(
    new Set(sidecar.requests.map((r) => r.headers.connection)).size,
    1,
    'both calls should keep the same connection policy'
  );
  assert.equal(sidecar.requests.length, 2);
});

test('publish tolerates an absent payload, metadata, and error body', async (t) => {
  const sidecar = await recordingSidecar();
  t.after(() => sidecar.stop());

  await publish(
    { baseUrl: sidecar.baseUrl },
    { pubsubName: 'pubsub', topic: 't', data: undefined, options: { contentType: 'text/plain' } }
  );
  assert.equal(sidecar.requests[0].body.length, 0);
  assert.deepEqual(sidecar.requests[0].query, {});

  // An unresolved content type is omitted rather than sent as "undefined",
  // which node:http would reject outright.
  await publish(
    { baseUrl: sidecar.baseUrl },
    { pubsubName: 'pubsub', topic: 't', data: 'x', options: {} }
  );
  assert.equal('content-type' in sidecar.requests[1].headers, false);

  sidecar.state.status = 404;
  sidecar.state.body = '';
  await assert.rejects(
    publish({ baseUrl: sidecar.baseUrl }, request()),
    (err) => err.code === ErrorCodes.PUBLISH_FAILED && /404/.test(err.message)
  );
});

test('a payload that cannot be serialized is an invalid message, not a sidecar outage', async (t) => {
  const sidecar = await recordingSidecar();
  t.after(() => sidecar.stop());

  // An explicit JSON content type bypasses inferContentType's payload-shape
  // check, so these only fail at serialization time — and that is a local defect
  // in the message, not the sidecar being unreachable.
  const circular = { name: 'loop' };
  circular.self = circular;

  for (const data of [1n, circular]) {
    await assert.rejects(
      publish(
        { baseUrl: sidecar.baseUrl },
        request({ data, options: { contentType: 'application/json', metadata: {} } })
      ),
      (err) =>
        err instanceof DaprError &&
        err.code === ErrorCodes.INVALID_MESSAGE &&
        /serialize/i.test(err.message)
    );
  }
  assert.equal(sidecar.requests.length, 0, 'no request should reach the sidecar');
});

test('every JSON media type serializes as JSON, including parameters and +json suffixes', async (t) => {
  const sidecar = await recordingSidecar();
  t.after(() => sidecar.stop());

  // Media types carry parameters and are case-insensitive (RFC 9110 section 8.3),
  // and any "+json" structured suffix is JSON (RFC 6839 section 3.1). Exact-string
  // matching silently serialized object payloads as "[object Object]".
  for (const contentType of [
    'application/json',
    'application/json; charset=utf-8',
    'APPLICATION/JSON',
    'application/cloudevents+json',
    'application/cloudevents+json; charset=utf-8',
    'application/vnd.example+json',
  ]) {
    await publish(
      { baseUrl: sidecar.baseUrl },
      request({ data: { id: 1 }, options: { contentType, metadata: {} } })
    );
    assert.equal(
      sidecar.requests.at(-1).body.toString(),
      '{"id":1}',
      `${contentType} must serialize as JSON`
    );
    // Matching is normalized; the header itself is sent exactly as configured.
    assert.equal(sidecar.requests.at(-1).headers['content-type'], contentType);
  }

  // A non-JSON type is still text, parameters and all.
  await publish(
    { baseUrl: sidecar.baseUrl },
    request({
      data: 'raw text',
      options: { contentType: 'text/plain; charset=utf-8', metadata: {} },
    })
  );
  assert.equal(sidecar.requests.at(-1).body.toString(), 'raw text');
});

// --- publishBulk ---

const bulkRequest = (over = {}) => ({
  pubsubName: 'pubsub',
  topic: 'orders',
  entries: [{ entryId: 'e1', event: { id: 1 }, contentType: 'application/json', metadata: {} }],
  options: { metadata: {} },
  ...over,
});

test('publishBulk POSTs the entries array to the bulk endpoint as JSON, with request metadata as query params', async (t) => {
  const sidecar = await recordingSidecar(204);
  t.after(() => sidecar.stop());

  await publishBulk(
    { baseUrl: sidecar.baseUrl, token: 'realtoken', timeoutMs: 5000 },
    bulkRequest({ options: { metadata: { partitionKey: 'p1' } } })
  );

  const [received] = sidecar.requests;
  assert.equal(received.method, 'POST');
  assert.equal(received.path, '/v1.0/publish/bulk/pubsub/orders');
  assert.deepEqual(received.query, { 'metadata.partitionKey': 'p1' });
  assert.equal(received.headers['content-type'], 'application/json');
  assert.equal(received.headers['dapr-api-token'], 'realtoken');
  assert.deepEqual(JSON.parse(received.body.toString()), [
    { entryId: 'e1', event: { id: 1 }, contentType: 'application/json', metadata: {} },
  ]);
});

test('publishBulk encodes pubsub and topic so neither can escape the bulk publish route', async (t) => {
  const sidecar = await recordingSidecar(204);
  t.after(() => sidecar.stop());

  await publishBulk(
    { baseUrl: sidecar.baseUrl },
    bulkRequest({ pubsubName: '../secrets', topic: 'a/b' })
  );
  assert.equal(sidecar.requests[0].path, '/v1.0/publish/bulk/..%2Fsecrets/a%2Fb');
});

test('publishBulk resolves with no result on any 2xx status', async (t) => {
  for (const status of [200, 202, 204]) {
    const sidecar = await recordingSidecar(status);
    t.after(() => sidecar.stop());
    await assert.doesNotReject(publishBulk({ baseUrl: sidecar.baseUrl }, bulkRequest()));
  }
});

test('a non-2xx response carrying failedEntries becomes BULK_PUBLISH_PARTIAL with that result attached', async (t) => {
  const sidecar = await recordingSidecar(
    500,
    JSON.stringify({
      failedEntries: [{ entryId: 'e1', error: 'broker unavailable' }],
      errorCode: 'ERR_PUBSUB_PUBLISH_MESSAGE',
    })
  );
  t.after(() => sidecar.stop());

  await assert.rejects(publishBulk({ baseUrl: sidecar.baseUrl }, bulkRequest()), (err) => {
    assert.ok(err instanceof DaprError);
    assert.equal(err.code, ErrorCodes.BULK_PUBLISH_PARTIAL);
    assert.deepEqual(err.bulkResult, {
      failedEntries: [{ entryId: 'e1', error: 'broker unavailable' }],
      errorCode: 'ERR_PUBSUB_PUBLISH_MESSAGE',
    });
    return true;
  });
});

test('BULK_PUBLISH_PARTIAL is used even when every entry failed, not a separate total-failure code', async (t) => {
  const sidecar = await recordingSidecar(
    500,
    JSON.stringify({
      failedEntries: [
        { entryId: 'e1', error: 'broker unavailable' },
        { entryId: 'e2', error: 'broker unavailable' },
      ],
    })
  );
  t.after(() => sidecar.stop());

  await assert.rejects(
    publishBulk({ baseUrl: sidecar.baseUrl }, bulkRequest()),
    (err) =>
      err.code === ErrorCodes.BULK_PUBLISH_PARTIAL && err.bulkResult.failedEntries.length === 2
  );
});

test('an empty failedEntries array cannot identify retryable entries and remains PUBLISH_FAILED', async (t) => {
  const sidecar = await recordingSidecar(500, JSON.stringify({ failedEntries: [] }));
  t.after(() => sidecar.stop());

  await assert.rejects(
    publishBulk({ baseUrl: sidecar.baseUrl }, bulkRequest()),
    (err) => err instanceof DaprError && err.code === ErrorCodes.PUBLISH_FAILED
  );
});

test('a non-2xx response with no failedEntries (a request-level rejection) is PUBLISH_FAILED, not partial', async (t) => {
  // Matches real daprd's response to a duplicate/missing entryId or an
  // unsupported content type: its own generic error envelope, no
  // failedEntries key at all -- the whole batch was rejected, not partially
  // published.
  const sidecar = await recordingSidecar(
    400,
    JSON.stringify({ errorCode: 'ERR_PUBSUB_EVENTS_SER', message: 'entryId is duplicated' })
  );
  t.after(() => sidecar.stop());

  await assert.rejects(
    publishBulk({ baseUrl: sidecar.baseUrl }, bulkRequest()),
    (err) => err instanceof DaprError && err.code === ErrorCodes.PUBLISH_FAILED
  );
});

test('a non-JSON, non-2xx body is PUBLISH_FAILED, not partial', async (t) => {
  const sidecar = await recordingSidecar(502, 'upstream error');
  t.after(() => sidecar.stop());

  await assert.rejects(
    publishBulk({ baseUrl: sidecar.baseUrl }, bulkRequest()),
    (err) => err.code === ErrorCodes.PUBLISH_FAILED
  );
});

test('publishBulk forwards caller headers, dropping hop-by-hop and token overrides', async (t) => {
  const sidecar = await recordingSidecar(204);
  t.after(() => sidecar.stop());

  await publishBulk(
    { baseUrl: sidecar.baseUrl, token: 'realtoken' },
    bulkRequest({
      headers: {
        traceparent: '00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01',
        'transfer-encoding': 'chunked',
        'dapr-api-token': 'attacker-supplied',
        'content-type': 'text/plain',
      },
    })
  );
  const received = sidecar.requests[0].headers;
  assert.equal(received.traceparent, '00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01');
  assert.equal(received['transfer-encoding'], undefined);
  assert.equal(received['dapr-api-token'], 'realtoken');
  assert.equal(received['content-type'], 'application/json');
});

test('an unserializable direct bulk request is INVALID_MESSAGE before transport', async () => {
  await assert.rejects(
    publishBulk(
      { baseUrl: 'http://127.0.0.1:1' },
      bulkRequest({
        entries: [{ entryId: 'bad', event: 1n, contentType: 'application/json', metadata: {} }],
      })
    ),
    (err) => err instanceof DaprError && err.code === ErrorCodes.INVALID_MESSAGE
  );
});

test('an over-size bulk response body is RESPONSE_TOO_LARGE, not a sidecar outage', async (t) => {
  const sidecar = await recordingSidecar(500, 'x'.repeat(4096));
  t.after(() => sidecar.stop());

  await assert.rejects(
    publishBulk({ baseUrl: sidecar.baseUrl, maxResponseBytes: 1024 }, bulkRequest()),
    (err) => err instanceof DaprError && err.code === ErrorCodes.RESPONSE_TOO_LARGE
  );
});

test('publishBulk rejects when it exceeds its deadline', async (t) => {
  const sidecar = await fakeSidecar((req) => req.resume());
  t.after(() => sidecar.stop());

  await assert.rejects(
    publishBulk({ baseUrl: sidecar.baseUrl, timeoutMs: 200 }, bulkRequest()),
    (err) => err instanceof DaprError && err.code === ErrorCodes.SIDECAR_UNAVAILABLE
  );
});

test('publishBulk aborts in flight when its signal is aborted', async (t) => {
  const sidecar = await fakeSidecar((req) => req.resume());
  t.after(() => sidecar.stop());

  const controller = new AbortController();
  const pending = publishBulk(
    { baseUrl: sidecar.baseUrl, timeoutMs: 5000, signal: controller.signal },
    bulkRequest()
  );
  controller.abort();
  await assert.rejects(pending, (err) => err instanceof DaprError);
});

test('a bulk publish transport failure becomes SIDECAR_UNAVAILABLE, not PUBLISH_FAILED', async () => {
  await assert.rejects(
    publishBulk({ baseUrl: 'http://127.0.0.1:1', timeoutMs: 500 }, bulkRequest()),
    (err) => err instanceof DaprError && err.code === ErrorCodes.SIDECAR_UNAVAILABLE
  );
});
