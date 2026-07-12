'use strict';

const { test } = require('node:test');
const assert = require('node:assert/strict');
const http = require('node:http');

const { invoke, buildInvokePath, encodeBody, decodeBody } = require('../../lib/invoke-client');

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

test('invoke targets the sidecar invoke URL, preserves body and response verbatim', async (t) => {
  let received;
  const sidecar = await fakeSidecar((req, res) => {
    const chunks = [];
    req.on('data', (c) => chunks.push(c));
    req.on('end', () => {
      received = {
        method: req.method,
        url: req.url,
        headers: req.headers,
        body: Buffer.concat(chunks),
      };
      res.writeHead(201, { 'content-type': 'application/json', 'x-custom': 'y' });
      res.end(Buffer.from('{"ok":true}'));
    });
  });
  t.after(() => sidecar.stop());

  const result = await invoke(
    { baseUrl: sidecar.baseUrl, token: 'realtoken' },
    {
      appId: 'orders',
      methodPath: 'items/create',
      verb: 'POST',
      headers: {
        'content-type': 'application/json',
        'x-user': 'keep',
        'transfer-encoding': 'chunked', // hop-by-hop: must be stripped
        'dapr-api-token': 'HACK', // must not override the real token
      },
      query: { a: '1', b: '2' },
      body: Buffer.from('{"x":1}'),
      timeoutMs: 5000,
    }
  );

  assert.equal(received.method, 'POST');
  assert.equal(received.url, '/v1.0/invoke/orders/method/items/create?a=1&b=2');
  assert.equal(received.headers['x-user'], 'keep');
  assert.equal(received.headers['dapr-api-token'], 'realtoken'); // forced, not HACK
  assert.equal(received.headers['transfer-encoding'], undefined); // hop-by-hop stripped
  assert.deepEqual(received.body, Buffer.from('{"x":1}'));

  assert.equal(result.status, 201);
  assert.equal(result.headers['x-custom'], 'y');
  assert.ok(Buffer.isBuffer(result.body));
  assert.deepEqual(result.body, Buffer.from('{"ok":true}'));
});

test('invoke sends no body for a bodyless verb and preserves a binary response', async (t) => {
  const bytes = Buffer.from([0, 1, 2, 255]);
  const sidecar = await fakeSidecar((req, res) => {
    req.resume();
    res.writeHead(200, { 'content-type': 'application/octet-stream' });
    res.end(bytes);
  });
  t.after(() => sidecar.stop());

  const result = await invoke(
    { baseUrl: sidecar.baseUrl },
    { appId: 'app', methodPath: 'ping', verb: 'GET', timeoutMs: 5000 }
  );
  assert.equal(result.status, 200);
  assert.deepEqual(result.body, bytes);
});

test('invoke does not send a dapr-api-token header when no token is configured', async (t) => {
  let received;
  const sidecar = await fakeSidecar((req, res) => {
    received = req.headers;
    req.resume();
    res.writeHead(204).end();
  });
  t.after(() => sidecar.stop());

  await invoke({ baseUrl: sidecar.baseUrl }, { appId: 'a', methodPath: 'm', verb: 'GET' });
  assert.equal('dapr-api-token' in received, false);
});

test('buildInvokePath encodes app id and method segments', () => {
  assert.equal(
    buildInvokePath('orders', 'items/create'),
    '/v1.0/invoke/orders/method/items/create'
  );
  assert.equal(buildInvokePath('a b', 'm n'), '/v1.0/invoke/a%20b/method/m%20n');
  // A leading slash on the method is normalized away, not doubled.
  assert.equal(buildInvokePath('app', '/ping'), '/v1.0/invoke/app/method/ping');
});

test('buildInvokePath rejects path traversal and unsafe ids', () => {
  assert.throws(() => buildInvokePath('app', '../../../../v1.0/metadata'), /invalid|segment/i);
  assert.throws(() => buildInvokePath('app', 'a/../b'), /invalid|segment/i);
  assert.throws(() => buildInvokePath('a/b', 'm'), /app id|invalid/i);
  assert.throws(() => buildInvokePath('..', 'm'), /app id|invalid/i);
  assert.throws(() => buildInvokePath('app', ''), /method|invalid/i);
});

test('invoke does not let a traversal method escape the invoke route', async (t) => {
  let received;
  const sidecar = await fakeSidecar((req, res) => {
    received = req.url;
    req.resume();
    res.writeHead(204).end();
  });
  t.after(() => sidecar.stop());
  await assert.rejects(
    invoke(
      { baseUrl: sidecar.baseUrl },
      { appId: 'app', methodPath: '../../../../v1.0/metadata', verb: 'GET' }
    ),
    /invalid|segment/i
  );
  assert.equal(received, undefined, 'no request should reach the sidecar');
});

test('encodeBody infers a content type by payload shape', () => {
  assert.deepEqual(encodeBody(undefined), { body: undefined, contentType: undefined });
  assert.deepEqual(encodeBody(null), { body: undefined, contentType: undefined });
  const buf = Buffer.from([1, 2]);
  assert.deepEqual(encodeBody(buf), { body: buf, contentType: 'application/octet-stream' });
  assert.deepEqual(encodeBody('hi'), { body: 'hi', contentType: 'text/plain' });
  assert.deepEqual(encodeBody({ a: 1 }), { body: '{"a":1}', contentType: 'application/json' });
});

test('decodeBody parses by content type with safe fallbacks', () => {
  assert.equal(decodeBody(Buffer.alloc(0), 'application/json'), undefined);
  assert.equal(decodeBody(undefined, 'text/plain'), undefined);
  assert.deepEqual(decodeBody(Buffer.from('{"a":1}'), 'application/json'), { a: 1 });
  assert.deepEqual(decodeBody(Buffer.from('{"a":1}'), 'application/vnd.api+json'), { a: 1 });
  assert.equal(decodeBody(Buffer.from('not json'), 'application/json'), 'not json'); // fallback
  assert.equal(decodeBody(Buffer.from('plain'), 'text/csv'), 'plain');
  const bin = Buffer.from([0, 255, 7]);
  assert.deepEqual(decodeBody(bin, 'application/octet-stream'), bin);
});

test('invoke forwards every supported verb as the request method', async (t) => {
  const seen = [];
  const sidecar = await fakeSidecar((req, res) => {
    seen.push(req.method);
    req.resume();
    res.writeHead(200, { 'content-type': 'text/plain' }).end(req.method);
  });
  t.after(() => sidecar.stop());
  for (const verb of ['GET', 'POST', 'PUT', 'PATCH', 'DELETE']) {
    const r = await invoke(
      { baseUrl: sidecar.baseUrl },
      { appId: 'a', methodPath: 'm', verb, body: verb === 'GET' ? undefined : 'x', timeoutMs: 5000 }
    );
    assert.equal(r.body.toString(), verb);
  }
  assert.deepEqual(seen, ['GET', 'POST', 'PUT', 'PATCH', 'DELETE']);
});

test('invoke reuses a pooled keep-alive socket across sequential calls', async (t) => {
  const ports = new Set();
  const sidecar = await fakeSidecar((req, res) => {
    ports.add(req.socket.remotePort);
    req.resume();
    res.writeHead(204).end();
  });
  t.after(() => sidecar.stop());
  await invoke({ baseUrl: sidecar.baseUrl }, { appId: 'a', methodPath: 'm', verb: 'GET' });
  await invoke({ baseUrl: sidecar.baseUrl }, { appId: 'a', methodPath: 'm', verb: 'GET' });
  assert.equal(ports.size, 1, 'both calls should share one pooled client socket');
});

test('invoke returns a non-2xx response as data, not an error', async (t) => {
  const sidecar = await fakeSidecar((req, res) => {
    req.resume();
    res.writeHead(404, { 'content-type': 'application/json' }).end('{"error":"nope"}');
  });
  t.after(() => sidecar.stop());
  const r = await invoke(
    { baseUrl: sidecar.baseUrl },
    { appId: 'a', methodPath: 'm', verb: 'GET' }
  );
  assert.equal(r.status, 404);
  assert.deepEqual(decodeBody(r.body, r.headers['content-type']), { error: 'nope' });
});

test('invoke rejects when the request exceeds its deadline', async (t) => {
  const sidecar = await fakeSidecar((req) => {
    req.resume(); // never respond
  });
  t.after(() => sidecar.stop());

  await assert.rejects(
    invoke(
      { baseUrl: sidecar.baseUrl },
      { appId: 'a', methodPath: 'm', verb: 'GET', timeoutMs: 200 }
    ),
    /timed out|deadline/i
  );
});
