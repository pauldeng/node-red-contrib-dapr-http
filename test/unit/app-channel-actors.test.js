'use strict';

const { test } = require('node:test');
const assert = require('node:assert/strict');
const http = require('node:http');

const { acquireListener } = require('../../lib/app-channel');
const { httpRequest } = require('../helpers/http');
const { freePort } = require('../helpers/node-red');

const BIND = '127.0.0.1';
const url = (port, path) => `http://127.0.0.1:${port}${path}`;

// Sends the exact raw path over the wire, unlike test/helpers/http.js's
// httpRequest, which builds its request from `new URL(...)` -- and the WHATWG
// URL parser itself resolves percent-encoded dot segments in `.pathname`
// (e.g. "%2e%2e" -> ".." -> collapsed), which would silently defeat exactly
// the traversal cases this file tests before the request ever reached the
// server.
function rawPathRequest(port, rawPath, { method = 'GET', headers = {} } = {}) {
  return /* allow-promise: bridges node:http's callback API */ new Promise((resolve, reject) => {
    const req = http.request(
      { hostname: '127.0.0.1', port, path: rawPath, method, headers, agent: false },
      (res) => {
        const chunks = [];
        res.on('data', (c) => chunks.push(c));
        res.on('end', () =>
          resolve({
            status: res.statusCode,
            headers: res.headers,
            text: Buffer.concat(chunks).toString(),
          })
        );
      }
    );
    req.on('error', reject);
    req.end();
  });
}

function limits(over = {}) {
  return {
    bodyLimitBytes: 4096,
    requestTimeoutMs: 30000,
    headerLimitBytes: 16 * 1024,
    headerCountLimit: 100,
    headersTimeoutMs: 5000,
    drainTimeoutMs: 50,
    leaseGraceMs: 200,
    maxPending: 1000,
    ...over,
  };
}

async function acquire(t, opts) {
  const lease = await acquireListener({
    bindAddress: BIND,
    token: undefined,
    limits: limits(),
    ...opts,
  });
  t.after(async () => {
    lease.release({ graceMs: 0 });
    await lease.whenClosed();
  });
  return lease;
}

const actorConfig = () => ({
  entities: ['DemoActor'],
  reentrancy: { enabled: false },
  drainOngoingCallTimeout: '30s',
  drainRebalancedActors: true,
});

test('with no actor method registered, /actors/* falls through to the ordinary route map', async (t) => {
  const port = await freePort();
  const lease = await acquire(t, { port });
  lease.activate({
    routes: [
      {
        method: 'GET',
        path: '/actors/T/a',
        kind: 'service',
        handler: async () => ({ status: 200, body: 'svc' }),
      },
    ],
  });
  const res = await httpRequest(url(port, '/actors/T/a'));
  assert.equal(res.status, 200);
  assert.equal(res.text, 'svc');
});

test('with no actor method registered, /dapr/config 404s exactly as before', async (t) => {
  const port = await freePort();
  const lease = await acquire(t, { port });
  lease.activate({});
  const res = await httpRequest(url(port, '/dapr/config'));
  assert.equal(res.status, 404);
});

test('PUT /actors/{type}/{id}/method/{method} decodes each segment exactly once and dispatches', async (t) => {
  const port = await freePort();
  const lease = await acquire(t, { port });
  let seen;
  lease.activate({
    actorConfig: actorConfig(),
    actorFingerprint: 'fp1',
    getActorHandler: (type, method) =>
      type === 'DemoActor' && method === 'GetMyData' ? () => {} : undefined,
    actorInvoke: async ({ actorType, actorId, method, ctx }) => {
      seen = { actorType, actorId, method, callerAppId: ctx.callerAppId };
      return { status: 200, headers: { 'content-type': 'application/json' }, body: '{"ok":true}' };
    },
  });
  const res = await httpRequest(url(port, '/actors/DemoActor/demo%20one/method/GetMyData'), {
    method: 'PUT',
    body: '{}',
  });
  assert.equal(res.status, 200);
  assert.deepEqual(seen, {
    actorType: 'DemoActor',
    actorId: 'demo one',
    method: 'GetMyData',
    callerAppId: null, // accepted, but never exposed to the flow
  });
});

test('PUT accepts dapr-caller-app-id on the method-call shape without exposing it', async (t) => {
  const port = await freePort();
  const lease = await acquire(t, { port });
  let ctxSeen;
  lease.activate({
    actorConfig: actorConfig(),
    getActorHandler: () => () => {},
    actorInvoke: async ({ ctx }) => {
      ctxSeen = ctx;
      return { status: 200 };
    },
  });
  const res = await httpRequest(url(port, '/actors/DemoActor/a/method/M'), {
    method: 'PUT',
    headers: { 'dapr-caller-app-id': 'someapp' },
  });
  assert.equal(res.status, 200);
  assert.equal(ctxSeen.callerAppId, null);
});

test('an encoded traversal segment is rejected 400, never silently resolved', async (t) => {
  const port = await freePort();
  const lease = await acquire(t, { port });
  lease.activate({
    actorConfig: actorConfig(),
    getActorHandler: () => () => {},
    actorInvoke: async () => ({ status: 200 }),
  });
  const res = await rawPathRequest(port, '/actors/DemoActor/%2e%2e/method/M', { method: 'PUT' });
  assert.equal(res.status, 400);
});

test('an empty segment is rejected 400', async (t) => {
  const port = await freePort();
  const lease = await acquire(t, { port });
  lease.activate({
    actorConfig: actorConfig(),
    getActorHandler: () => () => {},
    actorInvoke: async () => ({ status: 200 }),
  });
  const res = await rawPathRequest(port, '/actors/DemoActor//method/M', { method: 'PUT' });
  assert.equal(res.status, 400);
});

test('a pathname matching neither actor shape is 404', async (t) => {
  const port = await freePort();
  const lease = await acquire(t, { port });
  lease.activate({
    actorConfig: actorConfig(),
    getActorHandler: () => () => {},
    actorInvoke: async () => ({ status: 200 }),
  });
  const res = await httpRequest(url(port, '/actors/DemoActor/a/unexpected/M'), { method: 'PUT' });
  assert.equal(res.status, 404);
});

test('an unrecognized actor type or method is 404 on a ready host', async (t) => {
  const port = await freePort();
  const lease = await acquire(t, { port });
  lease.activate({
    actorConfig: actorConfig(),
    getActorHandler: () => undefined,
    actorInvoke: async () => ({ status: 200 }),
  });
  const res = await httpRequest(url(port, '/actors/DemoActor/a/method/Unknown'), {
    method: 'PUT',
  });
  assert.equal(res.status, 404);
});

test('a non-PUT verb on the method-call shape is 405 with Allow: PUT', async (t) => {
  const port = await freePort();
  const lease = await acquire(t, { port });
  lease.activate({
    actorConfig: actorConfig(),
    getActorHandler: () => () => {},
    actorInvoke: async () => ({ status: 200 }),
  });
  const res = await httpRequest(url(port, '/actors/DemoActor/a/method/M'), { method: 'GET' });
  assert.equal(res.status, 405);
  assert.equal(res.headers.allow, 'PUT');
});

test('DELETE /actors/{type}/{id} answers 200 with no body and touches no handler', async (t) => {
  const port = await freePort();
  const lease = await acquire(t, { port });
  lease.activate({
    actorConfig: actorConfig(),
    getActorHandler: () => assert.fail('DELETE must not resolve a method handler'),
    actorInvoke: async () => assert.fail('DELETE must never reach actorInvoke'),
  });
  const res = await httpRequest(url(port, '/actors/DemoActor/a'), { method: 'DELETE' });
  assert.equal(res.status, 200);
  assert.equal(res.text, '');
});

test('DELETE /actors/{type}/{id} rejects dapr-caller-app-id (internal route class)', async (t) => {
  const port = await freePort();
  const lease = await acquire(t, { port });
  lease.activate({ actorConfig: actorConfig() });
  const res = await httpRequest(url(port, '/actors/DemoActor/a'), {
    method: 'DELETE',
    headers: { 'dapr-caller-app-id': 'someapp' },
  });
  assert.equal(res.status, 403);
});

test('a non-DELETE verb on the instance shape is 405 with Allow: DELETE', async (t) => {
  const port = await freePort();
  const lease = await acquire(t, { port });
  lease.activate({ actorConfig: actorConfig() });
  const res = await httpRequest(url(port, '/actors/DemoActor/a'), { method: 'PUT' });
  assert.equal(res.status, 405);
  assert.equal(res.headers.allow, 'DELETE');
});

test('GET /dapr/config serves the actor config, records the served fingerprint, and rejects the caller header', async (t) => {
  const port = await freePort();
  const lease = await acquire(t, { port });
  lease.activate({ actorConfig: actorConfig(), actorFingerprint: 'fp-actor' });
  const res = await httpRequest(url(port, '/dapr/config'));
  assert.equal(res.status, 200);
  assert.deepEqual(JSON.parse(res.text), actorConfig());
  assert.equal(lease.servedActorFingerprint(), 'fp-actor');

  const rejected = await httpRequest(url(port, '/dapr/config'), {
    headers: { 'dapr-caller-app-id': 'someapp' },
  });
  assert.equal(rejected.status, 403);
});

test('a non-GET verb on /dapr/config is 405', async (t) => {
  const port = await freePort();
  const lease = await acquire(t, { port });
  lease.activate({ actorConfig: actorConfig() });
  const res = await httpRequest(url(port, '/dapr/config'), { method: 'POST' });
  assert.equal(res.status, 405);
});

test('a token is required on actor routes exactly as on any other route', async (t) => {
  const port = await freePort();
  const lease = await acquire(t, { port, token: 'secret' });
  lease.activate({
    actorConfig: actorConfig(),
    getActorHandler: () => () => {},
    actorInvoke: async () => ({ status: 200 }),
  });
  const res = await httpRequest(url(port, '/actors/DemoActor/a/method/M'), { method: 'PUT' });
  assert.equal(res.status, 401);
});
