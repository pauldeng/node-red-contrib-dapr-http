'use strict';

const { test } = require('node:test');
const assert = require('node:assert/strict');
const http = require('node:http');
const { once } = require('node:events');
const { text: readBody } = require('node:stream/consumers');

const { acquireListener } = require('../../lib/app-channel');
const { httpRequest } = require('../helpers/http');
const { freePort } = require('../helpers/node-red');
const { FIXED_LIMITS } = require('../../lib/options');
const { REMINDER_METHOD } = require('../../lib/actor-messages');

const BIND = '127.0.0.1';
const url = (port, path) => `http://127.0.0.1:${port}${path}`;

// Sends the exact raw path over the wire, unlike test/helpers/http.js's
// httpRequest, which builds its request from `new URL(...)` -- and the WHATWG
// URL parser itself resolves percent-encoded dot segments in `.pathname`
// (e.g. "%2e%2e" -> ".." -> collapsed), which would silently defeat exactly
// the traversal cases this file tests before the request ever reached the
// server.
async function rawPathRequest(port, rawPath, { method = 'GET', headers = {} } = {}) {
  const req = http.request({
    hostname: '127.0.0.1',
    port,
    path: rawPath,
    method,
    headers,
    agent: false,
  });
  req.end();
  const [res] = await once(req, 'response');
  const text = await readBody(res);
  return { status: res.statusCode, headers: res.headers, text };
}

function limits(over = {}) {
  return {
    ...FIXED_LIMITS,
    bodyLimitBytes: 4096,
    requestTimeoutMs: 30000,
    headersTimeoutMs: 5000,
    drainTimeoutMs: 50,
    leaseGraceMs: 200,
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

// ---- milestone 3: actor tombstones ------------------------------------------
//
// daprd treats a 404 from an actor method PUT as a permanent "method not
// found" and never retries it -- unlike an ordinary route (staleRoutes,
// above), where 503 vs 404 only matters for a pub/sub delivery redelivery.
// A registration that drops out across a redeploy (a method removed, or its
// whole actor type left with none) must therefore keep answering retryable,
// never 404, exactly like staleRoutes: bounded by registrations ever made,
// cleared only when a real registration wins it back.

test('an unregistered method is retryable before the deferred activation installs its tombstone', async (t) => {
  const { createConnectionRegistry } = require('../../lib/connection-registry');
  const registry = createConnectionRegistry();
  const remove = registry.addActorMethod(
    { nodeId: 'method', actorType: 'DemoActor', method: 'GetMyData' },
    () => assert.fail('the removed method must never emit')
  );
  const port = await freePort();
  const lease = await acquire(t, { port });
  lease.activate({
    ...registry.activation(),
    getActorHandler: (type, method) => registry.actorHandlerFor(type, method),
    actorInvoke: async () => assert.fail('the removed method must never be invoked'),
  });

  // registerActorMethod's close callback removes synchronously but coalesces
  // applyActivation with setImmediate. Model a request in that gap.
  remove();
  const res = await httpRequest(url(port, '/actors/DemoActor/a/method/GetMyData'), {
    method: 'PUT',
  });
  assert.equal(res.status, 503);
  const unknown = await httpRequest(url(port, '/actors/DemoActor/a/method/NeverRegistered'), {
    method: 'PUT',
  });
  assert.equal(unknown.status, 404);
});

test('a method removed from an otherwise still-registered actor type stays retryable, not 404 -- the sibling method is unaffected', async (t) => {
  const port = await freePort();
  const lease = await acquire(t, { port });
  const getHandler = () => {};
  const setHandler = () => {};
  lease.activate({
    actorConfig: actorConfig(),
    actorMethodPairs: [
      { actorType: 'DemoActor', method: 'GetMyData' },
      { actorType: 'DemoActor', method: 'SetMyData' },
    ],
    getActorHandler: (type, method) =>
      type === 'DemoActor' && method === 'GetMyData'
        ? getHandler
        : type === 'DemoActor' && method === 'SetMyData'
          ? setHandler
          : undefined,
    actorInvoke: async () => ({ status: 200 }),
  });

  // GetMyData dropped; SetMyData (and the type) stay registered.
  lease.activate({
    actorConfig: actorConfig(),
    actorMethodPairs: [{ actorType: 'DemoActor', method: 'SetMyData' }],
    getActorHandler: (type, method) =>
      type === 'DemoActor' && method === 'SetMyData' ? setHandler : undefined,
    actorInvoke: async () => ({ status: 200 }),
  });

  const dropped = await httpRequest(url(port, '/actors/DemoActor/a/method/GetMyData'), {
    method: 'PUT',
  });
  assert.equal(dropped.status, 503, 'a dropped method must be retryable, never 404');

  const stillThere = await httpRequest(url(port, '/actors/DemoActor/a/method/SetMyData'), {
    method: 'PUT',
  });
  assert.equal(stillThere.status, 200, 'a sibling method on the same type is unaffected');
});

test('a whole actor type left with no method stays retryable even once actorConfig goes null', async (t) => {
  const port = await freePort();
  const lease = await acquire(t, { port });
  lease.activate({
    actorConfig: actorConfig(),
    actorMethodPairs: [{ actorType: 'DemoActor', method: 'GetMyData' }],
    getActorHandler: () => () => {},
    actorInvoke: async () => ({ status: 200 }),
  });

  // Every DemoActor method node removed: actorConfig now advertises nothing.
  lease.activate({ actorConfig: null, actorMethodPairs: [], getActorHandler: () => undefined });

  const put = await httpRequest(url(port, '/actors/DemoActor/a/method/GetMyData'), {
    method: 'PUT',
  });
  assert.equal(put.status, 503, 'the whole tombstoned type must be retryable, never 404');

  // DELETE still answers 200 unconditionally -- it is a no-op ack, tombstoned
  // type or not.
  const del = await httpRequest(url(port, '/actors/DemoActor/a'), { method: 'DELETE' });
  assert.equal(del.status, 200);

  // An unrelated, never-registered type is still a plain 404, tombstone or
  // not -- only a type/pair that actually dropped out is retryable.
  const other = await httpRequest(url(port, '/actors/OtherType/a/method/X'), { method: 'PUT' });
  assert.equal(other.status, 404);
});

test('a real registration wins its tombstone back', async (t) => {
  const port = await freePort();
  const lease = await acquire(t, { port });
  lease.activate({
    actorConfig: actorConfig(),
    actorMethodPairs: [{ actorType: 'DemoActor', method: 'GetMyData' }],
    getActorHandler: () => () => {},
    actorInvoke: async () => ({ status: 200 }),
  });
  lease.activate({ actorConfig: null, actorMethodPairs: [], getActorHandler: () => undefined });
  assert.equal(
    (await httpRequest(url(port, '/actors/DemoActor/a/method/GetMyData'), { method: 'PUT' }))
      .status,
    503
  );

  // Re-registered (e.g. the method node re-added on the next redeploy).
  lease.activate({
    actorConfig: actorConfig(),
    actorMethodPairs: [{ actorType: 'DemoActor', method: 'GetMyData' }],
    getActorHandler: () => () => {},
    actorInvoke: async () => ({ status: 200 }),
  });
  const res = await httpRequest(url(port, '/actors/DemoActor/a/method/GetMyData'), {
    method: 'PUT',
  });
  assert.equal(res.status, 200, 'a real registration must win the tombstone back');
});

test('a tombstone survives a reacquire (redeploy), same as staleRoutes', async (t) => {
  const port = await freePort();
  const l1 = await acquireListener({
    bindAddress: BIND,
    port,
    token: undefined,
    limits: limits({ leaseGraceMs: 1000 }),
  });
  l1.activate({
    actorConfig: actorConfig(),
    actorMethodPairs: [{ actorType: 'DemoActor', method: 'GetMyData' }],
    getActorHandler: () => () => {},
    actorInvoke: async () => ({ status: 200 }),
  });
  // The method node is gone on the next generation.
  l1.activate({ actorConfig: null, actorMethodPairs: [], getActorHandler: () => undefined });
  l1.release({ graceMs: 1000 });

  const l2 = await acquireListener({
    bindAddress: BIND,
    port,
    token: undefined,
    limits: limits({ leaseGraceMs: 1000 }),
  });
  t.after(async () => {
    l2.release({ graceMs: 0 });
    await l2.whenClosed();
  });
  l2.activate({ actorConfig: null, actorMethodPairs: [], getActorHandler: () => undefined });

  const res = await httpRequest(url(port, '/actors/DemoActor/a/method/GetMyData'), {
    method: 'PUT',
  });
  assert.equal(res.status, 503, 'the tombstone persists across reacquire');
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

test('with no actor methods left, a service route under /actors is served, not claimed by a tombstone', async (t) => {
  const port = await freePort();
  const lease = await acquire(t, { port });
  lease.activate({
    actorConfig: actorConfig(),
    actorMethodPairs: [{ actorType: 'DemoActor', method: 'GetMyData' }],
    getActorHandler: () => () => {},
    actorInvoke: async () => ({ status: 200 }),
  });

  // Every actor method removed, then a service node registered under /actors.
  lease.activate({
    actorConfig: null,
    actorMethodPairs: [],
    getActorHandler: () => undefined,
    routes: [
      { method: 'GET', path: '/actors/x', kind: 'service', handler: async () => ({ status: 200 }) },
    ],
  });

  const service = await httpRequest(url(port, '/actors/x'), { method: 'GET' });
  assert.equal(
    service.status,
    200,
    'a non-tombstoned /actors path falls through to service routes'
  );

  const tombstoned = await httpRequest(url(port, '/actors/DemoActor/a/method/GetMyData'), {
    method: 'PUT',
  });
  assert.equal(tombstoned.status, 503, 'the tombstoned type is still retryable');
});

// ---- milestone 4: actor REMINDERS ------------------------------------------

test('reminder callbacks reject a mesh caller before invoking the actor host', async (t) => {
  const port = await freePort();
  const lease = await acquire(t, { port, token: 'app-token' });
  let invoked = false;
  lease.activate({
    actorConfig: actorConfig(),
    actorMethodPairs: [{ actorType: 'DemoActor', method: REMINDER_METHOD }],
    getActorHandler: () => () => {},
    actorInvoke: async () => {
      invoked = true;
      return { status: 200 };
    },
  });
  const res = await httpRequest(url(port, '/actors/DemoActor/a/method/remind/demo_reminder'), {
    method: 'PUT',
    headers: { 'dapr-api-token': 'app-token', 'dapr-caller-app-id': 'other-app' },
    body: '{"data":1,"dueTime":"","period":""}',
  });
  assert.equal(res.status, 403);
  assert.equal(invoked, false);
});

test('PUT /actors/{type}/{id}/method/remind/{name} dispatches to the reminder handler with a trigger', async (t) => {
  const port = await freePort();
  const lease = await acquire(t, { port });
  let seen;
  lease.activate({
    actorConfig: actorConfig(),
    actorMethodPairs: [{ actorType: 'DemoActor', method: REMINDER_METHOD }],
    getActorHandler: (type, method) =>
      type === 'DemoActor' && method === REMINDER_METHOD ? () => {} : undefined,
    actorInvoke: async ({ actorType, actorId, method, ctx, trigger }) => {
      seen = { actorType, actorId, method, trigger, callerAppId: ctx.callerAppId };
      return { status: 200 };
    },
  });
  const res = await httpRequest(
    url(port, '/actors/DemoActor/demo%20one/method/remind/demo_reminder'),
    { method: 'PUT', body: '{"data":1,"dueTime":"","period":""}' }
  );
  assert.equal(res.status, 200);
  assert.deepEqual(seen, {
    actorType: 'DemoActor',
    actorId: 'demo one',
    method: REMINDER_METHOD,
    trigger: { kind: 'reminder', name: 'demo_reminder' },
    callerAppId: null,
  });
});

test('an encoded reminder name is decoded exactly once', async (t) => {
  const port = await freePort();
  const lease = await acquire(t, { port });
  let seenName;
  lease.activate({
    actorConfig: actorConfig(),
    actorMethodPairs: [{ actorType: 'DemoActor', method: REMINDER_METHOD }],
    getActorHandler: () => () => {},
    actorInvoke: async ({ trigger }) => {
      seenName = trigger.name;
      return { status: 200 };
    },
  });
  const res = await httpRequest(url(port, '/actors/DemoActor/a/method/remind/demo%20reminder'), {
    method: 'PUT',
  });
  assert.equal(res.status, 200);
  assert.equal(seenName, 'demo reminder');
});

test('an invalid (encoded traversal) reminder name is a 400', async (t) => {
  const port = await freePort();
  const lease = await acquire(t, { port });
  lease.activate({
    actorConfig: actorConfig(),
    actorMethodPairs: [{ actorType: 'DemoActor', method: REMINDER_METHOD }],
    getActorHandler: () => () => {},
    actorInvoke: async () => ({ status: 200 }),
  });
  const res = await rawPathRequest(port, '/actors/DemoActor/a/method/remind/%2e%2e', {
    method: 'PUT',
  });
  assert.equal(res.status, 400);
});

test('the timer callback shape (method/timer/{name}) still 404s -- timers are deferred', async (t) => {
  const port = await freePort();
  const lease = await acquire(t, { port });
  lease.activate({
    actorConfig: actorConfig(),
    actorMethodPairs: [{ actorType: 'DemoActor', method: REMINDER_METHOD }],
    getActorHandler: () => () => {},
    actorInvoke: async () => assert.fail('a timer path must never reach actorInvoke'),
  });
  const res = await httpRequest(url(port, '/actors/DemoActor/a/method/timer/t1'), {
    method: 'PUT',
  });
  assert.equal(res.status, 404);
});

test('a method-path with more than 4 segments other than .../method/remind/{name} is still 404', async (t) => {
  const port = await freePort();
  const lease = await acquire(t, { port });
  lease.activate({ actorConfig: actorConfig() });
  const res = await httpRequest(url(port, '/actors/DemoActor/a/method/M/extra'), {
    method: 'PUT',
  });
  assert.equal(res.status, 404);
});

test('a non-PUT verb on the reminder shape is 405 with Allow: PUT', async (t) => {
  const port = await freePort();
  const lease = await acquire(t, { port });
  lease.activate({
    actorConfig: actorConfig(),
    actorMethodPairs: [{ actorType: 'DemoActor', method: REMINDER_METHOD }],
    getActorHandler: () => () => {},
    actorInvoke: async () => ({ status: 200 }),
  });
  const res = await httpRequest(url(port, '/actors/DemoActor/a/method/remind/r1'), {
    method: 'GET',
  });
  assert.equal(res.status, 405);
  assert.equal(res.headers.allow, 'PUT');
});

test('an unregistered reminder on a hosted type is a plain 404', async (t) => {
  const port = await freePort();
  const lease = await acquire(t, { port });
  lease.activate({
    actorConfig: actorConfig(),
    actorMethodPairs: [{ actorType: 'DemoActor', method: 'GetMyData' }],
    getActorHandler: (type, method) =>
      type === 'DemoActor' && method === 'GetMyData' ? () => {} : undefined,
    actorInvoke: async () => ({ status: 200 }),
  });
  const res = await httpRequest(url(port, '/actors/DemoActor/a/method/remind/r1'), {
    method: 'PUT',
  });
  assert.equal(res.status, 404);
});

test('a dropped reminder registration is tombstoned retryable, never 404, like a method pair', async (t) => {
  const port = await freePort();
  const lease = await acquire(t, { port });
  lease.activate({
    actorConfig: actorConfig(),
    actorMethodPairs: [{ actorType: 'DemoActor', method: REMINDER_METHOD }],
    getActorHandler: () => () => {},
    actorInvoke: async () => ({ status: 200 }),
  });
  // The reminder registration is removed (e.g. its dapr-actor-method node
  // closed) but the type itself is still registered via another method.
  lease.activate({
    actorConfig: actorConfig(),
    actorMethodPairs: [{ actorType: 'DemoActor', method: 'GetMyData' }],
    getActorHandler: (type, method) =>
      type === 'DemoActor' && method === 'GetMyData' ? () => {} : undefined,
    actorInvoke: async () => ({ status: 200 }),
  });
  const dropped = await httpRequest(url(port, '/actors/DemoActor/a/method/remind/r1'), {
    method: 'PUT',
  });
  assert.equal(dropped.status, 503, 'a dropped reminder registration must be retryable, never 404');

  // Re-registering wins the tombstone back.
  lease.activate({
    actorConfig: actorConfig(),
    actorMethodPairs: [{ actorType: 'DemoActor', method: REMINDER_METHOD }],
    getActorHandler: () => () => {},
    actorInvoke: async () => ({ status: 200 }),
  });
  const restored = await httpRequest(url(port, '/actors/DemoActor/a/method/remind/r1'), {
    method: 'PUT',
  });
  assert.equal(restored.status, 200);
});

test('a duplicate reminder registration for the same actor type is rejected by the connection registry', () => {
  const { createConnectionRegistry } = require('../../lib/connection-registry');
  const registry = createConnectionRegistry();
  registry.addActorMethod({ nodeId: 'r1', actorType: 'DemoActor', reminder: true }, () => {});
  assert.throws(
    () =>
      registry.addActorMethod({ nodeId: 'r2', actorType: 'DemoActor', reminder: true }, () => {}),
    /duplicate actor method DemoActor\/remind\//
  );
});

test('a type with only a reminder registration is still advertised in /dapr/config', () => {
  const { createConnectionRegistry } = require('../../lib/connection-registry');
  const registry = createConnectionRegistry();
  registry.addActorMethod({ nodeId: 'r1', actorType: 'DemoActor', reminder: true }, () => {});
  const activation = registry.activation();
  assert.deepEqual(activation.actorConfig.entities, ['DemoActor']);
  assert.deepEqual(activation.actorMethodPairs, [
    { actorType: 'DemoActor', method: REMINDER_METHOD },
  ]);
});

// ---- fix: REMINDER_METHOD must be unrepresentable as a real method name ---

test('a method literally named "reminder" and a reminder registration coexist on one actor type', () => {
  const { createConnectionRegistry } = require('../../lib/connection-registry');
  const registry = createConnectionRegistry();
  const methodHandler = () => {};
  const reminderHandler = () => {};
  registry.addActorMethod(
    { nodeId: 'm1', actorType: 'DemoActor', method: 'reminder' },
    methodHandler
  );
  registry.addActorMethod(
    { nodeId: 'r1', actorType: 'DemoActor', reminder: true },
    reminderHandler
  );
  assert.equal(registry.actorHandlerFor('DemoActor', 'reminder'), methodHandler);
  assert.equal(registry.actorHandlerFor('DemoActor', REMINDER_METHOD), reminderHandler);
});

test('the reserved reminder registry key can never be registered as an ordinary method name', () => {
  const { createConnectionRegistry } = require('../../lib/connection-registry');
  const registry = createConnectionRegistry();
  assert.throws(
    () =>
      registry.addActorMethod(
        { nodeId: 'm1', actorType: 'DemoActor', method: REMINDER_METHOD },
        () => {}
      ),
    (err) => {
      assert.equal(err.code, 'INVALID_OPTIONS');
      return true;
    }
  );
});

test('PUT .../method/reminder reaches the ordinary method node, and PUT .../method/remind/x reaches the reminder node, on one actor type', async (t) => {
  const port = await freePort();
  const lease = await acquire(t, { port });
  let methodSeen = false;
  let reminderSeen = null;
  lease.activate({
    actorConfig: actorConfig(),
    actorMethodPairs: [
      { actorType: 'DemoActor', method: 'reminder' },
      { actorType: 'DemoActor', method: REMINDER_METHOD },
    ],
    getActorHandler: (type, method) => {
      if (type !== 'DemoActor') return undefined;
      if (method === 'reminder') return () => {};
      if (method === REMINDER_METHOD) return () => {};
      return undefined;
    },
    actorInvoke: async ({ method, trigger }) => {
      if (trigger) {
        reminderSeen = trigger.name;
      } else {
        methodSeen = true;
        assert.equal(
          method,
          'reminder',
          'the ordinary method path must resolve the literal method name'
        );
      }
      return { status: 200 };
    },
  });

  const methodRes = await httpRequest(url(port, '/actors/DemoActor/a/method/reminder'), {
    method: 'PUT',
  });
  assert.equal(methodRes.status, 200);
  assert.equal(methodSeen, true, 'the ordinary method call must reach the method handler');
  assert.equal(reminderSeen, null, 'the ordinary method call must not reach the reminder handler');

  const reminderRes = await httpRequest(url(port, '/actors/DemoActor/a/method/remind/x'), {
    method: 'PUT',
  });
  assert.equal(reminderRes.status, 200);
  assert.equal(reminderSeen, 'x', 'the reminder callback must reach the reminder handler');
});
