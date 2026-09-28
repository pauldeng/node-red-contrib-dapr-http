'use strict';

const { test } = require('node:test');
const assert = require('node:assert/strict');
const { once } = require('node:events');
const net = require('node:net');

const { acquireListener } = require('../../lib/app-channel');
const { httpRequest } = require('../helpers/http');
const { freePort } = require('../helpers/node-red');
const { DaprError, ErrorCodes } = require('../../lib/errors');
const { setTimeout: delay } = require('node:timers/promises');

const BIND = '127.0.0.1';
const url = (port, path) => `http://127.0.0.1:${port}${path}`;

function limits(over = {}) {
  return {
    bodyLimitBytes: 1024,
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

// Acquire with deterministic cleanup registered on the test.
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

const route = (path, handler, kind = 'service', method = 'GET') => ({
  method,
  path,
  kind,
  handler,
});

test('GET /healthz returns 204 and needs no token or activation', async (t) => {
  const port = await freePort();
  await acquire(t, { port, token: 'secret' });
  assert.equal((await httpRequest(url(port, '/healthz'))).status, 204);
});

test('a configured token is required on non-health routes', async (t) => {
  const port = await freePort();
  const lease = await acquire(t, { port, token: 'secret' });
  lease.activate({ subscriptions: [{ pubsubname: 'ps', topic: 't', route: '/x' }] });

  assert.equal((await httpRequest(url(port, '/dapr/subscribe'))).status, 401);
  assert.equal(
    (await httpRequest(url(port, '/dapr/subscribe'), { headers: { 'dapr-api-token': 'wrong' } }))
      .status,
    401
  );
  const ok = await httpRequest(url(port, '/dapr/subscribe'), {
    headers: { 'dapr-api-token': 'secret' },
  });
  assert.equal(ok.status, 200);
  assert.deepEqual(JSON.parse(ok.text), [{ pubsubname: 'ps', topic: 't', route: '/x' }]);
});

test('with no configured token, an activated listener serves discovery', async (t) => {
  const port = await freePort();
  const lease = await acquire(t, { port });
  lease.activate({});
  assert.equal((await httpRequest(url(port, '/dapr/subscribe'))).status, 200);
});

test('/dapr/subscribe rejects a request carrying dapr-caller-app-id', async (t) => {
  const port = await freePort();
  const lease = await acquire(t, { port });
  lease.activate({});
  const res = await httpRequest(url(port, '/dapr/subscribe'), {
    headers: { 'dapr-caller-app-id': 'evil' },
  });
  assert.equal(res.status, 403);
});

test('registered routes: dispatch by method, 404 unknown, 405 + Allow on wrong verb', async (t) => {
  const port = await freePort();
  const lease = await acquire(t, { port });
  lease.activate({
    routes: [
      route(
        '/node-red-dapr/subscriptions/s1',
        async (ctx) => ({ status: 200, body: ctx.body.toString() }),
        'internal',
        'POST'
      ),
    ],
  });

  const ok = await httpRequest(url(port, '/node-red-dapr/subscriptions/s1'), {
    method: 'POST',
    body: 'hello',
  });
  assert.equal(ok.status, 200);
  assert.equal(ok.text, 'hello');

  assert.equal((await httpRequest(url(port, '/nope'))).status, 404);

  const wrong = await httpRequest(url(port, '/node-red-dapr/subscriptions/s1'), { method: 'GET' });
  assert.equal(wrong.status, 405);
  assert.match(wrong.headers.allow || '', /POST/);
});

test('internal routes reject the caller header; service routes preserve caller app id', async (t) => {
  const port = await freePort();
  const lease = await acquire(t, { port });
  let seenCaller = 'unset';
  lease.activate({
    routes: [
      route('/node-red-dapr/subscriptions/s1', async () => ({ status: 200 }), 'internal', 'POST'),
      route(
        '/orders',
        async (ctx) => {
          seenCaller = ctx.callerAppId;
          return { status: 200 };
        },
        'service',
        'POST'
      ),
    ],
  });

  const internal = await httpRequest(url(port, '/node-red-dapr/subscriptions/s1'), {
    method: 'POST',
    headers: { 'dapr-caller-app-id': 'x' },
    body: '',
  });
  assert.equal(internal.status, 403);

  const service = await httpRequest(url(port, '/orders'), {
    method: 'POST',
    headers: { 'dapr-caller-app-id': 'checkout' },
    body: '',
  });
  assert.equal(service.status, 200);
  assert.equal(seenCaller, 'checkout');
});

test('a body over the size limit is rejected with 413', async (t) => {
  const port = await freePort();
  const lease = await acquire(t, { port, limits: limits({ bodyLimitBytes: 8 }) });
  lease.activate({ routes: [route('/orders', async () => ({ status: 200 }), 'service', 'POST')] });
  const res = await httpRequest(url(port, '/orders'), {
    method: 'POST',
    body: 'far too many bytes',
  });
  assert.equal(res.status, 413);
});

test('a second acquire on an active bind:port throws DUPLICATE_LISTENER', async (t) => {
  const port = await freePort();
  await acquire(t, { port });
  await assert.rejects(
    acquireListener({ bindAddress: BIND, port, token: undefined, limits: limits() }),
    (e) => e instanceof DaprError && e.code === ErrorCodes.DUPLICATE_LISTENER
  );
});

test('two different ports are isolated: closing one leaves the other serving', async (t) => {
  const p1 = await freePort();
  const p2 = await freePort();
  const l1 = await acquireListener({
    bindAddress: BIND,
    port: p1,
    token: undefined,
    limits: limits(),
  });
  await acquire(t, { port: p2 });

  l1.release({ graceMs: 0 });
  await l1.whenClosed();
  assert.equal((await httpRequest(url(p2, '/healthz'))).status, 204);
});

test('listener survives release + reacquire within the grace window (redeploy)', async (t) => {
  const port = await freePort();
  const l1 = await acquireListener({
    bindAddress: BIND,
    port,
    token: undefined,
    limits: limits({ leaseGraceMs: 1000 }),
  });
  l1.release({ graceMs: 1000 });
  const l2 = await acquire(t, { port, limits: limits({ leaseGraceMs: 1000 }) });

  assert.equal((await httpRequest(url(port, '/healthz'))).status, 204);
  assert.equal(l2.isClosed(), false);
});

test('a stale lease cannot close a reacquired listener', async (t) => {
  const port = await freePort();
  const l1 = await acquireListener({
    bindAddress: BIND,
    port,
    token: undefined,
    limits: limits({ leaseGraceMs: 1000 }),
  });
  l1.release({ graceMs: 1000 });
  const l2 = await acquire(t, { port, limits: limits({ leaseGraceMs: 1000 }) });

  l1.release({ graceMs: 0 }); // stale generation — must be a no-op
  await delay(50);
  assert.equal(l2.isClosed(), false);
  assert.equal((await httpRequest(url(port, '/healthz'))).status, 204);
});

test('graceful shutdown closes the server after the grace window', async () => {
  const port = await freePort();
  const lease = await acquireListener({
    bindAddress: BIND,
    port,
    token: undefined,
    limits: limits(),
  });
  lease.release({ graceMs: 30 });
  await lease.whenClosed();
  assert.equal(lease.isClosed(), true);
  await assert.rejects(httpRequest(url(port, '/healthz'), { timeoutMs: 500 }));
});

test('a reacquired generation returns 503 everywhere until it is re-activated', async (t) => {
  const port = await freePort();
  const l1 = await acquireListener({
    bindAddress: BIND,
    port,
    token: undefined,
    limits: limits({ leaseGraceMs: 1000 }),
  });
  l1.activate({
    subscriptions: [{ pubsubname: 'ps', topic: 't', route: '/r' }],
    routes: [route('/orders', async () => ({ status: 200, body: 'v1' }))],
  });
  assert.equal((await httpRequest(url(port, '/dapr/subscribe'))).status, 200);
  assert.equal((await httpRequest(url(port, '/orders'))).text, 'v1');

  // Released → grace: every non-health endpoint is a retryable 503.
  l1.release({ graceMs: 1000 });
  assert.equal((await httpRequest(url(port, '/dapr/subscribe'))).status, 503);
  assert.equal((await httpRequest(url(port, '/orders'))).status, 503);

  // Reacquired but not yet re-activated: stale subscriptions are NOT served, and
  // an unknown route is 503, not a drop-inducing 404.
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
  assert.equal((await httpRequest(url(port, '/dapr/subscribe'))).status, 503);
  assert.equal((await httpRequest(url(port, '/orders'))).status, 503);
  assert.equal((await httpRequest(url(port, '/brand-new'))).status, 503);

  // Re-activate the new generation → serves fresh state.
  l2.activate({ routes: [route('/orders', async () => ({ status: 200, body: 'v2' }))] });
  assert.equal((await httpRequest(url(port, '/orders'))).text, 'v2');
  assert.deepEqual(JSON.parse((await httpRequest(url(port, '/dapr/subscribe'))).text), []);
});

test('releasing a generation drains in-flight requests as 503 and aborts handlers', async () => {
  const port = await freePort();
  const lease = await acquireListener({
    bindAddress: BIND,
    port,
    token: undefined,
    limits: limits({ requestTimeoutMs: 5000, leaseGraceMs: 300 }),
  });
  let aborted = false;
  lease.activate({
    routes: [
      route(
        '/slow',
        (ctx) =>
          new Promise((resolve) => {
            ctx.signal.addEventListener('abort', () => {
              aborted = true;
              resolve({ status: 200 });
            });
          })
      ),
    ],
  });

  const started = Date.now();
  const reqP = httpRequest(url(port, '/slow'), { timeoutMs: 5000 });
  await delay(100); // let it become in-flight
  lease.release({ graceMs: 300 });
  const res = await reqP;
  await lease.whenClosed();

  assert.equal(res.status, 503);
  assert.ok(Date.now() - started < 1500, 'settled on release, not at the 5s deadline');
  assert.equal(aborted, true, 'handler observed the abort signal');
});

test('releasing during a partial body upload closes its socket after a retryable response', async (t) => {
  const port = await freePort();
  const lease = await acquire(t, { port });
  lease.activate({
    routes: [
      route('/upload', async () => assert.fail('partial body reached handler'), 'service', 'POST'),
    ],
  });

  const socket = net.connect(port, BIND);
  t.after(() => socket.destroy());
  const received = [];
  socket.on('data', (chunk) => received.push(chunk));
  await once(socket, 'connect');
  // Node sends 100 Continue from its request event before the body reader
  // awaits input. Observing it proves admission without a polling/sleep race.
  const continued = once(socket, 'data', { signal: AbortSignal.timeout(2000) });
  socket.write(
    'POST /upload HTTP/1.1\r\nHost: localhost\r\nContent-Length: 1000000\r\nExpect: 100-continue\r\n\r\npartial'
  );
  assert.match((await continued)[0].toString(), /HTTP\/1\.1 100 Continue/);
  const closed = once(socket, 'close', { signal: AbortSignal.timeout(2000) });
  // Keep the listener for replacement; its forced-close backstop must not
  // be what releases this cancelled upload.
  lease.release({ graceMs: 10000 });
  try {
    await closed;
    assert.match(Buffer.concat(received).toString(), /HTTP\/1\.1 503/);
  } finally {
    socket.destroy();
    const replacement = await acquire(t, { port });
    replacement.release();
    await replacement.whenClosed();
  }
});

test('concurrent acquisition yields exactly one owner and one DUPLICATE_LISTENER', async (t) => {
  const port = await freePort();
  const results = await Promise.allSettled([
    acquireListener({ bindAddress: BIND, port, token: undefined, limits: limits() }),
    acquireListener({ bindAddress: BIND, port, token: undefined, limits: limits() }),
  ]);
  const fulfilled = results.filter((r) => r.status === 'fulfilled');
  const rejected = results.filter((r) => r.status === 'rejected');
  assert.equal(fulfilled.length, 1);
  assert.equal(rejected.length, 1);
  assert.ok(
    rejected[0].reason instanceof DaprError &&
      rejected[0].reason.code === ErrorCodes.DUPLICATE_LISTENER,
    'the loser gets DUPLICATE_LISTENER, not a raw EADDRINUSE'
  );
  t.after(async () => {
    fulfilled[0].value.release({ graceMs: 0 });
    await fulfilled[0].value.whenClosed();
  });
});

test('a port can be rebound immediately after the previous listener closes', async (t) => {
  const port = await freePort();
  const l1 = await acquireListener({ bindAddress: BIND, port, token: undefined, limits: limits() });
  l1.release({ graceMs: 0 });
  await l1.whenClosed();

  const l2 = await acquire(t, { port });
  assert.equal((await httpRequest(url(port, '/healthz'))).status, 204);
  assert.ok(l2);
});

test('a bind failure frees the registry key, so a retry can still succeed', async (t) => {
  // An UNRELATED process holding the port is a different path from the
  // registry's own DUPLICATE_LISTENER: there is no entry to find, so listen()
  // itself fails. If the reserved key were left behind, every later attempt on
  // that bind:port would report "already in use" even once the squatter let go —
  // a Node-RED restart would be the only way out.
  const port = await freePort();
  const squatter = net.createServer();
  await new Promise((resolve) => squatter.listen(port, BIND, resolve));

  await assert.rejects(
    acquireListener({ bindAddress: BIND, port, token: undefined, limits: limits() }),
    (err) => {
      assert.equal(err.code, 'EADDRINUSE');
      assert.ok(!(err instanceof DaprError), 'a real bind error, not DUPLICATE_LISTENER');
      return true;
    }
  );

  await new Promise((resolve) => squatter.close(resolve));

  // The key was released, so the same bind:port is acquirable again.
  const lease = await acquire(t, { port });
  assert.equal((await httpRequest(url(port, '/healthz'))).status, 204);
  assert.ok(lease);
});

test('acquiring a closing listener waits for the port, then rebinds fresh', async (t) => {
  const port = await freePort();
  const l1 = await acquireListener({ bindAddress: BIND, port, token: undefined, limits: limits() });
  l1.activate({ subscriptions: [{ pubsubname: 'ps', topic: 'old', route: '/old' }] });

  // Release with no grace: the entry goes straight to 'closing'. Acquire while
  // that close is still in flight — without awaiting it the rebind would race
  // the OS releasing the port and fail EADDRINUSE.
  l1.release({ graceMs: 0 });
  const l2 = await acquireListener({ bindAddress: BIND, port, token: undefined, limits: limits() });
  t.after(async () => {
    l2.release({ graceMs: 0 });
    await l2.whenClosed();
  });

  assert.ok(l1.isClosed(), 'the previous generation finished closing');
  // A genuinely fresh listener, not the old entry adopted: its discovery state
  // starts empty rather than carrying the released generation's subscriptions.
  assert.equal(l2.servedFingerprint(), null);
  l2.activate({ subscriptions: [] });
  const res = await httpRequest(url(port, '/dapr/subscribe'));
  assert.equal(res.status, 200);
  assert.deepEqual(JSON.parse(res.text), []);
});

test('a handler that never responds is cut off with 503 at the request deadline', async (t) => {
  const port = await freePort();
  const lease = await acquire(t, { port, limits: limits({ requestTimeoutMs: 150 }) });
  lease.activate({ routes: [route('/hang', () => new Promise(() => {}))] });
  const started = Date.now();
  const res = await httpRequest(url(port, '/hang'), { timeoutMs: 5000 });
  assert.equal(res.status, 503);
  assert.ok(Date.now() - started < 2000, 'cut off near the deadline, not the client timeout');
});

test('a handler resolving after the deadline does not double-respond', async (t) => {
  const port = await freePort();
  const lease = await acquire(t, { port, limits: limits({ requestTimeoutMs: 100 }) });
  lease.activate({
    routes: [
      route(
        '/slow',
        () => new Promise((r) => setTimeout(() => r({ status: 200, body: 'late' }), 300))
      ),
    ],
  });
  assert.equal((await httpRequest(url(port, '/slow'), { timeoutMs: 5000 })).status, 503);
  await delay(400); // let the late resolve happen
  assert.equal((await httpRequest(url(port, '/healthz'))).status, 204); // server unharmed
});

test('concurrent requests beyond the pending cap get 503 before buffering', async (t) => {
  const port = await freePort();
  const lease = await acquire(t, {
    port,
    limits: limits({ maxPending: 1, requestTimeoutMs: 5000 }),
  });
  let release1;
  lease.activate({
    routes: [
      route(
        '/slot',
        () =>
          new Promise((r) => {
            release1 = () => r({ status: 200, body: 'ok' });
          })
      ),
    ],
  });

  const first = httpRequest(url(port, '/slot'), { timeoutMs: 5000 });
  await delay(100); // let the first occupy the only slot
  const second = await httpRequest(url(port, '/slot'), { timeoutMs: 5000 });
  assert.equal(second.status, 503);

  release1();
  assert.equal((await first).status, 200);
});

test('a request with more headers than allowed is rejected, not served', async (t) => {
  const port = await freePort();
  await acquire(t, { port, limits: limits({ headerCountLimit: 10 }) });
  const headers = {};
  for (let i = 0; i < 60; i += 1) {
    headers[`x-h-${i}`] = 'v';
  }
  const outcome = await httpRequest(url(port, '/healthz'), { headers, timeoutMs: 2000 }).catch(
    (err) => err
  );
  if (outcome instanceof Error) {
    assert.ok(true); // socket rejected the oversized header set
  } else {
    assert.notEqual(outcome.status, 204);
  }
});

test('a slow-header client is disconnected by the headers timeout', async (t) => {
  const port = await freePort();
  await acquire(t, { port, limits: limits({ headersTimeoutMs: 200 }) });
  const socket = net.connect(port, '127.0.0.1');
  t.after(() => socket.destroy());
  await new Promise((resolve, reject) => {
    socket.once('connect', resolve);
    socket.once('error', reject);
  });
  socket.on('data', () => {}); // drain any 408 the server sends
  socket.write('GET /healthz HTTP/1.1\r\nHost: x\r\n'); // deliberately incomplete — no blank line
  const started = Date.now();
  await new Promise((resolve) => socket.once('close', resolve));
  assert.ok(
    Date.now() - started < 2000,
    'server closed the slow-header connection near the timeout'
  );
});

test('activate onDiscovery fires when daprd fetches /dapr/subscribe', async (t) => {
  const port = await freePort();
  const lease = await acquire(t, { port });
  let discovered = 0;
  lease.activate({
    subscriptions: [{ pubsubname: 'ps', topic: 't', route: '/r' }],
    onDiscovery: () => {
      discovered += 1;
    },
  });
  assert.equal(discovered, 0);
  const res = await httpRequest(url(port, '/dapr/subscribe'));
  assert.equal(res.status, 200);
  assert.equal(discovered, 1);
});

test('servedFingerprint records the last fetched set and survives a reacquire', async (t) => {
  const port = await freePort();
  const l1 = await acquireListener({
    bindAddress: BIND,
    port,
    token: undefined,
    limits: limits({ leaseGraceMs: 1000 }),
  });
  l1.activate({
    subscriptions: [{ pubsubname: 'ps', topic: 't', route: '/r' }],
    fingerprint: 'fp1',
  });
  assert.equal(l1.servedFingerprint(), null, 'null until daprd fetches');
  await httpRequest(url(port, '/dapr/subscribe'));
  assert.equal(l1.servedFingerprint(), 'fp1');

  // Redeploy: the served fingerprint lives in the module-scoped entry, so the
  // reacquiring generation still knows what daprd last fetched.
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
  assert.equal(l2.servedFingerprint(), 'fp1', 'persists across reacquire');
});

test('a removed internal route stays retryable (503), never 404, even after a discovery fetch', async (t) => {
  const port = await freePort();
  const lease = await acquire(t, { port });
  lease.activate({
    subscriptions: [{ pubsubname: 'ps', topic: 't', route: '/r' }],
    routes: [
      { method: 'POST', path: '/r', kind: 'internal', handler: async () => ({ status: 200 }) },
    ],
    fingerprint: 'fp1',
  });
  assert.equal((await httpRequest(url(port, '/r'), { method: 'POST', body: '' })).status, 200);

  // Re-activate without that route (e.g. a rule removed): the old path must
  // stay retryable, not 404 — Dapr treats 404 as a permanent DROP.
  lease.activate({ subscriptions: [], routes: [], fingerprint: 'fp2' });
  const stale = await httpRequest(url(port, '/r'), { method: 'POST', body: '' });
  assert.equal(stale.status, 503);

  // A discovery fetch must NOT clear the placeholder. The listener cannot tell
  // daprd's own startup fetch from any other caller's — an operator's curl, a
  // monitoring probe, or (on the untokenized loopback default) any local
  // process. Clearing it on the wrong one turns the next delivery from a
  // still-stale sidecar into a 404, which Dapr treats as a permanent DROP: the
  // message is lost rather than retried.
  await httpRequest(url(port, '/dapr/subscribe'));
  const afterFetch = await httpRequest(url(port, '/r'), { method: 'POST', body: '' });
  assert.equal(afterFetch.status, 503, 'still retryable after a discovery fetch');
});

test('a re-added route at the same path wins over its own stale placeholder', async (t) => {
  const port = await freePort();
  const lease = await acquire(t, { port });
  const routeDef = (body) => [
    { method: 'POST', path: '/r', kind: 'internal', handler: async () => ({ status: 200, body }) },
  ];
  lease.activate({ subscriptions: [], routes: routeDef('v1'), fingerprint: 'fp1' });
  lease.activate({ subscriptions: [], routes: [], fingerprint: 'fp2' }); // removed -> stale
  lease.activate({ subscriptions: [], routes: routeDef('v2'), fingerprint: 'fp3' }); // re-added
  const res = await httpRequest(url(port, '/r'), { method: 'POST', body: '' });
  assert.equal(res.status, 200, 'the real route wins, not the stale placeholder');
  assert.equal(res.text, 'v2');
});

test('a removed service route 404s immediately — Dapr never caches service routes', async (t) => {
  const port = await freePort();
  const lease = await acquire(t, { port });
  lease.activate({
    subscriptions: [],
    routes: [
      { method: 'POST', path: '/svc', kind: 'service', handler: async () => ({ status: 200 }) },
    ],
  });
  assert.equal((await httpRequest(url(port, '/svc'), { method: 'POST', body: '' })).status, 200);
  lease.activate({ subscriptions: [], routes: [] });
  const res = await httpRequest(url(port, '/svc'), { method: 'POST', body: '' });
  assert.equal(res.status, 404, 'no stale-route protection for services');
});
