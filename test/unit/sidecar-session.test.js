'use strict';

const { test } = require('node:test');
const assert = require('node:assert/strict');

const {
  isUsableConnection,
  requireConnection,
  openSidecarSession,
} = require('../../lib/sidecar-session');
const { ErrorCodes } = require('../../lib/errors');

// A stand-in for the parts of a Node-RED node these helpers touch. Not a
// Node-RED import: lib/ modules stay directly unit-testable.
function fakeNode() {
  const node = {
    statuses: [],
    handlers: new Map(),
    status(s) {
      node.statuses.push(s);
    },
    on(event, handler) {
      node.handlers.set(event, handler);
    },
    emit(event, ...args) {
      return node.handlers.get(event)?.(...args);
    },
  };
  return node;
}

function fakeConnection({ healthy = true, options } = {}) {
  const listeners = new Set();
  return {
    options: options ?? {
      outbound: { baseUrl: 'http://127.0.0.1:3500' },
      daprApiToken: 'token',
      limits: { requestTimeoutMs: 30000, bodyLimitBytes: 4096 },
    },
    isSidecarHealthy: () => healthy,
    whenHealthKnown: async () => {},
    onSidecarHealth(listener) {
      listeners.add(listener);
      listener(healthy);
      return () => listeners.delete(listener);
    },
    listeners,
  };
}

// --- isUsableConnection ---

test('isUsableConnection accepts a fully constructed connection', () => {
  assert.equal(isUsableConnection(fakeConnection()), true);
});

test('isUsableConnection rejects a dangling reference or a failed-config connection', () => {
  assert.equal(isUsableConnection(undefined), false);
  assert.equal(isUsableConnection(null), false);
  // A connection whose own resolveOptions threw sets options = null.
  assert.equal(isUsableConnection({ ...fakeConnection(), options: null }), false);
});

test('isUsableConnection rejects a connection missing any health method', () => {
  for (const missing of ['isSidecarHealthy', 'whenHealthKnown', 'onSidecarHealth']) {
    const connection = fakeConnection();
    delete connection[missing];
    assert.equal(isUsableConnection(connection), false, `should reject without ${missing}`);
  }
});

// --- requireConnection ---

test('requireConnection passes a usable connection through untouched', () => {
  const node = fakeNode();
  assert.equal(requireConnection(node, fakeConnection()), true);
  assert.deepEqual(node.statuses, []);
  assert.equal(node.handlers.has('input'), false);
});

test('an unusable connection reports per message rather than dropping it silently', () => {
  const node = fakeNode();
  assert.equal(requireConnection(node, undefined), false);
  assert.deepEqual(node.statuses, [{ fill: 'red', shape: 'ring', text: 'missing connection' }]);

  // The installed handler must fail every message through done(), so a
  // misconfigured flow surfaces on each message instead of hanging.
  let failure;
  node.emit(
    'input',
    {},
    () => {},
    (err) => {
      failure = err;
    }
  );
  assert.equal(failure.code, ErrorCodes.INVALID_OPTIONS);
});

// --- openSidecarSession ---

test('the session mirrors sidecar health into node status', () => {
  const node = fakeNode();
  const connection = fakeConnection({ healthy: true });
  openSidecarSession(node, connection);
  assert.deepEqual(node.statuses.at(-1), { fill: 'green', shape: 'dot', text: 'ready' });

  for (const listener of connection.listeners) {
    listener(false);
  }
  assert.deepEqual(node.statuses.at(-1), {
    fill: 'red',
    shape: 'ring',
    text: 'sidecar unavailable',
  });
});

test('isReady() waits for the first probe, then reports current health', async () => {
  assert.equal(
    await openSidecarSession(fakeNode(), fakeConnection({ healthy: true })).isReady(),
    true
  );
  assert.equal(
    await openSidecarSession(fakeNode(), fakeConnection({ healthy: false })).isReady(),
    false
  );
});

test('call() hands the client the connection-derived transport options', async () => {
  const session = openSidecarSession(fakeNode(), fakeConnection());
  const transport = await session.call(async (t) => t);
  assert.equal(transport.baseUrl, 'http://127.0.0.1:3500');
  assert.equal(transport.token, 'token');
  assert.equal(transport.timeoutMs, 30000);
  assert.equal(transport.maxResponseBytes, 4096);
  assert.ok(transport.signal instanceof AbortSignal);
});

test('close aborts a call still in flight, and a settled call leaves nothing behind', async () => {
  const node = fakeNode();
  const session = openSidecarSession(node, fakeConnection());

  // Settled calls must not accumulate: closing after one completes must not
  // abort anything, which would be an already-delivered message failing late.
  let settledSignal;
  await session.call(async (t) => {
    settledSignal = t.signal;
  });

  let inFlightSignal;
  let release;
  const pending = session.call(async (t) => {
    inFlightSignal = t.signal;
    await new Promise((resolve) => {
      release = resolve;
    });
  });
  await new Promise((resolve) => setImmediate(resolve));

  let closed = false;
  node.emit('close', false, () => {
    closed = true;
  });
  assert.equal(closed, true);
  assert.equal(inFlightSignal.aborted, true, 'in-flight call must be aborted');
  assert.equal(settledSignal.aborted, false, 'a settled call must not be aborted after the fact');

  release();
  await pending;
});

test('close removes the health listener so a shared connection does not leak listeners', () => {
  const node = fakeNode();
  const connection = fakeConnection();
  openSidecarSession(node, connection);
  assert.equal(connection.listeners.size, 1);
  node.emit('close', false, () => {});
  assert.equal(connection.listeners.size, 0);
});
