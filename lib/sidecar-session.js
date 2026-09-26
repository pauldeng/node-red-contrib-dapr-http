'use strict';

const { DaprError, ErrorCodes } = require('./errors');

// The lifecycle every message-triggered node repeated verbatim: refuse to run
// without a usable connection, mirror sidecar health into node status, bound
// each outbound call with an AbortController, and abort whatever is still in
// flight when the node closes.
//
// Deliberately NOT a node factory. The four nodes differ in the client they
// call, the span they open, and how they shape a response onto msg — hiding
// that behind another layer would trade readable node bodies for indirection.
// Only the mechanics live here.

const REQUIRED_METHODS = ['isSidecarHealthy', 'whenHealthKnown', 'onSidecarHealth'];

// A config-node reference resolves to undefined when it is dangling, and to a
// node with `options === null` when that connection's own config failed to
// validate. Both mean "unusable", and so does a node that predates the health
// API — checking the methods keeps a partially-constructed connection from
// throwing later, mid-message.
function isUsableConnection(connection) {
  return Boolean(
    connection?.options && REQUIRED_METHODS.every((name) => typeof connection[name] === 'function')
  );
}

// Installs the "no usable connection" state: a red status plus an input
// handler that fails every message through done(), so a misconfigured flow
// reports per message instead of silently dropping it. Returns false when the
// node must stop constructing.
function requireConnection(node, connection) {
  if (isUsableConnection(connection)) {
    return true;
  }
  node.status({ fill: 'red', shape: 'ring', text: 'missing connection' });
  node.on('input', (_msg, _send, done) => {
    done(new DaprError(ErrorCodes.INVALID_OPTIONS, 'Dapr connection is unavailable'));
  });
  return false;
}

// Binds a node's lifetime to its connection. Registers the health-status
// mirror and the close handler once; the returned handle is what the input
// handler uses per message.
function openSidecarSession(node, connection) {
  const removeHealthListener = connection.onSidecarHealth((healthy) =>
    node.status(
      healthy
        ? { fill: 'green', shape: 'dot', text: 'ready' }
        : { fill: 'red', shape: 'ring', text: 'sidecar unavailable' }
    )
  );

  const inflight = new Set(); // AbortControllers for calls in flight
  let closed = false;

  node.on('close', (_removed, done) => {
    closed = true;
    removeHealthListener();
    // Abort anything still in flight so it cannot outlive the node and settle
    // against a closed one.
    for (const controller of inflight) {
      controller.abort();
    }
    inflight.clear();
    done();
  });

  return {
    // Wait for the connection's first health probe to land before judging the
    // sidecar down, so a message sent immediately after deploy is not failed
    // against a sidecar that is in fact up.
    async isReady() {
      // Never rejects. This is the FIRST await in every input handler, and it
      // sits ahead of that handler's own try/catch — so a rejection here would
      // become an unhandled rejection that no Catch node can see and that
      // never settles done(), silently stranding the message. Treating any
      // failure to establish health as "not ready" fails the message through
      // the caller's normal SIDECAR_UNAVAILABLE path instead.
      try {
        await connection.whenHealthKnown();
        return !closed && connection.isSidecarHealthy() === true;
      } catch {
        return false;
      }
    },

    // Run one outbound call under its own AbortController, tracked for the
    // close handler above and always released. `run` receives the transport
    // options every lib/*-client.js takes — built from the connection's own
    // already-resolved options, never a second resolution path.
    async call(run) {
      if (closed) {
        throw new DaprError(ErrorCodes.SIDECAR_UNAVAILABLE, 'Dapr node is closed');
      }
      const controller = new AbortController();
      inflight.add(controller);
      try {
        return await run({
          baseUrl: connection.options.outbound.baseUrl,
          token: connection.options.daprApiToken,
          timeoutMs: connection.options.limits.requestTimeoutMs,
          signal: controller.signal,
          maxResponseBytes: connection.options.limits.bodyLimitBytes,
        });
      } finally {
        inflight.delete(controller);
      }
    },
  };
}

module.exports = { isUsableConnection, requireConnection, openSidecarSession };
