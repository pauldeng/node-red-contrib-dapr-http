'use strict';

const http = require('node:http');
const { closeHttpServer } = require('./http');

// A throwaway HTTP server that JSON-decodes every POSTed body and appends it
// to `received`, in arrival order. Shared by every integration test that
// wires a flow's "http request" node back to itself as an observability
// hook — the only way a black-box test can see what a flow actually did.
async function startCapture() {
  const received = [];
  const waiters = new Set(); // (message) => void, notified as each body lands
  const server = http.createServer((req, res) => {
    const chunks = [];
    req.on('data', (c) => chunks.push(c));
    req.on('end', () => {
      let message;
      try {
        message = JSON.parse(Buffer.concat(chunks).toString());
      } catch {
        message = null;
      }
      received.push(message);
      // Copy first — a waiter removes itself while we iterate.
      for (const waiter of [...waiters]) {
        waiter(message);
      }
      res.writeHead(200).end('ok');
    });
  });
  await new Promise((resolve) => server.listen(0, '127.0.0.1', resolve));

  // Shared plumbing for both waiters below: re-check `settled()` on every
  // arrival and resolve with whatever it returns. Push, not poll — a test
  // never pays a polling interval's latency. Already-received messages are
  // checked first, since a body frequently lands before the test awaits it.
  async function waitUntil(settled, describe, timeoutMs) {
    const already = settled();
    if (already !== undefined) {
      return already;
    }
    // The one place a Promise constructor is unavoidable: bridging the
    // server's own callback into an awaitable.
    return new Promise((resolve, reject) => {
      // waiter closes over `timer`, declared just below: it is only ever
      // invoked from the server handler, long after both bindings exist.
      const waiter = () => {
        const value = settled();
        if (value === undefined) {
          return;
        }
        clearTimeout(timer);
        waiters.delete(waiter);
        resolve(value);
      };
      const timer = setTimeout(() => {
        waiters.delete(waiter);
        reject(
          new Error(`${describe} timed out after ${timeoutMs}ms (${received.length} received)`)
        );
      }, timeoutMs);
      waiters.add(waiter);
    });
  }

  return {
    received,
    url: `http://127.0.0.1:${server.address().port}/capture`,
    // Resolve with the first message matching `predicate`.
    waitForMessage(predicate, { timeoutMs = 10000 } = {}) {
      return waitUntil(() => received.find(predicate), 'waitForMessage', timeoutMs);
    },
    // Resolve with every message once at least `count` have arrived. For
    // assertions about how many messages a flow produced, not which.
    waitForCount(count, { timeoutMs = 10000 } = {}) {
      return waitUntil(
        () => (received.length >= count ? received : undefined),
        'waitForCount',
        timeoutMs
      );
    },
    stop: () => closeHttpServer(server),
  };
}

module.exports = { startCapture };
