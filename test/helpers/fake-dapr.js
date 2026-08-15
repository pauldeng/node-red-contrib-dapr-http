'use strict';

const http = require('node:http');
const { closeHttpServer } = require('./http');

// Focused fake Dapr HTTP sidecar for tests.
//
// It is a real `node:http` server on an ephemeral loopback port that records
// every request it receives and lets a test install per-route responders.
// Milestone 2 only needs it to exist, record, and respond; later milestones
// extend it with real Dapr endpoint semantics (publish, invoke, healthz).
//
// Usage:
//   const dapr = await createFakeDapr();
//   dapr.respond('GET', '/v1.0/healthz/outbound', (req, res) => {
//     res.writeHead(204).end();
//   });
//   // ... exercise ...
//   dapr.requests; // [{ method, path, query, headers, body }]
//   await dapr.stop();

function createFakeDapr() {
  const requests = [];
  const routes = new Map(); // `${METHOD} ${pathname}` -> (req, res, ctx) => void
  const requestWaiters = new Set(); // (ctx) => void, notified as each request lands

  const server = http.createServer((req, res) => {
    const chunks = [];
    req.on('data', (c) => chunks.push(c));
    req.on('end', () => {
      const url = new URL(req.url, 'http://127.0.0.1');
      const ctx = {
        method: req.method,
        path: url.pathname,
        query: Object.fromEntries(url.searchParams),
        headers: req.headers,
        body: Buffer.concat(chunks),
      };
      requests.push(ctx);
      // Notify before responding: a waiter must observe the request even if
      // the responder never calls res.end() (the hang/abort tests rely on that).
      // Copy the set first — a waiter removes itself while we iterate.
      for (const waiter of [...requestWaiters]) {
        waiter(ctx);
      }

      const responder = routes.get(`${req.method} ${url.pathname}`);
      if (responder) {
        responder(req, res, ctx);
        return;
      }
      // No responder: 404 so a wrong or unimplemented Dapr path fails loudly
      // rather than looking successful. Register an explicit responder for
      // every path a test expects to succeed. The request is still recorded.
      res.writeHead(404, { 'content-type': 'application/json' });
      res.end(
        JSON.stringify({ error: 'no fake responder', method: req.method, path: url.pathname })
      );
    });
  });

  const api = {
    requests,
    get url() {
      const addr = server.address();
      return `http://127.0.0.1:${addr.port}`;
    },
    get port() {
      return server.address().port;
    },
    // Install a responder for one METHOD + path. Last registration wins.
    respond(method, path, responder) {
      routes.set(`${method} ${path}`, responder);
      return api;
    },
    // Resolve as soon as a matching request lands — push, not poll, so a test
    // never pays a polling interval's latency (or misses a request between
    // ticks). `match` is a pathname or a predicate over the recorded ctx.
    //
    // Already-recorded requests are checked first: the request under test
    // frequently lands before the test gets around to awaiting it, and an
    // event-only API would hang forever on exactly that ordering.
    async waitForRequest(match, { timeoutMs = 10000 } = {}) {
      const predicate = typeof match === 'function' ? match : (request) => request.path === match;
      const existing = requests.find(predicate);
      if (existing) {
        return existing;
      }
      // The one place a Promise constructor is unavoidable: bridging the
      // server's own callback into an awaitable.
      return new Promise((resolve, reject) => {
        // waiter closes over `timer`, declared just below: it is only ever
        // invoked from the server handler, long after both bindings exist.
        const waiter = (request) => {
          if (!predicate(request)) {
            return;
          }
          clearTimeout(timer);
          requestWaiters.delete(waiter);
          resolve(request);
        };
        const timer = setTimeout(() => {
          requestWaiters.delete(waiter);
          reject(
            new Error(
              `waitForRequest timed out after ${timeoutMs}ms (saw: ${
                requests.map((r) => `${r.method} ${r.path}`).join(', ') || 'no requests'
              })`
            )
          );
        }, timeoutMs);
        requestWaiters.add(waiter);
      });
    },
    reset() {
      requests.length = 0;
      routes.clear();
    },
    start() {
      return new Promise((resolve, reject) => {
        server.once('error', reject);
        server.listen(0, '127.0.0.1', () => {
          server.removeListener('error', reject);
          resolve(api);
        });
      });
    },
    stop() {
      return closeHttpServer(server);
    },
  };

  return api;
}

async function createFakeDaprStarted() {
  return createFakeDapr().start();
}

module.exports = { createFakeDapr, createFakeDaprStarted };
