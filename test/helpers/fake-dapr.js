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
