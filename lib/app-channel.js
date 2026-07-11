'use strict';

const http = require('node:http');
const crypto = require('node:crypto');

const { DaprError, ErrorCodes } = require('./errors');

// Module-scoped registry of app-channel listeners, keyed by `bind:port`. It
// outlives any single Node-RED node instance, so an unchanged-flow redeploy
// (node closed then recreated) keeps the same socket open. On a full Node-RED
// process restart the module is re-required fresh, which is the intended reset.
//
// Each entry has an explicit lifecycle state:
//   starting → active → grace → closing → closed
// plus a `ready` flag: a generation serves traffic only once it has been
// atomically activated with its subscriptions and routes. The key is reserved
// ('starting') before binding so a concurrent second acquire reports a duplicate
// rather than racing on the port.
const registry = new Map();

const keyOf = (bind, port) => `${bind}:${port}`;

function timingSafeEqualStr(a, b) {
  if (typeof a !== 'string' || typeof b !== 'string') {
    return false;
  }
  const ab = Buffer.from(a);
  const bb = Buffer.from(b);
  if (ab.length !== bb.length) {
    return false;
  }
  return crypto.timingSafeEqual(ab, bb);
}

function authorized(token, req) {
  if (!token) {
    return true; // no token configured → no authentication to enforce
  }
  const provided = req.headers['dapr-api-token'];
  return typeof provided === 'string' && timingSafeEqualStr(provided, token);
}

// Fixed, safe responses — never echo error internals, tokens, or config.
function writeResponse(res, status, headers, body) {
  const h = { ...(headers || {}) };
  let payload;
  if (body === undefined || body === null) {
    payload = '';
  } else if (Buffer.isBuffer(body) || typeof body === 'string') {
    payload = body;
  } else {
    payload = JSON.stringify(body);
    if (!h['content-type']) {
      h['content-type'] = 'application/json';
    }
  }
  res.writeHead(status, h);
  res.end(payload);
}

const sendStatus = (res, status, headers) => writeResponse(res, status, headers, undefined);
const sendJson = (res, status, obj) => writeResponse(res, status, {}, obj);
const retryable = (res) => sendStatus(res, 503, { 'Retry-After': '1' });

// Read the request body, capped at `limit` bytes. Rejects with a BODY_TOO_LARGE
// error the moment the cap is exceeded so an oversized upload is not buffered.
function readBody(req, limit) {
  return new Promise((resolve, reject) => {
    const chunks = [];
    let size = 0;
    req.on('data', (chunk) => {
      size += chunk.length;
      if (size > limit) {
        const err = new Error('body too large');
        err.code = 'BODY_TOO_LARGE';
        reject(err);
        return;
      }
      chunks.push(chunk);
    });
    req.on('end', () => resolve(Buffer.concat(chunks)));
    req.on('error', reject);
  });
}

function buildRouteMap(defs = []) {
  const byPath = new Map(); // path -> Map<method, { kind, handler }>
  for (const { method, path, kind, handler } of defs) {
    if (!byPath.has(path)) {
      byPath.set(path, new Map());
    }
    byPath.get(path).set(method, { kind, handler });
  }
  return byPath;
}

async function handleRequest(entry, req, res) {
  const url = new URL(req.url, 'http://localhost');
  const path = url.pathname;
  const method = req.method;
  const limits = entry.limits;

  // Health probe: answered whenever the server is listening, regardless of lease
  // state, so an orchestrator can always check app liveness.
  if (path === '/healthz') {
    return method === 'GET' ? sendStatus(res, 204) : sendStatus(res, 405, { Allow: 'GET' });
  }

  // A generation serves traffic only when active AND activated. Between
  // generations (grace/closing, or reacquired-but-not-yet-reactivated) every
  // other endpoint — discovery, routes, and unknown paths alike — returns a
  // retryable 503, never a stale response or a drop-inducing 404.
  if (entry.state !== 'active' || !entry.ready) {
    return retryable(res);
  }

  if (!authorized(entry.token, req)) {
    return sendJson(res, 401, { error: 'unauthorized' });
  }

  // Subscription discovery — internal, so a mesh caller (dapr-caller-app-id)
  // must not reach it.
  if (path === '/dapr/subscribe') {
    if (method !== 'GET') {
      return sendStatus(res, 405, { Allow: 'GET' });
    }
    if (req.headers['dapr-caller-app-id'] !== undefined) {
      return sendJson(res, 403, { error: 'forbidden' });
    }
    return sendJson(res, 200, entry.subscriptions);
  }

  const byMethod = entry.routes.get(path);
  if (!byMethod) {
    return sendJson(res, 404, { error: 'not found' });
  }
  const route = byMethod.get(method);
  if (!route) {
    return sendStatus(res, 405, { Allow: [...byMethod.keys()].join(', ') });
  }

  // Internal delivery routes reject the caller header; service routes preserve
  // it so flows can make authorization decisions.
  if (route.kind === 'internal' && req.headers['dapr-caller-app-id'] !== undefined) {
    return sendJson(res, 403, { error: 'forbidden' });
  }

  // Bound concurrent in-flight requests before buffering any body.
  if (entry.inFlight >= limits.maxPending) {
    return retryable(res);
  }
  entry.inFlight += 1;

  const controller = new AbortController();
  const ctl = {};
  let finished = false;
  const cleanup = () => {
    clearTimeout(deadline);
    entry.inFlight -= 1;
    entry.inflightReqs.delete(ctl);
  };
  const respond = (status, headers, body) => {
    if (finished) {
      return;
    }
    finished = true;
    cleanup();
    writeResponse(res, status, headers, body);
  };
  const abandon = () => {
    if (finished) {
      return;
    }
    finished = true;
    cleanup();
    controller.abort();
  };
  // Whole-request wall-clock deadline (covers body read + handler). On expiry we
  // answer 503, abort the handler, and destroy the socket so a slow body or a
  // never-resolving handler cannot hold the connection. A late handler result
  // cannot double-respond — `finished` guards it.
  const deadline = setTimeout(() => {
    respond(503, { 'Retry-After': '1' }, { error: 'request timeout' });
    controller.abort();
    req.destroy();
  }, limits.requestTimeoutMs);

  ctl.respond = respond;
  ctl.abort = () => controller.abort();
  entry.inflightReqs.add(ctl);
  res.on('close', abandon);

  let body;
  try {
    body = await readBody(req, limits.bodyLimitBytes);
  } catch (err) {
    if (err.code === 'BODY_TOO_LARGE') {
      respond(413, {}, { error: 'payload too large' });
      req.destroy();
    } else {
      respond(500, {}, { error: 'internal error' });
    }
    return undefined;
  }
  if (finished) {
    return undefined; // deadline fired, released, or client disconnected mid-read
  }

  try {
    const ctx = {
      method,
      path,
      query: Object.fromEntries(url.searchParams),
      headers: req.headers,
      body,
      callerAppId: route.kind === 'service' ? (req.headers['dapr-caller-app-id'] ?? null) : null,
      signal: controller.signal,
    };
    const result = (await route.handler(ctx)) || {};
    respond(result.status ?? 200, result.headers, result.body);
  } catch {
    respond(500, {}, { error: 'internal error' });
  }
  return undefined;
}

function createServer(entry) {
  const { limits } = entry;
  const server = http.createServer(
    {
      maxHeaderSize: limits.headerLimitBytes,
      // Bound the header-receipt phase so a slow-header client cannot hold a
      // connection open (the per-request deadline only starts once headers
      // arrive). Node checks this only every connectionsCheckingInterval, so the
      // interval is bounded by the timeout to keep enforcement timely.
      headersTimeout: limits.headersTimeoutMs,
      connectionsCheckingInterval: Math.min(limits.headersTimeoutMs, 30000),
    },
    (req, res) => {
      handleRequest(entry, req, res).catch(() => {
        if (!res.headersSent) {
          sendJson(res, 500, { error: 'internal error' });
        }
      });
    }
  );
  server.maxHeadersCount = limits.headerCountLimit;
  server.on('connection', (socket) => {
    entry.sockets.add(socket);
    socket.on('close', () => entry.sockets.delete(socket));
  });
  return server;
}

// Settle every in-flight request of a generation as a retryable 503 and abort
// its handler, so a released generation never leaves work running silently.
function drainInflight(entry) {
  for (const ctl of [...entry.inflightReqs]) {
    ctl.abort();
    ctl.respond(503, { 'Retry-After': '1' }, { error: 'connection restarting' });
  }
}

function closeEntry(entry) {
  if (entry.state === 'closing' || entry.state === 'closed') {
    return;
  }
  entry.state = 'closing';
  entry.server.close(() => {
    clearTimeout(entry.drainTimer);
    registry.delete(entry.key);
    entry.state = 'closed';
    entry.resolveClosed();
  });
  // Bounded drain: force-close any still-open sockets after the drain window.
  entry.drainTimer = setTimeout(() => {
    for (const socket of entry.sockets) {
      socket.destroy();
    }
  }, entry.limits.drainTimeoutMs);
}

function makeLease(entry) {
  const gen = entry.generation;
  const current = () => entry.generation === gen;
  return {
    bindAddress: entry.bindAddress,
    port: entry.port,
    // Atomically install this generation's subscriptions and routes and mark it
    // ready to serve. Until this is called (fresh start or after a reacquire),
    // the generation returns 503.
    activate({ subscriptions = [], routes = [] } = {}) {
      if (!current()) {
        return;
      }
      entry.subscriptions = subscriptions;
      entry.routes = buildRouteMap(routes);
      entry.ready = true;
    },
    release({ graceMs = 0 } = {}) {
      if (!current() || entry.state !== 'active') {
        return;
      }
      entry.state = 'grace';
      entry.ready = false;
      drainInflight(entry);
      if (graceMs > 0) {
        entry.graceTimer = setTimeout(() => closeEntry(entry), graceMs);
      } else {
        closeEntry(entry);
      }
    },
    isClosed() {
      return entry.state === 'closed';
    },
    whenClosed() {
      return entry.closedPromise;
    },
  };
}

// Acquire the app-channel listener for one Dapr connection. Reuses a listener
// still in its release grace window (redeploy), rejects a genuinely concurrent
// second owner of the same bind:port, and rebinds a fresh listener once a
// closing one has fully released the port.
async function acquireListener({ bindAddress, port, token, limits }) {
  const key = keyOf(bindAddress, port);
  const existing = registry.get(key);

  if (existing) {
    if (existing.state === 'active' || existing.state === 'starting') {
      throw new DaprError(ErrorCodes.DUPLICATE_LISTENER, `app-channel ${key} is already in use`);
    }
    if (existing.state === 'closing') {
      await existing.closedPromise; // wait for the port to free, then rebind fresh
      return acquireListener({ bindAddress, port, token, limits });
    }
    // grace → reacquire: cancel the pending close, adopt the new config, bump the
    // generation (so the previous lease can no longer act), and require an
    // activate() for this generation before it serves again.
    if (existing.graceTimer) {
      clearTimeout(existing.graceTimer);
      existing.graceTimer = null;
    }
    existing.state = 'active';
    existing.generation += 1;
    existing.ready = false;
    existing.token = token;
    existing.limits = limits;
    return makeLease(existing);
  }

  // Reserve the key BEFORE binding so a concurrent acquire sees 'starting'.
  const entry = {
    key,
    bindAddress,
    port,
    state: 'starting',
    generation: 1,
    ready: false,
    token,
    limits,
    subscriptions: [],
    routes: new Map(),
    sockets: new Set(),
    inflightReqs: new Set(),
    inFlight: 0,
    server: null,
    graceTimer: null,
    drainTimer: null,
    closedPromise: null,
    resolveClosed: null,
  };
  entry.closedPromise = new Promise((resolve) => {
    entry.resolveClosed = resolve;
  });
  registry.set(key, entry);
  entry.server = createServer(entry);

  try {
    await new Promise((resolve, reject) => {
      const onError = (err) => reject(err);
      entry.server.once('error', onError);
      entry.server.listen(port, bindAddress, () => {
        entry.server.removeListener('error', onError);
        resolve();
      });
    });
  } catch (err) {
    registry.delete(key); // failed to bind → free the key for a retry
    throw err;
  }

  entry.state = 'active';
  return makeLease(entry);
}

module.exports = { acquireListener };
