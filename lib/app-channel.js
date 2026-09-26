'use strict';

const http = require('node:http');
const crypto = require('node:crypto');

const { DaprError, ErrorCodes } = require('./errors');
const { sanitizeResponseHeaders } = require('./http-headers');
const { validateActorSegment } = require('./actor-messages');

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

// Fixed, safe responses — never echo error internals, tokens, or config. Headers
// are sanitized (framing/hop-by-hop dropped, illegal names/values rejected) so a
// handler-supplied header can never corrupt the response framing or throw from
// writeHead; on any residual write failure the socket is destroyed rather than
// left half-open.
function writeResponse(res, status, headers, body) {
  const h = sanitizeResponseHeaders(headers);
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
  try {
    res.writeHead(status, h);
    res.end(payload);
  } catch {
    res.destroy();
  }
}

const sendStatus = (res, status, headers) => writeResponse(res, status, headers, undefined);
const sendJson = (res, status, obj) => writeResponse(res, status, {}, obj);
const retryable = (res) => sendStatus(res, 503, { 'Retry-After': '1' });

// Read the request body, capped at `limit` bytes. Rejects with a BODY_TOO_LARGE
// error the moment the cap is exceeded so an oversized upload is not buffered.
function readBody(req, limit) {
  // Bridges three independent 'data'/'end'/'error' events into one settlement;
  // no single event/promise API captures all three.
  return /* allow-promise: data/end/error settlement */ new Promise((resolve, reject) => {
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

// Parse the RAW request pathname (never `url.pathname`, which resolves
// %2e%2e/.. dot segments before any validator sees them -- a segment could
// disappear into a different, unintended shape instead of being rejected) for
// one of the two actor route shapes. Every segment is decoded exactly once
// and validated before use; a decode failure, an empty segment, a decoded
// segment containing "/", or one failing validateActorSegment is a 400 (the
// segment itself is malformed) — a pathname that decodes cleanly but matches
// neither known shape is a 404 (there is simply no such route).
function parseActorPath(rawPath) {
  const rest = rawPath.split('/').slice(2); // drop leading '' and 'actors'
  if (rest.length === 0) {
    return { error: 404 };
  }
  const decoded = [];
  for (const raw of rest) {
    if (raw === '') {
      return { error: 400 };
    }
    let segment;
    try {
      segment = decodeURIComponent(raw);
    } catch {
      return { error: 400 };
    }
    if (segment.includes('/')) {
      return { error: 400 };
    }
    try {
      validateActorSegment(segment, 'actor path segment');
    } catch {
      return { error: 400 };
    }
    decoded.push(segment);
  }
  if (decoded.length === 2) {
    return { shape: 'instance', type: decoded[0], id: decoded[1] };
  }
  if (decoded.length === 4 && decoded[2] === 'method') {
    return { shape: 'method', type: decoded[0], id: decoded[1], method: decoded[3] };
  }
  return { error: 404 };
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
    sendJson(res, 200, entry.subscriptions);
    // Record what this fetch served (in the module-scoped entry, so it survives
    // a redeploy) and signal the owner to refresh restart state. This records
    // that SOMEONE fetched, not that daprd did — nothing in the request
    // distinguishes daprd's own startup fetch from an operator's curl — so
    // nothing destructive may hang off it. In particular it must not clear
    // entry.staleRoutes; see activate().
    entry.servedFingerprint = entry.activeFingerprint;
    if (entry.onDiscovery) {
      entry.onDiscovery();
    }
    return undefined;
  }

  // Actor config discovery — internal, like /dapr/subscribe above, and only
  // served once an actor method is actually registered; otherwise this falls
  // through to the ordinary route map below exactly as it always did (a plain
  // 404, since no route registers this path), so a connection with no actors
  // sees no behavior change at all.
  if (entry.actorConfig && path === '/dapr/config') {
    if (method !== 'GET') {
      return sendStatus(res, 405, { Allow: 'GET' });
    }
    if (req.headers['dapr-caller-app-id'] !== undefined) {
      return sendJson(res, 403, { error: 'forbidden' });
    }
    sendJson(res, 200, entry.actorConfig);
    entry.servedActorFingerprint = entry.activeActorFingerprint;
    return undefined;
  }

  let route;
  // Only once an actor method is registered does this connection claim the
  // /actors namespace; otherwise a pre-existing service route there (rare,
  // but not reserved — see lib/services.js) keeps matching below exactly as
  // before.
  const rawPath = req.url.split('?')[0];
  if (entry.actorConfig && (rawPath === '/actors' || rawPath.startsWith('/actors/'))) {
    const parsed = parseActorPath(rawPath);
    if (parsed.error) {
      return sendJson(res, parsed.error, {
        error: parsed.error === 400 ? 'bad request' : 'not found',
      });
    }
    if (parsed.shape === 'instance') {
      // DELETE /actors/{type}/{id}: daprd notifying a local deactivation.
      // Internal (rejects dapr-caller-app-id below), touches no state, and
      // needs no admission gate -- it does no work at all.
      if (method !== 'DELETE') {
        return sendStatus(res, 405, { Allow: 'DELETE' });
      }
      route = { kind: 'internal', handler: async () => ({ status: 200 }) };
    } else {
      // PUT /actors/{type}/{id}/method/{method}: an actor method invocation.
      if (method !== 'PUT') {
        return sendStatus(res, 405, { Allow: 'PUT' });
      }
      const emit = entry.getActorHandler(parsed.type, parsed.method);
      if (!emit) {
        return sendJson(res, 404, { error: 'not found' });
      }
      route = {
        kind: 'actor',
        handler: (ctx) =>
          entry.actorInvoke({
            actorType: parsed.type,
            actorId: parsed.id,
            method: parsed.method,
            emit,
            ctx,
          }),
      };
    }
  } else {
    const byMethod = entry.routes.get(path);
    if (!byMethod) {
      // A stale, not-yet-restarted sidecar may still be posting here — see
      // activate(). Retryable, never a 404 (Dapr treats 404 as a permanent DROP).
      if (entry.staleRoutes.has(path)) {
        return retryable(res);
      }
      return sendJson(res, 404, { error: 'not found' });
    }
    route = byMethod.get(method);
    if (!route) {
      return sendStatus(res, 405, { Allow: [...byMethod.keys()].join(', ') });
    }
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
  // cannot double-respond — `finished` guards it. `startMs` is when this
  // deadline is armed, so an actor handler can compute the same absolute
  // deadline (ctx.deadlineAt, below) off the same clock.
  const startMs = Date.now();
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
      deadlineAt: startMs + limits.requestTimeoutMs,
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
    async (req, res) => {
      // The listener's own error boundary: handleRequest must never reject
      // into an unhandled rejection, and a half-written response must still
      // get a bounded, non-leaking body.
      try {
        await handleRequest(entry, req, res);
      } catch {
        if (!res.headersSent) {
          sendJson(res, 500, { error: 'internal error' });
        }
      }
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
    activate({
      subscriptions = [],
      routes = [],
      fingerprint = null,
      onDiscovery = null,
      actorConfig = null,
      actorFingerprint = null,
      getActorHandler = () => undefined,
      actorInvoke = null,
    } = {}) {
      if (!current()) {
        return;
      }
      // A pub/sub delivery ('internal') path that existed before this
      // activation but not after stays registered as a retryable placeholder
      // (see handleRequest) for the rest of this listener's life: a stale,
      // not-yet-restarted sidecar may still be posting to it, and Dapr treats
      // a 404 as a permanent DROP, not a retry. Nothing clears a placeholder,
      // deliberately: a request cannot be attributed to daprd, so any "daprd has
      // caught up" trigger would also fire for an operator's curl and drop that
      // sidecar's next delivery. Keeping them costs a bounded Set of path
      // strings and only ever answers a retryable 503. Service routes are
      // exempt — Dapr never caches those, so removing one should 404 at once.
      const oldInternalPaths = new Set();
      for (const [path, byMethod] of entry.routes) {
        for (const { kind } of byMethod.values()) {
          if (kind === 'internal') {
            oldInternalPaths.add(path);
            break;
          }
        }
      }
      const newInternalPaths = new Set(
        routes.filter((r) => r.kind === 'internal').map((r) => r.path)
      );
      for (const path of oldInternalPaths) {
        if (!newInternalPaths.has(path)) {
          entry.staleRoutes.add(path);
        }
      }
      for (const path of newInternalPaths) {
        entry.staleRoutes.delete(path); // a real route always wins over a stale placeholder
      }
      entry.subscriptions = subscriptions;
      entry.routes = buildRouteMap(routes);
      entry.activeFingerprint = fingerprint;
      entry.onDiscovery = onDiscovery;
      entry.actorConfig = actorConfig;
      entry.activeActorFingerprint = actorFingerprint;
      entry.getActorHandler = getActorHandler;
      entry.actorInvoke = actorInvoke;
      entry.ready = true;
    },
    // The actor fingerprint last SERVED from /dapr/config, or null if nothing
    // has fetched it since this listener started (or no actor method is
    // registered) — mirrors servedFingerprint() below, kept separate because
    // subscriptions and actor config are fetched independently and restart
    // for different reasons.
    servedActorFingerprint() {
      return entry.servedActorFingerprint;
    },
    // The fingerprint last SERVED from /dapr/subscribe, or null if nothing has
    // fetched it since this listener started. Not "what daprd fetched": the
    // request carries nothing that identifies daprd, so any caller that can
    // reach the listener updates this (see the handler). Lives in the
    // module-scoped entry, so it persists across a redeploy's reacquire.
    servedFingerprint() {
      return entry.servedFingerprint;
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
    onDiscovery: null,
    activeFingerprint: null,
    servedFingerprint: null,
    // Actor config (null when no actor method is registered on this
    // connection), its fingerprint, and the resolvers activate() installs —
    // see makeLease.activate() and handleRequest's /actors branch.
    actorConfig: null,
    activeActorFingerprint: null,
    servedActorFingerprint: null,
    getActorHandler: () => undefined,
    actorInvoke: null,
    // Pub/sub delivery paths ('internal' kind) that were registered before the
    // most recent activate() but not after — see activate(). Retained for this
    // listener's whole life; a Node-RED process restart is what resets them.
    staleRoutes: new Set(),
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
  // Exposes a resolver captured for later, unrelated calls (closeEntry) to
  // settle — not a single awaited operation.
  entry.closedPromise = /* allow-promise: resolver captured for closeEntry */ new Promise(
    (resolve) => {
      entry.resolveClosed = resolve;
    }
  );
  registry.set(key, entry);
  entry.server = createServer(entry);

  try {
    // Bridges the server's one-shot 'error'/listen-callback pair, which
    // node:http exposes as neither a promise nor a single event.
    await /* allow-promise: error/listen-callback bridge */ new Promise((resolve, reject) => {
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
