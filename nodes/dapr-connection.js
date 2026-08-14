'use strict';

const { resolveOptions } = require('../lib/options');
const { acquireListener } = require('../lib/app-channel');
const { DaprError, ErrorCodes } = require('../lib/errors');
const { fingerprint, discoveryEntry } = require('../lib/subscriptions');
const { PendingRegistry } = require('../lib/pending');
const { sidecarRequest } = require('../lib/sidecar-http');
const { getMetadata } = require('../lib/metadata-client');
const telemetry = require('../lib/telemetry');

// Fixed, generic text per error code -- never the sidecar's own response
// body or a raw Node.js error message, mirroring this package's own "never
// return stack traces... over HTTP" invariant for the app channel, applied
// here to the admin side.
const METADATA_ERROR_MESSAGE = {
  [ErrorCodes.SIDECAR_UNAVAILABLE]: 'could not reach the Dapr sidecar',
  [ErrorCodes.METADATA_OPERATION_FAILED]: 'the Dapr sidecar could not provide metadata',
  [ErrorCodes.RESPONSE_TOO_LARGE]: 'the sidecar response was too large',
};

const HEALTH_PATH = '/v1.0/healthz/outbound';
const HEALTHY_INTERVAL_MS = 10000;
const INITIAL_BACKOFF_MS = 1000;
const MAX_BACKOFF_MS = 30000;
const HEALTH_TIMEOUT_MS = 2000;

module.exports = function registerDaprConnection(RED) {
  // Once, at node-type registration — not per connection instance. Gives
  // every node in every flow a span the moment any connection enables
  // tracing; a no-op span (nothing has) costs nothing and needs no guard.
  telemetry.registerFlowSpanHooks(RED.hooks);

  // "Test Connection" calls GET /v1.0/metadata against the DEPLOYED
  // connection (never the
  // still-open dialog's unsaved fields, which sidesteps re-implementing
  // credential-masking for a token the editor may only show as a
  // placeholder). Reuses the connection's own already-resolved
  // baseUrl/token/timeout/body-limit -- the same options object the health
  // poll already uses -- rather than inventing a second resolution path.
  RED.httpAdmin.get(
    '/dapr-connection/:id/metadata',
    RED.auth.needsPermission('dapr-connection.read'),
    async (req, res) => {
      res.set('Cache-Control', 'no-store');
      const node = RED.nodes.getNode(req.params.id);
      if (!node || node.type !== 'dapr-connection') {
        res.status(404).json({ ok: false, message: 'connection not found — deploy it first' });
        return;
      }
      if (!node.options) {
        res.status(409).json({ ok: false, message: 'connection configuration is invalid' });
        return;
      }

      const controller = new AbortController();
      node.metadataRequests.add(controller);
      const abortIfOpen = () => {
        if (!res.writableEnded) {
          controller.abort();
        }
      };
      res.on('close', abortIfOpen);
      try {
        const result = await getMetadata({
          baseUrl: node.options.outbound.baseUrl,
          token: node.options.daprApiToken,
          timeoutMs: node.options.limits.requestTimeoutMs,
          signal: controller.signal,
          maxResponseBytes: node.options.limits.bodyLimitBytes,
        });
        if (!res.destroyed) {
          res.json({ ok: true, ...result.data });
        }
      } catch (err) {
        if (!res.destroyed) {
          res.status(502).json({
            ok: false,
            message: METADATA_ERROR_MESSAGE[err.code] || 'metadata request failed',
          });
        }
      } finally {
        res.off('close', abortIfOpen);
        node.metadataRequests.delete(controller);
      }
    }
  );

  function DaprConnectionNode(config) {
    RED.nodes.createNode(this, config);
    const node = this;
    node.lease = null;
    node.metadataRequests = new Set();
    node.tracingHandle = null;

    let options;
    try {
      options = resolveOptions({ config, credentials: node.credentials || {}, env: process.env });
    } catch (err) {
      node.options = null;
      node.status({ fill: 'red', shape: 'ring', text: 'invalid config' });
      node.error(err.message);
      return;
    }
    node.options = options;

    // An unset app API token means the app channel enforces no authentication at
    // all (lib/app-channel.js). resolveOptions already refuses a non-loopback
    // bind without one; on loopback it is allowed but never silent, because
    // anything sharing this host/namespace can then post deliveries into flows.
    if (!options.inbound.appApiToken) {
      node.warn(
        `no app API token configured: the Dapr app channel on ${options.inbound.bindAddress}:${options.inbound.port} accepts unauthenticated requests from anything that can reach it — set the App API token credential or APP_API_TOKEN`
      );
    }

    // For operator-facing log lines only — the request itself is built from
    // baseUrl + HEALTH_PATH below.
    const healthUrl = `${options.outbound.baseUrl}${HEALTH_PATH}`;
    // Dapr requires the API token on API requests when token auth is enabled;
    // send it on the health probe so a token-secured sidecar is not seen as down.
    const healthHeaders = options.daprApiToken
      ? { 'dapr-api-token': options.daprApiToken }
      : undefined;

    // Resolves true only on a clear 2xx. Goes through the one outbound path
    // (lib/sidecar-http.js) like every other sidecar call, but opts out of the
    // keep-alive pool: a sidecar that is going down must not leave a pooled
    // socket behind for the next poll to inherit.
    const probeHealth = async () => {
      try {
        const res = await sidecarRequest(options.outbound.baseUrl, {
          path: HEALTH_PATH,
          headers: healthHeaders,
          timeoutMs: HEALTH_TIMEOUT_MS,
          maxResponseBytes: options.limits.bodyLimitBytes,
          agent: false,
        });
        return res.status >= 200 && res.status < 300;
      } catch {
        return false;
      }
    };
    let pollTimer = null;
    let backoff = INITIAL_BACKOFF_MS;
    let stopped = false;
    let healthy = false;
    let healthReported = false;
    const healthListeners = new Set();

    // Resolves once the first health probe has settled (or the listener failed to
    // start). Until then `healthy` is only a default, not an observation — a
    // message that arrives in that window must not be failed as "sidecar down"
    // when the sidecar is in fact up.
    let markHealthKnown;
    const healthKnown = new Promise((resolve) => {
      markHealthKnown = resolve;
    });

    node.isSidecarHealthy = () => healthy;
    node.whenHealthKnown = () => healthKnown;
    node.onSidecarHealth = (listener) => {
      healthListeners.add(listener);
      listener(healthy);
      return () => healthListeners.delete(listener);
    };
    // Health state drives node status AND one log line per transition. Status
    // alone is invisible to a headless deployment: without this, a sidecar that
    // goes down while no messages happen to flow leaves no trace at all. Logging
    // on transition (and on the first observation, so a sidecar that is already
    // down at deploy is reported too) cannot spam the way per-probe logging would.
    const setHealthy = (next) => {
      const changed = healthy !== next;
      healthy = next;
      if (!stopped && (changed || !healthReported)) {
        healthReported = true;
        if (healthy) {
          node.log('Dapr sidecar is available');
        } else {
          node.warn(
            `Dapr sidecar is unavailable (${healthUrl}); publishes and invocations fail until it recovers`
          );
        }
      }
      if (changed) {
        for (const listener of healthListeners) {
          listener(healthy);
        }
      }
    };

    // ---- Subscription + service hub: subscribe/ack/service/response nodes
    // coordinate here. ----
    const subscriptions = new Map(); // nodeId -> { definition, handler }
    const services = new Map(); // nodeId -> { definition, handler }
    const internalRoutes = new Map(); // nodeId -> { definition, handler }
    const pendingAcks = new PendingRegistry({ max: options.limits.maxPending });
    const pendingResponses = new PendingRegistry({ max: options.limits.maxPending });
    let desiredFingerprint = fingerprint([]);
    let activateScheduled = false;
    let warnedFingerprint = null; // rate-limits the restart warning to once per change

    const refreshStatus = () => {
      if (!node.lease) {
        node.status({ fill: 'grey', shape: 'ring', text: 'connecting' });
        return;
      }
      if (!healthy) {
        node.status({ fill: 'red', shape: 'ring', text: 'sidecar unavailable' });
        return;
      }
      const served = node.lease.servedFingerprint();
      // A restart is required whenever the set last SERVED from /dapr/subscribe
      // differs from the current one — including removing the last subscription
      // (empty set). "Served", not "fetched by daprd": the app channel cannot
      // attribute a fetch to a caller, so a non-daprd fetch (an operator's curl,
      // a probe) clears this warning too, while daprd stays stale. That is a
      // documented operator footgun, not something this status can detect — see
      // lib/app-channel.js and docs/subscriptions.md.
      if (served !== null && served !== desiredFingerprint) {
        if (warnedFingerprint !== desiredFingerprint) {
          warnedFingerprint = desiredFingerprint;
          node.warn(
            'subscription definitions changed; restart the Dapr sidecar so it re-reads /dapr/subscribe'
          );
        }
        node.status({
          fill: 'yellow',
          shape: 'ring',
          text: 'restart sidecar: subscriptions changed',
        });
        return;
      }
      warnedFingerprint = null;
      if (served === null && subscriptions.size > 0) {
        node.status({ fill: 'yellow', shape: 'ring', text: 'waiting for sidecar discovery' });
        return;
      }
      node.status({ fill: 'green', shape: 'dot', text: 'connected' });
    };

    // Atomically re-activate the app channel with the aggregated subscription
    // set and delivery routes, and fingerprint it for restart detection.
    const applyActivation = () => {
      if (!node.lease) {
        return;
      }
      const defs = [...subscriptions.values()].map((entry) => entry.definition);
      desiredFingerprint = fingerprint(defs);
      // One app-channel route per subscription route (each CEL rule plus the
      // default/fallback), all sharing the subscription's single handler — the
      // handler distinguishes which route matched from ctx.path.
      const deliveryRoutes = [...subscriptions.values()].flatMap(({ definition, handler }) =>
        definition.routes.map((route) => ({
          method: 'POST',
          path: route.path,
          kind: 'internal',
          handler,
        }))
      );
      // Service methods are app-channel routes but not part of the Dapr
      // subscription set, so they are neither advertised nor fingerprinted.
      const serviceRoutes = [...services.values()].map(({ definition, handler }) => ({
        method: definition.verb,
        path: definition.path,
        kind: 'service',
        handler,
      }));
      const dynamicInternalRoutes = [...internalRoutes.values()].map(({ definition, handler }) => ({
        method: definition.verb,
        path: definition.path,
        kind: 'internal',
        handler,
      }));
      node.lease.activate({
        subscriptions: defs.map(discoveryEntry),
        routes: [...deliveryRoutes, ...serviceRoutes, ...dynamicInternalRoutes],
        fingerprint: desiredFingerprint,
        onDiscovery: refreshStatus,
      });
      refreshStatus();
    };

    // Coalesce the synchronous burst of subscribe-node registrations in one
    // deploy into a single activation.
    const scheduleActivation = () => {
      if (activateScheduled) {
        return;
      }
      activateScheduled = true;
      setImmediate(() => {
        activateScheduled = false;
        applyActivation();
      });
    };

    node.registerSubscription = (definition, handler) => {
      for (const { definition: existing } of subscriptions.values()) {
        if (
          existing.nodeId !== definition.nodeId &&
          existing.pubsubName === definition.pubsubName &&
          existing.topic === definition.topic
        ) {
          throw new DaprError(
            ErrorCodes.INVALID_OPTIONS,
            `duplicate subscription for ${definition.pubsubName}/${definition.topic}`
          );
        }
      }
      subscriptions.set(definition.nodeId, { definition, handler });
      scheduleActivation();
      return () => {
        subscriptions.delete(definition.nodeId);
        scheduleActivation();
      };
    };
    node.addPendingAck = (ackId, ackOptions) => pendingAcks.add(ackId, ackOptions);
    node.settleAck = (ackId, status) => pendingAcks.settle(ackId, status);

    const registerRoute = (registry, definition, handler, label) => {
      for (const { definition: existing } of [...services.values(), ...internalRoutes.values()]) {
        if (
          existing.nodeId !== definition.nodeId &&
          existing.verb === definition.verb &&
          existing.path === definition.path
        ) {
          throw new DaprError(
            ErrorCodes.INVALID_OPTIONS,
            `duplicate ${label} ${definition.verb} ${definition.path}`
          );
        }
      }
      registry.set(definition.nodeId, { definition, handler });
      scheduleActivation();
      return () => {
        registry.delete(definition.nodeId);
        scheduleActivation();
      };
    };
    node.registerService = (definition, handler) =>
      registerRoute(services, definition, handler, 'service method');
    node.registerInternalRoute = (definition, handler) =>
      registerRoute(internalRoutes, definition, handler, 'internal route');
    node.addPendingResponse = (id, responseOptions) => pendingResponses.add(id, responseOptions);
    node.settleResponse = (id, response) => pendingResponses.settle(id, response);

    const schedule = (ms) => {
      pollTimer = setTimeout(pollOnce, ms);
    };

    // Fail closed: any probe outcome other than a clear 2xx — including an
    // unexpected error — reports "sidecar unavailable" and reschedules with
    // backoff. Health polling must never throw and crash the flow.
    async function pollOnce() {
      if (stopped) {
        return;
      }
      // No initial value: both branches below assign one, and a dead initializer
      // hides which path actually set it. probeHealth() already resolves false on
      // every failure, so the catch is defence in depth for this must-never-throw
      // path rather than the expected route.
      let nextHealthy;
      try {
        nextHealthy = await probeHealth();
      } catch {
        nextHealthy = false;
      }
      if (stopped) {
        markHealthKnown();
        return;
      }
      setHealthy(nextHealthy);
      markHealthKnown();
      if (healthy) {
        backoff = INITIAL_BACKOFF_MS;
        schedule(HEALTHY_INTERVAL_MS);
      } else {
        // Do not node.error on every failed probe (log spam) — status only.
        schedule(backoff);
        backoff = Math.min(backoff * 2, MAX_BACKOFF_MS);
      }
      refreshStatus();
    }

    let closing = false;
    node.status({ fill: 'grey', shape: 'ring', text: 'connecting' });

    // Opt-in, disabled by default. telemetry.acquire() is async; mirrors the
    // acquireListener pattern below — if the node closes before it resolves,
    // release immediately rather than leaking a registered provider. Fails
    // open: a setup error only warns, it never fails the connection.
    if (config.tracingEnabled) {
      const enableTracing = async () => {
        try {
          const handle = await telemetry.acquire();
          if (closing) {
            await handle.release();
            return;
          }
          node.tracingHandle = handle;
        } catch (err) {
          node.warn(`tracing could not be enabled: ${err.message}`);
        }
      };
      void enableTracing();
    }

    // acquireListener is async; if the node is torn down before it resolves,
    // release the lease immediately so a redeploy race cannot leak a listener.
    const startListener = async () => {
      try {
        const lease = await acquireListener({
          bindAddress: options.inbound.bindAddress,
          port: options.inbound.port,
          token: options.inbound.appApiToken,
          limits: options.limits,
        });
        if (closing) {
          await lease.release({ graceMs: 0 });
          return;
        }
        node.lease = lease;
        // Activate with whatever subscribe nodes have registered so far; further
        // registrations re-activate through scheduleActivation.
        applyActivation();
        pollOnce();
      } catch (err) {
        const text =
          err.code === ErrorCodes.DUPLICATE_LISTENER ? 'app port in use' : 'listen failed';
        node.status({ fill: 'red', shape: 'ring', text });
        node.error(err.message);
        // No listener means no health poll will ever run — release anything
        // waiting on the first probe instead of hanging it forever.
        markHealthKnown();
      }
    };
    void startListener();

    node.on('close', async (removed, done) => {
      closing = true;
      stopped = true;
      setHealthy(false);
      healthListeners.clear();
      markHealthKnown(); // never leave an input handler awaiting a probe that will not run
      if (pollTimer) {
        clearTimeout(pollTimer);
      }
      for (const controller of node.metadataRequests) {
        controller.abort();
      }
      node.metadataRequests.clear();
      // Resolve in-flight explicit acks as RETRY and pending service requests as
      // 503, letting their handlers write the response (one tick) before the
      // listener is released — so a pending delivery/request completes cleanly
      // rather than hitting the release backstop.
      const drained =
        pendingAcks.drain('RETRY') +
        pendingResponses.drain({ status: 503, headers: {}, body: 'connection restarting' });
      if (drained > 0) {
        await new Promise((resolve) => setImmediate(resolve));
      }
      if (node.lease) {
        // On a redeploy (removed === false) hold the listener through a short
        // grace window so the replacement node reacquires it; on delete keep
        // nothing.
        node.lease.release({ graceMs: removed ? 0 : options.limits.leaseGraceMs });
        // Drop the reference with the lease: a subscribe/service node closing
        // after us must not reach applyActivation() and re-activate a released
        // generation.
        node.lease = null;
      }
      if (node.tracingHandle) {
        await node.tracingHandle.release();
        node.tracingHandle = null;
      }
      done();
    });
  }

  RED.nodes.registerType('dapr-connection', DaprConnectionNode, {
    credentials: {
      daprApiToken: { type: 'password' },
      appApiToken: { type: 'password' },
    },
  });
};
