'use strict';

const http = require('node:http');

const { resolveOptions } = require('../lib/options');
const { acquireListener } = require('../lib/app-channel');
const { DaprError, ErrorCodes } = require('../lib/errors');
const { fingerprint, discoveryEntry } = require('../lib/subscriptions');
const { PendingRegistry } = require('../lib/pending');

const HEALTHY_INTERVAL_MS = 10000;
const INITIAL_BACKOFF_MS = 1000;
const MAX_BACKOFF_MS = 30000;
const HEALTH_TIMEOUT_MS = 2000;

// Minimal GET resolving true on a 2xx response, false otherwise. Non-pooled and
// time-bounded so a down sidecar never hangs or lingers the status poll. Sends
// the given headers (e.g. the Dapr API token) so a token-secured sidecar does
// not reject the probe with 401.
function probe(url, timeoutMs, headers) {
  return new Promise((resolve) => {
    let settled = false;
    const finish = (value) => {
      if (settled) {
        return;
      }
      settled = true;
      clearTimeout(timer);
      resolve(value);
    };
    const u = new URL(url);
    const req = http.request(
      {
        hostname: u.hostname,
        port: u.port,
        path: u.pathname,
        method: 'GET',
        agent: false,
        headers,
      },
      (res) => {
        res.resume();
        finish(res.statusCode >= 200 && res.statusCode < 300);
      }
    );
    const timer = setTimeout(
      () => {
        req.destroy();
        finish(false);
      },
      Math.max(1, timeoutMs)
    );
    req.on('error', () => finish(false));
    req.end();
  });
}

module.exports = function registerDaprConnection(RED) {
  function DaprConnectionNode(config) {
    RED.nodes.createNode(this, config);
    const node = this;
    node.lease = null;

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

    // The base URL is validated in resolveOptions, so this is always parseable.
    const healthUrl = `${options.outbound.baseUrl}/v1.0/healthz/outbound`;
    // Dapr requires the API token on API requests when token auth is enabled;
    // send it on the health probe so a token-secured sidecar is not seen as down.
    const healthHeaders = options.daprApiToken
      ? { 'dapr-api-token': options.daprApiToken }
      : undefined;
    let pollTimer = null;
    let backoff = INITIAL_BACKOFF_MS;
    let stopped = false;
    let healthy = false;
    const healthListeners = new Set();

    node.isSidecarHealthy = () => healthy;
    node.onSidecarHealth = (listener) => {
      healthListeners.add(listener);
      listener(healthy);
      return () => healthListeners.delete(listener);
    };
    const setHealthy = (next) => {
      if (healthy !== next) {
        healthy = next;
        for (const listener of healthListeners) {
          listener(healthy);
        }
      }
    };

    // ---- Subscription hub: dapr-subscribe and dapr-ack coordinate here. ----
    const subscriptions = new Map(); // nodeId -> { definition, handler }
    const pendingAcks = new PendingRegistry({ max: options.limits.maxPending });
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
      // A restart is required whenever daprd has fetched a set that differs from
      // the current one — including removing the last subscription (empty set).
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
      node.lease.activate({
        subscriptions: defs.map(discoveryEntry),
        routes: defs.map((def) => ({
          method: 'POST',
          path: def.route,
          kind: 'internal',
          handler: subscriptions.get(def.nodeId).handler,
        })),
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
      let nextHealthy = false;
      try {
        nextHealthy = await probe(healthUrl, HEALTH_TIMEOUT_MS, healthHeaders);
      } catch {
        nextHealthy = false;
      }
      if (stopped) {
        return;
      }
      setHealthy(nextHealthy);
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

    // acquireListener is async; if the node is torn down before it resolves,
    // release the lease immediately so a redeploy race cannot leak a listener.
    acquireListener({
      bindAddress: options.inbound.bindAddress,
      port: options.inbound.port,
      token: options.inbound.appApiToken,
      limits: options.limits,
    })
      .then((lease) => {
        if (closing) {
          lease.release({ graceMs: 0 });
          return;
        }
        node.lease = lease;
        // Activate with whatever subscribe nodes have registered so far; further
        // registrations re-activate through scheduleActivation.
        applyActivation();
        pollOnce();
      })
      .catch((err) => {
        const text =
          err.code === ErrorCodes.DUPLICATE_LISTENER ? 'app port in use' : 'listen failed';
        node.status({ fill: 'red', shape: 'ring', text });
        node.error(err.message);
      });

    node.on('close', async (removed, done) => {
      closing = true;
      stopped = true;
      setHealthy(false);
      healthListeners.clear();
      if (pollTimer) {
        clearTimeout(pollTimer);
      }
      // Resolve in-flight explicit acks as RETRY and let their handlers write the
      // 200 { RETRY } response (one tick) before the listener is released — so a
      // pending delivery completes as RETRY, not the 503 release backstop.
      if (pendingAcks.drain('RETRY') > 0) {
        await new Promise((resolve) => setImmediate(resolve));
      }
      if (node.lease) {
        // On a redeploy (removed === false) hold the listener through a short
        // grace window so the replacement node reacquires it; on delete keep
        // nothing.
        node.lease.release({ graceMs: removed ? 0 : options.limits.leaseGraceMs });
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
