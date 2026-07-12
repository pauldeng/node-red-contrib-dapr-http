'use strict';

const http = require('node:http');

const { resolveOptions } = require('../lib/options');
const { acquireListener } = require('../lib/app-channel');
const { ErrorCodes } = require('../lib/errors');

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
        node.status({ fill: 'green', shape: 'dot', text: 'connected' });
        backoff = INITIAL_BACKOFF_MS;
        schedule(HEALTHY_INTERVAL_MS);
      } else {
        // Status only — do not node.error on every failed probe (log spam).
        node.status({ fill: 'red', shape: 'ring', text: 'sidecar unavailable' });
        schedule(backoff);
        backoff = Math.min(backoff * 2, MAX_BACKOFF_MS);
      }
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
        // Activate the generation. This milestone has no subscribe/service
        // nodes, so the app channel serves discovery (empty) and health only;
        // later milestones re-activate with real subscriptions and routes.
        lease.activate({ subscriptions: [], routes: [] });
        pollOnce();
      })
      .catch((err) => {
        const text =
          err.code === ErrorCodes.DUPLICATE_LISTENER ? 'app port in use' : 'listen failed';
        node.status({ fill: 'red', shape: 'ring', text });
        node.error(err.message);
      });

    node.on('close', (removed, done) => {
      closing = true;
      stopped = true;
      setHealthy(false);
      healthListeners.clear();
      if (pollTimer) {
        clearTimeout(pollTimer);
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
