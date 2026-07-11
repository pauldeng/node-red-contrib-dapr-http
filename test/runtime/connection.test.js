'use strict';

const { test } = require('node:test');
const assert = require('node:assert/strict');

const { NodeRed, freePort } = require('../helpers/node-red');
const { createFakeDaprStarted } = require('../helpers/fake-dapr');
const { httpRequest } = require('../helpers/http');

// Poll an arbitrary predicate until it returns truthy or the deadline passes.
async function waitFor(fn, { timeoutMs = 15000, intervalMs = 150 } = {}) {
  const deadline = Date.now() + timeoutMs;
  let last;
  while (Date.now() < deadline) {
    try {
      const value = await fn();
      if (value) {
        return value;
      }
      last = value;
    } catch (err) {
      last = err;
    }
    await new Promise((r) => setTimeout(r, intervalMs));
  }
  throw new Error(`waitFor timed out (${last instanceof Error ? last.message : last})`);
}

const healthzOk = (port) => async () => {
  const r = await httpRequest(`http://127.0.0.1:${port}/healthz`, { timeoutMs: 1000 });
  return r.status === 204 ? r : null;
};

function connectionFlow({ appPort, daprPort }) {
  return [
    {
      id: 'c1',
      type: 'dapr-connection',
      name: 'sidecar',
      daprHost: '127.0.0.1',
      daprPort: String(daprPort),
      bindAddress: '127.0.0.1',
      appPort: String(appPort),
    },
  ];
}

test(
  'brings up the app-channel listener and probes sidecar health',
  { timeout: 60000 },
  async (t) => {
    const dapr = await createFakeDaprStarted();
    t.after(() => dapr.stop());
    dapr.respond('GET', '/v1.0/healthz/outbound', (req, res) => {
      res.writeHead(204).end();
    });

    const appPort = await freePort();
    const nr = new NodeRed();
    await nr.start();
    t.after(() => nr.stop());

    await nr.deploy(connectionFlow({ appPort, daprPort: dapr.port }));

    const health = await waitFor(healthzOk(appPort));
    assert.equal(health.status, 204);

    await waitFor(() => dapr.requests.some((r) => r.path === '/v1.0/healthz/outbound') || null);
  }
);

test(
  'reports a duplicate app-channel port without crashing node-red',
  { timeout: 60000 },
  async (t) => {
    const appPort = await freePort();
    const nr = new NodeRed();
    await nr.start();
    t.after(() => nr.stop());

    await nr.deploy([
      {
        id: 'c1',
        type: 'dapr-connection',
        name: 'a',
        bindAddress: '127.0.0.1',
        appPort: String(appPort),
      },
      {
        id: 'c2',
        type: 'dapr-connection',
        name: 'b',
        bindAddress: '127.0.0.1',
        appPort: String(appPort),
      },
    ]);

    // Node-RED stays healthy and the first listener serves the port.
    assert.equal((await httpRequest(nr.adminUrl('/settings'), { timeoutMs: 2000 })).status, 200);
    await waitFor(healthzOk(appPort));
    // The second node reports the conflict rather than crashing.
    await waitFor(() => (/in use/i.test(nr.logText()) ? true : null));
  }
);

test('the app-channel listener survives a full redeploy', { timeout: 60000 }, async (t) => {
  const appPort = await freePort();
  const nr = new NodeRed();
  await nr.start();
  t.after(() => nr.stop());

  await nr.deploy(connectionFlow({ appPort, daprPort: 3500 }));
  await waitFor(healthzOk(appPort));

  await nr.deploy(connectionFlow({ appPort, daprPort: 3500 }));
  const health = await waitFor(healthzOk(appPort));
  assert.equal(health.status, 204);
});

test('shutting down node-red closes the app-channel listener', { timeout: 60000 }, async () => {
  const appPort = await freePort();
  const nr = new NodeRed();
  await nr.start();

  await nr.deploy(connectionFlow({ appPort, daprPort: 3500 }));
  await waitFor(healthzOk(appPort));

  await nr.stop();
  await assert.rejects(httpRequest(`http://127.0.0.1:${appPort}/healthz`, { timeoutMs: 1000 }));
});

test(
  'a malformed sidecar endpoint is reported without crashing node-red',
  { timeout: 60000 },
  async (t) => {
    const appPort = await freePort();
    const nr = new NodeRed();
    await nr.start({ env: { DAPR_HTTP_ENDPOINT: 'not-a-url' } });
    t.after(() => nr.stop());

    await nr.deploy([
      {
        id: 'c1',
        type: 'dapr-connection',
        name: 'bad-endpoint',
        bindAddress: '127.0.0.1',
        appPort: String(appPort),
      },
    ]);

    // Give any (mishandled) async health poll a chance to crash the process.
    await new Promise((r) => setTimeout(r, 800));
    assert.equal((await httpRequest(nr.adminUrl('/settings'), { timeoutMs: 2000 })).status, 200);
    await waitFor(() => (/valid URL/i.test(nr.logText()) ? true : null));
  }
);

test('the health probe carries the configured Dapr API token', { timeout: 60000 }, async (t) => {
  const token = 'secret-token';
  const dapr = await createFakeDaprStarted();
  t.after(() => dapr.stop());
  // Token-secured sidecar: 204 only when the API token header is present.
  dapr.respond('GET', '/v1.0/healthz/outbound', (req, res) => {
    res.writeHead(req.headers['dapr-api-token'] === token ? 204 : 401).end();
  });

  const appPort = await freePort();
  const nr = new NodeRed();
  await nr.start({ env: { DAPR_API_TOKEN: token } });
  t.after(() => nr.stop());

  await nr.deploy(connectionFlow({ appPort, daprPort: dapr.port }));
  await waitFor(healthzOk(appPort));

  // The probe must reach the sidecar carrying the token, so a token-secured
  // sidecar accepts it instead of replying 401.
  await waitFor(() => {
    const probe = dapr.requests.find((r) => r.path === '/v1.0/healthz/outbound');
    return probe && probe.headers['dapr-api-token'] === token ? probe : null;
  });
});
