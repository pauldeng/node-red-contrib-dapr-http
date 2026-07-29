'use strict';

// Confirms the app-channel isolation invariant (AGENTS.md: "Never attach Dapr
// routes to RED.httpAdmin, RED.httpNode, or the editor port") holds in the
// full real-Dapr environment, not just against the runtime tier's fake
// sidecar — the same assertion, run once with a real daprd/Node-RED pair
// actually wired together.

const { test } = require('node:test');
const assert = require('node:assert/strict');

const { freePort } = require('../helpers/node-red');
const { ContainerNodeRed } = require('../helpers/node-red-container');
const { httpRequest } = require('../helpers/http');
const { startRedis, startDaprd } = require('../helpers/integration');
const { waitFor } = require('../helpers/wait-for');

test(
  'the app-channel port stays isolated from the Admin API, editor assets, and arbitrary HTTP In endpoints',
  { timeout: 60000 },
  async (t) => {
    const appId = 'it-admin-isolation';
    const appPort = await freePort();
    const daprHttpPort = await freePort();

    const nr = new ContainerNodeRed();
    t.after(() => nr.stop());
    await nr.start({
      flows: [
        { id: 'tab', type: 'tab', label: 'it-admin-isolation' },
        {
          id: 'c1',
          type: 'dapr-connection',
          daprHost: '127.0.0.1',
          daprPort: String(daprHttpPort),
          bindAddress: '127.0.0.1',
          appPort: String(appPort),
        },
        {
          id: 'svc',
          type: 'dapr-service',
          z: 'tab',
          connection: 'c1',
          verb: 'POST',
          methodPath: '/orders',
          wires: [['resp']],
        },
        {
          id: 'resp',
          type: 'dapr-response',
          z: 'tab',
          connection: 'c1',
          statusCode: '200',
          wires: [],
        },
        // An ordinary HTTP In endpoint, deployed on the SAME Node-RED process
        // (bound to httpNodeRoot on the editor/admin port) — this must not be
        // reachable through the separate app-channel port either.
        {
          id: 'httpin',
          type: 'http in',
          z: 'tab',
          url: '/arbitrary',
          method: 'get',
          wires: [['httpres']],
        },
        { id: 'httpres', type: 'http response', z: 'tab' },
      ],
    });
    await waitFor(async () => {
      const r = await httpRequest(`http://127.0.0.1:${appPort}/healthz`, { timeoutMs: 1000 });
      return r.status === 204 ? true : null;
    });

    const redis = await startRedis();
    t.after(() => redis.stop());
    const daprd = await startDaprd({
      appId,
      appPort,
      redisPort: redis.port,
      httpPort: daprHttpPort,
    });
    t.after(() => daprd.stop());

    // Baseline: the app-channel port itself is genuinely live and serving a
    // registered service method, through real daprd.
    const invoked = await waitFor(async () => {
      const r = await httpRequest(`${daprd.baseUrl}/v1.0/invoke/${appId}/method/orders`, {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: '{}',
        timeoutMs: 4000,
      });
      return r.status === 200 ? r : null;
    });
    assert.equal(invoked.status, 200);

    const appChannel = (p) => `http://127.0.0.1:${appPort}${p}`;
    assert.equal((await httpRequest(appChannel('/flows'), { timeoutMs: 2000 })).status, 404);
    assert.equal((await httpRequest(appChannel('/settings'), { timeoutMs: 2000 })).status, 404);
    assert.equal((await httpRequest(appChannel('/'), { timeoutMs: 2000 })).status, 404); // editor index
    assert.equal((await httpRequest(appChannel('/arbitrary'), { timeoutMs: 2000 })).status, 404);

    // The editor/admin origin, on the other hand, really does serve all of
    // these — confirming the isolation is specific to the app-channel port,
    // not a broken deploy.
    assert.equal((await httpRequest(nr.adminUrl('/flows'), { timeoutMs: 2000 })).status, 200);
    assert.equal((await httpRequest(nr.adminUrl('/settings'), { timeoutMs: 2000 })).status, 200);
    assert.equal((await httpRequest(nr.nodeUrl('/arbitrary'), { timeoutMs: 2000 })).status, 200);
  }
);
