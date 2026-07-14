'use strict';

const { test } = require('node:test');
const assert = require('node:assert/strict');

const { freePort } = require('../helpers/node-red');
const { ContainerNodeRed } = require('../helpers/node-red-container');
const { httpRequest } = require('../helpers/http');
const { startRedis, startDaprd } = require('../helpers/integration');
const { waitFor } = require('../helpers/wait-for');

test(
  'service invocation and dapr-invoke round-trip through real daprd, both directions',
  { timeout: 60000 },
  async (t) => {
    const appId = 'it-invoke-app';
    const appPort = await freePort();
    const daprHttpPort = await freePort();

    // dapr-invoke (below) targets this same app-id/method, so the whole call
    // genuinely round-trips out through this sidecar's real invoke API and
    // back in through the same sidecar's real service-invocation delivery —
    // a self-invoke, but every hop is real Dapr, not the fake sidecar.
    const nr = new ContainerNodeRed();
    await nr.start({
      flows: [
        { id: 'tab', type: 'tab', label: 'it-invoke' },
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
          methodPath: '/echo',
          wires: [['handle']],
        },
        {
          id: 'handle',
          type: 'function',
          z: 'tab',
          func: 'msg.payload = { echoed: msg.payload, callerAppId: msg.dapr.callerAppId }; return msg;',
          outputs: 1,
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
        { id: 'in', type: 'http in', z: 'tab', url: '/trigger', method: 'post', wires: [['inv']] },
        {
          id: 'inv',
          type: 'dapr-invoke',
          z: 'tab',
          connection: 'c1',
          appId,
          method: 'echo',
          verb: 'POST',
          wires: [['out']],
        },
        {
          id: 'out',
          type: 'function',
          z: 'tab',
          func: 'msg.statusCode = 200; return msg;',
          outputs: 1,
          wires: [['res']],
        },
        { id: 'res', type: 'http response', z: 'tab' },
      ],
    });
    t.after(() => nr.stop());
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

    // Direction 1: daprd's own invoke API calls straight into dapr-service.
    const direct = await waitFor(async () => {
      const r = await httpRequest(`${daprd.baseUrl}/v1.0/invoke/${appId}/method/echo`, {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({ n: 1 }),
        timeoutMs: 4000,
      });
      return r.status === 200 ? r : null;
    });
    const directBody = JSON.parse(direct.text);
    assert.deepEqual(directBody.echoed, { n: 1 });

    // Direction 2: dapr-invoke calls OUT through the sidecar's real invoke
    // API, which routes back in through the SAME sidecar's real
    // service-invocation delivery to dapr-service.
    const viaNode = await waitFor(async () => {
      const r = await httpRequest(nr.nodeUrl('/trigger'), {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({ n: 2 }),
        timeoutMs: 4000,
      });
      return r.status === 200 ? r : null;
    });
    const viaNodeBody = JSON.parse(viaNode.text);
    assert.deepEqual(viaNodeBody.echoed, { n: 2 });
  }
);
