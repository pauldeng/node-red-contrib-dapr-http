'use strict';

const { test } = require('node:test');
const assert = require('node:assert/strict');

const { freePort } = require('../helpers/node-red');
const { ContainerNodeRed } = require('../helpers/node-red-container');
const { httpRequest } = require('../helpers/http');
const { startRedis, startDaprd } = require('../helpers/integration');
const { waitFor } = require('../helpers/wait-for');
const { startCapture } = require('../helpers/capture');

test(
  'publishing through real daprd + Redis delivers to a Node-RED subscribe node',
  { timeout: 60000 },
  async (t) => {
    const appPort = await freePort();
    const daprHttpPort = await freePort();
    const capture = await startCapture();
    t.after(() => capture.stop());

    // Startup ordering: deploy Node-RED (the "app") FIRST and wait for its own
    // app-channel /healthz before starting daprd — daprd fetches
    // /dapr/subscribe exactly once at startup (docs/testing.md). daprd's port
    // is pre-allocated so the connection node can be configured with it from
    // the start, matching production (no redeploy needed once daprd is up).
    const nr = new ContainerNodeRed();
    await nr.start({
      flows: [
        { id: 'tab', type: 'tab', label: 'it-pubsub' },
        {
          id: 'c1',
          type: 'dapr-connection',
          daprHost: '127.0.0.1',
          daprPort: String(daprHttpPort),
          bindAddress: '127.0.0.1',
          appPort: String(appPort),
        },
        {
          id: 'sub1',
          type: 'dapr-subscribe',
          z: 'tab',
          connection: 'c1',
          pubsubName: 'pubsub',
          topic: 'orders',
          ackMode: 'auto',
          metadata: '{}',
          wires: [['fwd']],
        },
        {
          id: 'fwd',
          type: 'function',
          z: 'tab',
          func: `msg.url = ${JSON.stringify(capture.url)};
msg.method = 'POST';
msg.headers = { 'content-type': 'application/json' };
msg.payload = JSON.stringify({ payload: msg.payload, topic: msg.dapr.topic });
return msg;`,
          outputs: 1,
          wires: [['req']],
        },
        {
          id: 'req',
          type: 'http request',
          z: 'tab',
          method: 'use',
          ret: 'txt',
          url: '',
          wires: [[]],
        },
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
      appId: 'it-pubsub-app',
      appPort,
      redisPort: redis.port,
      httpPort: daprHttpPort,
    });
    t.after(() => daprd.stop());

    const res = await waitFor(async () => {
      const r = await httpRequest(`${daprd.baseUrl}/v1.0/publish/pubsub/orders`, {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({ orderId: 42 }),
        timeoutMs: 4000,
      });
      return r.status === 204 ? r : null;
    });
    assert.equal(res.status, 204);

    const captured = await waitFor(() => capture.received[0] || null);
    assert.deepEqual(captured, { payload: { orderId: 42 }, topic: 'orders' });
  }
);
