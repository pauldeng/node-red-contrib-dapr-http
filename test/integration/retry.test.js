'use strict';

const { test } = require('node:test');
const assert = require('node:assert/strict');

const { freePort } = require('../helpers/node-red');
const { ContainerNodeRed } = require('../helpers/node-red-container');
const { httpRequest } = require('../helpers/http');
const { startRedis, startDaprd } = require('../helpers/integration');
const { waitFor } = require('../helpers/wait-for');
const { startCapture } = require('../helpers/capture');
const { setTimeout: delay } = require('node:timers/promises');

test(
  'a fastRetry Resiliency policy makes daprd redeliver a RETRY-acknowledged message',
  { timeout: 60000 },
  async (t) => {
    const appId = 'it-retry-app';
    const appPort = await freePort();
    const daprHttpPort = await freePort();
    const capture = await startCapture();
    t.after(() => capture.stop());

    // "decide" reports every delivery ATTEMPT to the capture server (so the
    // test can observe redelivery from outside), fails the first two
    // attempts (RETRY), and succeeds on the third — using flow context to
    // count across redeliveries, since each redelivery is a fresh HTTP
    // request with no shared msg state.
    const nr = new ContainerNodeRed();
    await nr.start({
      flows: [
        { id: 'tab', type: 'tab', label: 'it-retry' },
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
          ackMode: 'manual',
          metadata: '{}',
          wires: [['decide']],
        },
        {
          id: 'decide',
          type: 'function',
          z: 'tab',
          func: `const attempt = (context.get('attempt') || 0) + 1;
context.set('attempt', attempt);
const ackId = msg.dapr.ackId;
const status = attempt < 3 ? 'RETRY' : 'SUCCESS';
msg.url = ${JSON.stringify(capture.url)};
msg.method = 'POST';
msg.headers = { 'content-type': 'application/json' };
msg.payload = JSON.stringify({ attempt, status });
msg.dapr = { ackId, status };
return msg;`,
          outputs: 1,
          wires: [['report']],
        },
        {
          id: 'report',
          type: 'http request',
          z: 'tab',
          method: 'use',
          ret: 'txt',
          url: '',
          wires: [['ack']],
        },
        { id: 'ack', type: 'dapr-ack', z: 'tab', connection: 'c1', wires: [[]] },
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
      extraFixtures: ['resiliency-retry.yaml'],
    });
    t.after(() => daprd.stop());

    await waitFor(async () => {
      const r = await httpRequest(`${daprd.baseUrl}/v1.0/publish/pubsub/orders`, {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({ orderId: 7 }),
        timeoutMs: 4000,
      });
      return r.status === 204 ? r : null;
    });

    // Wait for the third (successful) attempt to be reported, then confirm no
    // further redelivery happens once SUCCESS is returned.
    await waitFor(() => capture.received.some((r) => r && r.attempt === 3) || null);
    await delay(1500); // past fastRetry's window
    const attempts = capture.received.map((r) => r && r.attempt).filter(Boolean);
    assert.deepEqual(
      attempts,
      [1, 2, 3],
      'daprd redelivers on RETRY and stops once SUCCESS is returned'
    );
  }
);
