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
  'a permanently-failing delivery with no retry policy lands on the dead-letter topic',
  { timeout: 60000 },
  async (t) => {
    const appId = 'it-dlt-app';
    const appPort = await freePort();
    const daprHttpPort = await freePort();
    const capture = await startCapture();
    t.after(() => capture.stop());

    // No Resiliency fixture in this suite (deliberately separate from
    // retry.test.js): per Dapr's docs, without a retry policy configured, any
    // failing message goes straight to the dead-letter topic — this suite is
    // exactly what proves that "no retry policy" case on its own.
    const nr = new ContainerNodeRed();
    await nr.start({
      flows: [
        { id: 'tab', type: 'tab', label: 'it-dlt' },
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
          deadLetterTopic: 'orders-dlq',
          metadata: '{}',
          wires: [['fail']],
        },
        {
          id: 'fail',
          type: 'function',
          z: 'tab',
          func: "msg.dapr.status = 'RETRY'; return msg;", // always fails
          outputs: 1,
          wires: [['ack']],
        },
        { id: 'ack', type: 'dapr-ack', z: 'tab', connection: 'c1', wires: [[]] },
        {
          id: 'sub2',
          type: 'dapr-subscribe',
          z: 'tab',
          connection: 'c1',
          pubsubName: 'pubsub',
          topic: 'orders-dlq',
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
      appId,
      appPort,
      redisPort: redis.port,
      httpPort: daprHttpPort,
    });
    t.after(() => daprd.stop());

    await waitFor(async () => {
      const r = await httpRequest(`${daprd.baseUrl}/v1.0/publish/pubsub/orders`, {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({ orderId: 99 }),
        timeoutMs: 4000,
      });
      return r.status === 204 ? r : null;
    });

    const dlq = await waitFor(() => capture.received[0] || null);
    assert.deepEqual(dlq, { payload: { orderId: 99 }, topic: 'orders-dlq' });
  }
);
