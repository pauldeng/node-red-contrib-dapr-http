'use strict';

const { test } = require('node:test');
const assert = require('node:assert/strict');

const { freePort } = require('../helpers/node-red');
const { ContainerNodeRed } = require('../helpers/node-red-container');
const { httpRequest } = require('../helpers/http');
const { startRedis, startDaprd } = require('../helpers/integration');
const { waitFor } = require('../helpers/wait-for');
const { startCapture } = require('../helpers/capture');

// Plain delivery and the dead-letter path share one Redis + daprd + Node-RED
// session (both are ordinary pub/sub against the same component, so nothing
// about sharing changes what either proves) and run as sequential subtests.
// The dead-letter scenario uses its own `dlt-`-prefixed topic/DLQ so the two
// subscriptions can never observe each other's messages, and both forward
// through the same capture server -- subtests distinguish their own delivery
// by the `topic` tag each publish carries, not by array position.
test(
  'real daprd + Redis pub/sub: plain delivery and a dead-lettered delivery',
  { timeout: 60000 },
  async (t) => {
    const appPort = await freePort();
    const daprHttpPort = await freePort();
    const capture = await startCapture();
    t.after(() => capture.stop());

    // Startup ordering: deploy Node-RED (the "app") FIRST and wait for its own
    // app-channel /healthz before starting daprd — daprd fetches
    // /dapr/subscribe exactly once at startup (docs/testing.md), so both
    // subscriptions below must be present in this one, first deploy. daprd's
    // port is pre-allocated so the connection node can be configured with it
    // from the start, matching production (no redeploy needed once daprd is up).
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
          id: 'subDlt',
          type: 'dapr-subscribe',
          z: 'tab',
          connection: 'c1',
          pubsubName: 'pubsub',
          topic: 'dlt-orders',
          ackMode: 'manual',
          deadLetterTopic: 'dlt-orders-dlq',
          metadata: '{}',
          wires: [['fail']],
        },
        {
          id: 'fail',
          type: 'function',
          z: 'tab',
          func: "msg.ackStatus = 'RETRY'; return msg;", // always fails
          outputs: 1,
          wires: [['ack']],
        },
        {
          id: 'ack',
          type: 'dapr-ack',
          z: 'tab',
          connection: 'c1',
          ackStatusSource: 'message',
          ackStatus: 'SUCCESS',
          wires: [[]],
        },
        {
          id: 'subDlq',
          type: 'dapr-subscribe',
          z: 'tab',
          connection: 'c1',
          pubsubName: 'pubsub',
          topic: 'dlt-orders-dlq',
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

    const receive = (topic) =>
      waitFor(() => capture.received.find((m) => m.topic === topic) || null);

    await t.test(
      'publishing through real daprd + Redis delivers to a Node-RED subscribe node',
      async () => {
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

        const captured = await receive('orders');
        assert.deepEqual(captured, { payload: { orderId: 42 }, topic: 'orders' });
      }
    );

    await t.test(
      'a permanently-failing delivery with no retry policy lands on the dead-letter topic',
      async () => {
        // No Resiliency fixture in this suite (deliberately separate from
        // retry.test.js): per Dapr's docs, without a retry policy configured,
        // any failing message goes straight to the dead-letter topic — this
        // scenario is exactly what proves that "no retry policy" case on its
        // own.
        await waitFor(async () => {
          const r = await httpRequest(`${daprd.baseUrl}/v1.0/publish/pubsub/dlt-orders`, {
            method: 'POST',
            headers: { 'content-type': 'application/json' },
            body: JSON.stringify({ orderId: 99 }),
            timeoutMs: 4000,
          });
          return r.status === 204 ? r : null;
        });

        const dlq = await receive('dlt-orders-dlq');
        assert.deepEqual(dlq, { payload: { orderId: 99 }, topic: 'dlt-orders-dlq' });
      }
    );
  }
);
