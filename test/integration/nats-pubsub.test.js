'use strict';

// Re-runs the broker-sensitive slice of the Milestone 8 matrix against real
// NATS JetStream instead of Redis: publish/subscribe, the CloudEvents round
// trip, and raw payload. Invocation, ACL, Admin-API isolation, and the SDK
// adapter's serialization concerns never reach a broker-specific code path
// (dapr-client.js calls the SDK's generic pubsub.publish()), so none of that
// matrix is re-run here. This component's own AddConsumer() call means the
// baseline component below carries no durableName/queueGroupName — those are
// component-wide and reserved for the dedicated competing-consumers fixture.

const { test } = require('node:test');
const assert = require('node:assert/strict');

const { freePort } = require('../helpers/node-red');
const { ContainerNodeRed } = require('../helpers/node-red-container');
const { httpRequest } = require('../helpers/http');
const { startDaprd } = require('../helpers/integration');
const { startNats, provisionStream, jetstreamComponentYaml } = require('../helpers/nats');
const { waitFor } = require('../helpers/wait-for');
const { startCapture } = require('../helpers/capture');

function forwardingSub(id, topic, extra = {}) {
  return {
    id,
    type: 'dapr-subscribe',
    z: 'tab',
    connection: 'c1',
    pubsubName: 'pubsub',
    topic,
    ackMode: 'auto',
    metadata: '{}',
    wires: [[`fwd-${id}`]],
    ...extra,
  };
}

function forwardingFn(id, captureUrl) {
  return {
    id: `fwd-${id}`,
    type: 'function',
    z: 'tab',
    func: `msg.url = ${JSON.stringify(captureUrl)};
msg.method = 'POST';
msg.headers = { 'content-type': 'application/json' };
const payload = Buffer.isBuffer(msg.payload) ? msg.payload.toString('utf8') : msg.payload;
msg.payload = JSON.stringify({
  topic: msg.dapr.topic,
  isBuffer: Buffer.isBuffer(msg.payload),
  payload,
  cloudEvent: msg.dapr.cloudEvent,
});
return msg;`,
    outputs: 1,
    wires: [[`req-${id}`]],
  };
}

function forwardingReq(id) {
  return {
    id: `req-${id}`,
    type: 'http request',
    z: 'tab',
    method: 'use',
    ret: 'txt',
    url: '',
    wires: [[]],
  };
}

test(
  'publish/subscribe, the CloudEvents round trip, and raw payload all behave correctly against real NATS JetStream',
  { timeout: 60000 },
  async (t) => {
    const appId = 'it-nats-pubsub';
    const appPort = await freePort();
    const daprHttpPort = await freePort();
    const capture = await startCapture();
    t.after(() => capture.stop());

    const nr = new ContainerNodeRed();
    t.after(() => nr.stop());
    await nr.start({
      flows: [
        { id: 'tab', type: 'tab', label: 'it-nats-pubsub' },
        {
          id: 'c1',
          type: 'dapr-connection',
          daprHost: '127.0.0.1',
          daprPort: String(daprHttpPort),
          bindAddress: '127.0.0.1',
          appPort: String(appPort),
        },
        forwardingSub('sub-orders', 'orders'),
        forwardingFn('sub-orders', capture.url),
        forwardingReq('sub-orders'),
        forwardingSub('sub-raw', 'raw-topic', { rawPayload: true }),
        forwardingFn('sub-raw', capture.url),
        forwardingReq('sub-raw'),
      ],
    });
    await waitFor(async () => {
      const r = await httpRequest(`http://127.0.0.1:${appPort}/healthz`, { timeoutMs: 1000 });
      return r.status === 204 ? true : null;
    });

    const nats = await startNats();
    t.after(() => nats.stop());
    await provisionStream(nats.port, {
      streamName: 'nrdapr-it',
      subjects: ['orders', 'raw-topic'],
    });

    const component = jetstreamComponentYaml({
      name: 'pubsub',
      natsPort: nats.port,
      streamName: 'nrdapr-it',
    });
    const daprd = await startDaprd({
      appId,
      appPort,
      httpPort: daprHttpPort,
      components: [{ filename: 'pubsub-jetstream.yaml', yaml: component }],
    });
    t.after(() => daprd.stop());

    const publish = (topic, body, { query = '', contentType = 'application/json' } = {}) =>
      waitFor(async () => {
        const r = await httpRequest(`${daprd.baseUrl}/v1.0/publish/pubsub/${topic}${query}`, {
          method: 'POST',
          headers: { 'content-type': contentType },
          body,
          timeoutMs: 4000,
        });
        return r.status === 204 ? r : null;
      });

    // Basic publish/subscribe plus the CloudEvents round trip: the envelope
    // daprd delivers must carry the required attributes intact.
    await publish('orders', JSON.stringify({ n: 1 }));
    const ordersDelivered = await waitFor(
      () => capture.received.find((r) => r && r.topic === 'orders') || null
    );
    assert.equal(ordersDelivered.isBuffer, false);
    assert.deepEqual(ordersDelivered.payload, { n: 1 });
    for (const attr of ['specversion', 'id', 'source', 'type', 'pubsubname', 'topic']) {
      assert.ok(
        ordersDelivered.cloudEvent && typeof ordersDelivered.cloudEvent[attr] === 'string',
        `CloudEvent envelope preserves "${attr}"`
      );
    }

    // Raw payload: both the publish and the subscription must be marked raw
    // (see docs/testing.md's Milestone 8 finding — this is broker-agnostic,
    // reconfirmed here against a different component).
    await publish('raw-topic', JSON.stringify({ hello: 'raw' }), {
      query: '?metadata.rawPayload=true',
    });
    const rawDelivered = await waitFor(
      () => capture.received.find((r) => r && r.topic === 'raw-topic') || null
    );
    assert.equal(rawDelivered.isBuffer, true);
    assert.equal(rawDelivered.payload, '{"hello":"raw"}');
  }
);
