'use strict';

// Tests this component's own ackWait/maxDeliver consumer-level redelivery
// with NO Dapr Resiliency policy configured, isolating that layer
// specifically. Confirmed by reading dapr/dapr's
// pkg/runtime/subscription/subscription.go: Dapr's Resiliency retry loop
// always runs first, at the runtime layer, before the component ever gets a
// chance to Nak() the underlying broker message — these layers stack, they
// are not alternatives, so this suite deliberately configures no Resiliency
// fixture at all, to observe only the component-level layer.
//
// backOff is deliberately not exercised here: the flow below always
// explicitly acks RETRY (via dapr-ack), which the component NAKs with
// NakWithDelay(ackWait) — a fixed delay, confirmed in jetstream.go — not the
// backOff schedule, which only governs a genuine ack-wait *timeout* (the app
// never responding at all). Testing that path deliberately is out of scope
// here; this suite proves ackWait/maxDeliver in isolation.

const { test } = require('node:test');
const assert = require('node:assert/strict');

const { freePort } = require('../helpers/node-red');
const { ContainerNodeRed } = require('../helpers/node-red-container');
const { httpRequest } = require('../helpers/http');
const { startDaprd } = require('../helpers/integration');
const { startNats, provisionStream, jetstreamComponentYaml } = require('../helpers/nats');
const { waitFor } = require('../helpers/wait-for');
const { startCapture } = require('../helpers/capture');
const { setTimeout: delay } = require('node:timers/promises');

test(
  'this component redelivers per its own ackWait/maxDeliver metadata, with no Dapr Resiliency policy configured',
  { timeout: 60000 },
  async (t) => {
    const appId = 'it-nats-retry';
    const appPort = await freePort();
    const daprHttpPort = await freePort();
    const capture = await startCapture();
    t.after(() => capture.stop());

    const nr = new ContainerNodeRed();
    t.after(() => nr.stop());
    await nr.start({
      flows: [
        { id: 'tab', type: 'tab', label: 'it-nats-retry' },
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
          wires: [['route']],
        },
        {
          id: 'route',
          type: 'function',
          z: 'tab',
          func: `const attempt = (context.get('attempt') || 0) + 1;
context.set('attempt', attempt);
const report = Object.assign({}, msg);
report.url = ${JSON.stringify(capture.url)};
report.method = 'POST';
report.headers = { 'content-type': 'application/json' };
report.payload = JSON.stringify({ attempt });
return [report, msg];`,
          outputs: 2,
          wires: [['reportReq'], ['decide']],
        },
        {
          id: 'reportReq',
          type: 'http request',
          z: 'tab',
          method: 'use',
          ret: 'txt',
          url: '',
          wires: [[]],
        },
        {
          id: 'decide',
          type: 'function',
          z: 'tab',
          func: "msg.dapr.status = 'RETRY'; return msg;", // always fails, explicitly
          outputs: 1,
          wires: [['ack']],
        },
        { id: 'ack', type: 'dapr-ack', z: 'tab', connection: 'c1', wires: [[]] },
      ],
    });
    await waitFor(async () => {
      const r = await httpRequest(`http://127.0.0.1:${appPort}/healthz`, { timeoutMs: 1000 });
      return r.status === 204 ? true : null;
    });

    const nats = await startNats();
    t.after(() => nats.stop());
    await provisionStream(nats.port, { streamName: 'nrdapr-it', subjects: ['orders'] });

    // No Dapr Resiliency fixture — deliberately. maxDeliver is small so the
    // test stays fast; ackWait is short so redelivery isn't slow either.
    const component = jetstreamComponentYaml({
      name: 'pubsub',
      natsPort: nats.port,
      streamName: 'nrdapr-it',
      ackWait: '500ms',
      maxDeliver: 3,
    });
    const daprd = await startDaprd({
      appId,
      appPort,
      httpPort: daprHttpPort,
      components: [{ filename: 'pubsub-jetstream.yaml', yaml: component }],
    });
    t.after(() => daprd.stop());

    await waitFor(async () => {
      const r = await httpRequest(`${daprd.baseUrl}/v1.0/publish/pubsub/orders`, {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({}),
        timeoutMs: 4000,
      });
      return r.status === 204 ? r : null;
    });

    // Wait for the third (maxDeliver-th) attempt, then confirm no fourth one
    // ever arrives — the cap is honored, not just "it retries at all".
    await waitFor(() => capture.received.some((r) => r && r.attempt === 3) || null, {
      timeoutMs: 20000,
    });
    await delay(1500); // past another ackWait cycle
    const attempts = capture.received.map((r) => r && r.attempt).filter(Boolean);
    assert.deepEqual(
      attempts,
      [1, 2, 3],
      'daprd redelivers per maxDeliver with no Resiliency policy, and stops exactly there'
    );
  }
);
