'use strict';

// Confirmed limitation: deadLetterTopic stalls with the pubsub.jetstream
// component in Dapr 1.18.2. This is NOT the Milestone 8/Redis-tier behavior
// (immediate DLT publish on Resiliency exhaustion, proven working in
// dead-letter.test.js) — it was the ORIGINAL intent here too, until real
// daprd debug logs showed otherwise.
//
// Reproduced twice, independently: daprd logs "encountered a retriable
// error" for the original delivery, then "Publishing to topic orders-dlq" —
// and that is the last log line for at least the bounded window this test
// waits (a manual run once waited 45+ seconds with no further log line). No
// success, no error; the original message did not redeliver within the
// observed windows either. Removing
// the orders-dlq subscriber entirely made no difference, ruling out the DLQ
// side as the cause.
//
// The EXACT cause of the stall inside the component is NOT confirmed and is
// deliberately not asserted here — do not treat it as established. What IS
// confirmed, by reading both the pinned Dapr runtime and this component's
// source: the runtime creates a 30-second context specifically for the
// dead-letter publish (pkg/runtime/subscription/subscription.go,
// deadLetterPublishTimeout) and passes it into the publish call — but this
// component's own Publish(ctx, ...) accepts that context and never uses it,
// calling js.jsc.Publish(req.Topic, req.Data, opts...) with no context at
// all. That explains why the runtime's own 30-second safety net never
// actually bounds this call, but not what blocks the underlying NATS
// publish call in the first place; that would need reading the nats.go
// client's own internals, which this project doesn't ship or maintain.
// Worth reporting upstream as a reproducer, not further speculation here.
// Not a bug in this package: the exact same flow shape already works
// against Redis (Milestone 8's dead-letter.test.js).
//
// This test proves the actual observed behavior with a bounded wait, not an
// unbounded hang: exactly one delivery attempt, and no dead-letter message,
// within that bounded window.

const { test } = require('node:test');
const assert = require('node:assert/strict');
const { setTimeout: delay } = require('node:timers/promises');

const { freePort } = require('../helpers/node-red');
const { ContainerNodeRed } = require('../helpers/node-red-container');
const { httpRequest } = require('../helpers/http');
const { startDaprd } = require('../helpers/integration');
const { startNats, provisionStream, jetstreamComponentYaml } = require('../helpers/nats');
const { waitFor } = require('../helpers/wait-for');
const { startCapture } = require('../helpers/capture');

test(
  'deadLetterTopic stalls with real NATS JetStream in Dapr 1.18.4: bounded wait sees one delivery attempt and no dead-letter message',
  { timeout: 60000 },
  async (t) => {
    const appId = 'it-nats-dlt';
    const appPort = await freePort();
    const daprHttpPort = await freePort();
    const capture = await startCapture();
    t.after(() => capture.stop());

    const nr = new ContainerNodeRed();
    t.after(() => nr.stop());
    await nr.start({
      flows: [
        { id: 'tab', type: 'tab', label: 'it-nats-dlt' },
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
report.payload = JSON.stringify({ kind: 'delivery-attempt', attempt });
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
          func: "msg.ackStatus = 'RETRY'; return msg;", // always fails, explicitly
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
          id: 'sub2',
          type: 'dapr-subscribe',
          z: 'tab',
          connection: 'c1',
          pubsubName: 'pubsub',
          topic: 'orders-dlq',
          ackMode: 'auto',
          metadata: '{}',
          wires: [['dlqFwd']],
        },
        {
          id: 'dlqFwd',
          type: 'function',
          z: 'tab',
          func: `msg.url = ${JSON.stringify(capture.url)};
msg.method = 'POST';
msg.headers = { 'content-type': 'application/json' };
msg.payload = JSON.stringify({ kind: 'dlq', payload: msg.payload });
return msg;`,
          outputs: 1,
          wires: [['dlqReq']],
        },
        {
          id: 'dlqReq',
          type: 'http request',
          z: 'tab',
          method: 'use',
          ret: 'txt',
          url: '',
          wires: [[]],
        },
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
      subjects: ['orders', 'orders-dlq'],
    });

    const component = jetstreamComponentYaml({
      name: 'pubsub',
      natsPort: nats.port,
      streamName: 'nrdapr-it',
      ackWait: '2s',
      maxDeliver: 5,
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
        body: JSON.stringify({ orderId: 99 }),
        timeoutMs: 4000,
      });
      return r.status === 204 ? r : null;
    });

    // The one delivery attempt that happens before the dead-letter publish
    // stalls.
    await waitFor(() => capture.received.some((r) => r && r.kind === 'delivery-attempt') || null);

    // Bounded wait, not a claim about what happens after it: within this
    // window, neither a redelivery (this component's own ackWait/maxDeliver
    // never gets a chance to run while the handler is stalled) nor a
    // dead-letter message arrives.
    await delay(8000);
    const deliveryAttempts = capture.received.filter((r) => r && r.kind === 'delivery-attempt');
    const dlqMessages = capture.received.filter((r) => r && r.kind === 'dlq');
    assert.equal(
      deliveryAttempts.length,
      1,
      'exactly one delivery attempt happens within the window'
    );
    assert.equal(
      dlqMessages.length,
      0,
      'the dead-letter message does not arrive within the window — the publish stalls'
    );
  }
);
