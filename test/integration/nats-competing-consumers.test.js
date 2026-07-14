'use strict';

// Competing consumers: two independent Node-RED + daprd pairs, sharing one
// app-id, stream, durableName, and queueGroupName (the mechanism this
// component uses in place of consumerID — confirmed via Dapr's own docs).
// Not two dapr-subscribe nodes on one connection: that's rejected outright as
// a duplicate pubsub/topic registration (nodes/dapr-subscribe.js's
// registerSubscription), and the documented pattern itself is "one
// application or pod, same app-id" distributing across instances.
//
// durableName/queueGroupName are component-wide (parsed once, applied to
// every Subscribe() call regardless of topic — confirmed in jetstream.go), so
// this dedicated fixture exists only for this suite; the baseline component
// other suites use carries neither field.
//
// NATS picks one queue-group member per message at random — there is no
// guarantee a small finite batch splits across both instances. This does not
// assert fairness: it asserts every message appears exactly once across the
// two instances' combined captures (exclusivity, not fan-out to both), then
// proves the second instance is a genuine, independent participant — not a
// bystander that just never got picked — by stopping the first daprd and
// confirming every subsequent message lands on the second.

const { test } = require('node:test');
const assert = require('node:assert/strict');

const { freePort } = require('../helpers/node-red');
const { ContainerNodeRed } = require('../helpers/node-red-container');
const { httpRequest } = require('../helpers/http');
const { startDaprd } = require('../helpers/integration');
const { startNats, provisionStream, jetstreamComponentYaml } = require('../helpers/nats');
const { waitFor } = require('../helpers/wait-for');
const { startCapture } = require('../helpers/capture');

function consumerFlow({ daprHttpPort, appPort, captureUrl }) {
  return [
    { id: 'tab', type: 'tab', label: 'it-nats-competing' },
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
      func: `msg.url = ${JSON.stringify(captureUrl)};
msg.method = 'POST';
msg.headers = { 'content-type': 'application/json' };
msg.payload = JSON.stringify({ n: msg.payload.n });
return msg;`,
      outputs: 1,
      wires: [['req']],
    },
    { id: 'req', type: 'http request', z: 'tab', method: 'use', ret: 'txt', url: '', wires: [[]] },
  ];
}

// Transactional: if anything after starting the capture server fails (a bad
// flow, a healthz timeout, daprd failing to become healthy), everything
// already started is torn down before rethrowing — a caller that never gets
// a `stop` function back (because this function threw) would otherwise have
// no way to clean up whatever DID start.
async function startConsumerPair({ appId, componentYaml }) {
  const appPort = await freePort();
  const daprHttpPort = await freePort();
  const capture = await startCapture();
  let nr;
  let daprd;
  try {
    nr = new ContainerNodeRed();
    await nr.start({ flows: consumerFlow({ daprHttpPort, appPort, captureUrl: capture.url }) });
    await waitFor(async () => {
      const r = await httpRequest(`http://127.0.0.1:${appPort}/healthz`, { timeoutMs: 1000 });
      return r.status === 204 ? true : null;
    });

    daprd = await startDaprd({
      appId,
      appPort,
      httpPort: daprHttpPort,
      components: [{ filename: 'pubsub-jetstream.yaml', yaml: componentYaml }],
    });
  } catch (err) {
    await daprd?.stop();
    await nr?.stop();
    await capture.stop();
    throw err;
  }

  return {
    capture,
    nr,
    daprd,
    stop: async () => {
      await daprd.stop();
      await nr.stop();
      await capture.stop();
    },
  };
}

test(
  'two competing-consumer instances split delivery exclusively, and the survivor keeps working alone',
  { timeout: 90000 },
  async (t) => {
    const appId = 'it-nats-competing';

    const nats = await startNats();
    t.after(() => nats.stop());
    await provisionStream(nats.port, { streamName: 'nrdapr-it', subjects: ['orders'] });

    const componentYaml = jetstreamComponentYaml({
      name: 'pubsub',
      natsPort: nats.port,
      streamName: 'nrdapr-it',
      durableName: 'competing-durable',
      queueGroupName: 'competing-group',
    });

    const a = await startConsumerPair({ appId, componentYaml });
    t.after(() => a.stop());
    const b = await startConsumerPair({ appId, componentYaml });
    t.after(() => b.stop());

    const publishVia = (daprd, n) =>
      waitFor(async () => {
        const r = await httpRequest(`${daprd.baseUrl}/v1.0/publish/pubsub/orders`, {
          method: 'POST',
          headers: { 'content-type': 'application/json' },
          body: JSON.stringify({ n }),
          timeoutMs: 4000,
        });
        return r.status === 204 ? r : null;
      });

    // Phase 1: both instances active. Publish N messages one at a time and
    // wait for all N to land somewhere, combined.
    const total = 6;
    for (let n = 1; n <= total; n += 1) {
      await publishVia(a.daprd, n);
    }
    await waitFor(
      () => (a.capture.received.length + b.capture.received.length >= total ? true : null),
      { timeoutMs: 30000 }
    );

    const seenByA = a.capture.received.map((r) => r.n);
    const seenByB = b.capture.received.map((r) => r.n);
    const combined = [...seenByA, ...seenByB];
    assert.equal(combined.length, total, 'no message was fan-out-delivered to both instances');
    assert.deepEqual(
      [...new Set(combined)].sort((x, y) => x - y),
      Array.from({ length: total }, (_, i) => i + 1),
      'every published message appears exactly once across the combined captures'
    );

    // Phase 2: stop instance A entirely — removing it from the queue group at
    // the NATS level, not just silencing its own flow — and prove instance B
    // is a genuine, independently-working participant, not a bystander that
    // just never happened to be picked in phase 1.
    await a.stop();
    const beforeB = b.capture.received.length;
    for (let n = total + 1; n <= total + 3; n += 1) {
      await publishVia(b.daprd, n);
    }
    await waitFor(() => (b.capture.received.length >= beforeB + 3 ? true : null), {
      timeoutMs: 30000,
    });
    const survivorSeen = b.capture.received.slice(beforeB).map((r) => r.n);
    assert.deepEqual(
      survivorSeen.sort((x, y) => x - y),
      [total + 1, total + 2, total + 3],
      'every message published after instance A stops lands on the surviving instance B'
    );
  }
);
