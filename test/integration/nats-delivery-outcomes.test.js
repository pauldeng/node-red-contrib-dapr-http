'use strict';

// Two real-daprd-specific delivery outcomes not covered by
// nats-pubsub.test.js, retry.test.js, or dead-letter.test.js: an explicit
// DROP (no redelivery, no dead-letter needed), and our own ack-timeout
// firing RETRY without the flow ever calling dapr-ack — both need to
// observe REAL daprd's redelivery behavior (or absence of it), which a fake
// sidecar can't prove. Moved from Redis to NATS JetStream per Milestone 3's
// NATS-primary rebalance — these are Dapr runtime-layer behaviors
// (ack-timeout, DROP handling, sidecar recovery), not broker-specific ones.

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
  'an explicit DROP is never redelivered, and an unacknowledged delivery times out and IS redelivered, by real daprd backed by NATS JetStream',
  { timeout: 60000 },
  async (t) => {
    const appId = 'it-nats-delivery-outcomes';
    const appPort = await freePort();
    const daprHttpPort = await freePort();
    const capture = await startCapture();
    t.after(() => capture.stop());

    const nr = new ContainerNodeRed();
    t.after(() => nr.stop());
    await nr.start({
      flows: [
        { id: 'tab', type: 'tab', label: 'it-nats-delivery-outcomes' },
        {
          id: 'c1',
          type: 'dapr-connection',
          daprHost: '127.0.0.1',
          daprPort: String(daprHttpPort),
          bindAddress: '127.0.0.1',
          appPort: String(appPort),
          // Short so the ack-timeout scenario below doesn't need to wait out
          // a long default — this is the ONLY thing it changes.
          requestTimeoutSec: '1',
        },
        {
          id: 'sub-drop',
          type: 'dapr-subscribe',
          z: 'tab',
          connection: 'c1',
          pubsubName: 'pubsub',
          topic: 'drop-topic',
          ackMode: 'manual',
          metadata: '{}',
          wires: [['reportDrop']],
        },
        {
          id: 'reportDrop',
          type: 'function',
          z: 'tab',
          func: `msg.url = ${JSON.stringify(capture.url)};
msg.method = 'POST';
msg.headers = { 'content-type': 'application/json' };
const ackId = msg.dapr.ackId;
msg.payload = JSON.stringify({ topic: 'drop-topic' });
msg.dapr = { ackId, status: 'DROP' };
return msg;`,
          outputs: 1,
          wires: [['reportReq']],
        },
        {
          id: 'reportReq',
          type: 'http request',
          z: 'tab',
          method: 'use',
          ret: 'txt',
          url: '',
          wires: [['ackDrop']],
        },
        { id: 'ackDrop', type: 'dapr-ack', z: 'tab', connection: 'c1', wires: [[]] },

        // No ack node at all on this path — nothing ever settles the
        // delivery, so our own ack timeout (requestTimeoutSec above) fires
        // RETRY on our own, without the flow doing anything.
        {
          id: 'sub-noack',
          type: 'dapr-subscribe',
          z: 'tab',
          connection: 'c1',
          pubsubName: 'pubsub',
          topic: 'noack-topic',
          ackMode: 'manual',
          metadata: '{}',
          wires: [['reportNoAck']],
        },
        {
          id: 'reportNoAck',
          type: 'function',
          z: 'tab',
          func: `const attempt = (context.get('attempt') || 0) + 1;
context.set('attempt', attempt);
msg.url = ${JSON.stringify(capture.url)};
msg.method = 'POST';
msg.headers = { 'content-type': 'application/json' };
msg.payload = JSON.stringify({ topic: 'noack-topic', attempt });
return msg;`,
          outputs: 1,
          wires: [['req2']],
        },
        {
          id: 'req2',
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
      subjects: ['drop-topic', 'noack-topic'],
    });

    const component = jetstreamComponentYaml({
      name: 'pubsub',
      natsPort: nats.port,
      streamName: 'nrdapr-it',
    });
    // Without a Resiliency retry policy, real daprd gives a failing delivery
    // exactly ONE attempt and never redelivers it (matching dead-letter.test.js's
    // "no retry policy" finding) — the no-ack scenario below needs the SAME
    // fixture retry.test.js uses, or its own timeout-driven RETRY would just
    // be the message's only attempt. DROP is unaffected: it's a terminal
    // instruction, not a failure the retry policy evaluates.
    const daprd = await startDaprd({
      appId,
      appPort,
      httpPort: daprHttpPort,
      components: [{ filename: 'pubsub-jetstream.yaml', yaml: component }],
      extraFixtures: ['resiliency-retry.yaml'],
    });
    t.after(() => daprd.stop());

    const publish = (topic, body) =>
      waitFor(async () => {
        const r = await httpRequest(`${daprd.baseUrl}/v1.0/publish/pubsub/${topic}`, {
          method: 'POST',
          headers: { 'content-type': 'application/json' },
          body,
          timeoutMs: 4000,
        });
        return r.status === 204 ? r : null;
      });

    // DROP: delivered exactly once, never redelivered.
    await publish('drop-topic', JSON.stringify({ n: 1 }));
    await waitFor(() => capture.received.some((r) => r && r.topic === 'drop-topic') || null);
    await delay(3000);
    assert.equal(
      capture.received.filter((r) => r && r.topic === 'drop-topic').length,
      1,
      'a DROPped delivery must not be redelivered'
    );

    // No ack: our own short ack timeout fires
    // RETRY on its own, and real daprd redelivers because of it.
    await publish('noack-topic', JSON.stringify({ n: 2 }));
    await waitFor(
      () => capture.received.some((r) => r && r.topic === 'noack-topic' && r.attempt === 2) || null,
      { timeoutMs: 30000 }
    );
    const noAckAttempts = capture.received
      .filter((r) => r && r.topic === 'noack-topic')
      .map((r) => r.attempt);
    assert.deepEqual(
      noAckAttempts.slice(0, 2),
      [1, 2],
      'an unacknowledged delivery is redelivered after our own ack timeout fires RETRY'
    );
  }
);

test(
  'a real daprd going down mid-run fails fast, and publishing recovers once a fresh daprd starts, backed by NATS JetStream',
  { timeout: 60000 },
  async (t) => {
    const appId = 'it-nats-sidecar-recovery';
    const appPort = await freePort();
    const daprHttpPort = await freePort();

    const nr = new ContainerNodeRed();
    t.after(() => nr.stop());
    await nr.start({
      flows: [
        { id: 'tab', type: 'tab', label: 'it-nats-sidecar-recovery' },
        {
          id: 'c1',
          type: 'dapr-connection',
          daprHost: '127.0.0.1',
          daprPort: String(daprHttpPort),
          bindAddress: '127.0.0.1',
          appPort: String(appPort),
        },
        { id: 'in', type: 'http in', z: 'tab', url: '/publish', method: 'post', wires: [['pub']] },
        {
          id: 'pub',
          type: 'dapr-publish',
          z: 'tab',
          connection: 'c1',
          pubsubName: 'pubsub',
          topic: 'orders',
          wires: [['out']],
        },
        {
          id: 'out',
          type: 'function',
          z: 'tab',
          func: 'msg.statusCode = 200; return msg;',
          outputs: 1,
          wires: [['httpres']],
        },
        { id: 'httpres', type: 'http response', z: 'tab' },
        { id: 'catchPub', type: 'catch', z: 'tab', scope: ['pub'], wires: [['onErr']] },
        {
          id: 'onErr',
          type: 'function',
          z: 'tab',
          func: "msg.statusCode = 503; msg.payload = 'sidecar down'; return msg;",
          outputs: 1,
          wires: [['httpres']],
        },
      ],
    });
    await waitFor(async () => {
      const r = await httpRequest(`http://127.0.0.1:${appPort}/healthz`, { timeoutMs: 1000 });
      return r.status === 204 ? true : null;
    });

    const nats = await startNats();
    t.after(() => nats.stop());
    await provisionStream(nats.port, { streamName: 'nrdapr-it', subjects: ['orders'] });
    const component = jetstreamComponentYaml({
      name: 'pubsub',
      natsPort: nats.port,
      streamName: 'nrdapr-it',
    });
    const daprdComponents = [{ filename: 'pubsub-jetstream.yaml', yaml: component }];

    let daprd = await startDaprd({
      appId,
      appPort,
      httpPort: daprHttpPort,
      components: daprdComponents,
    });
    t.after(() => daprd.stop());

    const tryPublish = () =>
      httpRequest(nr.nodeUrl('/publish'), { method: 'POST', body: '{}', timeoutMs: 4000 });

    // Baseline: publish works while the real sidecar is up.
    const baseline = await waitFor(async () => {
      const r = await tryPublish();
      return r.status === 200 ? r : null;
    });
    assert.equal(baseline.status, 200);

    // Kill the real sidecar. Our own bounded health poll must mark it
    // unhealthy and fail subsequent publishes fast (503), not hang.
    await daprd.stop();
    const failFast = await waitFor(async () => {
      const t0 = Date.now();
      const r = await tryPublish();
      return r.status === 503 ? { r, elapsed: Date.now() - t0 } : null;
    });
    assert.equal(failFast.r.text, 'sidecar down');
    assert.ok(
      failFast.elapsed < 4000,
      `fail-fast publish should not hang (was ${failFast.elapsed}ms)`
    );

    // A fresh real daprd on the SAME port: health recovers and publishing
    // works again, without redeploying Node-RED at all.
    daprd = await startDaprd({
      appId,
      appPort,
      httpPort: daprHttpPort,
      components: daprdComponents,
    });
    t.after(() => daprd.stop());
    const recovered = await waitFor(async () => {
      const r = await tryPublish();
      return r.status === 200 ? r : null;
    });
    assert.equal(recovered.status, 200);
  }
);
