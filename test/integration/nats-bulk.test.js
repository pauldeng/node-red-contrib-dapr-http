'use strict';

// Proves real bulk-subscribe batching against NATS JetStream — but only once
// concurrency: parallel is set explicitly on the component. Confirmed by
// reading dapr/components-contrib's pubsub/jetstream/metadata.go (default is
// Concurrency: Single) and dapr/dapr's pkg/runtime/pubsub/default_bulksub.go
// (the fallback bulk adapter blocks each component Subscribe() callback until
// that message's full bulk-handler round trip completes): with the default,
// the batch accumulator never sees more than one in-flight message, so every
// batch would land as a singleton and this test would prove nothing.
//
// A Node-RED-level flow always fans a bulk delivery out into one msg.send()
// per entry (see nodes/dapr-subscribe.js) — there is no way to observe "one
// HTTP request with multiple entries" at the flow level. The proof used here
// is that every entry in the same underlying bulk delivery shares one
// msg.dapr.batchId.
//
// Also covers the mixed per-entry ack outcome (SUCCESS, DROP, and one entry
// that never gets acked at all) formerly proven separately against Redis in
// test/integration/bulk.test.js — merged here per Milestone 3's NATS-primary
// rebalance, since per-entry ack independence is a Dapr runtime-layer
// behavior, not a broker-specific one.

const { test } = require('node:test');
const assert = require('node:assert/strict');

const { freePort } = require('../helpers/node-red');
const { ContainerNodeRed } = require('../helpers/node-red-container');
const { httpRequest } = require('../helpers/http');
const { startDaprd } = require('../helpers/integration');
const { startNats, provisionStream, jetstreamComponentYaml } = require('../helpers/nats');
const { waitFor } = require('../helpers/wait-for');
const { startCapture } = require('../helpers/capture');
const { publishBulk } = require('../../lib/dapr-client');
const { setTimeout: delay } = require('node:timers/promises');

test(
  'real NATS JetStream delivers one bulk-publish request as one identifiable bulk subscription batch, and a mixed SUCCESS/DROP/no-ack outcome resolves each entry independently',
  { timeout: 60000 },
  async (t) => {
    const appId = 'it-nats-bulk';
    const appPort = await freePort();
    const daprHttpPort = await freePort();
    const capture = await startCapture();
    t.after(() => capture.stop());

    const nr = new ContainerNodeRed();
    t.after(() => nr.stop());
    await nr.start({
      flows: [
        { id: 'tab', type: 'tab', label: 'it-nats-bulk' },
        {
          id: 'c1',
          type: 'dapr-connection',
          daprHost: '127.0.0.1',
          daprPort: String(daprHttpPort),
          bindAddress: '127.0.0.1',
          appPort: String(appPort),
          // Short so entry n=3 (never acked) resolves its internal RETRY
          // timeout quickly, instead of waiting out a long default.
          requestTimeoutSec: '1',
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
          bulkEnabled: true,
          bulkMaxMessagesCount: '10',
          bulkMaxAwaitDurationMs: '2000',
          wires: [['route']],
        },
        {
          id: 'route',
          type: 'function',
          z: 'tab',
          func: `const n = msg.payload && msg.payload.n;
const key = 'attempt_' + n;
const attempt = (context.get(key) || 0) + 1;
context.set(key, attempt);
const report = Object.assign({}, msg);
report.url = ${JSON.stringify(capture.url)};
report.method = 'POST';
report.headers = { 'content-type': 'application/json' };
report.payload = JSON.stringify({
  entryId: msg.dapr.entryId,
  batchId: msg.dapr.batchId,
  n,
  attempt,
});
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
          func: `const n = msg.payload && msg.payload.n;
if (n === 3) {
  return null; // never acked — resolves as RETRY via our own timeout
}
msg.dapr.status = n === 2 ? 'DROP' : 'SUCCESS';
return msg;`,
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

    const component = jetstreamComponentYaml({
      name: 'pubsub',
      natsPort: nats.port,
      streamName: 'nrdapr-it',
      concurrency: 'parallel',
    });
    // Without a Resiliency retry policy, a failing delivery gets exactly one
    // attempt and is never redelivered — the whole point of entry n=3 here is
    // to prove redelivery targets ONLY the entry that failed, which needs
    // this fixture or there would be nothing to observe.
    const daprd = await startDaprd({
      appId,
      appPort,
      httpPort: daprHttpPort,
      components: [{ filename: 'pubsub-jetstream.yaml', yaml: component }],
      extraFixtures: ['resiliency-retry.yaml'],
    });
    t.after(() => daprd.stop());

    await publishBulk(
      { baseUrl: daprd.baseUrl, timeoutMs: 4000 },
      {
        pubsubName: 'pubsub',
        topic: 'orders',
        entries: [1, 2, 3].map((n) => ({
          entryId: `publish-${n}`,
          event: { n },
          contentType: 'application/json',
          metadata: {},
        })),
        options: { metadata: {} },
      }
    );

    await waitFor(
      () =>
        [1, 2, 3].every((n) => capture.received.some((r) => r.n === n && r.attempt === 1)) || null
    );
    const firstAttempts = capture.received.filter((r) => r.attempt === 1);
    const byN = new Map(firstAttempts.map((r) => [r.n, r]));
    assert.equal(byN.size, 3, 'all three entries were delivered to the flow');

    const batchIds = new Set(firstAttempts.map((r) => r.batchId));
    assert.equal(
      batchIds.size,
      1,
      'all three entries share one batchId — real daprd genuinely batched them'
    );
    assert.ok([...batchIds][0], 'the shared batchId is non-empty');

    const entryIds = new Set(firstAttempts.map((r) => r.entryId));
    assert.equal(entryIds.size, 3, 'each entry keeps its own distinct entryId within the batch');

    // Entry n=3 was never acked, so its own RETRY (fired by our ack timeout)
    // must actually be redelivered by real daprd, per the fastRetry policy —
    // and entries n=1 (SUCCESS) and n=2 (DROP) must NOT be redelivered
    // alongside it, proving redelivery targets only the failed entry within
    // the batch, not the whole batch over again.
    await waitFor(() => capture.received.filter((r) => r.n === 3).length >= 2 || null, {
      timeoutMs: 30000,
    });
    await delay(1500); // past fastRetry's window
    assert.equal(
      capture.received.filter((r) => r.n === 1).length,
      1,
      'entry n=1 (SUCCESS) must remain single-delivery'
    );
    assert.equal(
      capture.received.filter((r) => r.n === 2).length,
      1,
      'entry n=2 (DROP) must remain single-delivery'
    );
  }
);
