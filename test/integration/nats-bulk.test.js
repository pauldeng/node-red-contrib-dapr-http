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
// (and in test/integration/bulk.test.js's Redis-tier equivalent) is that
// every entry in the same underlying bulk delivery shares one msg.dapr.batchId.

const { test } = require('node:test');
const assert = require('node:assert/strict');

const { freePort } = require('../helpers/node-red');
const { ContainerNodeRed } = require('../helpers/node-red-container');
const { httpRequest } = require('../helpers/http');
const { startDaprd } = require('../helpers/integration');
const { startNats, provisionStream, jetstreamComponentYaml } = require('../helpers/nats');
const { waitFor } = require('../helpers/wait-for');
const { startCapture } = require('../helpers/capture');

test(
  'real NATS JetStream batches several publishes into one bulk delivery once concurrency: parallel is set',
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
          bulkEnabled: true,
          bulkMaxMessagesCount: '10',
          bulkMaxAwaitDurationMs: '2000',
          wires: [['report']],
        },
        {
          id: 'report',
          type: 'function',
          z: 'tab',
          func: `msg.url = ${JSON.stringify(capture.url)};
msg.method = 'POST';
msg.headers = { 'content-type': 'application/json' };
msg.payload = JSON.stringify({
  entryId: msg.dapr.entryId,
  batchId: msg.dapr.batchId,
  n: msg.payload && msg.payload.n,
});
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
    const daprd = await startDaprd({
      appId,
      appPort,
      httpPort: daprHttpPort,
      components: [{ filename: 'pubsub-jetstream.yaml', yaml: component }],
    });
    t.after(() => daprd.stop());

    const publish = (n) =>
      httpRequest(`${daprd.baseUrl}/v1.0/publish/pubsub/orders`, {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({ n }),
        timeoutMs: 4000,
      });

    // Fired back to back, well inside bulkMaxAwaitDurationMs, so daprd has
    // the chance to coalesce them into one bulk callback — which it can only
    // do because concurrency: parallel lets more than one Subscribe()
    // callback be in flight at once.
    const publishes = await Promise.all([publish(1), publish(2), publish(3)]);
    for (const r of publishes) {
      assert.equal(r.status, 204);
    }

    await waitFor(() => (capture.received.length >= 3 ? true : null));
    const byN = new Map(capture.received.map((r) => [r.n, r]));
    assert.equal(byN.size, 3, 'all three entries were delivered to the flow');

    const batchIds = new Set(capture.received.map((r) => r.batchId));
    assert.equal(
      batchIds.size,
      1,
      'all three entries share one batchId — real daprd genuinely batched them'
    );
    assert.ok([...batchIds][0], 'the shared batchId is non-empty');

    const entryIds = new Set(capture.received.map((r) => r.entryId));
    assert.equal(entryIds.size, 3, 'each entry keeps its own distinct entryId within the batch');
  }
);
