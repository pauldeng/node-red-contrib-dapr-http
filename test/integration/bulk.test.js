'use strict';

// Proves real daprd genuinely batches several published messages into one
// bulk HTTP delivery, and that a mixed per-entry ack outcome (SUCCESS, DROP,
// and one entry that never gets acked at all) is accepted without corrupting
// or blocking the entries that DID resolve. The runtime tier already proves
// our own bulk-response shape against a hand-crafted HTTP body (a fake
// sidecar can't batch anything); this is what only real daprd can confirm.

const { test } = require('node:test');
const assert = require('node:assert/strict');

const { freePort } = require('../helpers/node-red');
const { ContainerNodeRed } = require('../helpers/node-red-container');
const { httpRequest } = require('../helpers/http');
const { startRedis, startDaprd } = require('../helpers/integration');
const { waitFor } = require('../helpers/wait-for');
const { startCapture } = require('../helpers/capture');

test(
  'real daprd batches several publishes into one bulk delivery, and a mixed SUCCESS/DROP/no-ack outcome resolves each entry independently',
  { timeout: 60000 },
  async (t) => {
    const appId = 'it-bulk';
    const appPort = await freePort();
    const daprHttpPort = await freePort();
    const capture = await startCapture();
    t.after(() => capture.stop());

    const nr = new ContainerNodeRed();
    t.after(() => nr.stop());
    await nr.start({
      flows: [
        { id: 'tab', type: 'tab', label: 'it-bulk' },
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

    const redis = await startRedis();
    t.after(() => redis.stop());
    // Without a Resiliency retry policy, a failing delivery gets exactly one
    // attempt and is never redelivered (see delivery-outcomes.test.js) — the
    // whole point of entry n=3 here is to prove redelivery targets ONLY the
    // entry that failed, which needs this fixture or there would be nothing
    // to observe.
    const daprd = await startDaprd({
      appId,
      appPort,
      redisPort: redis.port,
      httpPort: daprHttpPort,
      extraFixtures: ['resiliency-retry.yaml'],
    });
    t.after(() => daprd.stop());

    const publish = (n) =>
      httpRequest(`${daprd.baseUrl}/v1.0/publish/pubsub/orders`, {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({ n }),
        timeoutMs: 4000,
      });

    // Fired back to back, well inside bulkMaxAwaitDurationMs, so real daprd
    // has the chance to coalesce them into one bulk callback.
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

    // Entry n=3 was never acked, so its own RETRY (fired by our ack timeout)
    // must actually be redelivered by real daprd, per the fastRetry policy —
    // and entries n=1 (SUCCESS) and n=2 (DROP) must NOT be redelivered
    // alongside it, proving redelivery targets only the failed entry within
    // the batch, not the whole batch over again.
    await waitFor(() => capture.received.filter((r) => r.n === 3).length >= 2 || null, {
      timeoutMs: 30000,
    });
    await new Promise((resolve) => setTimeout(resolve, 1500)); // past fastRetry's window
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
