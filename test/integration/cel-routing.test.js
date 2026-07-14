'use strict';

// Proves real daprd's own CEL evaluator resolves overlapping routing rules by
// order, first-match-wins — the runtime tier's CEL test delivers directly to
// each rule's own pre-known route (test/runtime/subscribe-routing.test.js),
// which proves our route/ruleId wiring but never actually exercises daprd's
// CEL parser or its precedence semantics. This does, through a real publish.
//
// Also confirms: CEL rules match against event.data.<field> (the published
// payload), not event.type/event.source, which real daprd fixes to constants
// (com.dapr.event.sent / the publishing app-id) regardless of payload content
// — matching on those would never differentiate real published messages.

const { test } = require('node:test');
const assert = require('node:assert/strict');

const { freePort } = require('../helpers/node-red');
const { ContainerNodeRed } = require('../helpers/node-red-container');
const { httpRequest } = require('../helpers/http');
const { startRedis, startDaprd } = require('../helpers/integration');
const { waitFor } = require('../helpers/wait-for');
const { startCapture } = require('../helpers/capture');

test(
  'real daprd evaluates overlapping CEL rules and routes first-match-wins',
  { timeout: 60000 },
  async (t) => {
    const appId = 'it-cel-routing';
    const appPort = await freePort();
    const daprHttpPort = await freePort();
    const capture = await startCapture();
    t.after(() => capture.stop());

    const nr = new ContainerNodeRed();
    t.after(() => nr.stop());
    await nr.start({
      flows: [
        { id: 'tab', type: 'tab', label: 'it-cel-routing' },
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
          // ruleA and ruleB both match kind == "shared" — listed first,
          // ruleA must win for it. ruleB alone matches kind == "onlyB".
          // Anything else falls through to the default (no ruleId).
          rules: [
            { id: 'ruleA', match: 'event.data.kind == "shared" || event.data.kind == "onlyA"' },
            { id: 'ruleB', match: 'event.data.kind == "shared" || event.data.kind == "onlyB"' },
          ],
          wires: [['fwd']],
        },
        {
          id: 'fwd',
          type: 'function',
          z: 'tab',
          func: `msg.url = ${JSON.stringify(capture.url)};
msg.method = 'POST';
msg.headers = { 'content-type': 'application/json' };
msg.payload = JSON.stringify({ ruleId: msg.dapr.ruleId, kind: msg.payload.kind });
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

    const redis = await startRedis();
    t.after(() => redis.stop());
    const daprd = await startDaprd({
      appId,
      appPort,
      redisPort: redis.port,
      httpPort: daprHttpPort,
    });
    t.after(() => daprd.stop());

    const publish = (kind) =>
      waitFor(async () => {
        const r = await httpRequest(`${daprd.baseUrl}/v1.0/publish/pubsub/orders`, {
          method: 'POST',
          headers: { 'content-type': 'application/json' },
          body: JSON.stringify({ kind }),
          timeoutMs: 4000,
        });
        return r.status === 204 ? r : null;
      });

    await publish('shared');
    await publish('onlyB');
    await publish('other');

    await waitFor(() => (capture.received.length >= 3 ? true : null));
    const byKind = new Map(capture.received.map((r) => [r.kind, r.ruleId]));
    assert.equal(
      byKind.get('shared'),
      'ruleA',
      'ruleA wins the overlap because it is listed first, even though ruleB also matches'
    );
    assert.equal(byKind.get('onlyB'), 'ruleB', 'ruleB matches when ruleA does not');
    assert.equal(
      byKind.get('other'),
      null,
      'no rule matches, so it falls through to the default route'
    );
  }
);
