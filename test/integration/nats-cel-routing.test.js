'use strict';

// One CEL-routing smoke test only: routing is evaluated by the Dapr runtime
// against the CloudEvent envelope, after the component delivers — broker-
// agnostic by construction, low risk. The Redis-tier cel-routing.test.js
// already proves real daprd's own CEL evaluator resolves overlapping rules
// by order; this just confirms the same mechanism still works when the
// underlying broker is NATS JetStream instead of Redis.

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
  'real daprd evaluates CEL routing rules the same way when backed by NATS JetStream',
  { timeout: 60000 },
  async (t) => {
    const appId = 'it-nats-cel';
    const appPort = await freePort();
    const daprHttpPort = await freePort();
    const capture = await startCapture();
    t.after(() => capture.stop());

    const nr = new ContainerNodeRed();
    t.after(() => nr.stop());
    await nr.start({
      flows: [
        { id: 'tab', type: 'tab', label: 'it-nats-cel' },
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
          rules: [{ id: 'ruleUrgent', match: 'event.data.kind == "urgent"' }],
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

    const nats = await startNats();
    t.after(() => nats.stop());
    await provisionStream(nats.port, { streamName: 'nrdapr-it', subjects: ['orders'] });

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

    await publish('urgent');
    await publish('normal');

    await waitFor(() => (capture.received.length >= 2 ? true : null));
    const byKind = new Map(capture.received.map((r) => [r.kind, r.ruleId]));
    assert.equal(byKind.get('urgent'), 'ruleUrgent', 'the matching rule routes correctly');
    assert.equal(byKind.get('normal'), null, 'a non-matching message falls through to the default');
  }
);
