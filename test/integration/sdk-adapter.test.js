'use strict';

// Reverifies lib/dapr-client.js's pinned @dapr/dapr 3.18.0 adapter directly
// against real daprd — not the fake factory used in test/unit/dapr-client.test.js.
// Publishes go through the REAL SDK's ClientRegistry, and delivery back to a
// Node-RED dapr-subscribe node is the only way to observe, from outside,
// exactly what the SDK put on the wire.

const { test } = require('node:test');
const assert = require('node:assert/strict');

const { freePort } = require('../helpers/node-red');
const { ContainerNodeRed } = require('../helpers/node-red-container');
const { httpRequest } = require('../helpers/http');
const { startRedis, startDaprd } = require('../helpers/integration');
const { ClientRegistry } = require('../../lib/dapr-client');
const { waitFor } = require('../helpers/wait-for');
const { startCapture } = require('../helpers/capture');

test(
  'the pinned SDK adapter publishes falsy bodies, Buffer bodies, and reused/rebuilt clients correctly against real daprd',
  { timeout: 60000 },
  async (t) => {
    const appId = 'it-sdk-adapter';
    const appPort = await freePort();
    const daprHttpPort = await freePort();
    const capture = await startCapture();
    t.after(() => capture.stop());

    const nr = new ContainerNodeRed();
    await nr.start({
      flows: [
        { id: 'tab', type: 'tab', label: 'it-sdk-adapter' },
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
          topic: 'probe',
          ackMode: 'auto',
          metadata: '{}',
          wires: [['fwd']],
        },
        {
          id: 'fwd',
          type: 'function',
          z: 'tab',
          func: `msg.url = ${JSON.stringify(capture.url)};
msg.method = 'POST';
msg.headers = { 'content-type': 'application/json' };
const payload = Buffer.isBuffer(msg.payload) ? msg.payload.toString('utf8') : msg.payload;
msg.payload = JSON.stringify({ payload });
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
    t.after(() => nr.stop());
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

    const clientOptions = {
      outbound: {
        mode: 'explicit',
        host: '127.0.0.1',
        port: daprHttpPort,
        baseUrl: daprd.baseUrl,
      },
      daprApiToken: undefined,
      limits: { bodyLimitBytes: 8 * 1024 * 1024 },
      keepAlive: true,
    };

    const registry = new ClientRegistry();
    // Each published value here is distinct, so matching the delivered
    // payload by exact value (via a JSON comparison, so 0/false/'' compare
    // correctly against their own type) is enough to correlate — no separate
    // sequence number needed.
    const publishAndAwait = async (lease, data, contentType) => {
      await lease.publish({
        pubsubName: 'pubsub',
        topic: 'probe',
        data,
        options: { contentType, metadata: {} },
      });
      const expected = JSON.stringify(data instanceof Buffer ? data.toString('utf8') : data);
      const match = await waitFor(
        () => capture.received.find((r) => r && JSON.stringify(r.payload) === expected) || null
      );
      return match.payload;
    };

    // Fail-fast readiness bypass: this is the very FIRST publish call on a
    // freshly acquired lease — lib/dapr-client.js forces
    // daprClient.setIsInitialized(true) instead of letting the SDK block on
    // its own readiness wait. If that bypass broke the real wire protocol,
    // this call would hang or fail outright, not just run "a bit faster".
    const lease1 = registry.acquire(clientOptions);
    const first = await publishAndAwait(lease1, { hello: 'world' }, 'application/json');
    assert.deepEqual(first, { hello: 'world' });

    // Falsy JSON bodies: the pinned SDK skips serialization when its data
    // argument is falsy, which is exactly what sdkData()'s truthy wrapper in
    // lib/dapr-client.js exists to work around. Prove each of these still
    // arrives as itself, not as an empty/omitted body.
    assert.equal(await publishAndAwait(lease1, 0, 'application/json'), 0);
    assert.equal(await publishAndAwait(lease1, false, 'application/json'), false);
    assert.equal(await publishAndAwait(lease1, '', 'application/json'), '');
    assert.equal(await publishAndAwait(lease1, null, 'application/json'), null);

    // Buffer body: binary content type, decoded back from data_base64 on the
    // subscribe side.
    assert.equal(
      await publishAndAwait(
        lease1,
        Buffer.from('binary-payload', 'utf8'),
        'application/octet-stream'
      ),
      'binary-payload'
    );

    // Publish after client-agent reuse: a second lease on the SAME options
    // shares the underlying SDK client/HTTP agent (registry.acquire() keys on
    // outbound target + token + body limit). Releasing the first lease must
    // not tear down the agent while the second is still active, and the
    // second lease must still be able to publish for real.
    const lease2 = registry.acquire(clientOptions);
    await lease1.release();
    assert.deepEqual(await publishAndAwait(lease2, { via: 'lease2' }, 'application/json'), {
      via: 'lease2',
    });
    await lease2.release();

    // Once every lease releases, the shared client is fully stopped
    // (client.stop() — the SDK's process-global HTTP agents are torn down).
    // A brand new acquire() must build a fresh client that still works
    // against the same real daprd.
    const lease3 = registry.acquire(clientOptions);
    assert.deepEqual(await publishAndAwait(lease3, { via: 'lease3' }, 'application/json'), {
      via: 'lease3',
    });
    await lease3.release();

    // The readiness bypass's actual payoff: without it, the SDK's own
    // internal wait-for-ready loop would retry for a long default duration
    // before a real network failure ever surfaces. Kill the real sidecar,
    // acquire a brand-new client against it (no warm connection to fall back
    // on), and confirm the failure surfaces fast — well inside the bound the
    // SDK's own readiness wait would otherwise blow through.
    await daprd.stop();
    const deadLease = registry.acquire(clientOptions);
    const start = Date.now();
    await assert.rejects(
      deadLease.publish({
        pubsubName: 'pubsub',
        topic: 'probe',
        data: { should: 'not deliver' },
        options: { contentType: 'application/json', metadata: {} },
      })
    );
    assert.ok(
      Date.now() - start < 5000,
      "publish against a dead sidecar must fail fast, not hang on the SDK's own readiness wait"
    );
    await deadLease.release();
  }
);
