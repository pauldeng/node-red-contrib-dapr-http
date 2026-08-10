'use strict';

// Verifies lib/dapr-client.js's own HTTP publish client against real daprd —
// not the local fake sidecar used in test/unit/dapr-client.test.js. Delivery
// back to a Node-RED dapr-subscribe node is the only way to observe, from
// outside, exactly what went on the wire.

const { test } = require('node:test');
const assert = require('node:assert/strict');

const { freePort } = require('../helpers/node-red');
const { ContainerNodeRed } = require('../helpers/node-red-container');
const { httpRequest } = require('../helpers/http');
const { startRedis, startDaprd } = require('../helpers/integration');
const { publish, publishBulk } = require('../../lib/dapr-client');
const { DaprError, ErrorCodes } = require('../../lib/errors');
const { waitFor } = require('../helpers/wait-for');
const { startCapture } = require('../helpers/capture');

test(
  'the HTTP publish client publishes falsy bodies, Buffer bodies, metadata, and extra headers correctly against real daprd',
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

    const target = { baseUrl: daprd.baseUrl, timeoutMs: 10000 };

    // Each published value here is distinct, so matching the delivered
    // payload by exact value (via a JSON comparison, so 0/false/'' compare
    // correctly against their own type) is enough to correlate — no separate
    // sequence number needed.
    const publishAndAwait = async (data, contentType, extra = {}) => {
      await publish(target, {
        pubsubName: 'pubsub',
        topic: 'probe',
        data,
        options: { contentType, metadata: {} },
        ...extra,
      });
      const expected = JSON.stringify(data instanceof Buffer ? data.toString('utf8') : data);
      const match = await waitFor(
        () => capture.received.find((r) => r && JSON.stringify(r.payload) === expected) || null
      );
      return match.payload;
    };

    // The very first call on a cold client: no readiness handshake, no warm
    // socket — it must reach real daprd and deliver.
    assert.deepEqual(await publishAndAwait({ hello: 'world' }, 'application/json'), {
      hello: 'world',
    });

    // Falsy JSON bodies must each arrive as themselves, not as an empty or
    // omitted body — the failure mode the previous SDK path needed a truthy
    // wrapper to avoid.
    assert.equal(await publishAndAwait(0, 'application/json'), 0);
    assert.equal(await publishAndAwait(false, 'application/json'), false);
    assert.equal(await publishAndAwait('', 'application/json'), '');
    assert.equal(await publishAndAwait(null, 'application/json'), null);

    // Buffer body: binary content type, decoded back from data_base64 on the
    // subscribe side.
    assert.equal(
      await publishAndAwait(Buffer.from('binary-payload', 'utf8'), 'application/octet-stream'),
      'binary-payload'
    );

    // Extra request headers (the traceparent case) are accepted by real daprd
    // and must not disturb delivery.
    assert.deepEqual(
      await publishAndAwait({ with: 'traceparent' }, 'application/json', {
        headers: { traceparent: '00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01' },
      }),
      { with: 'traceparent' }
    );

    // Publish-time metadata reaches daprd as metadata.* query parameters and
    // is accepted (ttlInSeconds is honored by the component, not observable
    // here — what matters is that daprd does not reject the request).
    assert.deepEqual(
      await publishAndAwait({ with: 'metadata' }, 'application/json', {
        options: { contentType: 'application/json', metadata: { ttlInSeconds: '60' } },
      }),
      { with: 'metadata' }
    );

    // Sequential publishes reuse the process-global keep-alive agent; the
    // fourth call after a released socket pool must still work.
    assert.deepEqual(await publishAndAwait({ via: 'pooled socket' }, 'application/json'), {
      via: 'pooled socket',
    });

    // A publish to a topic whose pubsub component does not exist must surface
    // daprd's own rejection as an error, not silently succeed.
    await assert.rejects(
      publish(target, {
        pubsubName: 'no-such-pubsub',
        topic: 'probe',
        data: { nope: true },
        options: { contentType: 'application/json', metadata: {} },
      }),
      /publish failed with status 4\d\d/
    );

    // Kill the real sidecar: the failure must surface fast, with no readiness
    // wait or retry loop of our own to hang on.
    await daprd.stop();
    const start = Date.now();
    await assert.rejects(
      publish(target, {
        pubsubName: 'pubsub',
        topic: 'probe',
        data: { should: 'not deliver' },
        options: { contentType: 'application/json', metadata: {} },
      }),
      /could not reach the sidecar/
    );
    assert.ok(Date.now() - start < 5000, 'publish against a dead sidecar must fail fast, not hang');
  }
);

test(
  'publishBulk delivers JSON, string, and binary entries to a real subscriber through real daprd + Redis',
  { timeout: 60000 },
  async (t) => {
    const appId = 'it-bulk-publish-client';
    const appPort = await freePort();
    const daprHttpPort = await freePort();
    const capture = await startCapture();
    t.after(() => capture.stop());

    const nr = new ContainerNodeRed();
    await nr.start({
      flows: [
        { id: 'tab', type: 'tab', label: 'it-bulk-publish-client' },
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
          topic: 'bulk-probe',
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

    const target = { baseUrl: daprd.baseUrl, timeoutMs: 10000 };

    await publishBulk(target, {
      pubsubName: 'pubsub',
      topic: 'bulk-probe',
      entries: [
        { entryId: 'json-1', event: { orderId: 1 }, contentType: 'application/json', metadata: {} },
        { entryId: 'string-1', event: 'plain text entry', contentType: 'text/plain', metadata: {} },
        {
          entryId: 'binary-1',
          event: Buffer.from('binary entry', 'utf8').toString('base64'),
          contentType: 'application/octet-stream',
          metadata: {},
        },
      ],
      options: { metadata: {} },
    });

    await waitFor(() => capture.received.filter((r) => r).length >= 3, { timeoutMs: 10000 });
    const payloads = capture.received.map((r) => r.payload);
    assert.deepEqual(
      payloads.find((p) => typeof p === 'object' && p?.orderId === 1),
      { orderId: 1 }
    );
    assert.ok(payloads.includes('plain text entry'));
    assert.ok(payloads.includes('binary entry'));
  }
);

test(
  'publishBulk surfaces a duplicate entryId as a whole-batch PUBLISH_FAILED, matching real daprd, not a partial result',
  { timeout: 60000 },
  async (t) => {
    const appId = 'it-bulk-publish-client-dup';
    const appPort = await freePort();
    const daprHttpPort = await freePort();

    const nr = new ContainerNodeRed();
    await nr.start({
      flows: [
        { id: 'tab', type: 'tab', label: 'it-bulk-publish-dup' },
        {
          id: 'c1',
          type: 'dapr-connection',
          daprHost: '127.0.0.1',
          daprPort: String(daprHttpPort),
          bindAddress: '127.0.0.1',
          appPort: String(appPort),
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

    await assert.rejects(
      publishBulk(
        { baseUrl: daprd.baseUrl, timeoutMs: 10000 },
        {
          pubsubName: 'pubsub',
          topic: 'bulk-dup',
          entries: [
            { entryId: 'dup', event: 1, contentType: 'application/json', metadata: {} },
            { entryId: 'dup', event: 2, contentType: 'application/json', metadata: {} },
          ],
          options: { metadata: {} },
        }
      ),
      (err) => err instanceof DaprError && err.code === ErrorCodes.PUBLISH_FAILED
    );
  }
);
