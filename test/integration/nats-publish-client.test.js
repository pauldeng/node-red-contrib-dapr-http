'use strict';

// Verifies lib/dapr-client.js's own HTTP publish client against real daprd —
// not the local fake sidecar used in test/unit/dapr-client.test.js. Delivery
// back to a Node-RED dapr-subscribe node is the only way to observe, from
// outside, exactly what went on the wire. Moved from Redis to NATS
// JetStream per Milestone 3's NATS-primary rebalance — the client only
// talks to daprd's own HTTP API, never the broker directly, so which broker
// backs daprd is not a variable this suite needs to hold constant.
//
// All three scenarios below already used distinct subjects/topics, so they
// share one NATS + daprd + Node-RED session and run as sequential subtests.
// Order matters for one reason only: the main publish-client scenario stops
// the shared daprd as its own final step (proving a dead sidecar fails
// publish fast), so it runs LAST -- the bulk scenarios run first while daprd
// is still alive.

const { test } = require('node:test');
const assert = require('node:assert/strict');

const { freePort } = require('../helpers/node-red');
const { ContainerNodeRed } = require('../helpers/node-red-container');
const { httpRequest } = require('../helpers/http');
const { startDaprd } = require('../helpers/integration');
const { startNats, provisionStream, jetstreamComponentYaml } = require('../helpers/nats');
const { publish, publishBulk } = require('../../lib/dapr-client');
const { DaprError, ErrorCodes } = require('../../lib/errors');
const { waitFor } = require('../helpers/wait-for');
const { startCapture } = require('../helpers/capture');

function subscribeNode(id, topic) {
  return {
    id,
    type: 'dapr-subscribe',
    z: 'tab',
    connection: 'c1',
    pubsubName: 'pubsub',
    topic,
    ackMode: 'auto',
    metadata: '{}',
    wires: [[`fwd-${id}`]],
  };
}

function forwardNode(id, captureUrl) {
  return {
    id: `fwd-${id}`,
    type: 'function',
    z: 'tab',
    func: `msg.url = ${JSON.stringify(captureUrl)};
msg.method = 'POST';
msg.headers = { 'content-type': 'application/json' };
const payload = Buffer.isBuffer(msg.payload) ? msg.payload.toString('utf8') : msg.payload;
msg.payload = JSON.stringify({ payload });
return msg;`,
    outputs: 1,
    wires: [[`req-${id}`]],
  };
}

function requestNode(id) {
  return {
    id: `req-${id}`,
    type: 'http request',
    z: 'tab',
    method: 'use',
    ret: 'txt',
    url: '',
    wires: [[]],
  };
}

test(
  'the HTTP publish client: bulk delivery, a duplicate entryId, and falsy/Buffer/metadata publishes against real daprd backed by NATS JetStream',
  { timeout: 60000 },
  async (t) => {
    const appId = 'it-nats-publish-client-app';
    const appPort = await freePort();
    const daprHttpPort = await freePort();
    const capture = await startCapture();
    t.after(() => capture.stop());

    const nr = new ContainerNodeRed();
    await nr.start({
      flows: [
        { id: 'tab', type: 'tab', label: 'it-nats-publish-client' },
        {
          id: 'c1',
          type: 'dapr-connection',
          daprHost: '127.0.0.1',
          daprPort: String(daprHttpPort),
          bindAddress: '127.0.0.1',
          appPort: String(appPort),
        },
        subscribeNode('sub1', 'probe'),
        forwardNode('sub1', capture.url),
        requestNode('sub1'),
        subscribeNode('subBulk', 'bulk-probe'),
        forwardNode('subBulk', capture.url),
        requestNode('subBulk'),
      ],
    });
    t.after(() => nr.stop());
    await waitFor(async () => {
      const r = await httpRequest(`http://127.0.0.1:${appPort}/healthz`, { timeoutMs: 1000 });
      return r.status === 204 ? true : null;
    });

    const nats = await startNats();
    t.after(() => nats.stop());
    await provisionStream(nats.port, {
      streamName: 'nrdapr-it',
      subjects: ['probe', 'bulk-probe', 'bulk-dup'],
    });
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

    const target = { baseUrl: daprd.baseUrl, timeoutMs: 10000 };

    await t.test(
      'publishBulk delivers JSON, string, and binary entries to a real subscriber through real daprd + NATS JetStream',
      async () => {
        await publishBulk(target, {
          pubsubName: 'pubsub',
          topic: 'bulk-probe',
          entries: [
            {
              entryId: 'json-1',
              event: { orderId: 1 },
              contentType: 'application/json',
              metadata: {},
            },
            {
              entryId: 'string-1',
              event: 'plain text entry',
              contentType: 'text/plain',
              metadata: {},
            },
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

    await t.test(
      'publishBulk surfaces a duplicate entryId as a whole-batch PUBLISH_FAILED, matching real daprd, not a partial result',
      async () => {
        await assert.rejects(
          publishBulk(target, {
            pubsubName: 'pubsub',
            topic: 'bulk-dup',
            entries: [
              { entryId: 'dup', event: 1, contentType: 'application/json', metadata: {} },
              { entryId: 'dup', event: 2, contentType: 'application/json', metadata: {} },
            ],
            options: { metadata: {} },
          }),
          (err) => err instanceof DaprError && err.code === ErrorCodes.PUBLISH_FAILED
        );
      }
    );

    // Runs last: ends by stopping the shared daprd to prove a dead sidecar
    // fails a publish fast, so nothing after this subtest can rely on daprd
    // still being up.
    await t.test(
      'publishes falsy bodies, Buffer bodies, metadata, and extra headers correctly, then fails fast against a dead sidecar',
      async () => {
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
        assert.ok(
          Date.now() - start < 5000,
          'publish against a dead sidecar must fail fast, not hang'
        );
      }
    );
  }
);
